use std::collections::HashMap;
use std::intrinsics::drop_in_place;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use anyhow::{Ok, Result};
use arc_swap::Guard;
use bytes::Bytes;
use log::{debug, info, trace};
use parking_lot::{Mutex, RwLock, MutexGuard};
use serde::de;

use crate::block::Block;
use crate::compact::{
    CompactionController, CompactionOptions, LeveledCompactionController, LeveledCompactionOptions,
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, TieredCompactionController,
};
use crate::iterators::merge_iterator::{MergeIterator};
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key;
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::manifest::Manifest;
use crate::mem_table::{MemTable};
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};
use crate::mvcc::LsmMvccInner;


pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

/// Represents the state of the storage engine.
#[derive(Clone)]
pub struct LsmStorageState {
    /// The current memtable.
    pub memtable: Arc<MemTable>,
    /// Immutable memtables, from latest to earliest.
    pub imm_memtables: Vec<Arc<MemTable>>,
    /// L0 SSTs, from latest to earliest.
    pub l0_sstables: Vec<usize>,
    /// SsTables sorted by key range; L1 - L_max for leveled compaction, or tiers for tiered
    /// compaction.
    pub levels: Vec<(usize, Vec<usize>)>,
    /// SST objects.
    pub sstables: HashMap<usize, Arc<SsTable>>,
}

pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
}

impl LsmStorageState {
    fn create(options: &LsmStorageOptions) -> Self {
        let levels = match &options.compaction_options {
            CompactionOptions::Leveled(LeveledCompactionOptions { max_levels, .. })
            | CompactionOptions::Simple(SimpleLeveledCompactionOptions { max_levels, .. }) => (1
                ..=*max_levels)
                .map(|level| (level, Vec::new()))
                .collect::<Vec<_>>(),
            CompactionOptions::Tiered(_) => Vec::new(),
            CompactionOptions::NoCompaction => vec![(1, Vec::new())],
        };
        Self {
            memtable: Arc::new(MemTable::create(0)),
            imm_memtables: Vec::new(),
            l0_sstables: Vec::new(),
            levels,
            sstables: Default::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LsmStorageOptions {
    // Block size in bytes
    pub block_size: usize,
    // SST size in bytes, also the approximate memtable capacity limit
    pub target_sst_size: usize,
    // Maximum number of memtables in memory, flush to L0 when exceeding this limit
    pub num_memtable_limit: usize,
    pub compaction_options: CompactionOptions,
    pub enable_wal: bool,
    pub serializable: bool,
}

impl LsmStorageOptions {
    pub fn default_for_week1_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 50,
            serializable: false,
        }
    }

    pub fn my_default() -> Self {
        Self {
            block_size: 1024,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 50,
            serializable: false,
        }
    }

    pub fn default_for_week1_day6_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 15,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }

    pub fn default_for_week2_test(compaction_options: CompactionOptions) -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 1 << 20, // 1MB
            compaction_options,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }
}

#[derive(Clone, Debug)]
pub enum CompactionFilter {
    Prefix(Bytes),
}

/// The storage interface of the LSM tree.
pub(crate) struct LsmStorageInner {
    pub(crate) state: Arc<RwLock<Arc<LsmStorageState>>>,
    pub(crate) state_lock: Mutex<()>,
    path: PathBuf,
    pub(crate) block_cache: Arc<BlockCache>,
    next_sst_id: AtomicUsize,
    pub(crate) options: Arc<LsmStorageOptions>,
    pub(crate) compaction_controller: CompactionController,
    pub(crate) manifest: Option<Manifest>,
    pub(crate) mvcc: Option<LsmMvccInner>,
    pub(crate) compaction_filters: Arc<Mutex<Vec<CompactionFilter>>>,
}

/// A thin wrapper for `LsmStorageInner` and the user interface for MiniLSM.
pub struct MiniLsm {
    pub(crate) inner: Arc<LsmStorageInner>,
    /// Notifies the L0 flush thread to stop working. (In week 1 day 6)
    flush_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread. (In week 1 day 6)
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Notifies the compaction thread to stop working. (In week 2)
    compaction_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread. (In week 2)
    compaction_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Drop for MiniLsm {
    fn drop(&mut self) {
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();
    }
}

impl MiniLsm {
    pub fn close(&self) -> Result<()> {
        info!("Close Lsm Storage");
        self.flush_notifier.send(()).ok();

        let mut flush_thread = self.flush_thread.lock();
        if let Some(flush_thread) = flush_thread.take() {
            flush_thread
                .join()
                .map_err(|e| anyhow::anyhow!("{:?}", e))?;
        }

        if !self.inner.state.read().memtable.is_empty() {
            self.inner.force_freeze_memtable(&self.inner.state_lock.lock())?;
        }

        while {
            let snapshot = self.inner.state.read();
            !snapshot.imm_memtables.is_empty()
        } {
            self.inner.force_flush_next_imm_memtable()?;
        }

        Ok(())
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Arc<Self>> {
        let inner = Arc::new(LsmStorageInner::open(path, options)?);
        let (tx1, rx) = crossbeam_channel::unbounded();
        let compaction_thread = inner.spawn_compaction_thread(rx)?;
        let (tx2, rx) = crossbeam_channel::unbounded();
        let flush_thread = inner.spawn_flush_thread(rx)?;
        Ok(Arc::new(Self {
            inner,
            flush_notifier: tx2,
            flush_thread: Mutex::new(flush_thread),
            compaction_notifier: tx1,
            compaction_thread: Mutex::new(compaction_thread),
        }))
    }

    pub fn new_txn(&self) -> Result<()> {
        self.inner.new_txn()
    }

    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        self.inner.write_batch(batch)
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        self.inner.add_compaction_filter(compaction_filter)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put(key, value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.delete(key)
    }

    pub fn sync(&self) -> Result<()> {
        self.inner.sync()
    }

    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        self.inner.scan(lower, upper)
    }

    /// Only call this in test cases due to race conditions
    pub fn force_flush(&self) -> Result<()> {
        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .force_freeze_memtable(&self.inner.state_lock.lock())?;
        }
        if !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        self.inner.force_full_compaction()
    }

}

impl LsmStorageInner {
    pub(crate) fn next_sst_id(&self) -> usize {
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        let path = path.as_ref();
        let state = LsmStorageState::create(&options);

        let compaction_controller = match &options.compaction_options {
            CompactionOptions::Leveled(options) => {
                CompactionController::Leveled(LeveledCompactionController::new(options.clone()))
            }
            CompactionOptions::Tiered(options) => {
                CompactionController::Tiered(TieredCompactionController::new(options.clone()))
            }
            CompactionOptions::Simple(options) => CompactionController::Simple(
                SimpleLeveledCompactionController::new(options.clone()),
            ),
            CompactionOptions::NoCompaction => CompactionController::NoCompaction,
        };

        let storage = Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            path: path.to_path_buf(),
            block_cache: Arc::new(BlockCache::new(1024)),
            next_sst_id: AtomicUsize::new(1),
            compaction_controller,
            manifest: None,
            options: options.into(),
            mvcc: None,
            compaction_filters: Arc::new(Mutex::new(Vec::new())),
        };

        Ok(storage)
    }

    pub fn sync(&self) -> Result<()> {
        unimplemented!()
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        let mut compaction_filters = self.compaction_filters.lock();
        compaction_filters.push(compaction_filter);
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    pub fn get(&self, _key: &[u8]) -> Result<Option<Bytes>> {
        let mut value = None;

        let state = self.state.read().clone();

        value = state.memtable.get(_key);
        if value.as_ref().is_some_and(|v| v.is_empty()) { return Ok(None); }

        if value.is_none() {
            let imems = state.imm_memtables.iter().rev();
            for imem in imems {
                if let Some(v) = imem.get(_key) {
                    if v.is_empty() {
                        return Ok(None);
                    } else {
                        value = Some(v);
                        break;
                    }
                }
            }

            if value.is_none() {
                for idx in state.l0_sstables.iter().rev() {
                    let sst_iter = SsTableIterator::create_and_seek_to_key(state.sstables.get(idx).unwrap().clone(), _key)?;
                    if _key == sst_iter.key().raw_ref() {
                        if sst_iter.value().is_empty() {
                            return Ok(None);
                        } else {
                            value = Some(Bytes::copy_from_slice(sst_iter.value()));
                            break;
                        }
                    }
                }
            }

            if value.is_none() {
                for idx in state.levels[0].1.iter().rev() {
                    let sst_iter = SsTableIterator::create_and_seek_to_key(state.sstables.get(idx).unwrap().clone(), _key)?;
                    if _key == sst_iter.key().raw_ref() {
                        if sst_iter.value().is_empty() {
                            return Ok(None);
                        } else {
                            value = Some(Bytes::copy_from_slice(sst_iter.value()));
                            break;
                        }
                    }
                }
            }
        }
            

        Ok(value)
    }

    /// Write a batch of data into the storage. Implement in week 2 day 7.
    pub fn write_batch<T: AsRef<[u8]>>(&self, _batch: &[WriteBatchRecord<T>]) -> Result<()> {
        unimplemented!()
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(&self, _key: &[u8], _value: &[u8]) -> Result<()> {
        let size = _key.len() + _value.len();
        let should_freeze = {
            let guard = self.state.read();
            guard.memtable.approximate_size() + size > self.options.target_sst_size
        };
        // debug!("put: {:?}, {}, {}", test, guard.memtable.approximate_size(), self.options.target_sst_size);
        if should_freeze {
            debug!("force_freeze_memtable");
            self.force_freeze_memtable(&self.state_lock.lock())?;
        }
        
        let guard = self.state.read();
        guard.memtable.put(_key, _value)?;
        trace!("Put key: {:?} value: {:?} to memtable_{}", _key, _value, guard.memtable.id());
        Ok(())
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(&self, _key: &[u8]) -> Result<()> {
        let snapshot = self.state.write();
        snapshot.memtable.put(_key, &[])
    }

    pub(crate) fn path_of_sst_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.sst", id))
    }

    pub(crate) fn path_of_sst(&self, id: usize) -> PathBuf {
        Self::path_of_sst_static(&self.path, id)
    }

    pub(crate) fn path_of_wal_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.wal", id))
    }

    pub(crate) fn path_of_wal(&self, id: usize) -> PathBuf {
        Self::path_of_wal_static(&self.path, id)
    }

    pub(super) fn sync_dir(&self) -> Result<()> {
        unimplemented!()
    }

    /// Force freeze the current memtable to an immutablea memtable
    pub fn force_freeze_memtable(&self, _state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        debug!("Main_thread: Acquire write lock");
        let mut guard = self.state.write();
        debug!("Main_thread: Acquire guard");
        let mut snapshot  = guard.as_ref().clone();
        debug!("Main_thread: Acquire sna;shot");

        let freeze_memtable = std::mem::replace(&mut snapshot.memtable, Arc::new(MemTable::create(self.next_sst_id())));
        debug!("Main_thread: Freeze memtable_{} and then", freeze_memtable.id());

        snapshot.imm_memtables.push(freeze_memtable.clone());
        *guard = Arc::new(snapshot);
        
        Ok(())
    }


    /// Force flush the earliest-created immutable memtable to disk
    /// Race Condition?
    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        debug!("Test");
        self.state_lock.lock();

        let flush_memtable = {
            let guard = self.state.read();
            guard.imm_memtables[0].clone()
        };

        let mut sst_builder = SsTableBuilder::new(self.options.block_size);
        flush_memtable.flush(&mut sst_builder);
        let flushed_id = flush_memtable.id();
        let sst = sst_builder.build(flushed_id, None, self.path_of_sst(flushed_id))?;
        
        let mut guard = self.state.write();
        let mut snapshot = guard.as_ref().clone();

        let imm_table = snapshot.imm_memtables.remove(0);

        snapshot.l0_sstables.push(flushed_id);
        snapshot.sstables.insert(flushed_id, Arc::new(sst));

        debug!("Create Sstable_{}", flushed_id);
        *guard = Arc::new(snapshot);

        Ok(())
    }

    pub fn new_txn(&self) -> Result<()> {
        // no-op
        Ok(())
    }

    /// Create an iterator over a range of keys.
    pub fn scan(
        &self,
        _lower: Bound<&[u8]>,
        _upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        let snapshot = self.state.read().clone();

        let mut mem_iters = Vec::with_capacity(snapshot.imm_memtables.len() + 1);

        let mem_iter = snapshot.memtable.scan(_lower, _upper);
        mem_iters.push(Box::new(mem_iter));
        for imm_table in snapshot.imm_memtables.iter().rev() {
            mem_iters.push(Box::new(imm_table.scan(_lower, _upper)));
        }

        let merge_mem_iter = MergeIterator::create(mem_iters);

        let mut l0_iters = Vec::with_capacity(snapshot.l0_sstables.len());

        for idx in snapshot.l0_sstables.iter().rev() {
            let table = snapshot.sstables[idx].clone();
            let iter = match _lower {
                Bound::Included(key) => {
                    SsTableIterator::create_and_seek_to_key(table.clone(), key)?
                }
                Bound::Excluded(key) => {
                    let mut iter = SsTableIterator::create_and_seek_to_key(table.clone(), key)?;

                    if iter.is_valid() && iter.key().raw_ref() == key {
                        iter.next()?;
                    }
                    iter
                }
                Bound::Unbounded => SsTableIterator::create_and_seek_to_first(table.clone())?,
            };
            l0_iters.push(Box::new(iter));
        }

        let merge_l0_iter = MergeIterator::create(l0_iters);
        
        let mut l1_iters = Vec::with_capacity(snapshot.levels[0].1.len());

        for idx in snapshot.levels[0].1.iter().rev() {
            let table = snapshot.sstables[idx].clone();
            let iter = match _lower {
                Bound::Included(key) => {
                    SsTableIterator::create_and_seek_to_key(table.clone(), key)?
                }
                Bound::Excluded(key) => {
                    let mut iter = SsTableIterator::create_and_seek_to_key(table.clone(), key)?;

                    if iter.is_valid() && iter.key().raw_ref() == key {
                        iter.next()?;
                    }
                    iter
                }
                Bound::Unbounded => SsTableIterator::create_and_seek_to_first(table.clone())?,
            };
            l1_iters.push(Box::new(iter));
        }

        let merge_l1_iter = MergeIterator::create(l1_iters);
        // merge_memtable_level0_iterator
        let mm0_iter = TwoMergeIterator::create(merge_mem_iter, merge_l0_iter)?;
        
        let iter = TwoMergeIterator::create(mm0_iter, merge_l1_iter)?;
        
        
        Ok(FusedIterator::new(LsmIterator::new(iter, _upper)?))
    }
}
