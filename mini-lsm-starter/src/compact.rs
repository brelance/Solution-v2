#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

mod leveled;
mod simple_leveled;
mod tiered;

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Ok, Result};
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use log::{info, trace};
use serde::{de, Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::StorageIterator;
use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

#[derive(Debug, Serialize, Deserialize)]
pub enum CompactionTask {
    Leveled(LeveledCompactionTask),
    Tiered(TieredCompactionTask),
    Simple(SimpleLeveledCompactionTask),
    ForceFullCompaction {
        l0_sstables: Vec<usize>,
        l1_sstables: Vec<usize>,
    },
}

impl CompactionTask {
    fn compact_to_bottom_level(&self) -> bool {
        match self {
            CompactionTask::ForceFullCompaction { .. } => true,
            CompactionTask::Leveled(task) => task.is_lower_level_bottom_level,
            CompactionTask::Simple(task) => task.is_lower_level_bottom_level,
            CompactionTask::Tiered(task) => task.bottom_tier_included,
        }
    }
}

pub(crate) enum CompactionController {
    Leveled(LeveledCompactionController),
    Tiered(TieredCompactionController),
    Simple(SimpleLeveledCompactionController),
    NoCompaction,
}

impl CompactionController {
    pub fn generate_compaction_task(&self, snapshot: &LsmStorageState) -> Option<CompactionTask> {
        match self {
            CompactionController::Leveled(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Leveled),
            CompactionController::Simple(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Simple),
            CompactionController::Tiered(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Tiered),
            CompactionController::NoCompaction => unreachable!(),
        }
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &CompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        match (self, task) {
            (CompactionController::Leveled(ctrl), CompactionTask::Leveled(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Simple(ctrl), CompactionTask::Simple(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Tiered(ctrl), CompactionTask::Tiered(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            _ => unreachable!(),
        }
    }
}

impl CompactionController {
    pub fn flush_to_l0(&self) -> bool {
        matches!(
            self,
            Self::Leveled(_) | Self::Simple(_) | Self::NoCompaction
        )
    }
}

#[derive(Debug, Clone)]
pub enum CompactionOptions {
    /// Leveled compaction with partial compaction + dynamic level support (= RocksDB's Leveled
    /// Compaction)
    Leveled(LeveledCompactionOptions),
    /// Tiered compaction (= RocksDB's universal compaction)
    Tiered(TieredCompactionOptions),
    /// Simple leveled compaction
    Simple(SimpleLeveledCompactionOptions),
    /// In no compaction mode (week 1), always flush to L0
    NoCompaction,
}

impl LsmStorageInner {
    fn compact(&self, _task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        let mut compacted_sst = Vec::new();

        match _task {
            CompactionTask::ForceFullCompaction { l0_sstables, l1_sstables } => {
                compacted_sst = self.compact_two_level(l0_sstables, l1_sstables)?;
            },
            CompactionTask::Simple(task) => {
                compacted_sst = self.compact_two_level(&task.upper_level_sst_ids, &task.lower_level_sst_ids)?;
            },
            _ => {},
        }

        Ok(compacted_sst)
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        
        let (l0_sstables, l1_sstables) = {
            //optimization
            let state = self.state.read();
            (state.l0_sstables.clone(), state.levels[0].1.clone())
        };

        let new_sst = self.compact(
            &CompactionTask::ForceFullCompaction { 
            l0_sstables, 
            l1_sstables,
        })?;

        let new_sst_id = new_sst.iter().map(|sst| sst.sst_id()).collect();
        
        {
            self.state_lock.lock();
            let mut state = self.state.write();
            let mut snapshot = state.as_ref().clone();

            for l0_idx in snapshot.l0_sstables {
                snapshot.sstables.remove(&l0_idx);
            }

            snapshot.l0_sstables = Vec::new();

            snapshot.levels[0] = (1, new_sst_id);
            for sst in new_sst {
                snapshot.sstables.insert(sst.sst_id(), sst);
            }
            *state = Arc::new(snapshot);
        };
        
        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        let snapshot = self.state.read();
        if let Some(compact_task) = self.compaction_controller.generate_compaction_task(&snapshot) {
            let compacted_sst = self.compact(&compact_task)?;
            let compacted_sst_ids: Vec<usize> = compacted_sst.iter().map(|sst| sst.sst_id()).collect();
            let (mut state, deleted_sst_id) = self.compaction_controller
                .apply_compaction_result(
                    self.state.read().as_ref(), 
                    &compact_task, 
            &compacted_sst_ids
                );
            for id in deleted_sst_id {
                state.sstables.remove(&id);
            }

            for sst in compacted_sst {
                state.sstables.insert(sst.sst_id(), sst);
            }
        }
        Ok(())
    }

    fn compact_two_level(&self, upper_level: &Vec<usize>, lower_level: &Vec<usize>) -> Result<Vec<Arc<SsTable>>> {
        let mut compacted_sst = Vec::new();
        let mut sst_iter = Vec::new();
                for idx in upper_level.iter().rev() {
                    let table: Arc<SsTable> = {
                        let state = self.state.read();
                        //Bug
                        state.sstables.get(idx).unwrap().clone()
                    };

                    sst_iter.push(Box::new(SsTableIterator::create_and_seek_to_first(table)?))
                }

                for idx in lower_level.iter().rev() {
                    let table = {
                        let state = self.state.read();
                        //Bug
                        state.sstables.get(idx).unwrap().clone()
                    };

                    sst_iter.push(Box::new(SsTableIterator::create_and_seek_to_first(table)?))
                }


                let mut merger_iter = MergeIterator::create(sst_iter);
                let mut sst_builder = SsTableBuilder::new(self.options.block_size);

                while merger_iter.is_valid() {
                    if sst_builder.estimated_size() > self.options.target_sst_size {
                        let sst_id = self.next_sst_id();
                        let sst = Arc::new(sst_builder.build(sst_id, None, self.path_of_sst(sst_id))?);
                        compacted_sst.push(sst.clone());
                        sst_builder = SsTableBuilder::new(self.options.block_size);
                    }
                    if !merger_iter.value().is_empty() {
                        sst_builder.add(merger_iter.key(), merger_iter.value());
                    }
                    merger_iter.next();
                }

                //build the last sst
                let sst_id = self.next_sst_id();
                let sst = Arc::new(sst_builder.build(sst_id, None, self.path_of_sst(sst_id))?);
                compacted_sst.push(sst.clone());
                Ok(compacted_sst)
    }

    pub(crate) fn spawn_compaction_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        if let CompactionOptions::Leveled(_)
        | CompactionOptions::Simple(_)
        | CompactionOptions::Tiered(_) = self.options.compaction_options
        {
            let this = self.clone();
            let handle = std::thread::spawn(move || {
                let ticker = crossbeam_channel::tick(Duration::from_millis(50));
                loop {
                    crossbeam_channel::select! {
                        recv(ticker) -> _ => if let Err(e) = this.trigger_compaction() {
                            eprintln!("compaction failed: {}", e);
                        },
                        recv(rx) -> _ => return
                    }
                }
            });
            return Ok(Some(handle));
        }
        Ok(None)
    }

    fn trigger_flush(&self) -> Result<()> {
        let res = {
            let state = self.state.read();
            state.imm_memtables.len() >= self.options.num_memtable_limit
        };

        if res {
            self.force_flush_next_imm_memtable()?;
        }
        
        Ok(())
    }

    pub(crate) fn spawn_flush_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let this = self.clone();
        let handle = std::thread::spawn(move || {
            let ticker = crossbeam_channel::tick(Duration::from_millis(50));
            loop {
                crossbeam_channel::select! {
                    recv(ticker) -> _ => if let Err(e) = this.trigger_flush() {
                        eprintln!("flush failed: {}", e);
                    },
                    recv(rx) -> _ => return
                }
            }
        });
        Ok(Some(handle))
    }
}
