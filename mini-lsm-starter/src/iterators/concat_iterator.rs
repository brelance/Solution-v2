#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::{any::Any, ops::Sub, sync::Arc};

use anyhow::Result;
use log::{debug, info};

use super::StorageIterator;
use crate::{
    key::{Key, KeySlice},
    table::{SsTable, SsTableIterator},
};

/// Concat multiple iterators ordered in key order and their key ranges do not overlap. We do not want to create the
/// iterators when initializing this iterator to reduce the overhead of seeking.
pub struct SstConcatIterator {
    current: Option<SsTableIterator>,
    next_sst_idx: usize,
    sstables: Vec<Arc<SsTable>>,
    is_valid: bool,
}

impl SstConcatIterator {
    pub fn create_and_seek_to_first(sstables: Vec<Arc<SsTable>>) -> Result<Self> {
        let mut current = None;
        let mut is_valid = false;
        if !sstables.is_empty() {
            current = Some(SsTableIterator::create_and_seek_to_first(sstables[0].clone())?);
            is_valid = true;
        }

        Ok(Self {
            current,
            next_sst_idx: sstables[0].sst_id(),
            sstables,
            is_valid,
        })
    }

    pub fn create_and_seek_to_key(sstables: Vec<Arc<SsTable>>, key: KeySlice) -> Result<Self> {
        let mut low = 0;
        let mut high = sstables.len();
        let mut current = None;
        let mut next_sst_idx = 0;
        let mut is_valid = false;

        //debug!("search key: {:?} in sstables_len_{}", key.for_testing_to_bytes(), high);
        while low < high {
            let mid = low + (high - low) / 2;
            let mid_first = sstables[mid].first_key().as_key_slice();
            let mid_last = sstables[mid].last_key().as_key_slice();
    
            //debug!("Low: {}, High: {}", low, high);
            //debug!("Mid: {}, first_key: {:?}, last_key:{:?}", mid, mid_first.for_testing_to_bytes(), mid_last.for_testing_to_bytes());
            if key >= mid_first {
                if key <= mid_last {
                    low = mid;
                    break;
                } else {
                    low = mid + 1;
                }
            } else {
                high = mid;
            }
        }

        if low < sstables.len() {
            current = Some(SsTableIterator::create_and_seek_to_key(sstables[low].clone(), key.raw_ref())?);
            next_sst_idx = sstables[low].sst_id();
            is_valid = true;
        }

        Ok(Self {
            current,
            next_sst_idx,
            sstables,
            is_valid,
        })

    }
}

impl StorageIterator for SstConcatIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        // if let Some(sst) = self.current {
        //     return sst.key();
        // }
        self.current.as_ref().unwrap().key()
    }

    fn value(&self) -> &[u8] {
        // if let Some(sst) = self.current {
        //     return sst.value();
        // }
        self.current.as_ref().unwrap().value()
    }

    fn is_valid(&self) -> bool {
        self.is_valid
    }

    fn next(&mut self) -> Result<()> {
        if self.is_valid {
            self.current.as_mut().unwrap().next()?;
            if !self.current.as_ref().unwrap().is_valid() {
                self.next_sst_idx += 1;
                if self.next_sst_idx == self.sstables.len() {
                    self.is_valid = false;
                } else {
                    self.current = Some(SsTableIterator::create_and_seek_to_first(self.sstables[self.next_sst_idx].clone())?);
                }
            }
        }

        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        1
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_partition_point() {
        let v = [1, 2, 3, 4, 5].to_vec();
        let idx = v.partition_point(|&i| i < 4);
        assert_eq!(idx, 3);

        let v = [1, 2, 3, 3, 5, 6, 7];
        let i = v.partition_point(|&x| x < 5);

        assert_eq!(i, 4);
        assert!(v[..i].iter().all(|&x| x < 5));
        assert!(v[i..].iter().all(|&x| !(x < 5)));
    }

}