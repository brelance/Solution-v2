#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use anyhow::{Ok, Result, bail};
use bytes::Bytes;

use crate::{
    iterators::{merge_iterator::MergeIterator, two_merge_iterator::TwoMergeIterator, StorageIterator}, mem_table::{MemTableIterator, map_bound}, table::SsTableIterator
};

use std::ops::Bound;

/// Represents the internal type for an LSM iterator. This type will be changed across the tutorial for multiple times.
// type LsmIteratorInner = TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<SsTableIterator>>;

type LsmIteratorInner = TwoMergeIterator<
    TwoMergeIterator<MergeIterator<MemTableIterator>, 
    MergeIterator<SsTableIterator>>, MergeIterator<SsTableIterator>
>;

pub struct LsmIterator {
    inner: LsmIteratorInner,
    end_bound: Bound<Bytes>,
    is_valid: bool,
}

impl LsmIterator {
    pub(crate) fn new(iter: LsmIteratorInner, bound: Bound<&[u8]>) -> Result<Self> {
        let end_bound = map_bound(bound);
        let mut iter = Self {
            is_valid: iter.is_valid(),
            inner: iter,
            end_bound,
        };
        iter.skip_deleted()?;
        Ok(iter)
    }
}

impl StorageIterator for LsmIterator {
    type KeyType<'a> = &'a [u8];

    fn is_valid(&self) -> bool {
        self.is_valid
    }

    fn key(&self) -> &[u8] {
        self.inner.key().raw_ref()
    }

    fn value(&self) -> &[u8] {
        self.inner.value()
    }

    fn next(&mut self) -> Result<()> {
        if !self.is_valid {
            return Ok(());
        }

        self.inner.next()?;
        if !self.inner.is_valid() {
            self.is_valid = false;
        }
        self.skip_deleted();
        
        if self.is_valid {
            match self.end_bound.as_ref() {
                Bound::Unbounded => {}
                Bound::Included(key) => {
                    self.is_valid = self.inner.key().raw_ref() <= key.as_ref();
                },
                Bound::Excluded(key) => {
                    self.is_valid = self.inner.key().raw_ref() < key.as_ref();
                },
            }
        }
        
        Ok(())
    }
}

impl LsmIterator {
    fn skip_deleted(&mut self) -> Result<()> {
        while self.is_valid() && self.value().is_empty() {
            self.next()?;
        }
        Ok(())
    }
}

/// A wrapper around existing iterator, will prevent users from calling `next` when the iterator is
/// invalid. If an iterator is already invalid, `next` does not do anything. If `next` returns an error,
/// `is_valid` should return false, and `next` should always return an error.
pub struct FusedIterator<I: StorageIterator> {
    iter: I,
    has_error: bool,
}

impl<I: StorageIterator> FusedIterator<I> {
    pub fn new(iter: I) -> Self {
        Self {
            iter,
            has_error: false, 
        }
    }
}

impl<I: StorageIterator> StorageIterator for FusedIterator<I> {
    type KeyType<'a> = I::KeyType<'a> where Self: 'a;

    fn is_valid(&self) -> bool {
        !self.has_error && self.iter.is_valid()
    }

    fn key(&self) -> Self::KeyType<'_> {
        self.iter.key()
    }

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn next(&mut self) -> Result<()> {
        if self.has_error {
            bail!("the iterator is tainted");
        }

        if self.iter.is_valid() {
            if let Err(e) = self.iter.next() {
                self.has_error = true;
                return Err(e);
            }
        }
        Ok(())
    }
}
