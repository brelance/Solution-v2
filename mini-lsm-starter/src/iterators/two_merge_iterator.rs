use anyhow::{Ok, Result};

use super::StorageIterator;
use std::{cmp::Ordering};

/// Merges two iterators of different types into one. If the two iterators have the same key, only
/// produce the key once and prefer the entry from A.
pub struct TwoMergeIterator<A: StorageIterator, B: StorageIterator> {
    iter_a: A,
    iter_b: B,
    current: bool,
    // Add fields as need
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > TwoMergeIterator<A, B>
{
    pub fn create(iter_a: A, iter_b: B) -> Result<Self> {
        let mut current = true;

        if !(iter_a.is_valid() && iter_b.is_valid()) {
            if iter_b.is_valid() { current = false; }
            return Ok(Self { iter_a, iter_b, current, });
        }

        if iter_a.key() > iter_b.key() {
            current = false;
        }

        Ok(Self {
            iter_a,
            iter_b,
            current,
        })
    }
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > StorageIterator for TwoMergeIterator<A, B>
{
    type KeyType<'a> = A::KeyType<'a>;

    fn key(&self) -> Self::KeyType<'_> {
        if self.current {
            self.iter_a.key()
        } else {
            self.iter_b.key()
        }
    }

    fn value(&self) -> &[u8] {
        if self.current {
            self.iter_a.value()
        } else {
            self.iter_b.value()
        }
    }

    fn is_valid(&self) -> bool {
        self.iter_a.is_valid() || self.iter_b.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        if !self.is_valid() { return Ok(()) }

        if self.current == true {
            while self.iter_b.is_valid() && self.iter_a.key() == self.iter_b.key() { self.iter_b.next()?; }

            self.iter_a.next()?;
            if !self.iter_a.is_valid() {
                self.current = false;
                return Ok(());
            }

            if !self.iter_b.is_valid() { return Ok(()); }

            match self.iter_a.key().cmp(&self.iter_b.key()) {
                Ordering::Equal | Ordering::Less => {},
                Ordering::Greater => self.current = false,
            }
        } else {
            self.iter_b.next()?;
            if !self.iter_b.is_valid() {
                self.current = true;
                return Ok(());
            }

            if !self.iter_a.is_valid() { return Ok(()); }

            match self.iter_b.key().cmp(&self.iter_a.key()) {
                Ordering::Equal | Ordering::Greater => { self.current = true; },
                Ordering::Less => {}
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use anyhow::Ok;

    use crate::iterators::StorageIterator;
    use crate::mem_table::*;
    use crate::table::*;
    use anyhow::Result;
    use std::sync::Arc;


    use super::TwoMergeIterator;
    use bytes::Bytes;
    const BLOCK_SIZE: usize = 16;

    pub fn as_bytes(value: &[u8]) -> Bytes {
        Bytes::copy_from_slice(value)
    }

    #[test]
    fn test1() -> Result<()> {
        let mut memtable = MemTable::create(BLOCK_SIZE);
        memtable.put(b"1", b"1111");
        memtable.put(b"2", b"2222");

        let mut ssbuilder = SsTableBuilder::new(4096);
        ssbuilder.add(b"1", b"423424");

        ssbuilder.add(b"3", b"111");
        ssbuilder.add(b"4", b"3333");
        ssbuilder.add(b"5", b"3333");


        let table = ssbuilder.build_for_test("./test")?;
        
        let mut mem_iter = memtable.scan(std::ops::Bound::Included(b"1"), std::ops::Bound::Included(b"2"));
        let mut sst_iter = SsTableIterator::create_and_seek_to_first(Arc::new(table))?;
        
        
        let mut two_merger_iter = TwoMergeIterator::create(mem_iter, sst_iter)?;
        
        while two_merger_iter.is_valid() {
            println!("key {:?} : value {:?}", as_bytes(two_merger_iter.key().raw_ref()), as_bytes(two_merger_iter.value()));
            two_merger_iter.next();
        }

        assert!(!two_merger_iter.is_valid());
        Ok(())
    }

    #[test]
    fn test2() -> Result<()> {
        let mut memtable = MemTable::create(BLOCK_SIZE);
        memtable.put(b"2", b"");

        let mut ssbuilder = SsTableBuilder::new(4096);
        ssbuilder.add(b"1", b"233");
        let table = ssbuilder.build_for_test("./test")?;
        
        let mut mem_iter = memtable.scan(std::ops::Bound::Included(b"1"), std::ops::Bound::Included(b"2"));
        let mut sst_iter = SsTableIterator::create_and_seek_to_first(Arc::new(table))?;
        let mut two_merger_iter = TwoMergeIterator::create(mem_iter, sst_iter)?;
        
        while two_merger_iter.is_valid() {
            println!("key {:?} : value {:?}", as_bytes(two_merger_iter.key().raw_ref()), as_bytes(two_merger_iter.value()));
            two_merger_iter.next();
        }

        assert!(!two_merger_iter.is_valid());

        
        Ok(())
    }  
}




