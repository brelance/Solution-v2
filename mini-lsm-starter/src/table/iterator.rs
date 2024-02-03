#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::sync::Arc;

use anyhow::Result;

use super::SsTable;
use crate::{block::BlockIterator, iterators::StorageIterator, key::KeySlice};

/// An iterator over the contents of an SSTable.
pub struct SsTableIterator {
    table: Arc<SsTable>,
    block_iterator: BlockIterator,
    block_idx: usize,
}

impl SsTableIterator {
    /// Create a new iterator and seek to the first key-value pair in the first data block.
    pub fn create_and_seek_to_first(table: Arc<SsTable>) -> Result<Self> {
        let block_iterator = Self::create_and_seek_to_(table.clone(), 0)?;

        Ok(Self {
            table: table,
            block_iterator,
            block_idx: 0,
        })
    }

    /// Seek to the first key-value pair in the first data block.
    pub fn seek_to_first(&mut self) -> Result<()> {
        let first_block_iterator= Self::create_and_seek_to_(self.table.clone(), 0)?;
        self.block_iterator = first_block_iterator;
        self.block_idx = 0;
        Ok(())
    }

    /// Create a new iterator and seek to the first key-value pair which >= `key`.
    pub fn create_and_seek_to_key(table: Arc<SsTable>, key: &[u8]) -> Result<Self> {
        let (block_iterator, block_idx) = Self::create_block_iterator_with_key(table.clone(), key)?;

        Ok(Self {
            table,
            block_iterator,
            block_idx,
        })
    }

    /// Seek to the first key-value pair which >= `key`.
    /// Note: You probably want to review the handout for detailed explanation when implementing
    /// this function.
    pub fn seek_to_key(&mut self, key: &[u8]) -> Result<()> {
        let (mut block_iterator, mut block_idx) = Self::create_block_iterator_with_key(self.table.clone(), key)?;

        if block_iterator.key().raw_ref() < key {
            block_idx += 1;
            if block_idx < self.table.num_of_blocks() {
                block_iterator = Self::create_and_seek_to_(self.table.clone(), block_idx)?;
                block_iterator.seek_to_key(key);
            }
        }

        self.block_iterator = block_iterator;
        self.block_idx = block_idx;
        Ok(())
    }

    fn create_and_seek_to_(table: Arc<SsTable>, index: usize) -> Result<BlockIterator> {
        let block: Arc<crate::block::Block> = table.read_block(index)?;

        Ok(BlockIterator::create_and_seek_to_first(block))
    }

    fn create_block_iterator_with_key(table: Arc<SsTable>, key: &[u8]) -> Result<(BlockIterator, usize)> {
        let block_index = table.find_block_idx(key);
        let target_block = table.read_block(block_index)?;

        let mut target_iterator: BlockIterator = BlockIterator::new(target_block);
        target_iterator.seek_to_key(key);

        Ok((target_iterator, block_index))
    }
}

impl StorageIterator for SsTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    /// Return the `key` that's held by the underlying block iterator.
    fn key(&self) -> KeySlice {
        self.block_iterator.key()
    }

    /// Return the `value` that's held by the underlying block iterator.
    fn value(&self) -> &[u8] {
        self.block_iterator.value()
    }

    /// Return whether the current block iterator is valid or not.
    fn is_valid(&self) -> bool {
        self.block_iterator.is_valid()
    }

    /// Move to the next `key` in the block.
    /// Note: You may want to check if the current block iterator is valid after the move.
    fn next(&mut self) -> Result<()> {
        self.block_iterator.next();

        if !self.is_valid() {
            if self.block_idx == self.table.num_of_blocks() - 1 {
                //All blocks have finished iterating
                return Ok(());
            }

            // current block iterator is not valid

            self.block_idx += 1;
            let new_block_iterator = Self::create_and_seek_to_(self.table.clone(), self.block_idx)?;
            self.block_iterator = new_block_iterator;

            return Ok(());
        }

        Ok(())
    }
}



#[cfg(test)]
mod tests {
    use super::{SsTableIterator, SsTable};
    use tempfile::{TempDir, tempdir};
    use crate::{iterators::StorageIterator, table::SsTableBuilder};
    use crate::key::KeySlice;
    use std::{io::Read, sync::Arc};
    use bytes::Bytes;
    
    
    fn key_of(idx: usize) -> Vec<u8> {
        format!("key_{:03}", idx * 5).into_bytes()
    }

    fn value_of(idx: usize) -> Vec<u8> {
        format!("value_{:010}", idx).into_bytes()
    }

    fn num_of_keys() -> usize {
        10
    }

    fn generate_sst() -> (TempDir, SsTable) {
        let mut builder = SsTableBuilder::new(128);
        for idx in 0..num_of_keys() {
            let key = key_of(idx);
            let value = value_of(idx);
            builder.add(KeySlice::from_slice(&key[..]), &value[..]);
        }
        let dir = tempdir().unwrap();
        let path = dir.path().join("1.sst");
        (dir, builder.build_for_test(path).unwrap())
    }

    
    fn as_bytes(x: &[u8]) -> Bytes {
        Bytes::copy_from_slice(x)
    }

    
    #[test]
    fn sst_test1() {
        let (_dir, sst) = generate_sst();
        let sst = Arc::new(sst);

        let mut iter = SsTableIterator::create_and_seek_to_first(sst).expect("error");
        assert_eq!(iter.key().raw_ref(), key_of(0));
        assert_eq!(iter.value(), value_of(0))
    }

    #[test]
    fn test_sst_seek_key() {
        let (_dir, sst) = generate_sst();
        let sst = Arc::new(sst);
        let mut iter = SsTableIterator::create_and_seek_to_key(sst, &key_of(0)).unwrap();
        for offset in 1..=5 {
            for i in 0..num_of_keys() {
                let key = iter.key().raw_ref();
                let value = iter.value();
                assert_eq!(
                    key,
                    key_of(i),
                    "expected key: {:?}, actual key: {:?}",
                    as_bytes(&key_of(i)),
                    as_bytes(key)
                );
                assert_eq!(
                    value,
                    value_of(i),
                    "expected value: {:?}, actual value: {:?}",
                    as_bytes(&value_of(i)),
                    as_bytes(value)
                );
                iter.seek_to_key(&format!("key_{:03}", i * 5 + offset).into_bytes())
                    .unwrap();
            }
            iter.seek_to_key(b"k").unwrap();
        }
    }
}