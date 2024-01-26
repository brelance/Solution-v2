use bytes::BufMut;

use super::Block;
use std::{collections::{BTreeMap}};

/// Builds a block.
pub struct BlockBuilder {
    block_size: usize,
    buffer: BTreeMap<Vec<u8>, Vec<u8>>,
    rest_size: usize,
    pub num_of_elements: usize,
}


impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            block_size,
            buffer: BTreeMap::new(),
            rest_size: block_size,
            num_of_elements: 0,
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: &[u8], value: &[u8]) -> bool {
        let insert_size = key.len() + value.len() + 6;
        if self.rest_size < insert_size {
            return false;
        } else {
            if !self.buffer.contains_key(key) {
                self.num_of_elements += 1;
            }

            self.buffer.insert(key.to_vec(), value.to_vec());
            self.rest_size -= insert_size;
            
            return true;
        }
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.num_of_elements == 0
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        let data_vec_size = self.block_size - self.num_of_elements * 2 - 2;
        let mut data: Vec<u8> = Vec::with_capacity(data_vec_size);

        let mut offsets: Vec<u16> = Vec::new();
        let mut offset: u16 = 0;

        for (key, value) in self.buffer.iter() {
                offsets.push(offset);

                let key_len = key.len() as u16;
                let value_len = value.len() as u16;

                data.put_u16(key_len);
                data.extend_from_slice(key.as_slice());
                data.put_u16(value_len);
                data.extend_from_slice(value.as_slice());

                offset = offset + 2 + key_len + 2 + value_len;
        }

        while data.len() < data.capacity() {
            data.push(0);
        }

        Block {data, offsets}    
    }

    pub fn build_ref(&self) -> Block {
        let data_vec_size = self.block_size - self.num_of_elements * 2 - 2;
        let mut data: Vec<u8> = Vec::with_capacity(data_vec_size);

        let mut offsets: Vec<u16> = Vec::new();
        let mut offset: u16 = 0;

        for (key, value) in self.buffer.iter() {
                offsets.push(offset);

                let key_len = key.len() as u16;
                let value_len = value.len() as u16;

                data.put_u16(key_len);
                data.extend_from_slice(key.as_slice());
                data.put_u16(value_len);
                data.extend_from_slice(value.as_slice());

                offset = offset + 2 + key_len + 2 + value_len;
        }

        while data.len() < data.capacity() {
            data.push(0);
        }

        Block {data, offsets}    
    }
}

struct Key {
    key: Vec<u8>,
}

impl PartialEq for Key {
    fn eq(&self, other: &Self) -> bool {
        self.key.as_slice() == other.key.as_slice()
    }
}

impl PartialOrd for Key {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.key.as_slice().cmp(&other.key.as_slice()))
    }
}


#[cfg(test)]
mod user_tests {
    use super::BlockBuilder;
    use std::collections::BTreeMap;
    use std::string;
    #[test]
    fn build_test() {
        let mut block_builder = BlockBuilder::new(4096);
        block_builder.add(b"1", b"432");
        block_builder.add(b"3", b"432");
        block_builder.add(b"2", b"233333");

        let block = block_builder.build();
        
    }

    #[test]
    fn string_test() {
        let string1 = "11".to_string();
        let string2 = "2".to_string();
        
        println!("string1.cmp(&string2): {:?}", string1.cmp(&string2)); // Ordering::Less
    }
}