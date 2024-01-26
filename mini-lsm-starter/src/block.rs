#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

mod builder;
mod iterator;


pub use builder::BlockBuilder;
/// You may want to check `bytes::BufMut` out when manipulating continuous chunks of memory
use bytes::{Bytes, BufMut};
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree.
/// It is a collection of sorted key-value pairs.
/// The `actual` storage format is as below (After `Block::encode`):
///
/// ----------------------------------------------------------------------------------------------------
/// |             Data Section             |              Offset Section             |      Extra      |
/// ----------------------------------------------------------------------------------------------------
/// | Entry #1 | Entry #2 | ... | Entry #N | Offset #1 | Offset #2 | ... | Offset #N | num_of_elements |
/// ----------------------------------------------------------------------------------------------------

pub const BLOCK_SIZE: usize =  4096;

#[derive(Debug, Clone)]
pub struct Block {
    pub data: Vec<u8>,
    pub offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the tutorial
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let mut encoded: Vec<u8> = self.data.clone();
        for &offset in self.offsets.iter() {
            encoded.put_u16(offset);
        }

        // Add nums of elements
        encoded.put_u16(self.offsets.len() as u16);
        Bytes::from(encoded)
    }
        
    pub fn decode(data: &[u8]) -> Self {
        let src_len = data.len();
        
        let num_of_elements = (data[src_len - 2] as u16 ) << 8 | data[src_len - 1] as u16;

        let offset_start_pos = src_len - 2 - num_of_elements as usize * 2;

        let kv_data = data[0..offset_start_pos].to_vec();
        let offset_slice = &data[offset_start_pos..src_len - 2];
        
        let mut offsets = Vec::new();
        for pair in offset_slice.chunks(2) {
            match pair {
                &[high, low] => {
                    let offset = (high as u16) << 8 | low as u16;
                    offsets.push(offset);
                }
                _ => {}
            }
        }
        
        Block {data: kv_data, offsets} 
        // let block_builder: BlockBuilder = BlockBuilder::new(BLOCK_SIZE);
    }

    pub fn size(&self) -> usize {
        // length of data + length of keys + length of nums
        return self.data.len() + self.offsets.len() * 2 + 2;
    }

    pub fn first_key(&self) -> Bytes {
        let mut key_len_buf = [0u8; 2];
        key_len_buf[0] = self.data[0];
        key_len_buf[1] = self.data[1];

        let key_len = u16::from_be_bytes(key_len_buf) as usize;
        Bytes::copy_from_slice(&self.data[2..key_len + 2])
    }

    pub fn last_key(&self) -> Bytes {
        let offset = self.offsets.last().unwrap().clone() as usize;
        let mut key_len_buf = [0u8; 2];
        key_len_buf[0] = self.data[offset];
        key_len_buf[1] = self.data[offset + 1];

        let key_len = u16::from_be_bytes(key_len_buf) as usize;
        Bytes::copy_from_slice(&self.data[offset + 2..offset + key_len + 2])
    }
}

impl PartialEq for Block {
    fn eq(&self, other: &Self) -> bool {
        self.first_key() == other.first_key()
    }
}

impl PartialOrd for Block {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.first_key().cmp(&other.first_key()))
    }
}

impl Eq for Block {}


impl Ord for Block {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.first_key().cmp(&other.first_key())
    }
}


#[cfg(test)]
mod test {
    use crate::block::{BlockBuilder, Block};
    #[test]
    fn test_chunk() {
        let numbers: &[u8] = &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10];

        // let offsets: Vec<u16> = numbers
        //     .chunks(2)
        //     .into_iter()
        //     .map(|&[high, low]| (high as u16) << 8 | low as u16 )
        //     .collect();
       
        for pair in numbers.chunks(2) {
            match pair {
                &[a, b] => println!("Pair: {}, {}", a, b),
                &[a] => println!("Single element: {}", a),
                _ => unreachable!(),
            }
        }
    }

    #[test]
    fn test_encode_decode() {
        let mut builder = BlockBuilder::new(16);
        builder.add(b"233", b"233333");
        builder.add(b"122", b"122222");
        let mut block =  builder.build();
        let data = block.encode();
        Block::decode(&data);
    }

    #[test]
    fn compare_block() {
        let mut builder = BlockBuilder::new(32);
        builder.add(b"233", b"233333");
        builder.add(b"122", b"122222");
        let block = builder.build();

        let mut builder1 = BlockBuilder::new(32);
        builder1.add(b"333", b"233333");
        builder1.add(b"422", b"122222");
        let block1 = builder1.build();

        let mut builder2 = BlockBuilder::new(32);
        builder2.add(b"733", b"233333");
        builder2.add(b"922", b"122222");
        let block2 = builder2.build();

        let mut builder3: BlockBuilder = BlockBuilder::new(32);
        builder3.add(b"033", b"233333");
        builder3.add(b"122", b"122222");
        let block3 = builder3.build();

        assert_eq!(block1.cmp(&block), std::cmp::Ordering::Greater);
        assert_eq!(block.cmp(&block1), std::cmp::Ordering::Less);

        let block_ = block3.clone();

        let mut btset = std::collections::BTreeSet::new();
        btset.insert(block1);
        btset.insert(block);
        btset.insert(block2);
        btset.insert(block3);

        assert_eq!(btset.first(), Some(&block_));
    }

    #[test]
    fn test_vec_sort() {
        let t1 = b"Key_3".to_vec();
        let t2 = b"Key_12".to_vec();
        assert_eq!(t1.cmp(&t2), std::cmp::Ordering::Greater);
        
    }
}


