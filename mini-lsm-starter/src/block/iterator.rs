use super::Block;
use std::sync::Arc;
use crate::key::{KeySlice, KeyVec};

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// The corresponding value, can be empty
    value: Vec<u8>,
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,

    nums_of_elements: usize,

    is_valid: bool,
}

impl BlockIterator {
    pub fn new(block: Arc<Block>) -> Self {
        Self {
            nums_of_elements: block.offsets.len(),
            block,
            key: KeyVec::new(),
            value: Vec::new(),
            idx: 0,
            is_valid: true,
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let (key, value) = BlockIterator::seek_kv_within_index(block.clone(), 0);
        let nums_of_elements = block.offsets.len();
        BlockIterator { 
            block, 
            key, 
            value,
            idx: 0,
            nums_of_elements,
            is_valid: true,
        }
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: &[u8]) -> Self {
        let (key, value, idx) = BlockIterator::seek_key(block.clone(), key);
        let nums_of_elements = block.offsets.len();

        BlockIterator { 
            block, 
            key,
            value,
            idx, 
            nums_of_elements,
            is_valid: true,
        }
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice {
        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        &self.value
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        self.is_valid
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        let (key, value) =
            BlockIterator::seek_kv_within_index(self.block.clone(), 0);

        self.key = key;
        self.value = value;
        self.idx = 0;
        self.is_valid = true;
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        if !self.is_valid() { return; }

        self.idx += 1;
        if self.idx < self.nums_of_elements {
            let (key, value) =
            BlockIterator::seek_kv_within_index(self.block.clone(), self.idx);
            self.key = key;
            self.value = value;
        } else {
            self.is_valid = false;
        }
    }

    // pub fn next_without_check(&mut self) {
    //     let (key, value) =
    //     BlockIterator::seek_kv_within_index(self.block.clone(), self.idx + 1);
    //     self.key = key;
    //     self.value = value;
    //     self.idx += 1;
    // }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by callers.
    pub fn seek_to_key(&mut self, key: &[u8]) {
        let (key, value, idx) = BlockIterator::seek_key(self.block.clone(), key);
        self.key = key;
        self.value = value;
        self.idx = idx;
    }

    ///Note: This implement may cause bug. eg: "11".compare("2") == Less
    fn seek_key(block: Arc<Block>, key: &[u8]) -> (KeyVec, Vec<u8>, usize) {
        let mut left = 0;
        let elem_num: usize = block.offsets.len();
        let mut right = elem_num;
        let (result_key, value) :(KeyVec, Vec<u8>);

        while left < right {
            let mid: usize = left + (right - left) / 2;
            let cur_key = BlockIterator::seek_key_within_index(block.clone(), mid);

            match cur_key.raw_ref().cmp(key) {
                std::cmp::Ordering::Less => {
                    left = mid + 1;
                }
                std::cmp::Ordering::Greater => {
                    right = mid;
                }
                std::cmp::Ordering::Equal => {
                    (result_key, value) = BlockIterator::seek_kv_within_index(block.clone(), mid);
                    return (result_key, value, mid);
                }
            }
        }

        // if key greater than max element in the block, we return max elems in the block
        if left >= elem_num { left = elem_num - 1; }
        (result_key, value) = BlockIterator::seek_kv_within_index(block.clone(), left);
        (result_key, value, left)
    }

    fn seek_key_within_index(block: Arc<Block>, index: usize) -> KeyVec {
        let offset = block.offsets[index] as usize;

        let key_len = u16::from_be_bytes([block.data[offset], block.data[offset + 1]]) as usize;

        KeyVec::from_vec(block.data[offset + 2..offset + key_len + 2].to_vec())
    }

    fn seek_kv_within_index(block: Arc<Block>, index: usize) -> (KeyVec, Vec<u8>) {
        let offset = block.offsets[index] as usize;
        let key_len = u16::from_be_bytes([block.data[offset], block.data[offset + 1]]) as usize;

        let value_len_pos = offset + key_len + 2;
        let value_len = u16::from_be_bytes([block.data[value_len_pos], block.data[value_len_pos + 1]]) as usize;
        
        let key = KeyVec::from_vec(block.data[offset + 2..offset + 2 + key_len].to_vec());
        let value_pos = offset + key_len + 4;
        let value = block.data[value_pos..(value_pos + value_len)].to_vec();
        (key, value)
    }
}


#[cfg(test)]
mod test {
    use bytes::Bytes;

    use crate::{block::{BlockIterator, Block, BlockBuilder, builder}, iterators};
    use std::{iter, sync::Arc, vec};
    fn binary_search(nums: &[i32], target: i32) -> usize {
        let mut left = 0;
        let mut right = nums.len();
    
        while left < right {
            let mid = left + (right - left) / 2;
    
            if nums[mid] <= target {
                left = mid + 1;
            } else {
                right = mid;
            }
        }
    
        left
    }

    #[test]
    fn binary_search_test() {
        let nums = vec![1, 3, 5, 7, 9];
        let target = 9;
    
        let result = binary_search(&nums, target);
        println!("Position: {}", result);
    }

    
    #[test]
    fn iterator_test() {
        let mut builder = BlockBuilder::new(1024);
        builder.add(b"233", b"233333");
        builder.add(b"122", b"122222");
        
        let mut block =  builder.build();
        let mut iterator = BlockIterator::create_and_seek_to_first(Arc::new(block));
        
        assert_eq!(iterator.nums_of_elements, 2);
        assert_eq!(b"122", iterator.key().raw_ref());
        assert_eq!(b"122222", iterator.value());
   
        iterator.next();

        assert_eq!(b"233", iterator.key().raw_ref());
        assert_eq!(b"233333", iterator.value());
    }

    #[test]
    fn iterator_seek_key_test1() {
        let mut builder = BlockBuilder::new(1024);
        builder.add(b"1", b"1");
        builder.add(b"2", b"1");
        builder.add(b"4", b"1");
        builder.add(b"5", b"1");
        builder.add(b"8", b"1");
        let mut block =  builder.build();
        let mut iterator = BlockIterator::create_and_seek_to_first(Arc::new(block));
        
        assert_eq!(iterator.nums_of_elements, 5);
        iterator.seek_to_key(b"3");
        assert_eq!(b"4", iterator.key().raw_ref())
        
    }


    #[test]
    fn iterator_seek_key_test2() {
        let mut builder = BlockBuilder::new(1024);
        builder.add(b"key_2", b"1");
        builder.add(b"key_1", b"1");
        builder.add(b"key_8", b"1");
        builder.add(b"key_4", b"1");
        builder.add(b"key_5", b"1");

        let mut block =  builder.build();
        let mut iterator = BlockIterator::create_and_seek_to_first(Arc::new(block));
        
        assert_eq!(iterator.nums_of_elements, 5);
        iterator.seek_to_key(b"key_3");
        assert_eq!(b"key_4", iterator.key().raw_ref());

        iterator.seek_to_key(b"key_2");
        assert_eq!(b"key_2", iterator.key().raw_ref());

        iterator.seek_to_key(b"key_1");
        assert_eq!(b"key_1", iterator.key().raw_ref());

        iterator.seek_to_key(b"key_8");
        assert_eq!(b"key_8", iterator.key().raw_ref());


        iterator.seek_to_key(b"key_4");
        assert_eq!(b"key_4", iterator.key().raw_ref());


        iterator.seek_to_key(b"key_5");
        assert_eq!(b"key_5", iterator.key().raw_ref());
    }

    #[test]
    fn iterator_seek_key_test3() {
        let mut builder = BlockBuilder::new(1024);

        builder.add(b"key_2", b"1");
        builder.add(b"key_1", b"1");
        builder.add(b"key_3", b"Hello");
        builder.add(b"key_8", b"World");
        builder.add(b"key_4", b"42");

    
        let block = builder.build();
        let mut iterator = BlockIterator::create_and_seek_to_first(Arc::new(block));

        assert_eq!(iterator.nums_of_elements, 5);

    
        iterator.seek_to_key(b"key_3");
        assert_eq!(b"Hello", iterator.value());

        iterator.seek_to_key(b"key_2");
        assert_eq!(b"1", iterator.value());

        iterator.seek_to_key(b"key_4");
        assert_eq!(b"42", iterator.value());

        iterator.seek_to_key(b"key_5");
        assert_eq!(b"World", iterator.value());
    }

    #[test]
    fn sorting_test() {
        use std::string::String;
        let s1 = "key_12342".to_string();
        let s2 = "key_1".to_string();

        let v1 = b"key_12342".to_vec();
        let v2 = b"key_14".to_vec();

        assert_eq!(v1.cmp(&v2), std::cmp::Ordering::Less);
    }

    #[test]
    fn iterator_seek_key_test4() {
        let block = generate_block();
        let mut iter = BlockIterator::new(Arc::new(block));
        for idx in 0..num_of_keys() {
            let key = key_of(idx);
            let value = value_of(idx);
            iter.seek_to_key(&key);
            assert_eq!(&key, iter.key().raw_ref());
            assert_eq!(&value, iter.value());
        }

        iter.seek_to_key(&format!("key_{:03}", 4).into_bytes());
        let key_m = b"key_005";
        assert_eq!(
            iter.key().raw_ref(),
            key_m,
            "expected key: {:?}, actual key: {:?}",
            as_bytes(key_m),
            as_bytes(iter.key().raw_ref())
        );
    }

    #[test]
    fn iterator_seek_key_test5() {
        let block = generate_block();
        let mut iter = BlockIterator::new(Arc::new(block));
        for idx in 0..10 {
            let key = key_of(idx);
            let value = value_of(idx);
            iter.seek_to_key(&key);
            assert_eq!(&key, iter.key().raw_ref());
            assert_eq!(&value, iter.value());
        }

        iter.seek_to_key(&format!("key_{:03}", 46).into_bytes());
        // let key_m = b"key_005";
        // assert_eq!(
        //     iter.key().raw_ref(),
        //     key_m,
        //     "expected key: {:?}, actual key: {:?}",
        //     as_bytes(key_m),
        //     as_bytes(iter.key().raw_ref())
        // );
    }
    

    fn as_bytes(x: &[u8]) -> Bytes {
        Bytes::copy_from_slice(x)
    }


    fn key_of(idx: usize) -> Vec<u8> {
        format!("key_{:03}", idx * 5).into_bytes()
    }
    
    fn value_of(idx: usize) -> Vec<u8> {
        format!("value_{:010}", idx).into_bytes()
    }
    
    fn num_of_keys() -> usize {
        100
    }
    
    fn generate_block() -> Block {
        let mut builder = BlockBuilder::new(10000);
        for idx in 0..num_of_keys() {
            let key = key_of(idx);
            let value = value_of(idx);
            assert!(builder.add(&key[..], &value[..]));
        }
        builder.build()
    }

    #[test]
    fn debug_test() {
        let s1 = b"Key_000";
        let s2 = b"Key_004";
        assert_eq!(s1.cmp(&s2), std::cmp::Ordering::Less);
    }

}