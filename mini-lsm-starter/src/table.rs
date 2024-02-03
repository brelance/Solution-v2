#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

pub(crate) mod bloom;
mod builder;
mod iterator;

use std::fs::{File, OpenOptions};
use std::io::Read;
use std::os::windows::fs::FileExt;
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
pub use builder::SsTableBuilder;
use bytes::{Buf, BufMut, Bytes};
pub use iterator::SsTableIterator;

use crate::block::Block;
use crate::key::{KeyBytes, KeySlice};
use crate::lsm_storage::BlockCache;

use self::bloom::Bloom;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    /// Offset of this data block.
    pub offset: usize,
    /// The first key of the data block.
    pub first_key: KeyBytes,
    /// The last key of the data block.
    pub last_key: KeyBytes,
}

impl BlockMeta {
    /// Encode block meta to a buffer.
    /// You may add extra fields to the buffer,
    /// in order to help keep track of `first_key` when decoding from the same buffer in the future.
    pub fn encode_block_meta(
        block_meta: &[BlockMeta],
        #[allow(clippy::ptr_arg)] // remove this allow after you finish
        buf: &mut Vec<u8>,
    ) {
        for meta in block_meta {
            // Put meta block offset
            buf.put_u32(meta.offset as u32);
            // Put the length of first key in the block
            buf.put_u16(meta.first_key.len() as u16);
            buf.extend_from_slice(meta.first_key.raw_ref());

            // Put the length of last key in the block
            buf.put_u16(meta.last_key.len() as u16);
            buf.extend_from_slice(meta.last_key.raw_ref());
        }
    }

    /// Decode block meta from a buffer.
    pub fn decode_block_meta(mut buf: impl Buf) -> Vec<BlockMeta> {
        let mut metas = Vec::new();
        
        while buf.has_remaining() {
            let offset = buf.get_u32() as usize;
            let first_key_len = buf.get_u16() as usize;
            let first_key = KeyBytes::from_bytes(buf.copy_to_bytes(first_key_len));

            let last_key_len = buf.get_u16() as usize;
            let last_key = KeyBytes::from_bytes(buf.copy_to_bytes(last_key_len));

            metas.push(BlockMeta {
                offset,
                first_key,
                last_key,
            })
        } 

        metas
    }

    pub fn size(&self) -> usize {
        self.first_key.len() + self.last_key.len() + std::mem::size_of::<usize>()
    } 
}

/// A file object.
pub struct FileObject(Option<File>, u64);

impl FileObject {
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        // unimplemented!()
        // use std::os::fs::FileExt;
        // let mut data = vec![0; len as usize];
        // self.0
        //     .as_ref()
        //     .unwrap()
        //     .read_exact_at(&mut data[..], offset)?;
        // Ok(data)

        let mut data = vec![0; len as usize];
        let read_len = self.0.as_ref().unwrap().seek_read(&mut data, offset)?;
        assert_eq!(len as usize, read_len);
        Ok(data)
    }

    pub fn read_to_buf(&self, offset: u64, buf: &mut [u8]) -> Result<()> {
        self.0.as_ref().unwrap().seek_read(buf, offset)?;
        Ok(())
    }

    pub fn size(&self) -> u64 {
        self.1
    }

    /// Create a new file object (day 2) and write the file to the disk (day 4).
    pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
        std::fs::write(path, &data)?;
        let file = OpenOptions::new().write(true).open(path)?.sync_all()?;
        // let file = File::open(path)?.sync_all()?;
        Ok(FileObject(
            Some(File::options().read(true).write(false).open(path)?),
            data.len() as u64,
        ))
    }

    pub fn open(path: &Path) -> Result<Self> {
        let file = File::options().read(true).write(false).open(path)?;
        let size = file.metadata()?.len();
        Ok(FileObject(Some(file), size))
    }
}

/// An SSTable.
pub struct SsTable {
    /// The actual storage unit of SsTable, the format is as above.
    pub(crate) file: FileObject,
    /// The meta blocks that hold info for data blocks.
    pub(crate) block_meta: Vec<BlockMeta>,
    /// The offset that indicates the start point of meta blocks in `file`.
    pub(crate) block_meta_offset: usize,
    id: usize,
    block_cache: Option<Arc<BlockCache>>,
    first_key: KeyBytes,
    last_key: KeyBytes,
    pub(crate) bloom: Option<Bloom>,
    block_size: usize,
    max_ts: u64,
}

impl SsTable {
    #[cfg(test)]
    pub(crate) fn open_for_test(file: FileObject) -> Result<Self> {
        Self::open(0, None, file)
    }

    /// Open SSTable from a file.
    pub fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        let mut offset = file.size() - 4;
        let mut buf = [0u8; 4];

        //Read block size
        file.read_to_buf(offset, &mut buf);
        let block_size = u32::from_be_bytes(buf) as usize;
        offset -= 4;

        file.read_to_buf(offset, &mut buf);
        let block_meta_offset = u32::from_be_bytes(buf) as usize;
        let upbound = offset;
        
        let mut key_len_buf = [0u8; 2];
        let mut block_meta = Vec::new();
        let mut meta_offset = block_meta_offset as u64;
        

        while meta_offset < upbound {
            // Read offset of block
            file.read_to_buf(meta_offset, &mut buf);
            let block_offset: u32 = u32::from_be_bytes(buf);
            meta_offset += 4;

            file.read_to_buf(meta_offset, &mut key_len_buf);
            let key_len = u16::from_be_bytes(key_len_buf) as u64;
            meta_offset += 2;

            let first_key = Bytes::from(file.read(meta_offset, key_len)?);
            meta_offset += key_len;

            file.read_to_buf(meta_offset, &mut key_len_buf);
            let key_len = u16::from_be_bytes(key_len_buf) as u64;
            meta_offset += 2;

            let last_key = Bytes::from(file.read(meta_offset, key_len)?);
            meta_offset += key_len;

            
            block_meta.push(
                BlockMeta {
                    offset: block_offset as usize,
                    first_key: KeyBytes::from_bytes(first_key),
                    last_key: KeyBytes::from_bytes(last_key),
                }
            );
        }

        // let block_size = (block_meta[1].offset - block_meta[0].offset) as usize;
        let first_key = block_meta.first().unwrap().first_key.clone();
        let last_key = block_meta.last().unwrap().last_key.clone();
        Ok(SsTable {
             file,
             block_meta, 
             block_meta_offset, 
             id,
             block_cache: None,
             first_key,
             last_key,
             bloom: None,
             block_size,
             max_ts: 0,
        })
    }

    /// Create a mock SST with only first key + last key metadata
    pub fn create_meta_only(
        id: usize,
        file_size: u64,
        first_key: KeyBytes,
        last_key: KeyBytes,
    ) -> Self {
        Self {
            file: FileObject(None, file_size),
            block_meta: vec![],
            block_meta_offset: 0,
            id,
            block_cache: None,
            first_key,
            last_key,
            bloom: None,
            block_size: 0,
            max_ts: 0,
        }
    }

    /// Read a block from the disk.
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        let meta = &self.block_meta[block_idx];
        
        // bug
        let block_slice = self.file.read(meta.offset as u64, self.block_size as u64)?;
        let block = Block::decode(&block_slice);

        Ok(Arc::new(block))
    }

    /// Read a block from disk, with block cache. (Day 4)
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        unimplemented!()
    }

    /// Find the block that may contain `key`.
    /// Note: You may want to make use of the `first_key` stored in `BlockMeta`.
    /// You may also assume the key-value pairs stored in each consecutive block are sorted.
    pub fn find_block_idx(&self, key: &[u8]) -> usize {
        self.block_meta
            .partition_point(|meta| meta.first_key.raw_ref() <= key)
            .saturating_sub(1)
    }

    /// Get number of data blocks.
    pub fn num_of_blocks(&self) -> usize {
        self.block_meta.len()
    }

    pub fn first_key(&self) -> &KeyBytes {
        &self.first_key
    }

    pub fn last_key(&self) -> &KeyBytes {
        &self.last_key
    }

    pub fn table_size(&self) -> u64 {
        self.file.1
    }

    pub fn sst_id(&self) -> usize {
        self.id
    }

    pub fn max_ts(&self) -> u64 {
        self.max_ts
    }
}

#[cfg(test)]
mod tests {
    use tempfile::{tempdir, TempDir};
    use crate::table::{SsTable, SsTableBuilder};
    use crate::key::{KeyBytes, KeySlice};
    use bytes::Bytes;

    fn key_of(idx: usize) -> Vec<u8> {
        format!("key_{:03}", idx * 5).into_bytes()
    }
    
    fn value_of(idx: usize) -> Vec<u8> {
        format!("value_{:010}", idx).into_bytes()
    }
    
    fn num_of_keys() -> usize {
        5
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

    #[test]
    fn test1() {
        let (_dir, sst) = generate_sst();
        let meta = sst.block_meta.clone();
        let new_sst = SsTable::open_for_test(sst.file).unwrap();
        assert_eq!(new_sst.block_meta, meta);
        assert_eq!(new_sst.first_key().raw_ref(), &key_of(0));
        assert_eq!(new_sst.last_key().raw_ref(), &key_of(num_of_keys() - 1));
    }
}