use std::path::Path;
use std::sync::Arc;

use anyhow::{Result};
use bytes::BufMut;

use super::{BlockMeta, FileObject, SsTable};
use crate::{block::{BlockBuilder, Block}, lsm_storage::BlockCache};
use std::collections::BTreeSet;

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    // data: Vec<u8>,
    blocks: BTreeSet<Block>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            builder: BlockBuilder::new(block_size), 
            first_key: Vec::new(), 
            last_key: Vec::new(), 
            blocks: BTreeSet::new(), 
            meta: Vec::new(), 
            block_size,
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: &[u8], value: &[u8]) {
        if self.builder.add(key, value) {
            return;

        } else {
            // block don't contain enough capacity and we need split it.
            let block = self.builder.build_ref(); 
            
            self.blocks.insert(block);
            
            self.builder = BlockBuilder::new(self.block_size);
            self.builder.add(key, value);
        }
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        self.total_block_size() + self.total_meta_size()
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        if self.builder.num_of_elements != 0 {
            let last_block = self.builder.build_ref();
            self.blocks.insert(last_block);
        }

        let block_meta_offset = self.total_block_size();
        let mut offset = 0;
        if self.blocks.is_empty() {
            panic!("Sstable is empty");
        }
        
        let first_key = self.blocks.first().unwrap().first_key();
        let last_key = self.blocks.last().unwrap().last_key();

        let blocks = self.blocks.into_iter().map(|block| {
            self.meta.push(BlockMeta {
                offset,
                first_key: block.first_key(),
                last_key: block.last_key(),
            });
            offset += self.block_size;
            block.encode()
        });
        let mut data = Vec::new();

        for block in blocks {
            data.extend_from_slice(&block);
        }

        BlockMeta::encode_block_meta(&self.meta, &mut data);
        data.put_u32(block_meta_offset as u32);

        let mut file = FileObject::create(path.as_ref(), data)?;
        
        //put block_meta_offset
        Ok(SsTable {
            file,
            block_meta: self.meta,
            block_meta_offset,
            id,
            block_cache,
            first_key,
            last_key,
            bloom: None,
        })
    }

    fn total_block_size(&self) -> usize {
        self.blocks.len() * self.block_size
    }

    fn total_meta_size(&self)-> usize {
        self.meta.iter().fold(0, |acc, meta| acc + meta.size())
    }


    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
