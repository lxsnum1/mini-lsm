use std::sync::Arc;

use anyhow::Result;

use super::SsTable;
use crate::{block::BlockIterator, iterators::StorageIterator, key::KeySlice};

/// An iterator over the contents of an SSTable.
pub struct SsTableIterator {
    table: Arc<SsTable>,
    blk_iter: BlockIterator,
    blk_idx: usize,
}

impl SsTableIterator {
    /// Create a new iterator and seek to the first key-value pair in the first data block.
    pub fn create_and_seek_to_first(table: Arc<SsTable>) -> Result<Self> {
        let blk = table.read_block(0)?;
        let blk_iter = BlockIterator::create_and_seek_to_first(blk);
        Ok(Self {
            table,
            blk_iter,
            blk_idx: 0,
        })
    }

    /// Seek to the first key-value pair in the first data block.
    pub fn seek_to_first(&mut self) -> Result<()> {
        self.seek_to_idx(0)
    }

    /// Create a new iterator and seek to the first key-value pair which >= `key`.
    pub fn create_and_seek_to_key(table: Arc<SsTable>, key: KeySlice) -> Result<Self> {
        let idx = table.find_block_idx(key);
        let blk = table.read_block(idx)?;
        let blk_iter = BlockIterator::create_and_seek_to_key(blk, key);
        Ok(Self {
            table,
            blk_iter,
            blk_idx: idx,
        })
    }

    /// Seek to the first key-value pair which >= `key`.
    /// Note: You probably want to review the handout for detailed explanation when implementing
    /// this function.
    pub fn seek_to_key(&mut self, key: KeySlice) -> Result<()> {
        unimplemented!()
    }

    fn create_and_seek_to_idx(table: Arc<SsTable>, blk_idx: usize) -> Result<Self> {
        let blk = table.read_block(blk_idx)?;
        let blk_iter = BlockIterator::create_and_seek_to_first(blk);
        Ok(Self {
            table,
            blk_iter,
            blk_idx,
        })
    }
}

impl StorageIterator for SsTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    /// Return the `key` that's held by the underlying block iterator.
    fn key(&self) -> KeySlice {
        unimplemented!()
    }

    /// Return the `value` that's held by the underlying block iterator.
    fn value(&self) -> &[u8] {
        unimplemented!()
    }

    /// Return whether the current block iterator is valid or not.
    fn is_valid(&self) -> bool {
        unimplemented!()
    }

    /// Move to the next `key` in the block.
    /// Note: You may want to check if the current block iterator is valid after the move.
    fn next(&mut self) -> Result<()> {
        unimplemented!()
    }
}
