use crate::{
    block::SIZEOF_U16,
    key::{KeySlice, KeyVec},
};

use super::Block;

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: vec![0],
            data: Vec::with_capacity(block_size),
            block_size,
            first_key: KeyVec::new(),
        }
    }

    fn estimated_size(&self) -> usize {
        SIZEOF_U16 /* number of key-value pairs in the block */ +  self.offsets.len() * SIZEOF_U16 /* offsets */ + self.data.len()
        // key-value pairs
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        assert!(!key.is_empty(), "key empty");

        match self.offsets.last() {
            Some(_) => {
                if self.estimated_size() + key.len() + value.len() + SIZEOF_U16 * 3 /* key_len, value_len and offset */
                    > self.block_size
                {
                    return false;
                }
            }
            None => self.first_key.append(key.raw_ref()),
        }

        self.offsets.push(self.data.len() as u16);

        let overlap = compute_overlap(self.first_key.as_key_slice(), key);
        self.data.extend((overlap as u16).to_ne_bytes());
        self.data
            .extend(((key.len() - overlap) as u16).to_ne_bytes());

        self.data.extend(&key.raw_ref()[overlap..]);
        self.data.extend((value.len() as u16).to_ne_bytes());
        self.data.extend(value);
        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        if self.is_empty() {
            panic!("block should not be empty");
        }

        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}

fn compute_overlap(first_key: KeySlice, key: KeySlice) -> usize {
    first_key
        .raw_ref()
        .iter()
        .zip(key.raw_ref())
        .take_while(|(a, b)| a == b)
        .count()
}
