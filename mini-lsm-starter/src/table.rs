pub(crate) mod bloom;
mod builder;
mod iterator;

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use anyhow::{bail, Result};
pub use builder::SsTableBuilder;
use bytes::{Buf, BufMut};
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
    ///
    /// ```
    /// metadata encoding:
    /// |   u32    |   bytes    | ... |    bytes   |   u32    |
    /// | meta num | block meta | ... | block meta | checksum |
    ///
    /// block meta:
    /// |     u32      |      u16      |   bytes   |     u16      |   bytes   |
    /// | block offset | first key len | first key | last key len | first key |
    /// ```
    pub fn encode_block_meta(block_meta: &[BlockMeta], buf: &mut Vec<u8>) {
        let original_len = buf.len();
        buf.put_u32(block_meta.len() as u32);
        for meta in block_meta {
            buf.put_u32(meta.offset as u32);
            buf.put_u16(meta.first_key.len() as u16);
            buf.put_slice(meta.first_key.raw_ref());
            buf.put_u16(meta.last_key.len() as u16);
            buf.put_slice(meta.last_key.raw_ref());
        }
        buf.put_u32(crc32fast::hash(&buf[original_len..]));
    }

    /// Decode block meta from a buffer.
    pub fn decode_block_meta(mut buf: &[u8]) -> Result<Vec<BlockMeta>> {
        let checksum = crc32fast::hash(&buf[..buf.len() - 4]);
        let num = buf.get_u32() as usize;
        let mut vec = Vec::with_capacity(num);
        for _ in 0..num {
            let offset = buf.get_u32() as usize;
            let first_key_len = buf.get_u16() as usize;
            let first_key = KeyBytes::from_bytes(buf.copy_to_bytes(first_key_len));

            let last_key_len = buf.get_u16() as usize;
            let last_key = KeyBytes::from_bytes(buf.copy_to_bytes(last_key_len));
            vec.push(BlockMeta {
                offset,
                first_key,
                last_key,
            })
        }

        if buf.get_u32() != checksum {
            bail!("meta checksum mismatched");
        }
        Ok(vec)
    }
}

/// A file object.
pub struct FileObject(Option<File>, u64);

impl FileObject {
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        use std::os::unix::fs::FileExt;
        let mut data = vec![0; len as usize];
        self.0
            .as_ref()
            .unwrap()
            .read_exact_at(&mut data[..], offset)?;
        Ok(data)
    }

    pub fn size(&self) -> u64 {
        self.1
    }

    /// Create a new file object (day 2) and write the file to the disk (day 4).
    pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
        std::fs::write(path, &data)?;
        File::open(path)?.sync_all()?;
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
///
/// The SSTable is structured as follows:
/// ```
/// |  Block Section (bytes)  |  bytes   |     u32     |   bytes    |      u32     |
/// | block 1 | ... | block n | metadata | meta offset | bloom data | bloom offset |
/// ```
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
    /// The maximum timestamp stored in this SST, implemented in week 3.
    max_ts: u64,
}

impl SsTable {
    #[cfg(test)]
    pub(crate) fn open_for_test(file: FileObject) -> Result<Self> {
        Self::open(0, None, file)
    }

    /// Open SSTable from a file.
    pub fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        let len = file.size();
        let raw_bloom_offset = file.read(len - 4, 4)?;
        let bloom_offset = (&raw_bloom_offset[..]).get_u32() as u64;
        let raw_bloom = file.read(bloom_offset, len - 4 - bloom_offset)?;
        let bloom_filter = Bloom::decode(&raw_bloom)?;

        let raw_meta_offset = file.read(bloom_offset - 4, 4)?;
        let meta_offset = (&raw_meta_offset[..]).get_u32() as u64;
        let raw_meta = file.read(meta_offset, bloom_offset - 4 - meta_offset)?;
        let block_meta = BlockMeta::decode_block_meta(&raw_meta)?;

        Ok(Self {
            file,
            first_key: block_meta.first().unwrap().first_key.clone(),
            last_key: block_meta.last().unwrap().last_key.clone(),
            block_meta,
            block_meta_offset: meta_offset as usize,
            id,
            block_cache,
            bloom: Some(bloom_filter),
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
            max_ts: 0,
        }
    }

    /// Read a block from the disk.
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        if block_idx > self.block_meta.len() - 1 {
            bail!("block idx overflow");
        }

        let offset = self.block_meta[block_idx].offset;
        let offset_end = self
            .block_meta
            .get(block_idx + 1)
            .map_or(self.block_meta_offset, |meta| meta.offset);
        let block_len = offset_end - offset - 4;
        let raw_block_checksum = self.file.read(offset as u64, (block_len + 4) as u64)?;
        let checksum = crc32fast::hash(&raw_block_checksum[..block_len]);
        if (&raw_block_checksum[block_len..]).get_u32() != checksum {
            bail!("block checksum mismatched");
        }
        Ok(Arc::new(Block::decode(&raw_block_checksum[..block_len])))
    }

    /// Read a block from disk, with block cache. (Day 4)
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        unimplemented!()
    }

    /// Find the block that may contain `key`.
    /// Note: You may want to make use of the `first_key` stored in `BlockMeta`.
    /// You may also assume the key-value pairs stored in each consecutive block are sorted.
    pub fn find_block_idx(&self, key: KeySlice) -> usize {
        let result = self
            .block_meta
            .binary_search_by_key(&key, |a| a.first_key.as_key_slice())
            .unwrap_or_else(|n| n - 1);
        // self.block_meta.partition_point(pred)

        result
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
