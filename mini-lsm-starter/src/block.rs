mod builder;
mod iterator;

use bytes::{Buf, BufMut, Bytes};

pub use builder::BlockBuilder;
pub use iterator::BlockIterator;

use crate::key::KeyVec;

pub(crate) const SIZE_U16: usize = std::mem::size_of::<u16>();

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the tutorial
    ///
    /// Block encoding: `| data | offsets | offsets start pos (u16) |`
    pub fn encode(&self) -> Bytes {
        let mut buf =
            Vec::with_capacity(self.data.len() + self.offsets.len() * SIZE_U16 + SIZE_U16);
        buf.extend(&self.data);
        for offset in &self.offsets {
            buf.put_u16(*offset);
        }
        buf.put_u16(self.offsets.len() as u16);
        Bytes::from(buf)
    }

    fn first_key(&self) -> KeyVec {
        let first_entry_start = *self.offsets.first().unwrap() as usize;
        KeyVec::from_vec(self.data[..first_entry_start].to_vec())
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        assert!(data.len() > (2 + 1 + 1 + 1) * SIZE_U16, "illegal block");

        let l = data.len();
        let entry_num = (&data[l - SIZE_U16..]).get_u16() as usize;
        let offsets_start = l - SIZE_U16 - entry_num * SIZE_U16;
        let offsets = data[offsets_start..(l - SIZE_U16)]
            .chunks_exact(SIZE_U16)
            .map(|mut raw_bytes| raw_bytes.get_u16())
            .collect();

        Self {
            data: data[..offsets_start].to_vec(),
            offsets,
        }
    }
}
