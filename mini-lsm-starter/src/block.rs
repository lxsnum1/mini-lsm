mod builder;
mod iterator;

use bytes::Bytes;

pub use builder::BlockBuilder;
pub use iterator::BlockIterator;

pub(crate) const SIZE_U16: usize = std::mem::size_of::<u16>();

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the tutorial
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let mut buf =
            Vec::with_capacity(self.data.len() + self.offsets.len() * SIZE_U16 + SIZE_U16);
        buf.extend(&self.data);
        self.offsets.iter().for_each(|a| {
            buf.extend(a.to_ne_bytes());
        });
        buf.extend((self.offsets.len() as u16).to_ne_bytes());
        Bytes::from(buf)
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        assert!(data.len() > (2 + 1 + 1 + 1) * SIZE_U16, "illegal block");

        let l = data.len();
        let n = u16::from_ne_bytes([data[l - 2], data[l - 1]]) as usize;
        let offsets_start = l - n * SIZE_U16 - SIZE_U16;
        let offsets = data[offsets_start..(l - SIZE_U16)]
            .chunks_exact(SIZE_U16)
            .map(|a| u16::from_ne_bytes([a[0], a[1]]))
            .collect();

        Self {
            data: data[..offsets_start].to_vec(),
            offsets,
        }
    }
}
