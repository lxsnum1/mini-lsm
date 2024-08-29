use std::fs::{File, OpenOptions};
use std::hash::Hasher;
use std::io::{BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use bytes::{Buf, BufMut, Bytes};
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;

pub struct Wal {
    file: Arc<Mutex<BufWriter<File>>>,
}

impl Wal {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(
                OpenOptions::new()
                    .create_new(true)
                    .read(true)
                    .write(true)
                    .open(path)
                    .context("failed to create WAL")?,
            ))),
        })
    }

    pub fn recover(path: impl AsRef<Path>, skiplist: &SkipMap<Bytes, Bytes>) -> Result<Self> {
        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(&path)
            .context("failed to recover from WAL")?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        let mut buf = buf.as_slice();
        while buf.has_remaining() {
            let key_len = buf.get_u16_ne() as usize;
            let key = Bytes::copy_from_slice(&buf[..key_len]);
            buf.advance(key_len);

            let value_len = buf.get_u16_ne() as usize;
            let value = Bytes::copy_from_slice(&buf[..value_len]);
            buf.advance(value_len);

            let mut hasher = crc32fast::Hasher::new();
            hasher.write_u16(key_len as u16);
            hasher.write(&key);
            hasher.write_u16(value_len as u16);
            hasher.write(&value);
            if hasher.finalize() != buf.get_u32() {
                bail!("WAL checksum mismatch");
            }

            skiplist.insert(key, value);
        }

        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let mut buf =
            Vec::with_capacity(key.len() + value.len() + size_of::<u16>() * 2 + size_of::<u32>());
        buf.put_u16_ne(key.len() as u16);
        buf.put_slice(key);
        buf.put_u16_ne(value.len() as u16);
        buf.put_slice(value);
        let checksum = crc32fast::hash(&buf);
        buf.put_u32(checksum);

        let mut w = self.file.lock();
        w.write_all(&buf)?;
        Ok(())
    }

    /// Implement this in week 3, day 5.
    pub fn put_batch(&self, _data: &[(&[u8], &[u8])]) -> Result<()> {
        unimplemented!()
    }

    pub fn sync(&self) -> Result<()> {
        let mut file = self.file.lock();
        file.flush()?;
        // file.get_mut().sync_all()?;
        Ok(())
    }
}
