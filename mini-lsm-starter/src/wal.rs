use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use bytes::{Buf, BufMut, Bytes};
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;

use crate::key::{KeyBytes, KeySlice};

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

    pub fn recover(path: impl AsRef<Path>, skiplist: &SkipMap<KeyBytes, Bytes>) -> Result<Self> {
        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(path)
            .context("failed to recover from WAL")?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        let mut buf = buf.as_slice();

        while buf.has_remaining() {
            let batch_size = buf.get_u32_ne() as usize;
            if buf.remaining() < batch_size {
                bail!("incomplete WAL");
            }

            let mut batch_buf = &buf[..batch_size];
            let mut kv_pairs = Vec::new();
            let checksum = crc32fast::hash(&batch_buf[..(batch_buf.len() - size_of::<u32>())]);
            while batch_buf.remaining() > size_of::<u32>() {
                let key_len = batch_buf.get_u16() as usize;
                let key = Bytes::copy_from_slice(&batch_buf[..key_len]);
                batch_buf.advance(key_len);
                let ts = batch_buf.get_u64();
                let value_len = batch_buf.get_u16() as usize;
                let value = Bytes::copy_from_slice(&batch_buf[..value_len]);
                batch_buf.advance(value_len);
                kv_pairs.push((key, ts, value));
            }
            if batch_buf.get_u32() != checksum {
                bail!("WAL checksum mismatch");
            }
            buf.advance(batch_size);

            for (key, ts, value) in kv_pairs {
                skiplist.insert(KeyBytes::from_bytes_with_ts(key, ts), value);
            }
        }

        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    pub fn put(&self, key: KeySlice, value: &[u8]) -> Result<()> {
        self.put_batch(&[(key, value)])
    }

    /// Implement this in week 3, day 5.
    pub fn put_batch(&self, data: &[(KeySlice, &[u8])]) -> Result<()> {
        let mut buf = Vec::new();
        for (key, value) in data {
            buf.put_u16(key.key_len() as u16);
            buf.put_slice(key.key_ref());
            buf.put_u64(key.ts());
            buf.put_u16(value.len() as u16);
            buf.put_slice(value);
        }
        buf.put_u32(crc32fast::hash(&buf));

        let mut file = self.file.lock();
        file.write_all(&(buf.len() as u32).to_ne_bytes())?;
        file.write_all(&buf)?;
        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        let mut file = self.file.lock();
        file.flush()?;
        // file.get_mut().sync_all()?;
        Ok(())
    }
}
