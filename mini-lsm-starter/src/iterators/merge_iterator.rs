use std::cmp::{self};
use std::collections::binary_heap::PeekMut;
use std::collections::BinaryHeap;

use anyhow::{anyhow, Result};

use crate::key::KeySlice;

use super::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.1
            .key()
            .cmp(&other.1.key())
            .then(self.0.cmp(&other.0))
            .reverse()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, prefer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        let mut heap = BinaryHeap::new();
        for (idx, iter) in iters.into_iter().enumerate() {
            if iter.is_valid() {
                heap.push(HeapWrapper(idx, iter));
            }
        }

        let current = heap.pop();
        Self {
            iters: heap,
            current,
        }
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current.as_ref().unwrap().1.key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().1.value()
    }

    fn is_valid(&self) -> bool {
        self.current.is_some()
        // self.current
        //     .as_ref()
        //     .map(|x| x.1.is_valid())
        //     .unwrap_or(false)
    }

    fn next(&mut self) -> Result<()> {
        let mut cur =
            std::mem::take(&mut self.current).ok_or_else(|| anyhow!("invalid merged iter"))?;
        let cur_key = cur.1.key().to_key_vec();
        let cur_key = cur_key.as_key_slice();

        cur.1.next()?;
        if cur.1.is_valid() {
            self.iters.push(cur);
        }

        while let Some(mut inner_iter) = self.iters.peek_mut() {
            debug_assert!(inner_iter.1.key() >= cur_key, "heap invariant violated");

            if inner_iter.1.key() != cur_key {
                break;
            }

            if let a @ Err(_) = inner_iter.1.next() {
                PeekMut::pop(inner_iter);
                return a;
            }
            if !inner_iter.1.is_valid() {
                PeekMut::pop(inner_iter);
            }
        }

        self.current = self.iters.pop();

        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.iters
            .iter()
            .map(|i| i.1.num_active_iterators())
            .sum::<usize>()
            + self
                .current
                .as_ref()
                .map(|i| i.1.num_active_iterators())
                .unwrap_or(1)
    }
}
