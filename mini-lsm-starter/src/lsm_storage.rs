use std::collections::{BTreeSet, HashMap};
use std::fs::File;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use anyhow::{Context, Result};
use bytes::Bytes;
use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::block::Block;
use crate::compact::{
    CompactionController, CompactionOptions, LeveledCompactionController, LeveledCompactionOptions,
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, TieredCompactionController,
};
use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::{KeyBytes, KeySlice};
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::manifest::{Manifest, ManifestRecord};
use crate::mem_table::{self, map_bound, MemTable};
use crate::mvcc::LsmMvccInner;
use crate::table::{FileObject, SsTable, SsTableBuilder, SsTableIterator};

// key: (sst_id, block_id)
pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

/// Represents the state of the storage engine.
#[derive(Clone)]
pub struct LsmStorageState {
    /// The current memtable.
    pub memtable: Arc<MemTable>,
    /// Immutable memtables, from latest to earliest.
    pub imm_memtables: Vec<Arc<MemTable>>,
    /// L0 SSTs, from latest to earliest.
    pub l0_sstables: Vec<usize>,
    /// SsTables sorted by key range; L1 - L_max for leveled compaction, or tiers for tiered
    /// compaction.
    pub levels: Vec<(usize, Vec<usize>)>,
    /// SST objects.
    pub sstables: HashMap<usize, Arc<SsTable>>,
}

pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
}

impl LsmStorageState {
    fn create(options: &LsmStorageOptions) -> Self {
        let levels = match &options.compaction_options {
            CompactionOptions::Leveled(LeveledCompactionOptions { max_levels, .. })
            | CompactionOptions::Simple(SimpleLeveledCompactionOptions { max_levels, .. }) => (1
                ..=*max_levels)
                .map(|level| (level, Vec::new()))
                .collect::<Vec<_>>(),
            CompactionOptions::Tiered(_) => Vec::new(),
            CompactionOptions::NoCompaction => vec![(1, Vec::new())],
        };
        Self {
            memtable: Arc::new(MemTable::create(0)),
            imm_memtables: Vec::new(),
            l0_sstables: Vec::new(),
            levels,
            sstables: Default::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LsmStorageOptions {
    // Block size in bytes
    pub block_size: usize,
    // SST size in bytes, also the approximate memtable capacity limit
    pub target_sst_size: usize,
    // Maximum number of memtables in memory, flush to L0 when exceeding this limit
    pub num_memtable_limit: usize,
    pub compaction_options: CompactionOptions,
    pub enable_wal: bool,
    pub serializable: bool,
}

impl LsmStorageOptions {
    pub fn default_for_week1_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 50,
            serializable: false,
        }
    }

    pub fn default_for_week1_day6_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }

    pub fn default_for_week2_test(compaction_options: CompactionOptions) -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 1 << 20, // 1MB
            compaction_options,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }
}

#[derive(Clone, Debug)]
pub enum CompactionFilter {
    Prefix(Bytes),
}

/// The storage interface of the LSM tree.
pub(crate) struct LsmStorageInner {
    pub(crate) state: Arc<RwLock<Arc<LsmStorageState>>>,
    pub(crate) state_lock: Mutex<()>,
    path: PathBuf,
    pub(crate) block_cache: Arc<BlockCache>,
    next_sst_id: AtomicUsize,
    pub(crate) options: Arc<LsmStorageOptions>,
    pub(crate) compaction_controller: CompactionController,
    pub(crate) manifest: Option<Manifest>,
    pub(crate) mvcc: Option<LsmMvccInner>,
    pub(crate) compaction_filters: Arc<Mutex<Vec<CompactionFilter>>>,
}

/// A thin wrapper for `LsmStorageInner` and the user interface for MiniLSM.
pub struct MiniLsm {
    pub(crate) inner: Arc<LsmStorageInner>,
    /// Notifies the L0 flush thread to stop working. (In week 1 day 6)
    flush_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the flush thread. (In week 1 day 6)
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Notifies the compaction thread to stop working. (In week 2)
    compaction_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread. (In week 2)
    compaction_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Drop for MiniLsm {
    fn drop(&mut self) {
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();
    }
}

impl MiniLsm {
    pub fn close(&self) -> Result<()> {
        self.inner.sync_dir()?;
        self.compaction_notifier.send(())?;
        self.flush_notifier.send(())?;

        let mut compaction_thread = self.compaction_thread.lock();
        if let Some(compaction_thread) = compaction_thread.take() {
            compaction_thread
                .join()
                .map_err(|e| anyhow::anyhow!("{:?}", e))?;
        }

        let mut flush_thread = self.flush_thread.lock();
        if let Some(flush_thread) = flush_thread.take() {
            flush_thread
                .join()
                .map_err(|e| anyhow::anyhow!("{:?}", e))?;
        }

        if self.inner.options.enable_wal {
            self.inner.sync()?;
            self.inner.sync_dir()?;
            return Ok(());
        }

        // freeze memtable and skip updating manifest
        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .freeze_memtable_with_memtable(Arc::new(MemTable::create(
                    self.inner.next_sst_id(),
                )))?;
        }

        while {
            let snapshot = self.inner.state.read();
            !snapshot.imm_memtables.is_empty()
        } {
            self.inner.force_flush_next_imm_memtable()?;
        }

        self.inner.sync_dir()
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Arc<Self>> {
        let inner = Arc::new(LsmStorageInner::open(path, options)?);
        let (tx1, rx) = crossbeam_channel::unbounded();
        let compaction_thread = inner.spawn_compaction_thread(rx)?;
        let (tx2, rx) = crossbeam_channel::unbounded();
        let flush_thread = inner.spawn_flush_thread(rx)?;
        Ok(Arc::new(Self {
            inner,
            flush_notifier: tx2,
            flush_thread: Mutex::new(flush_thread),
            compaction_notifier: tx1,
            compaction_thread: Mutex::new(compaction_thread),
        }))
    }

    pub fn new_txn(&self) -> Result<()> {
        self.inner.new_txn()
    }

    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        self.inner.write_batch(batch)
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        self.inner.add_compaction_filter(compaction_filter)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put(key, value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.delete(key)
    }

    pub fn sync(&self) -> Result<()> {
        self.inner.sync()
    }

    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        self.inner.scan(lower, upper)
    }

    /// Only call this in test cases due to race conditions
    pub fn force_flush(&self) -> Result<()> {
        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .force_freeze_memtable(&self.inner.state_lock.lock())?;
        }
        if !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        self.inner.force_full_compaction()
    }
}

impl LsmStorageInner {
    pub(crate) fn next_sst_id(&self) -> usize {
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        let path = path.as_ref();
        if !path.exists() {
            std::fs::create_dir_all(path).context("failed to create DB dir")?;
        }

        let compaction_controller = match &options.compaction_options {
            CompactionOptions::Leveled(options) => {
                CompactionController::Leveled(LeveledCompactionController::new(options.clone()))
            }
            CompactionOptions::Tiered(options) => {
                CompactionController::Tiered(TieredCompactionController::new(options.clone()))
            }
            CompactionOptions::Simple(options) => CompactionController::Simple(
                SimpleLeveledCompactionController::new(options.clone()),
            ),
            CompactionOptions::NoCompaction => CompactionController::NoCompaction,
        };

        let mut state = LsmStorageState::create(&options);
        let block_cache = Arc::new(BlockCache::new(1 << 20));
        let mut next_sst_id = 1;
        let manifest_path = path.join("MAINFEST");
        let mainfest;

        if !manifest_path.exists() {
            let id = state.memtable.id();
            if options.enable_wal {
                state.memtable = Arc::new(mem_table::MemTable::create_with_wal(
                    id,
                    Self::path_of_wal_static(path, id),
                )?);
            }
            mainfest = Manifest::create(manifest_path)?;
            mainfest.add_record_when_init(ManifestRecord::NewMemtable(id))?;
        } else {
            let (m, records) = Manifest::recover(manifest_path)?;
            let mut memtables = BTreeSet::new();
            for record in records {
                match record {
                    ManifestRecord::Flush(sst_id) => {
                        let res = memtables.remove(&sst_id);
                        assert!(res, "memtable not exist?");
                        if compaction_controller.flush_to_l0() {
                            state.l0_sstables.insert(0, sst_id);
                        } else {
                            state.levels.insert(0, (sst_id, vec![sst_id]));
                        }
                        next_sst_id = next_sst_id.max(sst_id);
                    }
                    ManifestRecord::NewMemtable(x) => {
                        memtables.insert(x);
                        next_sst_id = next_sst_id.max(x);
                    }
                    ManifestRecord::Compaction(task, output) => {
                        let (new_state, _) = compaction_controller
                            .apply_compaction_result(&state, &task, &output, true);
                        state = new_state;
                        next_sst_id =
                            next_sst_id.max(output.iter().max().copied().unwrap_or_default());
                    }
                }
            }

            // recover SSTs
            for table_id in state
                .l0_sstables
                .iter()
                .chain(state.levels.iter().flat_map(|(_, ids)| ids))
            {
                let table_id = *table_id;
                let sst = SsTable::open(
                    table_id,
                    Some(block_cache.clone()),
                    FileObject::open(&Self::path_of_sst_static(path, table_id))
                        .with_context(|| format!("failed to open SST: {}", table_id))?,
                )?;
                state.sstables.insert(table_id, Arc::new(sst));
            }

            // Sort SSTs on each level (only for leveled compaction)
            if let CompactionController::Leveled(_) = compaction_controller {
                for (_, ssts) in &mut state.levels {
                    ssts.sort_by(|i, j| {
                        state.sstables[i]
                            .first_key()
                            .cmp(state.sstables[j].first_key())
                    })
                }
            }

            next_sst_id += 1;

            // recover memtables
            if options.enable_wal {
                let mut wal_cnt = 0;
                for id in memtables.iter() {
                    let memtable =
                        MemTable::recover_from_wal(*id, Self::path_of_wal_static(path, *id))?;
                    if !memtable.is_empty() {
                        state.imm_memtables.insert(0, Arc::new(memtable));
                        wal_cnt += 1;
                    }
                }
                println!("{} WALs recovered", wal_cnt);
                state.memtable = Arc::new(MemTable::create_with_wal(
                    next_sst_id,
                    Self::path_of_wal_static(path, next_sst_id),
                )?);
            } else {
                state.memtable = Arc::new(MemTable::create(next_sst_id));
            }
            m.add_record_when_init(ManifestRecord::NewMemtable(state.memtable.id()))?;
            next_sst_id += 1;
            mainfest = m;
        }

        let storage = Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            path: path.to_path_buf(),
            block_cache,
            next_sst_id: AtomicUsize::new(next_sst_id),
            compaction_controller,
            manifest: Some(mainfest),
            options: options.into(),
            mvcc: None,
            compaction_filters: Arc::new(Mutex::new(Vec::new())),
        };

        Ok(storage)
    }

    pub fn sync(&self) -> Result<()> {
        self.state.read().memtable.sync_wal()
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        let mut compaction_filters = self.compaction_filters.lock();
        compaction_filters.push(compaction_filter);
    }

    /// Get a key from the storage.
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        assert!(!key.is_empty(), "key should not be empty");
        let snapshot = {
            let state_gurad = self.state.read();
            Arc::clone(&state_gurad)
        };

        if let Some(v) = snapshot.memtable.get(key) {
            return if v.is_empty() { Ok(None) } else { Ok(Some(v)) };
        }

        for ele in snapshot.imm_memtables.iter() {
            if let Some(v) = ele.get(key) {
                return if v.is_empty() { Ok(None) } else { Ok(Some(v)) };
            }
        }

        let mut l0_iters = Vec::with_capacity(snapshot.l0_sstables.len());
        for table_id in snapshot.l0_sstables.iter() {
            let table = snapshot.sstables[table_id].clone();
            if keep_table(key, &table) {
                l0_iters.push(Box::new(SsTableIterator::create_and_seek_to_key(
                    table,
                    KeySlice::from_slice(key),
                )?));
            }
        }
        let merged_l0_iter = MergeIterator::create(l0_iters);

        let mut level_iters = Vec::with_capacity(snapshot.levels.len());
        for (_, level_sst_ids) in &snapshot.levels {
            let mut level_ssts = Vec::with_capacity(level_sst_ids.len());
            for id in level_sst_ids {
                let table = snapshot.sstables[id].clone();
                if keep_table(key, &table) {
                    level_ssts.push(table);
                }
            }
            let level_iter =
                SstConcatIterator::create_and_seek_to_key(level_ssts, KeySlice::from_slice(key))?;
            level_iters.push(Box::new(level_iter));
        }
        let merged_level_iter = MergeIterator::create(level_iters);

        let iter = TwoMergeIterator::create(merged_l0_iter, merged_level_iter)?;
        if iter.is_valid() && iter.key().raw_ref() == key && !iter.value().is_empty() {
            return Ok(Some(Bytes::copy_from_slice(iter.value())));
        }
        Ok(None)
    }

    /// Write a batch of data into the storage. Implement in week 2 day 7.
    pub fn write_batch<T: AsRef<[u8]>>(&self, _batch: &[WriteBatchRecord<T>]) -> Result<()> {
        unimplemented!()
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        assert!(!key.is_empty(), "key should not be empty");
        let size = {
            let state_guard = self.state.read();
            state_guard.memtable.put(key, value)?;
            state_guard.memtable.approximate_size()
        };

        self.try_freeze_memtable(size)
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.put(key, b"")
    }

    pub(crate) fn path_of_sst_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.sst", id))
    }

    pub(crate) fn path_of_sst(&self, id: usize) -> PathBuf {
        Self::path_of_sst_static(&self.path, id)
    }

    pub(crate) fn path_of_wal_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.wal", id))
    }

    pub(crate) fn path_of_wal(&self, id: usize) -> PathBuf {
        Self::path_of_wal_static(&self.path, id)
    }

    pub(super) fn sync_dir(&self) -> Result<()> {
        File::open(&self.path)?.sync_all()?;
        Ok(())
    }

    fn freeze_memtable_with_memtable(&self, memtable: Arc<MemTable>) -> Result<()> {
        let mut state_guard = self.state.write();
        let mut snapshot = state_guard.as_ref().clone();
        let old_memtable = std::mem::replace(&mut snapshot.memtable, memtable);
        snapshot.imm_memtables.insert(0, old_memtable.clone());
        *state_guard = Arc::new(snapshot);
        drop(state_guard);

        old_memtable.sync_wal()
    }

    /// Force freeze the current memtable to an immutable memtable
    pub fn force_freeze_memtable(&self, state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        let memtable_id = self.next_sst_id();
        let new_memtable = if self.options.enable_wal {
            Arc::new(MemTable::create_with_wal(
                memtable_id,
                self.path_of_wal(memtable_id),
            )?)
        } else {
            Arc::new(MemTable::create(memtable_id))
        };

        self.freeze_memtable_with_memtable(new_memtable)?;
        self.manifest.as_ref().unwrap().add_record(
            state_lock_observer,
            ManifestRecord::NewMemtable(memtable_id),
        )?;
        self.sync_dir()
    }

    fn try_freeze_memtable(&self, approximate_size: usize) -> Result<()> {
        if approximate_size >= self.options.target_sst_size {
            let state_lock = self.state_lock.lock();

            let approximate_size = self.state.read().memtable.approximate_size();
            if approximate_size >= self.options.target_sst_size {
                return self.force_freeze_memtable(&state_lock);
            }
        }

        Ok(())
    }

    /// Force flush the earliest-created immutable memtable to disk
    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        let state_lock = self.state_lock.lock();

        let flush_table = self
            .state
            .read()
            .imm_memtables
            .last()
            .expect("empty imm memtables")
            .clone();
        let mut builder = SsTableBuilder::new(self.options.block_size);
        flush_table.flush(&mut builder)?;
        let sst_id = flush_table.id();
        let sst = builder.build(
            sst_id,
            Some(self.block_cache.clone()),
            self.path_of_sst(sst_id),
        )?;

        {
            let mut state_guard = self.state.write();
            let mut storage_state = state_guard.as_ref().clone();
            let mem = storage_state.imm_memtables.pop().unwrap();
            assert_eq!(mem.id(), sst_id);

            if self.compaction_controller.flush_to_l0() {
                storage_state.l0_sstables.insert(0, sst_id);
            } else {
                storage_state.levels.insert(0, (sst_id, vec![sst_id]));
            }
            println!("flushed {}.sst with size={}", sst_id, sst.table_size());

            storage_state.sstables.insert(sst_id, Arc::new(sst));
            *state_guard = Arc::new(storage_state);
        }

        if self.options.enable_wal {
            std::fs::remove_file(self.path_of_wal(sst_id))?;
        }

        self.manifest
            .as_ref()
            .unwrap()
            .add_record(&state_lock, ManifestRecord::Flush(sst_id))?;

        self.sync_dir()
    }

    pub fn new_txn(&self) -> Result<()> {
        // no-op
        Ok(())
    }

    /// Create an iterator over a range of keys.
    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        let snapshot = self.state.read().clone();
        let mut memtable_iters = Vec::with_capacity(1 + snapshot.imm_memtables.len());
        memtable_iters.push(Box::new(snapshot.memtable.scan(lower, upper)));
        for mem_table in snapshot.imm_memtables.iter() {
            memtable_iters.push(Box::new(mem_table.scan(lower, upper)));
        }
        let merged_mem_iter = MergeIterator::create(memtable_iters);

        let mut l0_iters = Vec::with_capacity(snapshot.l0_sstables.len());
        for table_id in snapshot.l0_sstables.iter() {
            let sst = snapshot.sstables[table_id].clone();
            if !range_overlap(lower, upper, sst.first_key(), sst.last_key()) {
                continue;
            }

            let iter = match lower {
                Bound::Included(key) => {
                    SsTableIterator::create_and_seek_to_key(sst, KeySlice::from_slice(key))?
                }
                Bound::Excluded(key) => {
                    let mut iter =
                        SsTableIterator::create_and_seek_to_key(sst, KeySlice::from_slice(key))?;
                    if iter.is_valid() && iter.key().raw_ref() == key {
                        iter.next()?;
                    }
                    iter
                }
                Bound::Unbounded => SsTableIterator::create_and_seek_to_first(sst)?,
            };
            l0_iters.push(Box::new(iter));
        }
        let merged_l0_iter = MergeIterator::create(l0_iters);

        let mut level_iters = Vec::with_capacity(snapshot.levels.len());
        for (_, level_sst_ids) in &snapshot.levels {
            let mut level_ssts = Vec::with_capacity(level_sst_ids.len());
            for id in level_sst_ids {
                let sst = snapshot.sstables[id].clone();
                if range_overlap(lower, upper, sst.first_key(), sst.last_key()) {
                    level_ssts.push(sst);
                }
            }

            let level_iter = match lower {
                Bound::Included(key) => SstConcatIterator::create_and_seek_to_key(
                    level_ssts,
                    KeySlice::from_slice(key),
                )?,
                Bound::Excluded(key) => {
                    let mut iter = SstConcatIterator::create_and_seek_to_key(
                        level_ssts,
                        KeySlice::from_slice(key),
                    )?;
                    if iter.is_valid() && iter.key().raw_ref() == key {
                        iter.next()?;
                    }
                    iter
                }
                Bound::Unbounded => SstConcatIterator::create_and_seek_to_first(level_ssts)?,
            };
            level_iters.push(Box::new(level_iter));
        }
        let merged_level_iter = MergeIterator::create(level_iters);

        Ok(FusedIterator::new(LsmIterator::new(
            TwoMergeIterator::create(
                TwoMergeIterator::create(merged_mem_iter, merged_l0_iter)?,
                merged_level_iter,
            )?,
            map_bound(upper),
        )?))
    }
}

fn range_overlap(
    lower: Bound<&[u8]>,
    upper: Bound<&[u8]>,
    table_start: &KeyBytes,
    table_end: &KeyBytes,
) -> bool {
    match lower {
        Bound::Included(start) if start > table_end.raw_ref() => {
            return false;
        }
        Bound::Excluded(start) if start >= table_end.raw_ref() => {
            return false;
        }
        _ => {}
    }

    match upper {
        Bound::Included(end) if end < table_start.raw_ref() => {
            return false;
        }
        Bound::Excluded(end) if end <= table_start.raw_ref() => {
            return false;
        }
        _ => {}
    }

    true
}

fn keep_table(key: &[u8], table: &SsTable) -> bool {
    key >= table.first_key().raw_ref()
        && key <= table.last_key().raw_ref()
        && table
            .bloom
            .as_ref()
            .map_or(false, |b| b.may_contain(farmhash::fingerprint32(key)))
}
