use crate::Config;
use anyhow::{anyhow, bail, Result};
use async_lock::Mutex;
use hashlink::LinkedHashMap;
use shared_types::{ChunkArray, DataRoot, Transaction, CHUNK_SIZE};
use std::collections::VecDeque;
use std::ops::Add;
use std::time::{Duration, Instant};
use storage_async::Store;
use tokio::sync::mpsc::UnboundedSender;

// TODO(qhz): Suppose that file uploaded in sequence and following scenarios are to be resolved:
// 1) Uploaded not in sequence: costly to determine if all chunks uploaded, so as to finalize tx in store.
// 2) Upload concurrently: by one user or different users.

/// Used to cache chunks in memory pool and persist into db once log entry
/// retrieved from blockchain.
pub struct MemoryCachedFile {
    /// Indicate whether a thread is writing chunks into store.
    writing: bool,
    /// Memory cached segments before log entry retrieved from blockchain.
    pub segments: Option<VecDeque<ChunkArray>>,
    /// Next chunk index that used to cache or write chunks in sequence.
    next_index: usize,
    /// Total number of chunks for the cache file, which is updated from log entry.
    total_chunks: usize,
    /// Transaction seq that used to write chunks into store.
    pub tx_seq: u64,
    /// Used for garbage collection.
    expired_at: Instant,
}

impl MemoryCachedFile {
    fn new(timeout: Duration) -> Self {
        MemoryCachedFile {
            writing: false,
            segments: None,
            next_index: 0,
            total_chunks: 0,
            tx_seq: 0,
            expired_at: Instant::now().add(timeout),
        }
    }
}

impl MemoryCachedFile {
    /// Updates file with transaction once log entry retrieved from blockchain.
    /// So that, write memory cached segments into database.
    fn update_with_tx(&mut self, tx: &Transaction) {
        self.total_chunks = tx.size as usize / CHUNK_SIZE;
        if tx.size as usize % CHUNK_SIZE > 0 {
            self.total_chunks += 1;
        }

        self.tx_seq = tx.seq;
    }
}

struct Inner {
    config: Config,
    expiration_timeout: Duration,
    /// All cached files.
    files: LinkedHashMap<DataRoot, MemoryCachedFile>,
    /// Total number of chunks that cached in the memory pool.
    total_chunks: usize,
    /// Total number of threads that are writing chunks into store.
    total_writings: usize,
}

impl Inner {
    fn new(config: Config) -> Self {
        let expiration_timeout = Duration::from_secs(config.expiration_time_secs);
        Inner {
            config,
            expiration_timeout,
            files: Default::default(),
            total_chunks: 0,
            total_writings: 0,
        }
    }

    fn update_expiration_time(&mut self, root: &DataRoot) {
        if let Some(file) = self.files.to_back(root) {
            file.expired_at = Instant::now().add(self.expiration_timeout);
        }
    }

    fn garbage_collect(&mut self) {
        while let Some((_, file)) = self.files.front() {
            if file.expired_at > Instant::now() {
                return;
            }

            if let Some((r, f)) = self.files.pop_front() {
                self.update_total_chunks_when_remove_file(&f);
                debug!("Garbage collected for file {}", r);
            }
        }
    }

    fn update_total_chunks_when_remove_file(&mut self, file: &MemoryCachedFile) {
        if let Some(ref segments) = file.segments {
            for seg in segments.iter() {
                let seg_chunks = seg.data.len() / CHUNK_SIZE;
                assert!(self.total_chunks >= seg_chunks);
                self.total_chunks -= seg_chunks;
            }
        }
    }

    /// Try to cache the segment into memory pool if log entry not retrieved from blockchain yet.
    /// Otherwise, return segments to write into store asynchronously for different files.
    fn cache_or_write_segment(
        &mut self,
        root: DataRoot,
        segment: Vec<u8>,
        start_index: usize,
        maybe_tx: Option<Transaction>,
    ) -> Result<Option<(u64, VecDeque<ChunkArray>)>> {
        let file = self
            .files
            .entry(root)
            .or_insert_with(|| MemoryCachedFile::new(self.expiration_timeout));

        // Segment already uploaded.
        if start_index < file.next_index {
            return Ok(None);
        }

        // Suppose to upload in sequence.
        if start_index > file.next_index {
            bail!(
                "chunk index not in sequence, expected = {}, actual = {}",
                file.next_index,
                start_index
            );
        }

        // Already in progress
        if file.writing {
            bail!("Uploading already in progress");
        }

        // Update transaction in case that log entry already retrieved from blockchain
        // before any segment uploaded to storage node. In this case, segment will not
        // be cached in memory pool.
        if file.total_chunks == 0 {
            if let Some(tx) = maybe_tx {
                file.update_with_tx(&tx);
            }
        }

        // Prepare segments to write into store when log entry already retrieved.
        if file.total_chunks > 0 {
            // Limits the number of writing threads.
            if self.total_writings >= self.config.max_writings {
                bail!(anyhow!(
                    "too many data writing: {}",
                    self.config.max_writings
                ));
            }

            // Note, do not update the counter of cached chunks in this case.
            self.total_writings += 1;
            file.writing = true;
            file.segments
                .get_or_insert_with(Default::default)
                .push_back(ChunkArray {
                    data: segment,
                    start_index: start_index as u32,
                });
            return Ok(Some((file.tx_seq, file.segments.take().unwrap())));
        }

        // Otherwise, just cache segment in memory

        // Limits the cached chunks in the memory pool.
        let num_chunks = segment.len() / CHUNK_SIZE;
        if self.total_chunks + num_chunks > self.config.max_cached_chunks_all {
            bail!(anyhow!(
                "exceeds the maximum cached chunks of whole pool: {}",
                self.config.max_cached_chunks_all
            ));
        }

        // Cache segment and update the counter for cached chunks.
        self.total_chunks += num_chunks;
        file.next_index += num_chunks;
        file.segments
            .get_or_insert_with(Default::default)
            .push_back(ChunkArray {
                data: segment,
                start_index: start_index as u32,
            });

        Ok(None)
    }

    fn on_write_succeeded(
        &mut self,
        root: &DataRoot,
        cur_seg_chunks: usize,
        cached_segs_chunks: usize,
    ) -> bool {
        let file = match self.files.get_mut(root) {
            Some(f) => f,
            None => return false,
        };

        file.writing = false;
        file.next_index += cur_seg_chunks;

        assert!(self.total_chunks >= cached_segs_chunks);
        self.total_chunks -= cached_segs_chunks;
        assert!(self.total_writings > 0);
        self.total_writings -= 1;

        debug!("Succeeded to write segment, root={}, next_index={}({}), pool_total_chunks={}, total_writings={}",
            root, file.next_index, file.total_chunks, self.total_chunks, self.total_writings);

        // All chunks of file written into store.
        file.total_chunks > 0 && file.next_index >= file.total_chunks
    }

    fn on_write_failed(&mut self, root: &DataRoot, cached_segs_chunks: usize) {
        let file = match self.files.get_mut(root) {
            Some(f) => f,
            None => return,
        };

        file.writing = false;

        assert!(self.total_chunks >= cached_segs_chunks);
        self.total_chunks -= cached_segs_chunks;
        assert!(self.total_writings > 0);
        self.total_writings -= 1;
    }

    /// If log entry retrieved timely, we should update the transaction for the first 2 segments.
    fn requires_to_update_tx_before_cache(&self, root: &DataRoot, start_index: usize) -> bool {
        if start_index == 0 {
            return true;
        }

        // Must be updated by `update_file_info` by log entry poller.
        if start_index > 1 {
            return false;
        }

        let file = match self.files.get(root) {
            Some(f) => f,
            None => return false,
        };

        // Already updated
        if file.total_chunks > 0 {
            return false;
        }

        // Only required for the 2nd segment.
        match &file.segments {
            Some(segs) => segs.len() == 1,
            None => false,
        }
    }
}

/// Caches data chunks in memory before the entire file uploaded to storage node
/// and data root verified on blockchain.
pub struct MemoryChunkPool {
    config: Config,
    inner: Mutex<Inner>,
    log_store: Store,
    sender: UnboundedSender<DataRoot>,
}

impl MemoryChunkPool {
    pub(crate) fn new(config: Config, log_store: Store, sender: UnboundedSender<DataRoot>) -> Self {
        MemoryChunkPool {
            config: config.clone(),
            inner: Mutex::new(Inner::new(config)),
            log_store,
            sender,
        }
    }

    fn validate_segment_size(&self, segment: &Vec<u8>, chunk_start_index: usize) -> Result<usize> {
        if segment.is_empty() {
            bail!(anyhow!("data is empty"));
        }

        if segment.len() % CHUNK_SIZE != 0 {
            bail!(anyhow!("invalid data length"))
        }

        let num_chunks = segment.len() / CHUNK_SIZE;

        // Limits the maximum number of chunks of single file.
        // Note, it suppose that all chunks uploaded in sequence.
        if chunk_start_index + num_chunks > self.config.max_cached_chunks_per_file {
            bail!(anyhow!(
                "exceeds the maximum cached chunks of single file: {}",
                self.config.max_cached_chunks_per_file
            ));
        }

        Ok(num_chunks)
    }

    async fn get_tx_by_root(&self, root: &DataRoot) -> Result<Option<Transaction>> {
        match self.log_store.get_tx_seq_by_data_root(root).await? {
            Some(tx_seq) => self.log_store.get_tx_by_seq_number(tx_seq).await,
            None => Ok(None),
        }
    }

    /// Adds chunks into memory pool if log entry not retrieved from blockchain yet. Otherwise, write
    /// the segment into store directly.
    pub async fn add_chunks(
        &self,
        root: DataRoot,
        segment: Vec<u8>,
        start_index: usize,
    ) -> Result<()> {
        // Lazy GC when new chunks added.
        self.inner.lock().await.garbage_collect();

        self.add_chunks_inner(root, segment, start_index).await?;

        // Update expiration time when succeeded.
        self.inner.lock().await.update_expiration_time(&root);

        Ok(())
    }

    async fn add_chunks_inner(
        &self,
        root: DataRoot,
        segment: Vec<u8>,
        start_index: usize,
    ) -> Result<()> {
        let num_chunks = self.validate_segment_size(&segment, start_index)?;

        // Try to update file with transaction for the first 2 segments,
        // in case that log entry already retrieved from blockchain.
        //
        // Note, the log entry may be retrieved immediately before the mutex acquired to `cache_or_write_segment`.
        // So, the `maybe_tx` below may be `None` for the 1st segment. When the 2nd segment arrived, try again to
        // update the log entry info from db.
        let mut maybe_tx = None;
        if self
            .inner
            .lock()
            .await
            .requires_to_update_tx_before_cache(&root, start_index)
        {
            maybe_tx = self.get_tx_by_root(&root).await?;
        }

        debug!("Begin to cache or write segment, root={}, segment_size={}, start_chunk_index={}, tx={:?}",
            root, segment.len(), start_index, maybe_tx);

        // Cache segment in memory if log entry not retrieved yet, or write into store directly.
        let (tx_seq, mut segments) = match self.inner.lock().await.cache_or_write_segment(
            root,
            segment,
            start_index,
            maybe_tx,
        )? {
            Some(tuple) => tuple,
            None => return Ok(()),
        };

        let mut total_chunks_to_write = 0;
        for seg in segments.iter() {
            total_chunks_to_write += seg.data.len() / CHUNK_SIZE;
        }
        let pending_seg_chunks = total_chunks_to_write - num_chunks;

        // Write memory cached segments into store.
        while let Some(seg) = segments.pop_front() {
            // TODO(qhz): error handling
            // 1. Push the failed segment back to front. (enhance store to return Err(ChunkArray))
            // 2. Put the incompleted segments back to memory pool.
            if let Err(e) = self.log_store.put_chunks(tx_seq, seg).await {
                self.inner
                    .lock()
                    .await
                    .on_write_failed(&root, pending_seg_chunks);
                return Err(e);
            }
        }

        let all_uploaded =
            self.inner
                .lock()
                .await
                .on_write_succeeded(&root, num_chunks, pending_seg_chunks);

        // Notify to finalize transaction asynchronously.
        if all_uploaded {
            if let Err(e) = self.sender.send(root) {
                // Channel receiver will not be dropped until program exit.
                bail!(anyhow!("channel send error: {}", e));
            }
            debug!("Queue to finalize transaction for file {}", root);
        }

        Ok(())
    }

    /// Updates the cached file info when log entry retrieved from blockchain.
    pub async fn update_file_info(&self, tx: &Transaction) -> Result<bool> {
        let mut inner = self.inner.lock().await;

        // Do nothing if file not uploaded yet.
        let file = match inner.files.get_mut(&tx.data_merkle_root) {
            Some(f) => f,
            None => return Ok(false),
        };

        // Update the file info with transaction.
        file.update_with_tx(tx);

        // File partially uploaded and it's up to user thread
        // to write chunks into store and finalize transaction.
        if file.next_index < file.total_chunks {
            return Ok(true);
        }

        // Otherwise, notify to write all memory cached chunks and finalize transaction.
        if let Err(e) = self.sender.send(tx.data_merkle_root) {
            // Channel receiver will not be dropped until program exit.
            bail!(anyhow!("channel send error: {}", e));
        }

        Ok(true)
    }

    pub(crate) async fn remove_file(&self, root: &DataRoot) -> Option<MemoryCachedFile> {
        let mut inner = self.inner.lock().await;

        let file = inner.files.remove(root)?;
        inner.update_total_chunks_when_remove_file(&file);

        Some(file)
    }
}
