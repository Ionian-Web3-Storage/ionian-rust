use anyhow::{anyhow, bail, Result};
use async_lock::Mutex;
use shared_types::{ChunkArray, DataRoot, Transaction, CHUNK_SIZE};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use storage::log_store::Store;
use tokio::sync::mpsc::UnboundedSender;

// to avoid OOM
const MAX_CACHED_CHUNKS_PER_FILE: usize = 1024 * 1024; // 256M
const MAX_CACHED_CHUNKS_ALL: usize = 1024 * 1024 * 1024 / CHUNK_SIZE; // 1G

/// Used to cache chunks in memory pool and persist into db once log entry
/// retrieved from blockchain.
#[derive(Default)]
pub struct MemoryCachedFile {
    /// Indicate whether a thread is writing chunks into store.
    writing: bool,
    /// Memory cached segments before log entry retrieved from blockchain.
    pub segments: Option<VecDeque<ChunkArray>>,
    /// Next chunk index that used to cache or write chunks in sequence.
    next_index: usize,
    /// Total number of chunks for the cache file, which is updated from log entry.
    total_chunks: Option<usize>,
    /// Transaction seq that used to write chunks into store.
    pub tx_seq: u64,
}

impl MemoryCachedFile {
    /// Updates file with transaction once log entry retrieved from blockchain.
    /// So that, write memory cached segments into database.
    fn update_with_tx(&mut self, tx: &Transaction) {
        let mut total_chunks = tx.size as usize / CHUNK_SIZE;
        if tx.size as usize % CHUNK_SIZE > 0 {
            total_chunks += 1;
        }

        self.total_chunks = Some(total_chunks);
        self.tx_seq = tx.seq;
    }
}

#[derive(Default)]
struct Inner {
    /// All cached files.
    files: HashMap<DataRoot, MemoryCachedFile>,
    /// Total number of chunks that cached in the memory pool.
    total_chunks: usize,
}

impl Inner {
    // TODO(qhz): Suppose that file uploaded in sequence and following scenarios are to be resolved:
    // 1) Uploaded not in sequence: costly to determine if all chunks uploaded, so as to finalize tx in store.
    // 2) Upload concurrently: by one user or different users.

    /// Try to cache the segment into memory pool if log entry not retrieved from blockchain yet.
    /// Otherwise, return segments to write into store asynchronously for different files.
    fn cache_or_write_segment(
        &mut self,
        root: DataRoot,
        segment: Vec<u8>,
        start_index: usize,
        maybe_tx: Option<Transaction>,
    ) -> Result<Option<(u64, VecDeque<ChunkArray>)>> {
        let file = self.files.entry(root).or_default();

        // Segment already uploaded.
        if start_index < file.next_index {
            return Ok(None);
        }

        // Suppose to upload in sequence.
        if start_index > file.next_index {
            bail!(anyhow!(
                "chunk index not in sequence, expected = {}, actual = {}",
                file.next_index,
                start_index
            ));
        }

        // Already in progress
        if file.writing {
            bail!(anyhow!("Uploading in progress"));
        }

        // Update transaction in case that log entry already retrieved from blockchain
        // before any segment uploaded to storage node.
        if let Some(tx) = maybe_tx {
            file.update_with_tx(&tx);
        }

        // Prepare segments to write into store when log entry already retrieved.
        if file.total_chunks.is_some() {
            // Note, do not update the counter of cached chunks in this case.
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
        if self.total_chunks + num_chunks > MAX_CACHED_CHUNKS_ALL {
            bail!(anyhow!(
                "exceeds the maximum cached chunks of whole pool: {}",
                MAX_CACHED_CHUNKS_ALL
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
    ) {
        let file = match self.files.get_mut(root) {
            Some(f) => f,
            None => return,
        };

        file.writing = false;
        file.next_index += cur_seg_chunks;
        self.total_chunks -= cached_segs_chunks;
    }

    fn on_write_failed(&mut self, root: &DataRoot, cached_segs_chunks: usize) {
        let file = match self.files.get_mut(root) {
            Some(f) => f,
            None => return,
        };

        file.writing = false;
        self.total_chunks -= cached_segs_chunks;
    }
}

/// Caches data chunks in memory before the entire file uploaded to storage node
/// and data root verified on blockchain.
pub struct MemoryChunkPool {
    inner: Mutex<Inner>,
    log_store: Arc<dyn Store>,
    sender: UnboundedSender<DataRoot>,
}

impl MemoryChunkPool {
    pub(crate) fn new(log_store: Arc<dyn Store>, sender: UnboundedSender<DataRoot>) -> Self {
        MemoryChunkPool {
            inner: Default::default(),
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

        // Limits the maximum number of chunks of single segment.
        if num_chunks > super::NUM_CHUNKS_PER_SEGMENT {
            bail!(anyhow!(
                "exceeds the maximum cached chunks of single segment: {}",
                super::NUM_CHUNKS_PER_SEGMENT
            ));
        }

        // Limits the maximum number of chunks of single file.
        // Note, it suppose that all chunks uploaded in sequence.
        if chunk_start_index + num_chunks > MAX_CACHED_CHUNKS_PER_FILE {
            bail!(anyhow!(
                "exceeds the maximum cached chunks of single file: {}",
                MAX_CACHED_CHUNKS_PER_FILE
            ));
        }

        Ok(num_chunks)
    }

    async fn get_tx_by_root(&self, root: &DataRoot) -> Result<Option<Transaction>> {
        match self.log_store.get_tx_seq_by_data_root(root)? {
            Some(tx_seq) => self.log_store.get_tx_by_seq_number(tx_seq),
            None => return Ok(None),
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
        let num_chunks = self.validate_segment_size(&segment, start_index)?;

        // Try to update file with transaction for the first segment,
        // in case that log entry already retrieved from blockchain.
        let mut maybe_tx = None;
        if start_index == 0 {
            maybe_tx = self.get_tx_by_root(&root).await?;
        }

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
            total_chunks_to_write += seg.data.len();
        }
        let pending_seg_chunks = total_chunks_to_write - num_chunks;

        // Write memory cached segments into store.
        while let Some(seg) = segments.pop_front() {
            // TODO(qhz): error handling
            // 1. Push the failed segment back to front. (enhance store to return Err(ChunkArray))
            // 2. Put the incompleted segments back to memory pool.
            if let Err(e) = self.log_store.put_chunks(tx_seq, seg) {
                self.inner
                    .lock()
                    .await
                    .on_write_failed(&root, pending_seg_chunks);
                return Err(e);
            }
        }

        self.inner
            .lock()
            .await
            .on_write_succeeded(&root, num_chunks, pending_seg_chunks);

        Ok(())
    }

    // Updates the cached file info when log entry retrieved from blockchain.
    pub async fn update_file_info(&self, tx: &Transaction) -> Result<bool> {
        let mut inner = self.inner.lock().await;

        let file = match inner.files.get_mut(&tx.data_merkle_root) {
            Some(f) => f,
            None => return Ok(false),
        };

        file.update_with_tx(&tx);

        // File not uploaded completely
        if file.next_index < file.total_chunks.unwrap() {
            return Ok(true);
        }

        if let Err(e) = self.sender.send(tx.data_merkle_root) {
            // Channel receiver will not be dropped until program exit.
            bail!(anyhow!("channel send error: {}", e));
        }

        Ok(true)
    }

    pub(crate) async fn remove_file(&self, root: &DataRoot) -> Option<MemoryCachedFile> {
        let mut inner = self.inner.lock().await;

        let file = inner.files.remove(root)?;
        if let Some(total_chunks) = file.total_chunks {
            inner.total_chunks -= total_chunks;
        }

        Some(file)
    }

    // TODO(qhz): expire garbage items periodically or at the beginning of other methods.
    // What a pity, there is no double linked list in standard lib.
}
