use anyhow::{anyhow, bail, Result};
use parking_lot::RwLock;
use shared_types::{DataRoot, CHUNK_SIZE};
use std::collections::HashMap;
use tokio::sync::mpsc::UnboundedSender;

// to avoid OOM
const MAX_CACHED_CHUNKS_PER_FILE: usize = 1024 * 1024; // 256M
const MAX_CACHED_CHUNKS_ALL: usize = 1024 * 1024 * 1024 / CHUNK_SIZE; // 1G

#[derive(Default)]
pub struct MemoryCachedFile {
    pub data: Vec<u8>,   // cached data chunks
    next_index: usize,   // next chunk index to cache
    total_chunks: usize, // total number of chunks for the cached file
}

#[derive(Default)]
struct Inner {
    files: HashMap<DataRoot, MemoryCachedFile>,
    total_chunks: usize, // number of chunks for all files
}

/// Caches data chunks in memory before the entire file uploaded to storage node
/// and data root verified on blockchain.
pub struct MemoryChunkPool {
    inner: RwLock<Inner>,
    sender: UnboundedSender<DataRoot>,
}

impl MemoryChunkPool {
    pub(crate) fn new(sender: UnboundedSender<DataRoot>) -> Self {
        MemoryChunkPool {
            inner: Default::default(),
            sender,
        }
    }

    pub fn add_chunks(&self, root: DataRoot, chunks: Vec<u8>, start_index: usize) -> Result<()> {
        if chunks.is_empty() {
            bail!(anyhow!("data is empty"));
        }

        if chunks.len() % CHUNK_SIZE != 0 {
            bail!(anyhow!("invalid data length"))
        }

        let num_chunks = chunks.len() / CHUNK_SIZE;
        if num_chunks > super::NUM_CHUNKS_PER_SEGMENT {
            bail!(anyhow!(
                "exceeds the maximum cached chunks of single segment: {}",
                super::NUM_CHUNKS_PER_SEGMENT
            ));
        }

        // Limits the maximum number of chunks of single file.
        if start_index + num_chunks > MAX_CACHED_CHUNKS_PER_FILE {
            bail!(anyhow!(
                "exceeds the maximum cached chunks of single file: {}",
                MAX_CACHED_CHUNKS_PER_FILE
            ));
        }

        let mut inner = self.inner.write();

        // Limits the maximum number of chunks of the whole pool.
        if inner.total_chunks + num_chunks > MAX_CACHED_CHUNKS_ALL {
            bail!(anyhow!(
                "exceeds the maximum cached chunks of whole pool: {}",
                MAX_CACHED_CHUNKS_ALL
            ));
        }

        // Suppose to add chunks in sequence.
        if let Some(file) = inner.files.get(&root) {
            if file.next_index != start_index {
                bail!(anyhow!(
                    "chunk index not in sequence, expected = {}, actual = {}",
                    file.next_index,
                    start_index
                ));
            }
        }

        inner.total_chunks += num_chunks;

        let file = inner.files.entry(root).or_default();

        // TODO(qhz): try to update `total_chunks` from db for the 1st chunk,
        // in case that client do not upload chunks to storage node timly.

        file.data.extend_from_slice(&chunks);
        file.next_index += num_chunks;

        self.notify_if_file_ready(root, file)?;

        Ok(())
    }

    // Updates the cached file info when log entry retrieved from blockchain.
    pub fn update_file_info(&self, root: DataRoot, file_size: usize) -> Result<()> {
        if file_size == 0 {
            return Ok(());
        }

        let mut inner = self.inner.write();
        let file = inner.files.entry(root).or_default();

        file.total_chunks = file_size / CHUNK_SIZE;
        if file_size % CHUNK_SIZE > 0 {
            file.total_chunks += 1;
        }

        self.notify_if_file_ready(root, file)?;

        Ok(())
    }

    fn notify_if_file_ready(&self, root: DataRoot, file: &MemoryCachedFile) -> Result<()> {
        // file info not retrieved from blockchain
        if file.total_chunks == 0 {
            return Ok(());
        }

        // file not uploaded completely
        if file.next_index < file.total_chunks {
            return Ok(());
        }

        if let Err(e) = self.sender.send(root) {
            bail!(anyhow!("channel send error: {}", e));
        }

        Ok(())
    }

    pub(crate) fn remove_file(&self, root: &DataRoot) -> Option<MemoryCachedFile> {
        let mut inner = self.inner.write();

        let file = inner.files.remove(root)?;
        inner.total_chunks -= file.total_chunks;

        Some(file)
    }

    // TODO(qhz): expire garbage items periodically or at the beginning of other methods.
    // What a pity, there is no double linked list in standard lib.
}