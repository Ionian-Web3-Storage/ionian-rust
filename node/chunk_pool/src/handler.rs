use super::mem_pool::MemoryChunkPool;
use anyhow::Result;
use shared_types::DataRoot;
use std::sync::Arc;
use storage::log_store::Store;
use tokio::sync::mpsc::UnboundedReceiver;

/// Handle the cached file when uploaded completely and verified from blockchain.
/// Generally, the file will be persisted into log store.
pub struct ChunkPoolHandler {
    receiver: UnboundedReceiver<DataRoot>,
    mem_pool: Arc<MemoryChunkPool>,
    log_store: Arc<dyn Store>,
}

impl ChunkPoolHandler {
    pub(crate) fn new(
        receiver: UnboundedReceiver<DataRoot>,
        mem_pool: Arc<MemoryChunkPool>,
        log_store: Arc<dyn Store>,
    ) -> Self {
        ChunkPoolHandler {
            receiver,
            mem_pool,
            log_store,
        }
    }

    pub async fn handle(&mut self) -> Result<bool> {
        let root = match self.receiver.recv().await {
            Some(root) => root,
            None => return Ok(false),
        };

        let file = match self.mem_pool.remove_file(&root).await {
            Some(file) => file,
            None => return Ok(false),
        };

        let mut segments = match file.segments {
            Some(seg) => seg,
            None => return Ok(false),
        };

        // File will not be persisted if any error occurred.
        while let Some(segment) = segments.pop_front() {
            self.log_store.put_chunks(file.tx_seq, segment)?;
        }

        Ok(true)
    }
}
