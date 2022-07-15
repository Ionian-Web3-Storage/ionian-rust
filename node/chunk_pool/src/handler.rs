use super::mem_pool::MemoryChunkPool;
use anyhow::Result;
use network::NetworkMessage;
use shared_types::DataRoot;
use std::sync::Arc;
use storage_async::Store;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

/// Handle the cached file when uploaded completely and verified from blockchain.
/// Generally, the file will be persisted into log store.
pub struct ChunkPoolHandler {
    receiver: UnboundedReceiver<DataRoot>,
    mem_pool: Arc<MemoryChunkPool>,
    log_store: Store,
    sender: UnboundedSender<NetworkMessage>,
}

impl ChunkPoolHandler {
    pub(crate) fn new(
        receiver: UnboundedReceiver<DataRoot>,
        mem_pool: Arc<MemoryChunkPool>,
        log_store: Store,
        sender: UnboundedSender<NetworkMessage>,
    ) -> Self {
        ChunkPoolHandler {
            receiver,
            mem_pool,
            log_store,
            sender,
        }
    }

    /// Writes memory cached chunks into store and finalize transaction.
    /// Note, a separate thread should be spawned to call this method.
    pub async fn handle(&mut self) -> Result<bool> {
        let root = match self.receiver.recv().await {
            Some(root) => root,
            None => return Ok(false),
        };

        debug!("Received task to finalize transaction for file {}", root);

        // TODO(qhz): remove from memory pool after transaction finalized,
        // when store support to write chunks with reference.
        let file = match self.mem_pool.remove_file(&root).await {
            Some(file) => file,
            None => return Ok(false),
        };

        if let Some(mut segments) = file.segments {
            // When failed to write chunks or finalize transaction in rare case,
            // client need to upload the whole file again.
            while let Some(segment) = segments.pop_front() {
                self.log_store.put_chunks(file.tx_seq, segment).await?;
            }
        }

        self.log_store.finalize_tx(file.tx_seq).await?;

        debug!("Transaction finalized for seq {}", file.tx_seq);

        let msg = NetworkMessage::AnnounceLocalFile {
            tx_seq: file.tx_seq,
        };
        if let Err(e) = self.sender.send(msg) {
            error!(
                "Failed to send NetworkMessage::AnnounceLocalFile message, tx_seq={}, err={}",
                file.tx_seq, e
            );
        }

        Ok(true)
    }

    pub async fn run(mut self) {
        info!("Worker started to finalize transactions");

        loop {
            if let Err(e) = self.handle().await {
                warn!("Failed to write chunks or finalize transaction, {:?}", e);
            }
        }
    }
}
