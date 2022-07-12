use super::api::RpcServer;
use crate::error;
use crate::types::{FileInfo, RpcResult, Segment, Status};
use crate::Context;
use chunk_pool::MemoryChunkPool;
use jsonrpsee::core::async_trait;
use network::NetworkGlobals;
use network::NetworkMessage;
use shared_types::DataRoot;
use std::sync::Arc;
use storage::log_store::Store;
use storage::try_option;
use tokio::sync::mpsc::UnboundedSender;

pub struct RpcServerImpl {
    pub ctx: Context,
}

#[async_trait]
impl RpcServer for RpcServerImpl {
    #[tracing::instrument(skip(self), err)]
    async fn get_status(&self) -> RpcResult<Status> {
        info!("ionian_getStatus()");

        Ok(Status {
            connected_peers: self.network_globals()?.connected_peers(),
        })
    }

    #[tracing::instrument(skip(self), err)]
    async fn upload_segment(
        &self,
        data_root: DataRoot,
        data_segment: Segment,
        start_index: u32,
        _proof: Option<Vec<u8>>,
    ) -> RpcResult<()> {
        debug!("ionian_uploadSegment()");

        // Transaction already finalized for the specified file data root.
        let log_store = self.log_store()?;
        if let Some(tx_seq) = log_store.get_tx_seq_by_data_root(&data_root)? {
            if log_store.check_tx_completed(tx_seq)? {
                return Err(error::invalid_params(
                    "data_root",
                    "already uploaded and finalized",
                ));
            }
        }

        // TODO(qhz): unmarshal and validate proof

        // Chunk pool will validate the data size.
        self.chunk_pool()?
            .add_chunks(data_root, data_segment.0, start_index as usize)
            .await?;

        Ok(())
    }

    #[tracing::instrument(skip(self), err)]
    async fn download_segment(
        &self,
        data_root: DataRoot,
        start_index: u32,
        end_index: u32,
    ) -> RpcResult<Option<Segment>> {
        debug!("ionian_downloadSegment()");

        if start_index >= end_index {
            return Err(error::invalid_params("end_index", "invalid chunk index"));
        }

        if end_index - start_index > chunk_pool::NUM_CHUNKS_PER_SEGMENT as u32 {
            return Err(error::invalid_params(
                "end_index",
                format!(
                    "exceeds maximum chunks {}",
                    chunk_pool::NUM_CHUNKS_PER_SEGMENT
                ),
            ));
        }

        let log_store = self.log_store()?;
        let tx_seq = try_option!(log_store.get_tx_seq_by_data_root(&data_root)?);
        let segment = try_option!(log_store.get_chunks_by_tx_and_index_range(
            tx_seq,
            start_index as usize,
            end_index as usize
        )?);

        Ok(Some(Segment(segment.data)))
    }

    #[tracing::instrument(skip(self), err)]
    async fn get_file_info(&self, data_root: DataRoot) -> RpcResult<Option<FileInfo>> {
        debug!("get_file_info()");

        let log_store = self.log_store()?;
        let tx_seq = try_option!(log_store.get_tx_seq_by_data_root(&data_root)?);
        let tx = try_option!(log_store.get_tx_by_seq_number(tx_seq)?);

        Ok(Some(FileInfo {
            tx,
            finalized: log_store.check_tx_completed(tx_seq)?,
        }))
    }
}

impl RpcServerImpl {
    fn network_globals(&self) -> Result<&Arc<NetworkGlobals>, jsonrpsee::core::Error> {
        match &self.ctx.network_globals {
            Some(globals) => Ok(globals),
            None => Err(error::internal_error(
                "Network globals are not initialized.",
            )),
        }
    }

    #[allow(dead_code)]
    fn network_send(&self) -> Result<&UnboundedSender<NetworkMessage>, jsonrpsee::core::Error> {
        match &self.ctx.network_send {
            Some(network_send) => Ok(network_send),
            None => Err(error::internal_error("Network send is not initialized.")),
        }
    }

    fn chunk_pool(&self) -> Result<&Arc<MemoryChunkPool>, jsonrpsee::core::Error> {
        match &self.ctx.chunk_pool {
            Some(pool) => Ok(pool),
            None => Err(error::internal_error("chunk pool is not initialized")),
        }
    }

    fn log_store(&self) -> Result<&Arc<dyn Store>, jsonrpsee::core::Error> {
        match &self.ctx.log_store {
            Some(store) => Ok(store),
            None => Err(error::internal_error("log store is not initialized")),
        }
    }
}
