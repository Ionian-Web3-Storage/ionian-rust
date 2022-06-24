use super::api::RpcServer;
use crate::{error, Context};
use futures::prelude::*;
use jsonrpsee::core::async_trait;
use sync::SyncMessage;
use task_executor::ShutdownReason;
use tokio::sync::mpsc::UnboundedSender;

pub struct RpcServerImpl {
    pub ctx: Context,
}

#[async_trait]
impl RpcServer for RpcServerImpl {
    #[tracing::instrument(skip(self), err)]
    async fn shutdown(&self) -> Result<(), jsonrpsee::core::Error> {
        info!("admin_shutdown()");

        self.ctx
            .shutdown_sender
            .clone()
            .send(ShutdownReason::Success("Shutdown by admin"))
            .await
            .map_err(|e| error::internal_error(format!("Failed to send shutdown command: {:?}", e)))
    }

    #[tracing::instrument(skip(self), err)]
    async fn start_sync_file(
        &self,
        tx_seq: u64,
        num_chunks: usize,
    ) -> Result<(), jsonrpsee::core::Error> {
        info!("admin_startSyncFile({tx_seq})");

        self.sync_send()?
            .send(SyncMessage::StartSyncFile { tx_seq, num_chunks })
            .map_err(|e| error::internal_error(format!("Failed to send sync command: {:?}", e)))
    }
}

impl RpcServerImpl {
    fn sync_send(&self) -> Result<&UnboundedSender<SyncMessage>, jsonrpsee::core::Error> {
        match &self.ctx.sync_send {
            Some(sync_send) => Ok(sync_send),
            None => Err(error::internal_error("Sync send is not initialized.")),
        }
    }
}
