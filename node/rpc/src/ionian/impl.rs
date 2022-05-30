use super::api::RpcServer;
use crate::error;
use crate::types::Status;
use crate::Context;
use jsonrpsee::core::async_trait;
use network::{rpc::StatusMessage, NetworkGlobals};
use shared_types::DataChunk;
use shared_types::{RequestId, ServiceMessage};
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedSender;

pub struct RpcServerImpl {
    pub ctx: Context,
}

#[async_trait]
impl RpcServer for RpcServerImpl {
    #[tracing::instrument(skip(self), err)]
    async fn get_status(&self) -> Result<Status, jsonrpsee::core::Error> {
        info!("ionian_getStatus()");
        Ok(Status {
            connected_peers: self.network_globals()?.connected_peers(),
        })
    }

    #[tracing::instrument(skip(self), err)]
    async fn send_status(&self, data: u64) -> Result<(), jsonrpsee::core::Error> {
        info!("ionian_sendStatus()");

        let peer_ids = self
            .network_globals()?
            .peers
            .read()
            .peer_ids()
            .cloned()
            .collect::<Vec<_>>();

        for peer_id in peer_ids {
            let res = self.network_tx()?.send(ServiceMessage::SendRequest {
                peer_id,
                request: network::Request::Status(StatusMessage { data }),
                request_id: RequestId::Router,
            });

            if let Err(e) = res {
                warn!(%peer_id, "Failed to send status to peer: {:?}", e);
            }
        }

        Ok(())
    }

    #[tracing::instrument(skip(self, chunk_base64), err)]
    async fn upload_chunk(&self, chunk_base64: String) -> Result<(), jsonrpsee::core::Error> {
        info!("ionian_uploadChunk()");

        let raw: Vec<u8> = base64::decode(chunk_base64).map_err(|e| {
            error::invalid_params("chunk", format!("Failed to base64 decode chunk: {:?}", e))
        })?;

        trace!("received chunk: {:?}", raw);
        let chunk = DataChunk::new(raw);

        self.network_tx()?
            .send(ServiceMessage::NewDataChunk { chunk })
            .map_err(|_| error::internal_error(&"Failed to send service message"))
    }
}

impl RpcServerImpl {
    fn network_globals(&self) -> Result<&Arc<NetworkGlobals>, jsonrpsee::core::Error> {
        match &self.ctx.network_globals {
            Some(globals) => Ok(globals),
            None => Err(error::internal_error(
                &"network globals are not initialized.",
            )),
        }
    }

    fn network_tx(&self) -> Result<&UnboundedSender<ServiceMessage>, jsonrpsee::core::Error> {
        match &self.ctx.network_tx {
            Some(network_tx) => Ok(network_tx),
            None => Err(error::internal_error(&"network tx is not initialized.")),
        }
    }
}
