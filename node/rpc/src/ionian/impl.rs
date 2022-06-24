use super::api::RpcServer;
use crate::error;
use crate::types::Status;
use crate::Context;
use jsonrpsee::core::async_trait;
use network::{rpc::StatusMessage, NetworkGlobals};
use network::{NetworkMessage, RequestId};
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
            let command = NetworkMessage::SendRequest {
                peer_id,
                request: network::Request::Status(StatusMessage { data }),
                request_id: RequestId::Router,
            };

            self.network_send()?.send(command).map_err(|e| {
                error::internal_error(format!("Failed to send shutdown command: {:?}", e))
            })?;
        }

        Ok(())
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

    fn network_send(&self) -> Result<&UnboundedSender<NetworkMessage>, jsonrpsee::core::Error> {
        match &self.ctx.network_send {
            Some(network_send) => Ok(network_send),
            None => Err(error::internal_error("Network send is not initialized.")),
        }
    }
}
