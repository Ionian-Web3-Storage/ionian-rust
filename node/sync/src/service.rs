use crate::SyncNetworkContext;
use network::{
    rpc::GetChunksRequest, rpc::RPCResponseErrorCode, PeerId, PeerRequestId, RequestId,
    ServiceMessage,
};
use shared_types::{ChunkArrayWithProof, ChunkProof};
use std::sync::Arc;
use storage::log_store::Store;
use tokio::sync::mpsc;

#[derive(Debug)]
pub enum SyncMessage {
    GetChunksRequest {
        peer_id: PeerId,
        request_id: PeerRequestId,
        request: GetChunksRequest,
    },

    ChunksResponse {
        peer_id: PeerId,
        request_id: RequestId,
        response: ChunkArrayWithProof,
    },
}

pub struct SyncService {
    /// A receiving channel sent by the message processor thread.
    msg_recv: mpsc::UnboundedReceiver<SyncMessage>,

    /// A network context to contact the network service.
    ctx: SyncNetworkContext,

    /// Log and transaction storage.
    store: Arc<dyn Store>,
}

impl SyncService {
    pub fn spawn(
        executor: task_executor::TaskExecutor,
        network_send: mpsc::UnboundedSender<ServiceMessage>,
        store: Arc<dyn Store>,
    ) -> mpsc::UnboundedSender<SyncMessage> {
        let (sync_send, sync_recv) = mpsc::unbounded_channel::<SyncMessage>();

        let mut sync = SyncService {
            msg_recv: sync_recv,
            ctx: SyncNetworkContext::new(network_send),
            store,
        };

        debug!("Starting sync service");
        executor.spawn(async move { Box::pin(sync.main()).await }, "sync");

        sync_send
    }

    async fn main(&mut self) {
        // process any inbound messages
        loop {
            if let Some(msg) = self.msg_recv.recv().await {
                warn!("Sync received message {:?}", msg);

                match msg {
                    SyncMessage::GetChunksRequest {
                        request_id,
                        peer_id,
                        request,
                    } => {
                        self.on_get_chunks_request(peer_id, request_id, request);
                    }

                    SyncMessage::ChunksResponse {
                        peer_id,
                        request_id,
                        response,
                    } => {
                        self.on_chunks_response(peer_id, request_id, response);
                    }
                }
            }
        }
    }

    fn on_get_chunks_request(
        &mut self,
        peer_id: PeerId,
        request_id: PeerRequestId,
        request: GetChunksRequest,
    ) {
        info!("Received GetChunks request: {:?}", request);

        // TODO(thegaram): can we do this validation in the network layer?
        if request.index_start >= request.index_end {
            let _ = self.ctx.send(ServiceMessage::SendErrorResponse {
                peer_id,
                id: request_id,
                error: RPCResponseErrorCode::InvalidRequest,
                reason: "Invalid chunk indices".to_string(),
            });
        }

        // FIXME(thegaram): use get_chunks_with_proof_by_tx_and_index_range
        // FIXME(thegaram): unload IO to worker
        let result = self
            .store
            .get_chunks_by_tx_and_index_range(
                request.tx_seq,
                request.index_start as usize,
                request.index_end as usize,
            )
            .map(|maybe_chunks| {
                maybe_chunks.map(|chunks| ChunkArrayWithProof {
                    chunks,
                    start_proof: ChunkProof::new_empty(),
                    end_proof: ChunkProof::new_empty(),
                })
            });

        match result {
            Ok(Some(chunks)) => {
                let _ = self.ctx.send(ServiceMessage::SendResponse {
                    peer_id,
                    id: request_id,
                    response: network::Response::Chunks(chunks),
                });
            }
            Ok(None) => {
                // FIXME(thegaram): will this happen if the index is out-of-range,
                // and/or if the data is only partially available on this node?
                let _ = self.ctx.send(ServiceMessage::SendErrorResponse {
                    peer_id,
                    id: request_id,
                    error: RPCResponseErrorCode::InvalidRequest,
                    reason: "Chunk does not exist".to_string(),
                });
            }
            Err(e) => {
                let _ = self.ctx.send(ServiceMessage::SendErrorResponse {
                    peer_id,
                    id: request_id,
                    error: RPCResponseErrorCode::ServerError,
                    // FIXME(thegaram): it's probably best not to expose
                    // this to peers, but for now it's useful for debugging.
                    reason: format!("db error: {:?}", e),
                });
            }
        }
    }

    fn on_chunks_response(
        &mut self,
        _peer_id: PeerId,
        _request_id: RequestId,
        response: ChunkArrayWithProof,
    ) {
        info!("Received Chunks response: {:?}", response);

        // TODO(thegaram): validate response and proofs

        // FIXME(thegaram): unload IO to worker
        // FIXME(thegaram): find corresponding request
        let result = self.store.put_chunks(123, response.chunks);

        if let Err(e) = result {
            warn!("Unexpected DB error while storing chunks: {:?}", e);
        }
    }
}
