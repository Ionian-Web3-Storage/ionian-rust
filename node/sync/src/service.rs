use crate::controllers::SerialSyncController;
use crate::SyncNetworkContext;
use network::{
    rpc::GetChunksRequest, rpc::RPCResponseErrorCode, NetworkGlobals, NetworkMessage, PeerId,
    PeerRequestId, SyncId as RequestId,
};
use shared_types::{ChunkArrayWithProof, ChunkProof};
use std::{collections::HashMap, sync::Arc};
use storage::log_store::Store;
use tokio::sync::mpsc;

const HEARTBEAT_INTERVAL_SEC: u64 = 5;

#[derive(Debug)]
pub enum SyncMessage {
    AddPeer {
        tx_seq: u64,
        peer_id: PeerId,
    },

    PeerDisconnected {
        peer_id: PeerId,
    },

    StartSyncFile {
        tx_seq: u64,
        num_chunks: usize,
    },

    RequestChunks {
        peer_id: PeerId,
        request_id: PeerRequestId,
        request: GetChunksRequest,
    },

    ChunksResponse {
        peer_id: PeerId,
        request_id: RequestId,
        response: ChunkArrayWithProof,
    },

    RpcError {
        peer_id: PeerId,
        request_id: RequestId,
    },
}

pub struct SyncService {
    /// A receiving channel sent by the message processor thread.
    msg_recv: mpsc::UnboundedReceiver<SyncMessage>,

    /// A network context to contact the network service.
    ctx: Arc<SyncNetworkContext>,

    /// A reference to the network globals and peer-db.
    network_globals: Arc<NetworkGlobals>,

    /// Log and transaction storage.
    store: Arc<dyn Store>,

    /// A collection of file sync controllers.
    controllers: HashMap<u64, SerialSyncController>,

    /// Heartbeat interval for executing periodic tasks.
    heartbeat: tokio::time::Interval,
}

impl SyncService {
    pub fn spawn(
        executor: task_executor::TaskExecutor,
        network_send: mpsc::UnboundedSender<NetworkMessage>,
        network_globals: Arc<NetworkGlobals>,
        store: Arc<dyn Store>,
    ) -> mpsc::UnboundedSender<SyncMessage> {
        let (sync_send, sync_recv) = mpsc::unbounded_channel::<SyncMessage>();

        let heartbeat =
            tokio::time::interval(tokio::time::Duration::from_secs(HEARTBEAT_INTERVAL_SEC));

        let mut sync = SyncService {
            msg_recv: sync_recv,
            ctx: Arc::new(SyncNetworkContext::new(network_send)),
            network_globals,
            store,
            controllers: Default::default(),
            heartbeat,
        };

        debug!("Starting sync service");
        executor.spawn(async move { Box::pin(sync.main()).await }, "sync");

        sync_send
    }

    async fn main(&mut self) {
        loop {
            tokio::select! {
                // received sync message
                Some(msg) = self.msg_recv.recv() => self.on_sync_msg(msg),

                // heartbeat
                _ = self.heartbeat.tick() => self.on_heartbeat(),
            }
        }
    }

    fn on_sync_msg(&mut self, msg: SyncMessage) {
        warn!("Sync received message {:?}", msg);

        match msg {
            SyncMessage::AddPeer { tx_seq, peer_id } => {
                self.on_add_peer(tx_seq, peer_id);
            }

            SyncMessage::PeerDisconnected { peer_id } => {
                self.on_peer_disconnected(peer_id);
            }

            SyncMessage::RequestChunks {
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

            SyncMessage::StartSyncFile { tx_seq, num_chunks } => {
                self.on_start_sync_file(tx_seq, num_chunks);
            }

            SyncMessage::RpcError {
                peer_id,
                request_id,
            } => {
                self.on_rpc_error(peer_id, request_id);
            }
        }
    }

    fn on_add_peer(&mut self, tx_seq: u64, peer_id: PeerId) {
        info!("Adding new peer {peer_id:?} for tx_seq={tx_seq}");

        match self.controllers.get_mut(&tx_seq) {
            Some(controller) => {
                controller.on_peer_connected(peer_id);
                controller.transition();
            }
            None => {
                warn!("Received new peer {peer_id:?} for non-existent controller tx_seq={tx_seq}");
            }
        }
    }

    fn on_peer_disconnected(&mut self, peer_id: PeerId) {
        info!("Peer {peer_id:?} disconnected");

        for controller in self.controllers.values_mut() {
            controller.on_peer_disconnected(&peer_id);
            controller.transition();
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
            self.ctx.send(NetworkMessage::SendErrorResponse {
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
                request.index_start,
                request.index_end,
            )
            .map(|maybe_chunks| {
                maybe_chunks.map(|chunks| ChunkArrayWithProof {
                    chunks,
                    start_proof: ChunkProof {},
                    end_proof: ChunkProof {},
                })
            });

        match result {
            Ok(Some(chunks)) => {
                self.ctx.send(NetworkMessage::SendResponse {
                    peer_id,
                    id: request_id,
                    response: network::Response::Chunks(chunks),
                });
            }
            Ok(None) => {
                // FIXME(thegaram): will this happen if the index is out-of-range,
                // and/or if the data is only partially available on this node?
                self.ctx.send(NetworkMessage::SendErrorResponse {
                    peer_id,
                    id: request_id,
                    error: RPCResponseErrorCode::InvalidRequest,
                    reason: "Chunk does not exist".to_string(),
                });
            }
            Err(e) => {
                self.ctx.send(NetworkMessage::SendErrorResponse {
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
        peer_id: PeerId,
        request_id: RequestId,
        response: ChunkArrayWithProof,
    ) {
        info!(%peer_id, ?request_id, "Received Chunks response: {:?}", response.chunks);

        let tx_seq = match request_id {
            RequestId::SerialSync { tx_seq } => tx_seq,
        };

        match self.controllers.get_mut(&tx_seq) {
            Some(controller) => {
                controller.on_response(peer_id, response);
                controller.transition();
            }
            None => {
                warn!("Received chunks response for non-existent controller tx_seq={tx_seq}");
            }
        }
    }

    fn on_rpc_error(&mut self, peer_id: PeerId, request_id: RequestId) {
        info!(%peer_id, ?request_id, "Received RPC error");

        let tx_seq = match request_id {
            RequestId::SerialSync { tx_seq } => tx_seq,
        };

        match self.controllers.get_mut(&tx_seq) {
            Some(controller) => {
                controller.on_request_failed(peer_id);
                controller.transition();
            }
            None => {
                warn!("Received rpc error for non-existent controller tx_seq={tx_seq}");
            }
        }
    }

    fn on_start_sync_file(&mut self, tx_seq: u64, num_chunks: usize) {
        info!("Starting sync file for tx_seq={tx_seq}");

        // TODO(thegaram): add `put_tx` and `finalize_tx` logic

        let controller = self.controllers.entry(tx_seq).or_insert_with(|| {
            SerialSyncController::new(tx_seq, num_chunks, self.ctx.clone(), self.store.clone())
        });

        // trigger retry after failure
        if controller.is_failed() {
            controller.reset();
        }

        controller.transition();
    }

    fn on_heartbeat(&mut self) {
        for controller in self.controllers.values_mut() {
            controller.transition();
        }

        // TODO(thegaram): gc old controllers
    }
}
