use crate::{controllers::SerialSyncController, timestamp_now, SyncNetworkContext};
use network::{
    rpc::GetChunksRequest, rpc::RPCResponseErrorCode, types::AnnounceFile, Multiaddr,
    NetworkGlobals, NetworkMessage, PeerId, PeerRequestId, PubsubMessage, SyncId as RequestId,
};
use shared_types::{bytes_to_chunks, ChunkArrayWithProof};
use std::{
    collections::{hash_map::Entry, HashMap},
    sync::Arc,
};
use storage::log_store::Store as LogStore;
use storage_async::Store;
use tokio::sync::mpsc;

const HEARTBEAT_INTERVAL_SEC: u64 = 5;

pub type SyncSender = channel::Sender<SyncMessage, SyncRequest, SyncResponse>;

#[derive(Debug)]
pub enum SyncMessage {
    PeerConnected {
        peer_id: PeerId,
    },

    PeerDisconnected {
        peer_id: PeerId,
    },

    StartSyncFile {
        tx_seq: u64,
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

    FindFileGossip {
        tx_seq: u64,
    },

    AnnounceFileGossip {
        tx_seq: u64,
        peer_id: PeerId,
        addr: Multiaddr,
    },
}

#[derive(Debug)]
pub enum SyncRequest {
    SyncStatus { tx_seq: u64 },
}

#[derive(Debug)]
pub enum SyncResponse {
    SyncStatus { status: String },
}

pub struct SyncService {
    /// A receiving channel sent by the message processor thread.
    msg_recv: channel::Receiver<SyncMessage, SyncRequest, SyncResponse>,

    /// A network context to contact the network service.
    ctx: Arc<SyncNetworkContext>,

    /// A reference to the network globals and peer-db.
    #[allow(dead_code)]
    network_globals: Arc<NetworkGlobals>,

    /// Log and transaction storage.
    store: Store,

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
        store: Arc<dyn LogStore>,
    ) -> SyncSender {
        let (sync_send, sync_recv) = channel::Channel::unbounded();

        let heartbeat =
            tokio::time::interval(tokio::time::Duration::from_secs(HEARTBEAT_INTERVAL_SEC));

        let store = Store::new(store, executor.clone());

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
                Some(msg) = self.msg_recv.recv() => {
                    match msg {
                        channel::Message::Notification(msg) => self.on_sync_msg(msg).await,
                        channel::Message::Request(req, sender) => self.on_sync_request(req, sender),
                    }
                }

                // heartbeat
                _ = self.heartbeat.tick() => self.on_heartbeat(),
            }
        }
    }

    fn publish(&mut self, msg: PubsubMessage) {
        self.ctx.send(NetworkMessage::Publish {
            messages: vec![msg],
        });
    }

    async fn on_sync_msg(&mut self, msg: SyncMessage) {
        warn!("Sync received message {:?}", msg);

        match msg {
            SyncMessage::PeerConnected { peer_id } => {
                self.on_peer_connected(peer_id);
            }

            SyncMessage::PeerDisconnected { peer_id } => {
                self.on_peer_disconnected(peer_id);
            }

            SyncMessage::RequestChunks {
                request_id,
                peer_id,
                request,
            } => {
                self.on_get_chunks_request(peer_id, request_id, request)
                    .await;
            }

            SyncMessage::ChunksResponse {
                peer_id,
                request_id,
                response,
            } => {
                self.on_chunks_response(peer_id, request_id, response).await;
            }

            SyncMessage::StartSyncFile { tx_seq } => {
                self.on_start_sync_file(tx_seq).await;
            }

            SyncMessage::RpcError {
                peer_id,
                request_id,
            } => {
                self.on_rpc_error(peer_id, request_id);
            }

            SyncMessage::FindFileGossip { tx_seq } => {
                self.on_find_file_gossip(tx_seq).await;
            }

            SyncMessage::AnnounceFileGossip {
                tx_seq,
                peer_id,
                addr,
            } => {
                self.on_announce_file_gossip(tx_seq, peer_id, addr);
            }
        }
    }

    fn on_sync_request(&mut self, req: SyncRequest, sender: channel::ResponseSender<SyncResponse>) {
        match req {
            SyncRequest::SyncStatus { tx_seq } => {
                let status = match self.controllers.get_mut(&tx_seq) {
                    Some(controller) => controller.get_status(),
                    None => "unknown".to_string(),
                };

                let _ = sender.send(SyncResponse::SyncStatus { status });
            }
        }
    }

    fn on_peer_connected(&mut self, peer_id: PeerId) {
        info!(%peer_id, "Peer connected");

        for controller in self.controllers.values_mut() {
            // TODO(ionian-dev): only update controllers that need it?
            controller.on_peer_connected(peer_id);
            controller.transition();
        }
    }

    fn on_peer_disconnected(&mut self, peer_id: PeerId) {
        info!("Peer {peer_id:?} disconnected");

        for controller in self.controllers.values_mut() {
            controller.on_peer_disconnected(peer_id);
            controller.transition();
        }
    }

    async fn on_get_chunks_request(
        &mut self,
        peer_id: PeerId,
        request_id: PeerRequestId,
        request: GetChunksRequest,
    ) {
        info!("Received GetChunks request: {:?}", request);

        // TODO(ionian-dev): can we do this validation in the network layer?
        if request.index_start >= request.index_end {
            self.ctx.send(NetworkMessage::SendErrorResponse {
                peer_id,
                id: request_id,
                error: RPCResponseErrorCode::InvalidRequest,
                reason: "Invalid chunk indices".to_string(),
            });
        }

        // TODO(ionian-dev): consider only serving chunks for files
        // that we fully store.

        let result = self
            .store
            .get_chunks_with_proof_by_tx_and_index_range(
                request.tx_seq,
                request.index_start as usize,
                request.index_end as usize,
            )
            .await;

        match result {
            Ok(Some(chunks)) => {
                self.ctx.send(NetworkMessage::SendResponse {
                    peer_id,
                    id: request_id,
                    response: network::Response::Chunks(chunks),
                });
            }
            Ok(None) => {
                // FIXME(ionian-dev): will this happen if the index is out-of-range,
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
                    // FIXME(ionian-dev): it's probably best not to expose
                    // this to peers, but for now it's useful for debugging.
                    reason: format!("db error: {:?}", e),
                });
            }
        }
    }

    async fn on_chunks_response(
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
                controller.on_response(peer_id, response).await;
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

    async fn on_start_sync_file(&mut self, tx_seq: u64) {
        info!("Starting sync file for tx_seq={tx_seq}");

        let controller = match self.controllers.entry(tx_seq) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => {
                let tx = match self.store.get_tx_by_seq_number(tx_seq).await {
                    Ok(Some(tx)) => tx,
                    res => {
                        // TODO(ionian-dev): this is a silent failure, should we notify the caller?
                        warn!(%tx_seq, "Unable to start sync file, transaction not found: {res:?}");
                        return;
                    }
                };

                let num_chunks = match usize::try_from(tx.size) {
                    Ok(size) => bytes_to_chunks(size),
                    Err(_) => {
                        error!(%tx_seq, "Unexpected transaction size: {}", tx.size);
                        return;
                    }
                };

                let c = SerialSyncController::new(
                    tx_seq,
                    tx.data_merkle_root,
                    num_chunks,
                    self.ctx.clone(),
                    self.store.clone(),
                );

                entry.insert(c)
            }
        };

        // trigger retry after failure
        if controller.is_failed() {
            controller.reset();
        }

        controller.transition();
    }

    async fn on_find_file_gossip(&mut self, tx_seq: u64) {
        info!(%tx_seq, "Received FindFile gossip");

        // check if we have it
        if !matches!(self.store.check_tx_completed(tx_seq).await, Ok(true)) {
            return;
        }

        // announce file
        // TODO(ionian-dev): consider choosing a random listen address
        let addr = match self.network_globals.listen_multiaddrs.read().first() {
            Some(addr) => addr.clone(),
            None => {
                error!("No listen address available");
                return;
            }
        };

        self.publish(PubsubMessage::AnnounceFile(AnnounceFile {
            tx_seq,
            at: addr.into(),
            timestamp: timestamp_now(),
        }));
    }

    fn on_announce_file_gossip(&mut self, tx_seq: u64, peer_id: PeerId, addr: Multiaddr) {
        info!(%tx_seq, %addr, "Received AnnounceFile gossip");

        if let Some(controller) = self.controllers.get_mut(&tx_seq) {
            controller.on_peer_found(peer_id, addr);
            controller.transition();
        }
    }

    fn on_heartbeat(&mut self) {
        for controller in self.controllers.values_mut() {
            controller.transition();
        }

        // TODO(ionian-dev): gc old controllers
    }
}
