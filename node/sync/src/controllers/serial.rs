use crate::SyncNetworkContext;
use network::{
    rpc::GetChunksRequest, NetworkMessage, PeerAction, PeerId, ReportSource, SyncId as RequestId,
};
use rand::seq::IteratorRandom;
use shared_types::{ChunkArray, ChunkArrayWithProof, CHUNK_SIZE};
use std::{
    collections::HashSet,
    sync::Arc,
    time::{Duration, Instant},
};
use storage::log_store::Store;

const CHUNK_BATCH_SIZE: usize = 1024;

// TODO(thegaram): set an appropriate request size
const MAX_CHUNKS_TO_REQUEST: usize = CHUNK_BATCH_SIZE;

const PEER_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
const DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(5);

// TODO(thegaram):
// - peer discovery
// - parallel requests
// - limit number of retries

#[derive(Debug)]
enum SyncState {
    Idle,

    FindingPeers {
        since: Instant,
    },

    AwaitingDownload,

    Downloading {
        peer_id: PeerId,
        from_chunk: usize,
        to_chunk: usize,
        since: Instant,
    },

    Completed,

    #[allow(dead_code)]
    Failed {
        reason: String,
    },
}

pub struct SerialSyncController {
    /// The transaction sequence number.
    tx_seq: u64,

    /// The size of the file to be synced.
    num_chunks: usize,

    /// The next chunk id that we need to retrieve.
    next_chunk: usize,

    /// Current state of this request.
    state: SyncState,

    /// Peers that should have this file.
    available_peers: HashSet<PeerId>,

    /// Peers with which we have tried and failed.
    used_peers: HashSet<PeerId>,

    /// A network context to contact the network service.
    ctx: Arc<SyncNetworkContext>,

    /// Log and transaction storage.
    store: Arc<dyn Store>,
}

impl SerialSyncController {
    pub(crate) fn new(
        tx_seq: u64,
        num_chunks: usize,
        ctx: Arc<SyncNetworkContext>,
        store: Arc<dyn Store>,
    ) -> Self {
        SerialSyncController {
            tx_seq,
            num_chunks,
            next_chunk: 0,
            state: SyncState::Idle,
            available_peers: Default::default(),
            used_peers: Default::default(),
            ctx,
            store,
        }
    }

    pub fn is_failed(&self) -> bool {
        matches!(self.state, SyncState::Failed { .. })
    }

    pub fn reset(&mut self) {
        self.next_chunk = 0;
        self.state = SyncState::Idle;
    }

    fn try_request_next(&mut self) {
        // select a random peer
        let peer_id = match self.available_peers.iter().choose(&mut rand::thread_rng()) {
            Some(peer_id) => *peer_id,
            None => {
                warn!(%self.tx_seq, "No peers available");
                self.state = SyncState::Idle;
                return;
            }
        };

        // request next chunk array
        let from_chunk = self.next_chunk;
        let to_chunk = std::cmp::min(from_chunk + MAX_CHUNKS_TO_REQUEST, self.num_chunks);

        let request_id = network::RequestId::Sync(RequestId::SerialSync {
            tx_seq: self.tx_seq,
        });

        let request = network::Request::GetChunks(GetChunksRequest {
            tx_seq: self.tx_seq,
            // FIXME(thegaram): unify APIs with usize or u32
            index_start: from_chunk as u32,
            index_end: to_chunk as u32,
        });

        self.ctx.send(NetworkMessage::SendRequest {
            peer_id,
            request_id,
            request,
        });

        self.state = SyncState::Downloading {
            peer_id,
            from_chunk,
            to_chunk,
            since: Instant::now(),
        };
    }

    fn ban_peer(&mut self, peer_id: PeerId, msg: &'static str) {
        warn!(%self.tx_seq, "Banning peer {peer_id}");

        self.ctx.send(NetworkMessage::ReportPeer {
            peer_id,
            action: PeerAction::Fatal,
            source: ReportSource::SyncService,
            msg,
        });

        self.available_peers.remove(&peer_id);
        self.used_peers.insert(peer_id);
    }

    pub fn on_peer_connected(&mut self, peer_id: PeerId) {
        // TODO(thegaram): ignore if in used_peers?
        self.available_peers.insert(peer_id);
    }

    pub fn on_peer_disconnected(&mut self, dc_peer_id: &PeerId) {
        let removed = self.available_peers.remove(dc_peer_id);

        if removed && self.available_peers.is_empty() {
            self.state = SyncState::Idle;
        }
    }

    pub fn on_response(&mut self, from_peer_id: PeerId, response: ChunkArrayWithProof) {
        // execute cheap validation steps
        let (_from_chunk, to_chunk) = match self.state {
            SyncState::Downloading {
                peer_id,
                from_chunk,
                to_chunk,
                ..
            } => {
                // got response from wrong peer
                // this can happen if we get a response for a timeout request
                if from_peer_id != peer_id {
                    warn!(%self.tx_seq, "Got response from unexpected peer, expected={peer_id}, actual={from_peer_id}");
                    return; // ignore response
                }

                // invalid chunk range: ban and re-request
                let ChunkArray {
                    start_index,
                    end_index,
                    ..
                } = response.chunks;

                if start_index != from_chunk as u32 || end_index != to_chunk as u32 {
                    warn!(%self.tx_seq, "Got unexpected chunks from peer, expected={from_chunk}..{to_chunk}, actual={start_index}..{end_index}");
                    self.ban_peer(peer_id, "Invalid chunk response range");
                    self.state = SyncState::AwaitingDownload;
                    return;
                }

                // invalid chunk array size: ban and re-request
                debug_assert!(to_chunk > from_chunk, "Invalid chunk boundaries");

                if response.chunks.data.len() != (to_chunk - from_chunk) * CHUNK_SIZE {
                    warn!(%peer_id, %self.tx_seq, "Invalid chunk data length, expected={}, actual={}", (to_chunk - from_chunk) * CHUNK_SIZE, response.chunks.data.len());
                    self.ban_peer(peer_id, "Invalid chunk response data length");
                    self.state = SyncState::AwaitingDownload;
                    return;
                }

                // all good
                (from_chunk, to_chunk)
            }
            _ => {
                // wrong state
                return;
            }
        };

        // execute expensive validation steps
        // TODO(thegaram): validate Merkle proofs

        // store in db
        // FIXME(thegaram): do this in worker thread
        let result = self.store.put_chunks(self.tx_seq, response.chunks);

        // unexpected DB error while storing chunks
        if let Err(e) = result {
            let err = format!("Unexpected DB error while storing chunks: {:?}", e);
            error!("{}", err);
            self.state = SyncState::Failed { reason: err };
            return;
        }

        self.next_chunk = to_chunk;

        // check if this is the last chunk
        if self.next_chunk == self.num_chunks {
            self.state = SyncState::Completed;
            return;
        }

        // prepare to download next
        self.state = SyncState::AwaitingDownload;
    }

    pub fn on_request_failed(&mut self, from_peer_id: PeerId) {
        match self.state {
            SyncState::Downloading { peer_id, .. } => {
                if from_peer_id != peer_id {
                    // ignore
                    return;
                }
            }
            _ => {
                // wrong state
                return;
            }
        };

        // current request failed
        // TODO(thegaram): count failed attempts instead of banning directly,
        // and consider different action for different failure reasons.
        self.ban_peer(from_peer_id, "RPC Error");
        self.state = SyncState::AwaitingDownload;
    }

    pub fn transition(&mut self) {
        let mut keep_going = true;

        while keep_going {
            keep_going = false;

            match self.state {
                SyncState::Idle => {
                    if !self.available_peers.is_empty() {
                        self.state = SyncState::AwaitingDownload;
                        keep_going = true;
                        continue;
                    }

                    // TODO（thegaram): trigger peer request
                    info!(%self.tx_seq, "Requesting peers");

                    self.state = SyncState::FindingPeers {
                        since: Instant::now(),
                    };
                }

                SyncState::FindingPeers { since } => {
                    if !self.available_peers.is_empty() {
                        self.state = SyncState::AwaitingDownload;
                        keep_going = true;
                        continue;
                    }

                    if since.elapsed() >= PEER_REQUEST_TIMEOUT {
                        warn!(%self.tx_seq, "Peer request timeout");
                        self.state = SyncState::Idle;
                        keep_going = true;
                        continue;
                    }
                }

                SyncState::AwaitingDownload => {
                    self.try_request_next();
                    keep_going = true;
                }

                SyncState::Downloading { since, .. } => {
                    if since.elapsed() >= DOWNLOAD_TIMEOUT {
                        // TODO(thegaram): consider removing peer
                        self.state = SyncState::AwaitingDownload;
                        keep_going = true;
                        continue;
                    }
                }

                SyncState::Completed => {
                    // EMPTY
                }

                SyncState::Failed { .. } => {
                    // EMPTY
                }
            }
        }
    }
}
