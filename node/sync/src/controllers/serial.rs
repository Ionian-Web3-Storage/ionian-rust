use crate::{timestamp_now, SyncNetworkContext};
use network::{
    rpc::GetChunksRequest, types::FindFile, Multiaddr, NetworkMessage, PeerAction, PeerId,
    PubsubMessage, ReportSource, SyncId as RequestId,
};
use rand::seq::IteratorRandom;
use shared_types::{ChunkArray, ChunkArrayWithProof, DataRoot, CHUNK_SIZE};
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};
use storage_async::Store;

const CHUNK_BATCH_SIZE: usize = 1024;

// TODO(ionian-dev): set an appropriate request size
const MAX_CHUNKS_TO_REQUEST: usize = CHUNK_BATCH_SIZE;

const PEER_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
const DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(5);
const PEER_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const PEER_DISCONNECT_TIMEOUT: Duration = Duration::from_secs(5);

// TODO(ionian-dev):
// - parallel requests
// - limit number of retries

#[derive(Clone, Copy, Debug, PartialEq)]
enum PeerState {
    Found,
    Connecting,
    Connected,
    Disconnecting,
    Disconnected,
    Failed,
}

struct PeerInfo {
    /// The reported/connected address of the peer.
    pub addr: Multiaddr,

    /// The current state of the peer.
    pub state: PeerState,

    /// Timestamp of the last state change.
    pub since: Instant,
}

impl PeerInfo {
    fn update_state(&mut self, new_state: PeerState) {
        self.state = new_state;
        self.since = Instant::now();
    }
}

#[derive(Default)]
struct SyncPeers {
    peers: HashMap<PeerId, PeerInfo>,
}

impl SyncPeers {
    pub fn add_new_peer(&mut self, peer_id: PeerId, addr: Multiaddr) {
        self.peers.entry(peer_id).or_insert(PeerInfo {
            addr,
            state: PeerState::Found,
            since: Instant::now(),
        });
    }

    pub fn update_state(&mut self, peer_id: PeerId, from: PeerState, to: PeerState) {
        match self.peers.get_mut(&peer_id) {
            Some(info) if info.state == from => info.update_state(to),
            Some(PeerInfo { state, .. }) => {
                warn!(%peer_id, expected=?from, got=?state, "Received update_state in unexpected peer state");
                *state = PeerState::Failed;
            }
            None => {
                warn!(%peer_id, "Received update_state for nonexistent peer");
            }
        }
    }

    fn peer_state(&self, peer_id: PeerId) -> Option<PeerState> {
        self.peers.get(&peer_id).map(|info| info.state)
    }

    fn random_peer(&self, state: PeerState) -> Option<(PeerId, Multiaddr)> {
        self.peers
            .iter()
            .filter(|(_, info)| info.state == state)
            .map(|(peer_id, info)| (*peer_id, info.addr.clone()))
            .choose(&mut rand::thread_rng())
    }

    fn count(&self, states: &[PeerState]) -> usize {
        self.peers
            .values()
            .filter(|info| states.contains(&info.state))
            .count()
    }

    fn transition(&mut self) {
        for (peer_id, info) in self.peers.iter_mut() {
            match info.state {
                PeerState::Connecting => {
                    // handle timeout
                    // Note: timeouts and other connection issues generate SwarmEvents,
                    // however, these are currently not propagated to the higher layers.
                    // TODO(ionian-dev): consider handling connection failure events.
                    if info.since.elapsed() >= PEER_CONNECT_TIMEOUT {
                        warn!(%peer_id, "Peer connection timeout");
                        info.state = PeerState::Failed;
                    }
                }

                PeerState::Disconnecting => {
                    if info.since.elapsed() >= PEER_DISCONNECT_TIMEOUT {
                        warn!(%peer_id, "Peer disconnect timeout");
                        info.state = PeerState::Failed;
                    }
                }

                _ => {}
            }
        }

        // TODO(ionian-dev): gc peers?
    }
}

#[derive(Debug)]
enum SyncState {
    Idle,

    FindingPeers {
        since: Instant,
    },

    FoundPeers,

    ConnectingPeers,

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

    /// The transaction data root.
    data_root: DataRoot,

    /// The size of the file to be synced.
    num_chunks: usize,

    /// The next chunk id that we need to retrieve.
    next_chunk: usize,

    /// Current state of this request.
    state: SyncState,

    /// Sync peer manager.
    peers: SyncPeers,

    /// A network context to contact the network service.
    ctx: Arc<SyncNetworkContext>,

    /// Log and transaction storage.
    store: Store,
}

impl SerialSyncController {
    pub(crate) fn new(
        tx_seq: u64,
        data_root: DataRoot,
        num_chunks: usize,
        ctx: Arc<SyncNetworkContext>,
        store: Store,
    ) -> Self {
        SerialSyncController {
            tx_seq,
            data_root,
            num_chunks,
            next_chunk: 0,
            state: SyncState::Idle,
            peers: Default::default(),
            ctx,
            store,
        }
    }

    fn publish(&mut self, msg: PubsubMessage) {
        self.ctx.send(NetworkMessage::Publish {
            messages: vec![msg],
        });
    }

    pub fn is_failed(&self) -> bool {
        matches!(self.state, SyncState::Failed { .. })
    }

    pub fn get_status(&self) -> String {
        format!("{:?}", self.state)
    }

    pub fn reset(&mut self) {
        self.next_chunk = 0;
        self.state = SyncState::Idle;
        // TODO(ionian-dev): reset peers?
    }

    fn try_find_peers(&mut self) {
        info!(%self.tx_seq, "Finding peers");

        self.publish(PubsubMessage::FindFile(FindFile {
            tx_seq: self.tx_seq,
            timestamp: timestamp_now(),
        }));

        self.state = SyncState::FindingPeers {
            since: Instant::now(),
        };
    }

    fn try_connect(&mut self) {
        // select a random peer
        let (peer_id, address) = match self.peers.random_peer(PeerState::Found) {
            Some((peer_id, address)) => (peer_id, address),
            None => {
                warn!(%self.tx_seq, "No peers available");
                self.state = SyncState::Idle;
                return;
            }
        };

        // connect to peer
        info!(%peer_id, %address, "Attempting to connect to peer");
        self.ctx.send(NetworkMessage::DialPeer { address });

        self.peers
            .update_state(peer_id, PeerState::Found, PeerState::Connecting);

        self.state = SyncState::ConnectingPeers;
    }

    fn try_request_next(&mut self) {
        // select a random peer
        let peer_id = match self.peers.random_peer(PeerState::Connected) {
            Some((peer_id, _)) => peer_id,
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
            // FIXME(ionian-dev): unify APIs with usize or u32
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

        self.peers
            .update_state(peer_id, PeerState::Connected, PeerState::Disconnecting);
    }

    pub fn on_peer_found(&mut self, peer_id: PeerId, addr: Multiaddr) {
        info!(%peer_id, %addr, "Found new peer");
        self.peers.add_new_peer(peer_id, addr)
    }

    pub fn on_peer_connected(&mut self, peer_id: PeerId) {
        info!(%peer_id, "Peer connected");

        self.peers
            .update_state(peer_id, PeerState::Connecting, PeerState::Connected);
    }

    pub fn on_peer_disconnected(&mut self, peer_id: PeerId) {
        info!(%peer_id, "Peer disconnected");

        self.peers
            .update_state(peer_id, PeerState::Disconnecting, PeerState::Disconnected);
    }

    pub async fn on_response(&mut self, from_peer_id: PeerId, response: ChunkArrayWithProof) {
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
                let ChunkArray { start_index, .. } = response.chunks;
                let end_index = start_index + (response.chunks.data.len() / CHUNK_SIZE) as u32;

                if start_index != from_chunk as u32 || end_index != to_chunk as u32 {
                    warn!(%self.tx_seq, "Got unexpected chunks from peer, expected={from_chunk}..{to_chunk}, actual={start_index}..{end_index}");
                    self.ban_peer(peer_id, "Invalid chunk response range");
                    self.state = SyncState::Idle;
                    return;
                }

                // invalid chunk array size: ban and re-request
                debug_assert!(to_chunk > from_chunk, "Invalid chunk boundaries");

                if response.chunks.data.len() != (to_chunk - from_chunk) * CHUNK_SIZE {
                    warn!(%peer_id, %self.tx_seq, "Invalid chunk data length, expected={}, actual={}", (to_chunk - from_chunk) * CHUNK_SIZE, response.chunks.data.len());
                    self.ban_peer(peer_id, "Invalid chunk response data length");
                    self.state = SyncState::Idle;
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

        // validate Merkle proofs
        // TODO(ionian-dev): consider doing this in a worker task
        let validation_result = response.validate(&self.data_root, self.num_chunks);

        if !matches!(validation_result, Ok(true)) {
            self.ban_peer(from_peer_id, "Chunk array validation failed");
            self.state = SyncState::AwaitingDownload;
            return;
        }

        // store in db
        let result = self.store.put_chunks(self.tx_seq, response.chunks).await;

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
            // FIXME(ionian-dev): disconnect peer
            // TODO(ionian-dev): add `finalize_tx` logic
            self.state = SyncState::Completed;
            return;
        }

        // prepare to download next
        self.state = SyncState::Idle;
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
        // TODO(ionian-dev): count failed attempts instead of banning directly,
        // and consider different action for different failure reasons.
        self.ban_peer(from_peer_id, "RPC Error");
        self.state = SyncState::AwaitingDownload;
    }

    pub fn transition(&mut self) {
        use PeerState::*;

        // update peer connection states
        self.peers.transition();

        let mut keep_going = true;

        while keep_going {
            keep_going = false;

            match self.state {
                SyncState::Idle => {
                    if self.peers.count(&[Found, Connecting, Connected]) > 0 {
                        self.state = SyncState::FindingPeers {
                            since: Instant::now(),
                        };

                        keep_going = true;
                        continue;
                    }

                    // find new peers
                    self.try_find_peers();
                    keep_going = true;
                }

                SyncState::FindingPeers { since } => {
                    if self.peers.count(&[Found, Connecting, Connected]) > 0 {
                        self.state = SyncState::FoundPeers;
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

                SyncState::FoundPeers => {
                    if self.peers.count(&[Connecting, Connected]) > 0 {
                        self.state = SyncState::ConnectingPeers;
                        keep_going = true;
                        continue;
                    }

                    // connect to new peers
                    self.try_connect();
                    keep_going = true;
                }

                SyncState::ConnectingPeers => {
                    if self.peers.count(&[Connected]) > 0 {
                        self.state = SyncState::AwaitingDownload;
                        keep_going = true;
                        continue;
                    }

                    if self.peers.count(&[Connecting]) == 0 {
                        self.state = SyncState::Idle;
                        keep_going = true;
                        continue;
                    }
                }

                SyncState::AwaitingDownload => {
                    self.try_request_next();
                    keep_going = true;
                }

                SyncState::Downloading { peer_id, since, .. } => {
                    if !matches!(self.peers.peer_state(peer_id), Some(PeerState::Connected)) {
                        self.state = SyncState::Idle;
                        keep_going = true;
                        continue;
                    }

                    if since.elapsed() >= DOWNLOAD_TIMEOUT {
                        // TODO(ionian-dev): consider removing peer
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
