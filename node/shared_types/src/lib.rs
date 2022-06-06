use ethereum_types::H256;
use network::{
    rpc::{GoodbyeReason, RPCResponseErrorCode},
    PeerAction, PeerId, PeerRequestId, PubsubMessage, ReportSource, Request, Response,
};

/// Application level requests sent to the network.
#[derive(Debug, Clone, Copy)]
pub enum RequestId {
    Router,
}

/// Types of messages that the network service can receive.
#[derive(Debug)]
pub enum ServiceMessage {
    /// Send an RPC request to the libp2p service.
    SendRequest {
        peer_id: PeerId,
        request: Request,
        request_id: RequestId,
    },
    /// Send a successful Response to the libp2p service.
    SendResponse {
        peer_id: PeerId,
        response: Response,
        id: PeerRequestId,
    },
    /// Send an error response to an RPC request.
    SendErrorResponse {
        peer_id: PeerId,
        error: RPCResponseErrorCode,
        reason: String,
        id: PeerRequestId,
    },
    /// Publish a list of messages to the gossipsub protocol.
    Publish { messages: Vec<PubsubMessage> },
    /// Reports a peer to the peer manager for performing an action.
    ReportPeer {
        peer_id: PeerId,
        action: PeerAction,
        source: ReportSource,
        msg: &'static str,
    },
    /// Disconnect an ban a peer, providing a reason.
    GoodbyePeer {
        peer_id: PeerId,
        reason: GoodbyeReason,
        source: ReportSource,
    },
}

/// Placeholder types for transactions and chunks.
pub type TransactionHash = H256;
pub type DataRoot = H256;
// Each chunk is 32 bytes.
pub const CHUNK_SIZE: usize = 32;
pub struct Chunk(pub [u8; CHUNK_SIZE]);
pub struct ChunkProof {}
pub struct Transaction {
    hash: TransactionHash,
    size: u64,
    data_merkle_root: DataRoot,
}
pub struct ChunkWithProof {
    chunk: Chunk,
    proof: ChunkProof,
}
pub struct ChunkArrayWithProof {
    chunks: ChunkArray,
    start_proof: ChunkProof,
    end_proof: ChunkProof,
}
pub struct ChunkArray {
    // The length is exactly `(end_index - start_index) * CHUNK_SIZE`
    pub data: Vec<u8>,
    pub start_index: u32,
    pub end_index: u32,
}
impl ChunkArray {
    pub fn chunk_at(&self, index: u32) -> Option<Chunk> {
        if index >= self.end_index || index < self.start_index {
            return None;
        }
        let offset = (index - self.start_index) as usize * CHUNK_SIZE;
        Some(Chunk(
            self.data[offset..offset + CHUNK_SIZE]
                .try_into()
                .expect("length match"),
        ))
    }
}
