use network::{
    rpc::{GoodbyeReason, RPCResponseErrorCode},
    PeerAction, PeerId, PeerRequestId, PubsubMessage, ReportSource, Request, Response,
};

pub use ethereum_types::H256;
use sha3::{Digest, Sha3_256};

pub struct DataChunk {
    pub raw: Vec<u8>,
    pub hash: H256,
}

impl std::ops::Deref for DataChunk {
    type Target = Vec<u8>;

    fn deref(&self) -> &Self::Target {
        &self.raw
    }
}

impl std::fmt::Debug for DataChunk {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "chunk {}", self.hash)
    }
}

impl DataChunk {
    pub fn new(raw: Vec<u8>) -> Self {
        let mut hasher = Sha3_256::new();
        hasher.update(&raw[..]);
        let digest = hasher.finalize();

        let digest_raw: [u8; 32] = digest.as_slice().try_into().expect("Wrong length");
        let hash = H256::from(digest_raw);

        DataChunk { raw, hash }
    }
}

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
    Publish {
        messages: Vec<PubsubMessage>,
    },
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
    NewDataChunk {
        chunk: DataChunk,
    },
}
