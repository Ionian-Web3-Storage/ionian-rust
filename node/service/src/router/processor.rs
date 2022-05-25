use crate::service::{NetworkMessage, RequestId};
use network::rpc::*;
use network::{MessageId, NetworkGlobals, PeerId, PeerRequestId, Request, Response};
use std::sync::Arc;
use tokio::sync::mpsc;

/// Processes validated messages from the network. It relays necessary data to the syncing thread
/// and processes blocks from the pubsub network.
pub struct Processor {
    /// A network context to return and handle RPC requests.
    network: HandlerNetworkContext,
}

impl Processor {
    /// Instantiate a `Processor` instance
    pub fn new(
        _executor: task_executor::TaskExecutor,
        _network_globals: Arc<NetworkGlobals>,
        network_send: mpsc::UnboundedSender<NetworkMessage>,
    ) -> Self {
        // let (beacon_processor_send, beacon_processor_receive) =
        //     mpsc::channel(MAX_WORK_EVENT_QUEUE_LEN);

        // spawn the sync thread
        // let sync_send = crate::sync::manager::spawn(
        //     executor.clone(),
        //     beacon_chain.clone(),
        //     network_globals.clone(),
        //     network_send.clone(),
        //     beacon_processor_send.clone(),
        //     sync_logger,
        // );

        // BeaconProcessor {
        //     beacon_chain: Arc::downgrade(&beacon_chain),
        //     network_tx: network_send.clone(),
        //     sync_tx: sync_send.clone(),
        //     network_globals,
        //     executor,
        //     max_workers: cmp::max(1, num_cpus::get()),
        //     current_workers: 0,
        //     importing_blocks: Default::default(),
        //     log: log.clone(),
        // }
        // .spawn_manager(beacon_processor_receive, None);

        Processor {
            network: HandlerNetworkContext::new(network_send),
        }
    }

    /// Handle a peer disconnect.
    pub fn on_disconnect(&mut self, _peer_id: PeerId) {
        // EMPTY
    }

    /// An error occurred during an RPC request.
    pub fn on_rpc_error(&mut self, _peer_id: PeerId, _request_id: RequestId) {
        // EMPTY
    }

    /// Sends a `Status` message to the peer.
    ///
    /// Called when we first connect to a peer, or when the PeerManager determines we need to
    /// re-status.
    pub fn send_status(&mut self, peer_id: PeerId) {
        let status_message = StatusMessage { data: 123 }; // dummy status message
        debug!(%peer_id, ?status_message, "Sending Status request");
        self.network
            .send_processor_request(peer_id, Request::Status(status_message));
    }

    /// Handle a `Status` request.
    ///
    /// Processes the `Status` from the remote peer and sends back our `Status`.
    pub fn on_status_request(
        &mut self,
        peer_id: PeerId,
        request_id: PeerRequestId,
        status: StatusMessage,
    ) {
        debug!(%peer_id, ?status, "Received Status Request");
        let status_message = StatusMessage { data: 456 }; // dummy status message
        debug!(%peer_id, ?status_message, "Sending Status response");
        self.network
            .send_response(peer_id, Response::Status(status_message), request_id);
    }

    /// Process a `Status` response from a peer.
    pub fn on_status_response(&mut self, peer_id: PeerId, status: StatusMessage) {
        debug!(%peer_id, ?status, "Received Status response");
    }

    /// Handle a `DataByHash` request from the peer.
    pub fn on_data_by_hash_request(
        &mut self,
        _peer_id: PeerId,
        _request_id: PeerRequestId,
        _request: DataByHashRequest,
    ) {
        // EMPTY
    }

    /// Handle a `DataByHash` response from the peer.
    /// A `data` behaves as a stream which is terminated on a `None` response.
    pub fn on_data_by_hash_response(
        &mut self,
        peer_id: PeerId,
        _request_id: RequestId,
        data: Option<Box<IonianData>>,
    ) {
        trace!(%peer_id, ?data, "Received DataByHash response");
    }

    /// Process a gossip message declaring a new block.
    ///
    /// Attempts to apply to block to the beacon chain. May queue the block for later processing.
    ///
    /// Returns a `bool` which, if `true`, indicates we should forward the block to our peers.
    pub fn on_example_message_gossip(&mut self, message_id: MessageId, peer_id: PeerId, data: u64) {
        trace!(%peer_id, %message_id, ?data, "Received ExampleMessage gossip");
    }
}

/// Wraps a Network Channel to employ various RPC related network functionality for the
/// processor.
#[derive(Clone)]
pub struct HandlerNetworkContext {
    /// The network channel to relay messages to the Network service.
    network_send: mpsc::UnboundedSender<NetworkMessage>,
}

impl HandlerNetworkContext {
    pub fn new(network_send: mpsc::UnboundedSender<NetworkMessage>) -> Self {
        Self { network_send }
    }

    /// Sends a message to the network task.
    fn inform_network(&mut self, msg: NetworkMessage) {
        self.network_send
            .send(msg)
            .unwrap_or_else(|e| warn!(error = %e, "Could not send message to the network service"))
    }

    /// Sends a request to the network task.
    pub fn send_processor_request(&mut self, peer_id: PeerId, request: Request) {
        self.inform_network(NetworkMessage::SendRequest {
            peer_id,
            request_id: RequestId::Router,
            request,
        })
    }

    /// Sends a response to the network task.
    pub fn send_response(&mut self, peer_id: PeerId, response: Response, id: PeerRequestId) {
        self.inform_network(NetworkMessage::SendResponse {
            peer_id,
            id,
            response,
        })
    }

    /// Sends an error response to the network task.
    pub fn _send_error_response(
        &mut self,
        peer_id: PeerId,
        id: PeerRequestId,
        error: RPCResponseErrorCode,
        reason: String,
    ) {
        self.inform_network(NetworkMessage::SendErrorResponse {
            peer_id,
            error,
            id,
            reason,
        })
    }
}
