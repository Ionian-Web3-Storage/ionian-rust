//! This module handles incoming network messages.
//!
//! It routes the messages to appropriate services.
//! It handles requests at the application layer in its associated processor and directs
//! syncing-related responses to the Sync manager.

mod processor;

use crate::error;
use futures::prelude::*;
use network::{MessageId, NetworkGlobals, PeerId, PeerRequestId, PubsubMessage, Request, Response};
use network::{RequestId, ServiceMessage};
use processor::Processor;
use std::sync::Arc;
use storage::log_store::Store;
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;

/// Handles messages received from the network and client and organises syncing. This
/// functionality of this struct is to validate an decode messages from the network before
/// passing them to the internal message processor. The message processor spawns a syncing thread
/// which manages which blocks need to be requested and processed.
pub struct Router {
    /// Access to the peer db.
    network_globals: Arc<NetworkGlobals>,

    /// Processes validated and decoded messages from the network.
    /// Has direct access to the sync manager.
    processor: Processor,
}

/// Types of messages the handler can receive.
#[derive(Debug)]
pub enum RouterMessage {
    /// We have initiated a connection to a new peer.
    PeerDialed(PeerId),
    /// Peer has disconnected,
    PeerDisconnected(PeerId),
    /// An RPC request has been received.
    RPCRequestReceived {
        peer_id: PeerId,
        id: PeerRequestId,
        request: Request,
    },
    /// An RPC response has been received.
    RPCResponseReceived {
        peer_id: PeerId,
        request_id: RequestId,
        response: Response,
    },
    /// An RPC request failed
    RPCFailed {
        peer_id: PeerId,
        request_id: RequestId,
    },
    /// A gossip message has been received. The fields are: message id, the peer that sent us this
    /// message, the message itself and a bool which indicates if the message should be processed
    /// by the beacon chain after successful verification.
    PubsubMessage(MessageId, PeerId, PubsubMessage),
    /// The peer manager has requested we re-status a peer.
    StatusPeer(PeerId),
}

impl Router {
    /// Initializes and runs the Router.
    pub fn spawn(
        network_globals: Arc<NetworkGlobals>,
        network_send: mpsc::UnboundedSender<ServiceMessage>,
        executor: task_executor::TaskExecutor,
        store: Arc<dyn Store>,
    ) -> error::Result<mpsc::UnboundedSender<RouterMessage>> {
        trace!("Service starting");

        let (handler_send, handler_recv) = mpsc::unbounded_channel();

        // Initialise a message instance, which itself spawns the syncing thread.
        let processor = Processor::new(
            executor.clone(),
            network_globals.clone(),
            network_send,
            store,
        );

        // generate the Message handler
        let mut handler = Router {
            network_globals,
            processor,
        };

        // spawn handler task and move the message handler instance into the spawned thread
        executor.spawn(
            async move {
                debug!("Network message router started");
                UnboundedReceiverStream::new(handler_recv)
                    .for_each(move |msg| {
                        handler.handle_message(msg);
                        future::ready(())
                    })
                    .await;
            },
            "router",
        );

        Ok(handler_send)
    }

    /// Handle all messages incoming from the network service.
    fn handle_message(&mut self, message: RouterMessage) {
        match message {
            // we have initiated a connection to a peer or the peer manager has requested a
            // re-status
            RouterMessage::PeerDialed(peer_id) | RouterMessage::StatusPeer(peer_id) => {
                self.processor.send_status(peer_id);
            }
            // A peer has disconnected
            RouterMessage::PeerDisconnected(peer_id) => {
                self.processor.on_disconnect(peer_id);
            }
            RouterMessage::RPCRequestReceived {
                peer_id,
                id,
                request,
            } => {
                self.handle_rpc_request(peer_id, id, request);
            }
            RouterMessage::RPCResponseReceived {
                peer_id,
                request_id,
                response,
            } => {
                self.handle_rpc_response(peer_id, request_id, response);
            }
            RouterMessage::RPCFailed {
                peer_id,
                request_id,
            } => {
                self.processor.on_rpc_error(peer_id, request_id);
            }
            RouterMessage::PubsubMessage(id, peer_id, gossip) => {
                self.handle_gossip(id, peer_id, gossip);
            }
        }
    }

    /* RPC - Related functionality */

    /// A new RPC request has been received from the network.
    fn handle_rpc_request(&mut self, peer_id: PeerId, id: PeerRequestId, request: Request) {
        if !self.network_globals.peers.read().is_connected(&peer_id) {
            debug!(%peer_id, ?request, "Dropping request of disconnected peer");
            return;
        }

        match request {
            Request::Status(status_message) => {
                self.processor
                    .on_status_request(peer_id, id, status_message)
            }
            Request::DataByHash(request) => {
                self.processor.on_data_by_hash_request(peer_id, id, request)
            }
            Request::GetChunks(request) => {
                self.processor.on_get_chunks_request(peer_id, id, request);
            }
        }
    }

    /// An RPC response has been received from the network.
    // we match on id and ignore responses past the timeout.
    fn handle_rpc_response(&mut self, peer_id: PeerId, request_id: RequestId, response: Response) {
        // an error could have occurred.
        match response {
            Response::Status(status_message) => {
                self.processor.on_status_response(peer_id, status_message);
            }
            Response::DataByHash(data) => {
                self.processor
                    .on_data_by_hash_response(peer_id, request_id, data);
            }
            Response::Chunks(data) => {
                self.processor.on_chunks_response(peer_id, request_id, data);
            }
        }
    }

    /// Handle gossip messages.
    fn handle_gossip(&mut self, id: MessageId, peer_id: PeerId, gossip_message: PubsubMessage) {
        match gossip_message {
            PubsubMessage::ExampleMessage(data) => {
                self.processor.on_example_message_gossip(id, peer_id, data);
            }
        }
    }
}
