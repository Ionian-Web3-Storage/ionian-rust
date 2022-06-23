use futures::{channel::mpsc::Sender, prelude::*};
use miner::MinerMessage;
use network::{
    rpc::StatusMessage, BehaviourEvent, Libp2pEvent, MessageId, NetworkGlobals, NetworkMessage,
    PeerId, PeerRequestId, PubsubMessage, Request, RequestId, Response, Service as LibP2PService,
};
use std::sync::Arc;
use sync::{SyncMessage, SyncSender};
use task_executor::ShutdownReason;
use tokio::sync::mpsc;

/// Service that handles communication between internal services and the libp2p service.
pub struct RouterService {
    /// The underlying libp2p service that drives all the network interactions.
    libp2p: LibP2PService<RequestId>,

    /// A collection of global variables, accessible outside of the network service.
    network_globals: Arc<NetworkGlobals>,

    /// The receiver channel for Ionian to communicate with the network service.
    network_recv: mpsc::UnboundedReceiver<NetworkMessage>,

    /// A channel to the router service.
    network_send: mpsc::UnboundedSender<NetworkMessage>,

    /// A channel to the syncing service.
    sync_send: SyncSender,

    /// A channel to the miner service.
    #[allow(dead_code)]
    miner_send: mpsc::UnboundedSender<MinerMessage>,
}

impl RouterService {
    pub fn spawn(
        executor: task_executor::TaskExecutor,
        libp2p: LibP2PService<RequestId>,
        network_globals: Arc<NetworkGlobals>,
        network_recv: mpsc::UnboundedReceiver<NetworkMessage>,
        network_send: mpsc::UnboundedSender<NetworkMessage>,
        sync_send: SyncSender,
        miner_send: mpsc::UnboundedSender<MinerMessage>,
    ) {
        // create the network service and spawn the task
        let router = RouterService {
            libp2p,
            network_globals,
            network_recv,
            network_send,
            sync_send,
            miner_send,
        };

        // spawn service
        let shutdown_sender = executor.shutdown_sender();

        executor.spawn(
            async move { Box::pin(router.main(shutdown_sender)).await },
            "router",
        );
    }

    async fn main(mut self, mut shutdown_sender: Sender<ShutdownReason>) {
        loop {
            tokio::select! {
                // handle a message sent to the network
                Some(msg) = self.network_recv.recv() => self.on_network_msg(msg, &mut shutdown_sender).await,

                // handle event coming from the network
                event = self.libp2p.next_event() => self.on_libp2p_event(event, &mut shutdown_sender).await,
            }
        }
    }

    fn send_to_sync(&mut self, message: SyncMessage) {
        self.sync_send.notify(message).unwrap_or_else(|e| {
            warn!( error = %e, "Could not send message to the sync service");
        });
    }

    fn send_to_network(&mut self, message: NetworkMessage) {
        self.network_send.send(message).unwrap_or_else(|e| {
            warn!( error = %e, "Could not send message to the network service");
        });
    }

    /// Handle an event received from the network.
    async fn on_libp2p_event(
        &mut self,
        ev: Libp2pEvent<RequestId>,
        shutdown_sender: &mut Sender<ShutdownReason>,
    ) {
        debug!(?ev, "Received new event from libp2p");

        match ev {
            Libp2pEvent::Behaviour(event) => match event {
                BehaviourEvent::PeerConnectedOutgoing(peer_id) => {
                    self.on_peer_connected(peer_id);
                }
                BehaviourEvent::PeerConnectedIncoming(_)
                | BehaviourEvent::PeerBanned(_)
                | BehaviourEvent::PeerUnbanned(_) => {
                    // No action required for these events.
                }
                BehaviourEvent::PeerDisconnected(peer_id) => {
                    self.on_peer_disconnected(peer_id);
                }
                BehaviourEvent::RequestReceived {
                    peer_id,
                    id,
                    request,
                } => {
                    self.on_rpc_request(peer_id, id, request);
                }
                BehaviourEvent::ResponseReceived {
                    peer_id,
                    id,
                    response,
                } => {
                    self.on_rpc_response(peer_id, id, response);
                }
                BehaviourEvent::RPCFailed { id, peer_id } => {
                    self.on_rpc_error(peer_id, id);
                }
                BehaviourEvent::StatusPeer(peer_id) => {
                    self.send_status(peer_id);
                }
                BehaviourEvent::PubsubMessage {
                    id,
                    source,
                    message,
                    ..
                } => {
                    self.on_pubsub_message(source, id, message);
                }
            },
            Libp2pEvent::NewListenAddr(multiaddr) => {
                self.network_globals
                    .listen_multiaddrs
                    .write()
                    .push(multiaddr);
            }
            Libp2pEvent::ZeroListeners => {
                let _ = shutdown_sender
                    .send(ShutdownReason::Failure(
                        "All listeners are closed. Unable to listen",
                    ))
                    .await
                    .map_err(|e| {
                        warn!(
                            error = %e,
                            "failed to send a shutdown signal",
                        )
                    });
            }
        }
    }

    /// Handle a message sent to the network service.
    async fn on_network_msg(
        &mut self,
        msg: NetworkMessage,
        _shutdown_sender: &mut Sender<ShutdownReason>,
    ) {
        debug!(?msg, "Received new message");

        match msg {
            NetworkMessage::SendRequest {
                peer_id,
                request,
                request_id,
            } => {
                self.libp2p.send_request(peer_id, request_id, request);
            }
            NetworkMessage::SendResponse {
                peer_id,
                response,
                id,
            } => {
                self.libp2p.send_response(peer_id, id, response);
            }
            NetworkMessage::SendErrorResponse {
                peer_id,
                error,
                id,
                reason,
            } => {
                self.libp2p.respond_with_error(peer_id, id, error, reason);
            }
            NetworkMessage::Publish { messages } => {
                let mut topic_kinds = Vec::new();
                for message in &messages {
                    if !topic_kinds.contains(&message.kind()) {
                        topic_kinds.push(message.kind());
                    }
                }
                debug!(
                    count = messages.len(),
                    topics = ?topic_kinds,
                    "Sending pubsub messages",
                );
                self.libp2p.swarm.behaviour_mut().publish(messages);
            }
            NetworkMessage::ReportPeer {
                peer_id,
                action,
                source,
                msg,
            } => self.libp2p.report_peer(&peer_id, action, source, msg),
            NetworkMessage::GoodbyePeer {
                peer_id,
                reason,
                source,
            } => self.libp2p.goodbye_peer(&peer_id, reason, source),
        }
    }

    fn on_peer_connected(&mut self, peer_id: PeerId) {
        self.send_status(peer_id);
    }

    fn on_peer_disconnected(&mut self, _peer_id: PeerId) {
        // TODO
    }

    fn on_rpc_request(&mut self, peer_id: PeerId, request_id: PeerRequestId, request: Request) {
        if !self.network_globals.peers.read().is_connected(&peer_id) {
            debug!(%peer_id, ?request, "Dropping request of disconnected peer");
            return;
        }

        match request {
            Request::Status(status) => {
                self.on_status_request(peer_id, request_id, status);
            }
            Request::GetChunks(request) => {
                self.send_to_sync(SyncMessage::RequestChunks {
                    peer_id,
                    request_id,
                    request,
                });
            }
            Request::DataByHash(_) => {
                // ignore
            }
        }
    }

    fn on_rpc_response(&mut self, peer_id: PeerId, request_id: RequestId, response: Response) {
        match response {
            Response::Status(status_message) => {
                self.on_status_response(peer_id, status_message);
            }
            Response::Chunks(response) => {
                let request_id = match request_id {
                    RequestId::Sync(sync_id) => sync_id,
                    _ => unreachable!("All Chunks responses belong to sync"),
                };

                self.send_to_sync(SyncMessage::ChunksResponse {
                    peer_id,
                    request_id,
                    response,
                });
            }
            Response::DataByHash(_) => {
                // ignore
            }
        }
    }

    fn on_rpc_error(&mut self, peer_id: PeerId, request_id: RequestId) {
        // Check if the failed RPC belongs to sync
        if let RequestId::Sync(request_id) = request_id {
            self.send_to_sync(SyncMessage::RpcError {
                peer_id,
                request_id,
            });
        }
    }

    fn on_pubsub_message(&mut self, source: PeerId, id: MessageId, message: PubsubMessage) {
        match message {
            PubsubMessage::ExampleMessage(data) => {
                debug!(peer_id = %source, %id, ?data, "Received ExampleMessage gossip");
            }
        }
    }

    fn send_status(&mut self, peer_id: PeerId) {
        let status_message = StatusMessage { data: 123 }; // dummy status message
        debug!(%peer_id, ?status_message, "Sending Status request");

        self.send_to_network(NetworkMessage::SendRequest {
            peer_id,
            request_id: RequestId::Router,
            request: Request::Status(status_message),
        })
    }

    fn on_status_request(
        &mut self,
        peer_id: PeerId,
        request_id: PeerRequestId,
        status: StatusMessage,
    ) {
        debug!(%peer_id, ?status, "Received Status request");

        let status_message = StatusMessage { data: 456 }; // dummy status message
        debug!(%peer_id, ?status_message, "Sending Status response");

        self.send_to_network(NetworkMessage::SendResponse {
            peer_id,
            id: request_id,
            response: Response::Status(status_message),
        });
    }

    pub fn on_status_response(&mut self, peer_id: PeerId, status: StatusMessage) {
        debug!(%peer_id, ?status, "Received Status response");
    }
}

impl Drop for RouterService {
    fn drop(&mut self) {
        info!("Router service shutdown");
    }
}
