use crate::error;
use crate::router::{Router, RouterMessage};
use futures::channel::mpsc::Sender;
use futures::prelude::*;
use network::Service as LibP2PService;
use network::{BehaviourEvent, NetworkGlobals};
use network::{Context, Libp2pEvent, NetworkConfig};
use network::{RequestId, ServiceMessage};
use std::sync::Arc;
use storage::log_store::Store;
use task_executor::ShutdownReason;
use tokio::sync::mpsc;

/// Service that handles communication between internal services and the libp2p service.
pub struct NetworkService {
    /// The underlying libp2p service that drives all the network interactions.
    libp2p: LibP2PService<RequestId>,

    /// The receiver channel for Ionian to communicate with the network service.
    network_recv: mpsc::UnboundedReceiver<ServiceMessage>,

    /// The sending channel for the network service to send messages
    /// to be routed throughout Ionian.
    router_send: mpsc::UnboundedSender<RouterMessage>,

    /// A collection of global variables, accessible outside of the network service.
    network_globals: Arc<NetworkGlobals>,
}

impl NetworkService {
    pub async fn start(
        config: &NetworkConfig,
        executor: task_executor::TaskExecutor,
        store: Arc<dyn Store>,
    ) -> error::Result<(Arc<NetworkGlobals>, mpsc::UnboundedSender<ServiceMessage>)> {
        let (network_send, network_recv) = mpsc::unbounded_channel::<ServiceMessage>();

        // construct the libp2p service context
        let service_context = Context { config };

        // launch libp2p service
        let (network_globals, libp2p) =
            LibP2PService::new(executor.clone(), service_context).await?;

        // launch router task
        let router_send = Router::spawn(
            network_globals.clone(),
            network_send.clone(),
            executor.clone(),
            store,
        )?;

        // create the network service and spawn the task
        let network_service = NetworkService {
            libp2p,
            network_recv,
            router_send,
            network_globals: network_globals.clone(),
        };

        network_service.spawn_service(executor);

        Ok((network_globals, network_send))
    }

    fn send_to_router(&mut self, msg: RouterMessage) {
        if let Err(mpsc::error::SendError(msg)) = self.router_send.send(msg) {
            debug!(?msg, "Failed to send msg to router");
        }
    }

    fn spawn_service(mut self, executor: task_executor::TaskExecutor) {
        let mut shutdown_sender = executor.shutdown_sender();

        // spawn on the current executor
        let service_fut = async move {
            loop {
                tokio::select! {
                    // handle a message sent to the network
                    Some(msg) = self.network_recv.recv() => self.on_network_msg(msg, &mut shutdown_sender).await,

                    // handle event coming from the network
                    event = self.libp2p.next_event() => self.on_libp2p_event(event, &mut shutdown_sender).await,
                }
            }
        };
        executor.spawn(service_fut, "network_service");
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
                    self.send_to_router(RouterMessage::PeerDialed(peer_id));
                }
                BehaviourEvent::PeerConnectedIncoming(_)
                | BehaviourEvent::PeerBanned(_)
                | BehaviourEvent::PeerUnbanned(_) => {
                    // No action required for these events.
                }
                BehaviourEvent::PeerDisconnected(peer_id) => {
                    self.send_to_router(RouterMessage::PeerDisconnected(peer_id));
                }
                BehaviourEvent::RequestReceived {
                    peer_id,
                    id,
                    request,
                } => {
                    self.send_to_router(RouterMessage::RPCRequestReceived {
                        peer_id,
                        id,
                        request,
                    });
                }
                BehaviourEvent::ResponseReceived {
                    peer_id,
                    id,
                    response,
                } => {
                    self.send_to_router(RouterMessage::RPCResponseReceived {
                        peer_id,
                        request_id: id,
                        response,
                    });
                }
                BehaviourEvent::RPCFailed { id, peer_id } => {
                    self.send_to_router(RouterMessage::RPCFailed {
                        peer_id,
                        request_id: id,
                    });
                }
                BehaviourEvent::StatusPeer(peer_id) => {
                    self.send_to_router(RouterMessage::StatusPeer(peer_id));
                }
                BehaviourEvent::PubsubMessage {
                    id,
                    source,
                    message,
                    ..
                } => {
                    self.send_to_router(RouterMessage::PubsubMessage(id, source, message));
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
        msg: ServiceMessage,
        _shutdown_sender: &mut Sender<ShutdownReason>,
    ) {
        debug!(?msg, "Received new message");

        match msg {
            ServiceMessage::SendRequest {
                peer_id,
                request,
                request_id,
            } => {
                self.libp2p.send_request(peer_id, request_id, request);
            }
            ServiceMessage::SendResponse {
                peer_id,
                response,
                id,
            } => {
                self.libp2p.send_response(peer_id, id, response);
            }
            ServiceMessage::SendErrorResponse {
                peer_id,
                error,
                id,
                reason,
            } => {
                self.libp2p.respond_with_error(peer_id, id, error, reason);
            }
            ServiceMessage::Publish { messages } => {
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
            ServiceMessage::ReportPeer {
                peer_id,
                action,
                source,
                msg,
            } => self.libp2p.report_peer(&peer_id, action, source, msg),
            ServiceMessage::GoodbyePeer {
                peer_id,
                reason,
                source,
            } => self.libp2p.goodbye_peer(&peer_id, reason, source),
        }
    }
}

impl Drop for NetworkService {
    fn drop(&mut self) {
        info!("Network service shutdown");
    }
}
