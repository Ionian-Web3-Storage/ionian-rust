#![allow(dead_code)]

use network::NetworkMessage;
use tokio::sync::mpsc;

pub(crate) struct SyncNetworkContext {
    network_send: mpsc::UnboundedSender<NetworkMessage>,
}

impl SyncNetworkContext {
    pub fn new(network_send: mpsc::UnboundedSender<NetworkMessage>) -> Self {
        Self { network_send }
    }

    /// Sends an arbitrary network message.
    pub fn send(&self, msg: NetworkMessage) {
        self.network_send.send(msg).unwrap_or_else(|_| {
            warn!("Could not send message to the network service");
        })
    }
}
