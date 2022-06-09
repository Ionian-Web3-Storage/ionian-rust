#![allow(dead_code)]

use network::ServiceMessage;
use tokio::sync::mpsc;

pub(crate) struct MinerNetworkContext {
    network_send: mpsc::UnboundedSender<ServiceMessage>,
}

impl MinerNetworkContext {
    pub fn new(network_send: mpsc::UnboundedSender<ServiceMessage>) -> Self {
        Self { network_send }
    }

    /// Sends an arbitrary network message.
    pub fn send(&mut self, msg: ServiceMessage) -> Result<(), &'static str> {
        self.network_send.send(msg).map_err(|_| {
            debug!("Could not send message to the network service");
            "Network channel send Failed"
        })
    }
}
