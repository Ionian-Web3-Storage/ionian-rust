use crate::SyncNetworkContext;
use network::PeerId;
use network::ServiceMessage;
use tokio::sync::mpsc;

#[derive(Debug)]
pub enum SyncMessage {
    Test { peer_id: PeerId },
}

pub struct SyncService {
    /// A receiving channel sent by the message processor thread.
    msg_recv: mpsc::UnboundedReceiver<SyncMessage>,

    /// A network context to contact the network service.
    #[allow(dead_code)]
    network: SyncNetworkContext,
}

impl SyncService {
    pub fn spawn(
        executor: task_executor::TaskExecutor,
        network_send: mpsc::UnboundedSender<ServiceMessage>,
    ) -> mpsc::UnboundedSender<SyncMessage> {
        let (sync_send, sync_recv) = mpsc::unbounded_channel::<SyncMessage>();

        let mut sync = SyncService {
            msg_recv: sync_recv,
            network: SyncNetworkContext::new(network_send),
        };

        debug!("Starting sync service");
        executor.spawn(async move { Box::pin(sync.main()).await }, "sync");

        sync_send
    }

    async fn main(&mut self) {
        // process any inbound messages
        loop {
            if let Some(msg) = self.msg_recv.recv().await {
                warn!("Sync received message {:?}", msg);
            }
        }
    }
}
