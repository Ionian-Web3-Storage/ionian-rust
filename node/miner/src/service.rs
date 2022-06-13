use crate::MinerNetworkContext;
use network::ServiceMessage;
use tokio::sync::mpsc;

const HEARTBEAT_INTERVAL_SEC: u64 = 10;

#[derive(Debug)]
pub enum MinerMessage {
    Test,
}

pub struct MinerService {
    /// A receiving channel sent by the message processor thread.
    msg_recv: mpsc::UnboundedReceiver<MinerMessage>,

    /// A network context to contact the network service.
    #[allow(dead_code)]
    network: MinerNetworkContext,

    /// Heartbeat interval for periodically checking on-chain data.
    heartbeat: tokio::time::Interval,
}

impl MinerService {
    pub fn spawn(
        executor: task_executor::TaskExecutor,
        network_send: mpsc::UnboundedSender<ServiceMessage>,
    ) -> mpsc::UnboundedSender<MinerMessage> {
        let (miner_send, miner_recv) = mpsc::unbounded_channel::<MinerMessage>();

        let heartbeat =
            tokio::time::interval(tokio::time::Duration::from_secs(HEARTBEAT_INTERVAL_SEC));

        let mut miner = MinerService {
            msg_recv: miner_recv,
            network: MinerNetworkContext::new(network_send),
            heartbeat,
        };

        debug!("Starting miner service");
        executor.spawn(async move { Box::pin(miner.main()).await }, "miner");

        miner_send
    }

    async fn main(&mut self) {
        loop {
            tokio::select! {
                // handle a message from the network
                maybe_msg = self.msg_recv.recv() => {
                    if let Some(msg) = maybe_msg {
                        warn!("Miner received message {:?}", msg);
                    }
                }

                // periodic checks
                _ = self.heartbeat.tick() => {
                    debug!("Miner heartbeat");
                }
            }
        }
    }
}
