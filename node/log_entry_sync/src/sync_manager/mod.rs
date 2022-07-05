use crate::sync_manager::config::SyncManagerConfig;
use crate::sync_manager::log_entry_fetcher::LogEntryFetcher;
use anyhow::Result;
use jsonrpsee::tracing::error;
use shared_types::Transaction;
use task_executor::{ShutdownReason, TaskExecutor};
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::{
    channel, unbounded_channel, Receiver, Sender, UnboundedReceiver, UnboundedSender,
};

struct LogSyncManager {
    config: SyncManagerConfig,
    log_fetcher: LogEntryFetcher,

    next_tx_seq: u64,
    stop_rx: Receiver<()>,
}

impl LogSyncManager {
    #[allow(unused)]
    pub async fn spawn(
        config: SyncManagerConfig,
        executor: TaskExecutor,
        start_tx_seq: u64,
    ) -> Result<(UnboundedReceiver<Transaction>, Sender<()>)> {
        let log_fetcher =
            LogEntryFetcher::new(&config.rpc_endpoint_url, config.contract_address).await?;
        let (signal_tx, signal_rx) = channel(1);
        let mut log_sync_manager = Self {
            config,
            log_fetcher,
            next_tx_seq: start_tx_seq,
            stop_rx: signal_rx,
        };
        let (tx, rx) = unbounded_channel();
        let mut shutdown_sender = executor.shutdown_sender();
        executor.spawn(
            async move {
                // TODO: Do we need to notify that the recover process completes?
                if let Err(e) = log_sync_manager
                    .fetch_to_end(tx.clone(), start_tx_seq)
                    .await
                {
                    error!("log sync recovery failure: e={:?}", e);
                    shutdown_sender
                        .try_send(ShutdownReason::Failure("log sync recovery failure"))
                        .expect("shutdown send error");
                }
                if let Err(e) = log_sync_manager.sync(tx).await {
                    error!("log sync failure: e={:?}", e);
                    shutdown_sender
                        .try_send(ShutdownReason::Failure("log sync failure"))
                        .expect("shutdown send error");
                }
            },
            "log_sync",
        );
        Ok((rx, signal_tx))
    }

    pub async fn fetch_to_end(
        &mut self,
        sender: UnboundedSender<Transaction>,
        start_tx_seq: u64,
    ) -> Result<()> {
        let end_tx_seq = self.log_fetcher.num_log_entries().await?;
        for i in (start_tx_seq..end_tx_seq).step_by(self.config.fetch_batch_size) {
            if self.stop_rx.try_recv() != Err(TryRecvError::Empty) {
                break;
            }
            let log_list = self
                .log_fetcher
                .entry_at(i, Some(self.config.fetch_batch_size))
                .await?;
            for log in log_list {
                sender.send(log)?;
            }
            self.next_tx_seq = i;
        }
        Ok(())
    }

    pub async fn sync(&mut self, sender: UnboundedSender<Transaction>) -> Result<()> {
        loop {
            if self.stop_rx.try_recv() != Err(TryRecvError::Empty) {
                return Ok(());
            }
            self.fetch_to_end(sender.clone(), self.next_tx_seq).await?;
            tokio::time::sleep(self.config.sync_period).await;
        }
    }
}

mod config;
mod log_entry_fetcher;
#[tokio::test]
async fn test_recover() {}
