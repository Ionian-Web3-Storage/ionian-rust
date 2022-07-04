use crate::sync_manager::config::SyncManagerConfig;
use crate::sync_manager::log_entry_fetcher::LogEntryFetcher;
use anyhow::Result;
use shared_types::Transaction;
use std::sync::atomic;
use std::sync::atomic::AtomicU64;
use task_executor::TaskExecutor;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::{
    channel, unbounded_channel, Receiver, Sender, UnboundedReceiver, UnboundedSender,
};

struct LogSyncManager {
    config: SyncManagerConfig,
    log_fetcher: LogEntryFetcher,

    next_tx_seq: AtomicU64,
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
            next_tx_seq: start_tx_seq.into(),
            stop_rx: signal_rx,
        };
        let (tx, rx) = unbounded_channel();
        // TODO: How to handle error here?
        executor.spawn(
            async move {
                // TODO: Do we need to notify that the recover process completes?
                log_sync_manager
                    .fetch_to_end(tx.clone(), start_tx_seq)
                    .await
                    .unwrap();
                log_sync_manager.sync(tx).await.unwrap();
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
            self.next_tx_seq.store(i, atomic::Ordering::SeqCst);
        }
        Ok(())
    }

    pub async fn sync(&mut self, sender: UnboundedSender<Transaction>) -> Result<()> {
        // TODO: Use pubsub instead of periodic pull.
        loop {
            if self.stop_rx.try_recv() != Err(TryRecvError::Empty) {
                return Ok(());
            }
            let start_tx_seq = self.next_tx_seq.load(atomic::Ordering::SeqCst);
            self.fetch_to_end(sender.clone(), start_tx_seq).await?;
            tokio::time::sleep(self.config.sync_period).await;
        }
    }
}

mod config;
mod log_entry_fetcher;
#[tokio::test]
async fn test_recover() {}
