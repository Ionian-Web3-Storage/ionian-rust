use crate::sync_manager::config::LogSyncConfig;
use crate::sync_manager::log_entry_fetcher::LogEntryFetcher;
use anyhow::Result;
use jsonrpsee::tracing::{debug, error, info};
use shared_types::Transaction;
use std::future::Future;
use std::sync::Arc;
use storage::log_store::Store;
use task_executor::{ShutdownReason, TaskExecutor};
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};

pub struct LogSyncManager {
    config: LogSyncConfig,
    log_fetcher: LogEntryFetcher,

    next_tx_seq: u64,
}

impl LogSyncManager {
    pub fn spawn(
        config: LogSyncConfig,
        executor: TaskExecutor,
        store: Arc<dyn Store>,
    ) -> Result<()> {
        let next_tx_seq = store.next_tx_seq()?;
        let (tx, mut rx) = unbounded_channel();

        // Spawn the task to sync log entries from the blockchain.
        executor.spawn(
            run_and_log(executor.shutdown_sender(), async move {
                let log_fetcher =
                    LogEntryFetcher::new(&config.rpc_endpoint_url, config.contract_address).await?;
                let mut log_sync_manager = Self {
                    config,
                    log_fetcher,
                    next_tx_seq,
                };

                // TODO: Do we need to notify that the recover process completes?
                log_sync_manager
                    .fetch_to_end(tx.clone(), next_tx_seq)
                    .await?;
                log_sync_manager.sync(tx).await?;
                Ok(())
            }),
            "log_sync",
        );

        // Spawn the task to persist the downloaded log entries.
        executor.spawn(
            run_and_log(executor.shutdown_sender(), async move {
                while let Some(tx) = rx.recv().await {
                    store.put_tx(tx)?;
                }
                Ok(())
            }),
            "log_write",
        );
        Ok(())
    }

    async fn fetch_to_end(
        &mut self,
        sender: UnboundedSender<Transaction>,
        start_tx_seq: u64,
    ) -> Result<()> {
        let end_tx_seq = self.log_fetcher.num_log_entries().await?;
        info!("start_tx_seq={} end_tx_seq={}", start_tx_seq, end_tx_seq);
        for i in (start_tx_seq..end_tx_seq).step_by(self.config.fetch_batch_size) {
            let log_list = self
                .log_fetcher
                .entry_at(i, Some(self.config.fetch_batch_size))
                .await?;
            let log_len = log_list.len();
            for log in log_list {
                sender.send(log)?;
            }
            self.next_tx_seq = i + log_len as u64;
            if log_len != self.config.fetch_batch_size {
                debug!(
                    "last batch incomplete: get={} limit={}",
                    log_len, self.config.fetch_batch_size
                );
                break;
            }
        }
        Ok(())
    }

    async fn sync(&mut self, sender: UnboundedSender<Transaction>) -> Result<()> {
        loop {
            self.fetch_to_end(sender.clone(), self.next_tx_seq).await?;
            tokio::time::sleep(self.config.sync_period).await;
        }
    }
}

async fn run_and_log(
    mut shutdown_sender: futures::channel::mpsc::Sender<ShutdownReason>,
    f: impl Future<Output = Result<()>> + Send + 'static,
) {
    if let Err(e) = f.await {
        error!("log sync failure: e={:?}", e);
        shutdown_sender
            .try_send(ShutdownReason::Failure("log sync failure"))
            .expect("shutdown send error");
    }
}

pub(crate) mod config;
mod log_entry_fetcher;
