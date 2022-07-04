use crate::rpc_proxy::cfx::CfxClient;
use crate::rpc_proxy::eth::EthClient;
use crate::rpc_proxy::{Address, EvmRpcProxy};
use crate::sync_manager::config::SyncManagerConfig;
use crate::sync_manager::log_entry_fetcher::LogEntryFetcher;
use anyhow::{anyhow, Result};
use ethers::prelude::{Provider, Ws};
use shared_types::Transaction;
use std::sync::Arc;
use task_executor::TaskExecutor;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

struct LogSyncManager {
    log_fetcher: LogEntryFetcher,
}

impl LogSyncManager {
    pub async fn spawn(
        config: &SyncManagerConfig,
        executor: TaskExecutor,
    ) -> Result<UnboundedReceiver<Transaction>> {
        let log_sync_manager = Self {
            log_fetcher: LogEntryFetcher::new(&config.rpc_endpoint_url, config.contract_address)
                .await?,
        };
        let (tx, rx) = unbounded_channel();
        executor.spawn(
            async move {
                log_sync_manager.recover(tx.clone()).await;
                log_sync_manager.sync(tx).await;
            },
            "log_sync",
        );
        Ok(rx)
    }

    pub async fn recover(&self, sender: UnboundedSender<Transaction>) {
        todo!()
    }

    pub async fn sync(&self, sender: UnboundedSender<Transaction>) {
        todo!()
    }
}

mod config;
mod log_entry_fetcher;
#[tokio::test]
async fn test_recover() {}
