#[macro_use]
extern crate tracing;

use anyhow::bail;
use shared_types::{Chunk, ChunkArray};
use std::sync::Arc;
use storage::{error, error::Result, log_store::Store as LogStore};
use task_executor::TaskExecutor;
use tokio::sync::oneshot;

/// The name of the worker tokio tasks.
const WORKER_TASK_NAME: &str = "async_storage_worker";

macro_rules! delegate {
    ($name:tt($($v:ident: $t:ty),*)) => {
        delegate!($name($($v: $t),*) -> ());
    };

    ($name:tt($($v:ident: $t:ty),*) -> $ret:ty) => {
        pub async fn $name(&self, $($v: $t),*) -> $ret {
            let store = self.store.clone();
            let (tx, rx) = oneshot::channel();

            self.executor.spawn_blocking(
                move || {
                    let res = store.$name($($v),*);

                    if let Err(_) = tx.send(res) {
                        error!("Unable to complete async storage operation: the receiver dropped");
                    }
                },
                WORKER_TASK_NAME,
            );

            rx.await.unwrap_or_else(|_| bail!(error::Error::Custom("Receiver error".to_string())))
        }
    };
}

#[derive(Clone)]
pub struct Store {
    /// Log and transaction storage.
    store: Arc<dyn LogStore>,

    /// Tokio executor for spawning worker tasks.
    executor: TaskExecutor,
}

impl Store {
    pub fn new(store: Arc<dyn LogStore>, executor: TaskExecutor) -> Self {
        Store { store, executor }
    }

    delegate!(get_chunk_by_tx_and_index(tx_seq: u64, index: usize) -> Result<Option<Chunk>>);
    delegate!(get_chunks_by_tx_and_index_range(tx_seq: u64, index_start: usize, index_end: usize) -> Result<Option<ChunkArray>>);
    delegate!(put_chunks(tx_seq: u64, chunks: ChunkArray) -> Result<()>);
}
