#[macro_use]
extern crate tracing;

mod handler;
mod mem_pool;

pub use handler::ChunkPoolHandler;
pub use mem_pool::MemoryChunkPool;

use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct Config {
    pub max_cached_chunks_per_file: usize,
    pub max_cached_chunks_all: usize,
    pub max_writings: usize,
    pub expiration_time_secs: u64,
}

pub fn unbounded(
    config: Config,
    log_store: storage_async::Store,
) -> (Arc<MemoryChunkPool>, ChunkPoolHandler) {
    let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();

    let mem_pool = Arc::new(mem_pool::MemoryChunkPool::new(
        config,
        log_store.clone(),
        sender,
    ));
    let handler = handler::ChunkPoolHandler::new(receiver, mem_pool.clone(), log_store);

    (mem_pool, handler)
}
