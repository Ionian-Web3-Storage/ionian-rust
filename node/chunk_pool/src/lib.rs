#[macro_use]
extern crate tracing;

mod handler;
mod mem_pool;

pub use handler::ChunkPoolHandler;
pub use mem_pool::MemoryChunkPool;

use std::sync::Arc;

pub const NUM_CHUNKS_PER_SEGMENT: usize = 1024;

pub fn unbounded(log_store: Arc<storage_async::Store>) -> (Arc<MemoryChunkPool>, ChunkPoolHandler) {
    let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();

    let mem_pool = Arc::new(mem_pool::MemoryChunkPool::new(log_store.clone(), sender));
    let handler = handler::ChunkPoolHandler::new(receiver, mem_pool.clone(), log_store);

    (mem_pool, handler)
}
