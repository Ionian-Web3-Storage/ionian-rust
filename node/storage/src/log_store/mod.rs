use shared_types::{Chunk, ChunkArray, ChunkArrayWithProof, ChunkWithProof, Transaction};

use crate::error::Result;

mod simple_log_store;
pub use simple_log_store::SimpleLogStore;
#[cfg(test)]
mod tests;

/// The trait to read the transactions already appended to the log.
///
/// Implementation Rationale:
/// If the stored chunk is large, we can store the proof together with the chunk.
pub trait LogStoreRead: LogStoreChunkRead {
    /// Get a transaction by its global log sequence number.
    fn get_tx_by_seq_number(&self, seq: u64) -> Result<Option<Transaction>>;

    fn get_chunk_with_proof_by_tx_and_index(
        &self,
        tx_seq: u64,
        index: usize,
    ) -> Result<Option<ChunkWithProof>>;

    fn get_chunks_with_proof_by_tx_and_index_range(
        &self,
        tx_seq: u64,
        index_start: usize,
        index_end: usize,
    ) -> Result<Option<ChunkArrayWithProof>>;

    fn check_tx_completed(&self, tx_seq: u64) -> Result<bool>;
}

pub trait LogStoreChunkRead {
    /// Get a data chunk by the transaction sequence number and the chunk offset in the transaction.
    /// Accessing a single chunk is mostly used for mining.
    fn get_chunk_by_tx_and_index(&self, tx_seq: u64, index: usize) -> Result<Option<Chunk>>;

    /// Get a list of continuous chunks by the transaction sequence number and an index range (`index_end` excluded).
    fn get_chunks_by_tx_and_index_range(
        &self,
        tx_seq: u64,
        index_start: usize,
        index_end: usize,
    ) -> Result<Option<ChunkArray>>;

    fn get_chunk_index_list(&self, tx_seq: u64) -> Result<Vec<usize>>;
}

pub trait LogStoreWrite: LogStoreChunkWrite {
    /// Store a data entry metadata.
    fn put_tx(&self, tx: Transaction) -> Result<()>;
    /// Finalize a transaction storage.
    /// This will compute and the merkle tree, check the data root, and persist a part of the merkle
    /// tree for future queries.
    ///
    /// This will return error if not all chunks are stored. But since this check can be expensive,
    /// the caller is supposed to track chunk statuses and call this after storing all the chunks.
    fn finalize_tx(&self, tx_seq: u64) -> Result<()>;
}

pub trait LogStoreChunkWrite {
    /// Store data chunks of a data entry.
    fn put_chunks(&self, tx_seq: u64, chunks: ChunkArray) -> Result<()>;

    /// Delete all chunks of a tx.
    fn remove_all_chunks(&self, tx_seq: u64) -> Result<()>;
}

pub trait LogChunkStore: LogStoreChunkRead + LogStoreChunkWrite + Send + Sync + 'static {}
impl<T: LogStoreChunkRead + LogStoreChunkWrite + Send + Sync + 'static> LogChunkStore for T {}

pub trait Store: LogStoreRead + LogStoreChunkWrite + Send + Sync + 'static {}
impl<T: LogStoreRead + LogStoreChunkWrite + Send + Sync + 'static> Store for T {}
