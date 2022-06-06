use kvdb::KeyValueDB;

use shared_types::{Chunk, ChunkArray, Transaction, TransactionHash};

use crate::error::Result;

/// The trait to read the transactions already appended to the log.
///
/// Implementation Rationale:
/// If the stored chunk is large, we can store the proof together with the chunk.
pub trait LogStoreRead: LogStoreChunkRead {
    /// Get a transaction by its hash.
    fn get_tx_by_hash(&self, hash: &TransactionHash) -> Result<Transaction>;

    /// Get a transaction by its global log sequence number.
    fn get_tx_by_seq_number(&self, seq: u64) -> Result<Transaction>;
}

pub trait LogStoreChunkRead {
    /// Get a data chunk by the transaction sequence number and the chunk offset in the transaction.
    /// Accessing a single chunk is mostly used for mining.
    fn get_chunk_by_tx_and_index(&self, tx_seq: u64, index: u32) -> Result<Option<Chunk>>;

    /// Get a list of continuous chunks by the transaction sequence number and an index range (`index_end` excluded).
    fn get_chunks_by_tx_and_index_range(
        &self,
        tx_seq: u64,
        index_start: u32,
        index_end: u32,
    ) -> Result<Option<ChunkArray>>;
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
}

pub trait LogChunkStore: LogStoreChunkRead + LogStoreChunkWrite {}
impl<T: LogStoreChunkRead + LogStoreChunkWrite> LogChunkStore for T {}

mod simple_log_store;
