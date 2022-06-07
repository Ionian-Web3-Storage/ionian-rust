use crate::error::{Error, Result};
use crate::log_store::{LogChunkStore, LogStoreChunkRead, LogStoreChunkWrite, LogStoreWrite};
use crate::IonianKeyValueDB;
use kvdb_rocksdb::{Database, DatabaseConfig};
use shared_types::{Chunk, ChunkArray, DataRoot, Transaction, TransactionHash, CHUNK_SIZE};
use ssz::{Decode, Encode};
use std::cmp;
use std::path::Path;
use std::sync::Arc;

const COL_TX: u32 = 0;
const COL_CHUNK: u32 = 1;
const COL_NUM: u32 = 2;
// A chunk key is the concatenation of tx_seq(u64) and start_index(u32)
const CHUNK_KEY_SIZE: usize = 8 + 4;
const CHUNK_BATCH_SIZE: usize = 1024;

pub struct SimpleLogStore {
    kvdb: Arc<dyn IonianKeyValueDB>,
    chunk_store: Arc<dyn LogChunkStore>,
}

pub struct BatchChunkStore {
    kvdb: Arc<dyn IonianKeyValueDB>,
    batch_size: usize,
}

/// This represents a batch of chunks where the batch boundary is a multiple of batch size (except the last batch of a data entry).
struct BatchChunks {
    data: Vec<u8>,
    batch_merkle_root: DataRoot,
}

impl LogStoreChunkWrite for BatchChunkStore {
    /// For implementation simplicity and performance reasons, all chunks in a batch must be stored at once,
    /// meaning the caller need to process and store chunks with a batch size that is a multiple of `self.batch_size`
    /// and so is the batch boundary.
    fn put_chunks(&self, tx_seq: u64, chunks: ChunkArray) -> Result<()> {
        if chunks.start_index % self.batch_size as u32 != 0 {
            return Err(Error::InvalidBatchBoundary);
        }
        // TODO: If `chunks.end_index` is not in the boundary, we just assume it's the end for now.
        for index in (chunks.start_index..chunks.end_index).step_by(self.batch_size) {
            let key = chunk_key(tx_seq, index);
            let end = cmp::min(index + self.batch_size as u32, chunks.end_index) as usize;
            self.kvdb
                .put(COL_CHUNK, &key, &chunks.data[index as usize..end])?;
        }
        Ok(())
    }
}

impl LogStoreChunkRead for BatchChunkStore {
    fn get_chunk_by_tx_and_index(&self, tx_seq: u64, index: u32) -> Result<Option<Chunk>> {
        let maybe_chunk = self
            .get_chunks_by_tx_and_index_range(tx_seq, index, index + 1)?
            .map(|chunk_array| Chunk(chunk_array.data.try_into().expect("chunk data size match")));
        Ok(maybe_chunk)
    }

    fn get_chunks_by_tx_and_index_range(
        &self,
        tx_seq: u64,
        index_start: u32,
        index_end: u32,
    ) -> Result<Option<ChunkArray>> {
        if index_end <= index_start {
            return Err(Error::InvalidBatchBoundary);
        }
        let mut data = Vec::with_capacity(((index_end - index_end) as usize * CHUNK_SIZE) as usize);
        for index in (index_start..index_end).step_by(self.batch_size) {
            let key_index = index / self.batch_size as u32;
            let key = chunk_key(tx_seq, key_index);
            let end = cmp::min(key_index + self.batch_size as u32, index_end) as usize;
            let batch_data = self.kvdb.get(COL_CHUNK, &key)?;
            if batch_data.is_none() {
                return Ok(None);
            }
            data.extend_from_slice(
                &batch_data.unwrap()[index as usize % self.batch_size..end % self.batch_size],
            );
        }
        Ok(Some(ChunkArray {
            data,
            start_index: index_start,
            end_index: index_end,
        }))
    }
}

impl SimpleLogStore {
    pub fn open(path: &Path) -> Result<Self> {
        let mut config = DatabaseConfig::with_columns(COL_NUM);
        config.enable_statistics = true;
        let kvdb = Arc::new(Database::open(&config, path)?);
        Ok(Self {
            kvdb: kvdb.clone(),
            chunk_store: Arc::new(BatchChunkStore {
                kvdb,
                batch_size: CHUNK_BATCH_SIZE,
            }),
        })
    }
}

impl LogStoreChunkRead for SimpleLogStore {
    fn get_chunk_by_tx_and_index(&self, tx_seq: u64, index: u32) -> Result<Option<Chunk>> {
        self.chunk_store.get_chunk_by_tx_and_index(tx_seq, index)
    }

    fn get_chunks_by_tx_and_index_range(
        &self,
        tx_seq: u64,
        index_start: u32,
        index_end: u32,
    ) -> Result<Option<ChunkArray>> {
        self.chunk_store
            .get_chunks_by_tx_and_index_range(tx_seq, index_start, index_end)
    }
}

impl LogStoreChunkWrite for SimpleLogStore {
    fn put_chunks(&self, tx_seq: u64, chunks: ChunkArray) -> Result<()> {
        self.chunk_store.put_chunks(tx_seq, chunks)
    }
}

impl LogStoreWrite for SimpleLogStore {
    fn put_tx(&self, mut tx: Transaction) -> Result<()> {
        if *tx.hash() == TransactionHash::default() {
            tx.compute_hash();
        }
        let tx_hash = tx.hash();
        self.kvdb
            .put(COL_TX, tx_hash.as_bytes(), &tx.as_ssz_bytes())
            .map_err(Into::into)
    }

    fn finalize_tx(&self, tx_seq: u64) -> Result<()> {
        todo!()
    }
}

fn chunk_key(tx_seq: u64, index: u32) -> [u8; CHUNK_KEY_SIZE] {
    let mut key = [0u8; CHUNK_KEY_SIZE];
    key[0..8].copy_from_slice(&tx_seq.to_be_bytes());
    key[9..12].copy_from_slice(&index.to_be_bytes());
    key
}
