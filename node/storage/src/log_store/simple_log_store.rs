use crate::error::{Error, Result};
use crate::log_store::{
    LogChunkStore, LogStoreChunkRead, LogStoreChunkWrite, LogStoreRead, LogStoreWrite,
};
use crate::IonianKeyValueDB;
use kvdb_rocksdb::{Database, DatabaseConfig};
use merkle_tree::Sha3Algorithm;
use merkletree::merkle::MerkleTree;
use merkletree::store::VecStore;
use shared_types::{
    Chunk, ChunkArray, ChunkArrayWithProof, ChunkWithProof, DataRoot, Transaction, TransactionHash,
    CHUNK_SIZE,
};
use ssz::{Decode, DecodeError, Encode};
use std::cmp;
use std::path::Path;
use std::sync::Arc;

const COL_TX: u32 = 0;
const COL_TX_HASH_INDEX: u32 = 1;
const COL_TX_MERKLE: u32 = 2;
const COL_CHUNK: u32 = 3;
const COL_NUM: u32 = 4;
// A chunk key is the concatenation of tx_seq(u64) and start_index(u32)
const CHUNK_KEY_SIZE: usize = 8 + 4;
const CHUNK_BATCH_SIZE: usize = 1024;

pub struct SimpleLogStore {
    kvdb: Arc<dyn IonianKeyValueDB>,
    chunk_store: Arc<dyn LogChunkStore>,
    chunk_batch_size: usize,
}

pub struct BatchChunkStore {
    kvdb: Arc<dyn IonianKeyValueDB>,
    batch_size: usize,
}

impl LogStoreChunkWrite for BatchChunkStore {
    /// For implementation simplicity and performance reasons, all chunks in a batch must be stored at once,
    /// meaning the caller need to process and store chunks with a batch size that is a multiple of `self.batch_size`
    /// and so is the batch boundary.
    fn put_chunks(&self, tx_seq: u64, chunks: ChunkArray) -> Result<()> {
        if chunks.start_index % self.batch_size as u32 != 0 {
            return Err(Error::InvalidBatchBoundary);
        }
        let mut tx = self.kvdb.transaction();
        // TODO: If `chunks.end_index` is not in the boundary, we just assume it's the end for now.
        for index in (chunks.start_index..chunks.end_index).step_by(self.batch_size) {
            let key = chunk_key(tx_seq, index);
            let end = cmp::min(index + self.batch_size as u32, chunks.end_index) as usize;
            tx.put(COL_CHUNK, &key, &chunks.data[index as usize..end]);
        }
        self.kvdb.write(tx)?;
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
        let mut data =
            Vec::with_capacity(((index_start - index_end) as usize * CHUNK_SIZE) as usize);
        for index in (index_start..index_end).step_by(self.batch_size) {
            let key_index = index / self.batch_size as u32;
            let key = chunk_key(tx_seq, key_index);
            let end = cmp::min(key_index + self.batch_size as u32, index_end) as usize;
            let maybe_batch_data = self.kvdb.get(COL_CHUNK, &key)?;
            if maybe_batch_data.is_none() {
                return Ok(None);
            }
            let batch_data = maybe_batch_data.unwrap();
            let batch_end = end % self.batch_size;
            if batch_data.len() < batch_end {
                return Err(Error::Custom(format!(
                    "read a partial chunk batch: size={}, expected={}",
                    batch_data.len(),
                    batch_end
                )));
            }
            data.extend_from_slice(&batch_data[index as usize % self.batch_size..batch_end]);
        }
        Ok(Some(ChunkArray {
            data,
            start_index: index_start,
            end_index: index_end,
        }))
    }
}

impl SimpleLogStore {
    #[allow(unused)]
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
            chunk_batch_size: CHUNK_BATCH_SIZE,
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
        let mut db_tx = self.kvdb.transaction();
        db_tx.put(COL_TX, &tx.seq.to_be_bytes(), &tx.as_ssz_bytes());
        db_tx.put(COL_TX_HASH_INDEX, tx_hash.as_bytes(), &tx.seq.to_be_bytes());
        self.kvdb.write(db_tx).map_err(Into::into)
    }

    fn finalize_tx(&self, tx_seq: u64) -> Result<()> {
        let maybe_tx = self.get_tx_by_seq_number(tx_seq)?;
        if maybe_tx.is_none() {
            return Err(Error::Custom(format!(
                "finalize_tx: tx not in db, tx_seq={}",
                tx_seq
            )));
        }
        let tx = maybe_tx.unwrap();
        let mut chunk_index_end = tx.size as usize / CHUNK_SIZE;
        if chunk_index_end * CHUNK_SIZE < tx.size as usize {
            chunk_index_end += 1;
        }
        let mut chunk_batch_roots = Vec::with_capacity(chunk_index_end / self.chunk_batch_size + 1);
        for batch_start_index in (0..chunk_index_end).step_by(self.chunk_batch_size) {
            let batch_end_index =
                cmp::min(batch_start_index + self.chunk_batch_size, chunk_index_end);
            let chunks = self.chunk_store.get_chunks_by_tx_and_index_range(
                tx_seq,
                batch_start_index as u32,
                batch_end_index as u32,
            )?;
            if chunks.is_none() {
                return Err(Error::Custom(format!(
                    "finalize_tx: chunk batch not in db, start_index={}",
                    batch_start_index
                )));
            }
            let chunks_iter = chunks.as_ref().unwrap().data.chunks(CHUNK_SIZE);
            let merkle_tree = MerkleTree::<[u8; 32], Sha3Algorithm, VecStore<_>>::try_from_iter(
                chunks_iter.map(|chunk| chunk.try_into().map_err(|e| anyhow::anyhow!("{}", e))),
            )
            .map_err(|e| Error::Custom(format!("{:?}", e)))?;
            let merkle_root = merkle_tree.root();
            chunk_batch_roots.push(merkle_root);
        }
        let merkle_tree = MerkleTree::<[u8; 32], Sha3Algorithm, VecStore<_>>::try_from_iter(
            chunk_batch_roots.into_iter().map(Ok),
        )
        .map_err(|e| Error::Custom(format!("{:?}", e)))?;
        if merkle_tree.root() != tx.data_merkle_root.0 {
            // TODO: Delete all chunks?
            return Err(Error::Custom(format!(
                "finalize_tx: data merkle root unmatch, found={:?} expected={:?}",
                DataRoot::from(merkle_tree.root()),
                tx.data_merkle_root,
            )));
        }
        let mut tree_bytes = Vec::new();
        for h in &**merkle_tree.data().unwrap() {
            tree_bytes.extend_from_slice(h.as_slice());
        }
        self.kvdb
            .put(COL_TX_MERKLE, &tx_seq.to_be_bytes(), &tree_bytes)?;
        // let tree = (**merkle_tree.data().unwrap()).to_vec();
        // let new_merkle_tree = MerkleTree::from_data_store(VecStore::<[u8; 32]>(tree), chunk_index_end - 1);
        // TODO: Mark the tx as completed.
        Ok(())
    }
}

impl LogStoreRead for SimpleLogStore {
    fn get_tx_by_hash(&self, hash: &TransactionHash) -> Result<Option<Transaction>> {
        let maybe_value = self.kvdb.get(COL_TX_HASH_INDEX, hash.as_bytes())?;
        if maybe_value.is_none() {
            return Ok(None);
        }
        let value = maybe_value.unwrap();
        if value.len() != 4 {
            return Err(Error::ValueDecodingError(DecodeError::InvalidByteLength {
                len: value.len(),
                expected: 4,
            }));
        }
        let seq = u64::from_be_bytes(value.try_into().unwrap());
        self.get_tx_by_seq_number(seq)
    }

    fn get_tx_by_seq_number(&self, seq: u64) -> Result<Option<Transaction>> {
        let maybe_value = self.kvdb.get(COL_TX, &seq.to_be_bytes())?;
        match maybe_value {
            None => Ok(None),
            Some(value) => {
                let mut tx = Transaction::from_ssz_bytes(&value)?;
                tx.compute_hash();
                Ok(Some(tx))
            }
        }
    }

    fn get_chunk_with_proof_by_tx_and_index(
        &self,
        _tx_seq: u64,
        _index: u32,
    ) -> Result<Option<ChunkWithProof>> {
        todo!()
    }

    fn get_chunks_with_proof_by_tx_and_index_range(
        &self,
        _tx_seq: u64,
        _index_start: u32,
        _index_end: u32,
    ) -> Result<Option<ChunkArrayWithProof>> {
        todo!()
    }
}

fn chunk_key(tx_seq: u64, index: u32) -> [u8; CHUNK_KEY_SIZE] {
    let mut key = [0u8; CHUNK_KEY_SIZE];
    key[0..8].copy_from_slice(&tx_seq.to_be_bytes());
    key[9..12].copy_from_slice(&index.to_be_bytes());
    key
}
