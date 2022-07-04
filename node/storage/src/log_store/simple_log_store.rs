use crate::error::{Error, Result};
use crate::log_store::{
    LogChunkStore, LogStoreChunkRead, LogStoreChunkWrite, LogStoreRead, LogStoreWrite,
};
use crate::IonianKeyValueDB;
use anyhow::{anyhow, bail};
use kvdb_rocksdb::{Database, DatabaseConfig};
use merkle_light::hash::{Algorithm, Hashable};
use merkle_light::merkle::MerkleTree;
use merkle_light::proof::Proof;
use merkle_tree::RawLeafSha3Algorithm;
use rayon::prelude::*;
use shared_types::{
    Chunk, ChunkArray, ChunkArrayWithProof, ChunkProof, ChunkWithProof, DataRoot, Transaction,
    TransactionHash, CHUNK_SIZE,
};
use ssz::{Decode, Encode};
use std::cmp;
use std::path::Path;
use std::sync::Arc;

const COL_TX: u32 = 0;
const COL_TX_HASH_INDEX: u32 = 1;
const COL_TX_DATA_ROOT_INDEX: u32 = 2;
const COL_TX_MERKLE: u32 = 3;
const COL_CHUNK: u32 = 4;
const COL_TX_COMPLETED: u32 = 5;
const COL_NUM: u32 = 6;
// A chunk key is the concatenation of tx_seq(u64) and start_index(u32)
const CHUNK_KEY_SIZE: usize = 8 + 4;
const CHUNK_BATCH_SIZE: usize = 1024;

/// This represents the subtree of a chunk or the whole data merkle tree.
pub type SubMerkleTree = MerkleTree<[u8; 32], RawLeafSha3Algorithm>;
/// This can only be used to represent the top tree where the leaves are chunk subtree roots.
pub type TopMerkleTree = MerkleTree<[u8; 32], RawLeafSha3Algorithm>;
type DataProof = Proof<[u8; 32]>;

macro_rules! try_option {
    ($r: ident) => {
        match $r {
            Some(v) => v,
            None => return Ok(None),
        }
    };
    ($e: expr) => {
        match $e {
            Some(v) => v,
            None => return Ok(None),
        }
    };
}

/// Here we only encode the top tree leaf data because we cannot build `VecStore` from raw bytes.
/// If we want to save the whole tree, we'll need to save it as files using disk-related store,
/// or fork the dependency to expose the VecStore initialization method.
fn encode_merkle_tree<A: Algorithm<[u8; 32]>>(merkle_tree: &MerkleTree<[u8; 32], A>) -> Vec<u8> {
    let data = merkle_tree.as_slice();
    let mut data_bytes = Vec::new();
    data_bytes.extend_from_slice(&(merkle_tree.leafs() as u32).to_be_bytes());
    for leaf in &data[0..merkle_tree.leafs()] {
        data_bytes.extend_from_slice(leaf.as_slice());
    }
    data_bytes
}

fn decode_merkle_tree(bytes: &[u8]) -> Result<TopMerkleTree> {
    if bytes.len() < 4 {
        bail!(anyhow!(
            "Merkle tree encoding too short: len={}",
            bytes.len()
        ));
    }
    let leaf_count = u32::from_be_bytes(bytes[0..4].try_into().unwrap()) as usize;
    let expected_len = 4 + 32 * leaf_count;
    if bytes.len() != expected_len {
        bail!(anyhow!(
            "Merkle tree encoding incorrect length: len={} expected={}",
            bytes.len(),
            expected_len
        ));
    }
    let mut data: Vec<[u8; 32]> = vec![Default::default(); leaf_count];
    for (i, leaf) in data.iter_mut().enumerate() {
        let offset = 4 + i * 32;
        leaf.copy_from_slice(&bytes[offset..offset + 32]);
    }
    Ok(TopMerkleTree::new(data))
}

/// This should be called with input checked.
pub fn sub_merkle_tree(leaf_data: &[u8]) -> Result<SubMerkleTree> {
    if leaf_data.len() % CHUNK_SIZE != 0 {
        bail!(anyhow!("merkle_tree: unmatch data size"));
    }
    let leaf_count = leaf_data.len() / CHUNK_SIZE;
    let mut data: Vec<Chunk> = vec![Chunk([0; CHUNK_SIZE]); leaf_count];
    for (i, chunk) in data.iter_mut().enumerate() {
        let offset = i * CHUNK_SIZE;
        chunk
            .0
            .copy_from_slice(&leaf_data[offset..offset + CHUNK_SIZE]);
    }
    Ok(SubMerkleTree::new(
        data.into_par_iter()
            .map(|e| {
                let mut a = RawLeafSha3Algorithm::default();
                e.hash(&mut a);
                a.hash()
            })
            .collect::<Vec<_>>(),
    ))
}

#[allow(unused)]
fn tree_size(leafs: usize) -> usize {
    let mut expected_size = leafs;
    let mut next_layer = leafs;
    while next_layer != 0 {
        next_layer /= next_layer;
        expected_size += next_layer;
    }
    expected_size
}

pub struct SimpleLogStore {
    kvdb: Arc<dyn IonianKeyValueDB>,
    chunk_store: Arc<dyn LogChunkStore>,
    pub chunk_batch_size: usize,
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

    fn get_top_tree(&self, tx_seq: u64) -> Result<Option<TopMerkleTree>> {
        let tree_bytes = try_option!(self.kvdb.get(COL_TX_MERKLE, &tx_seq.to_be_bytes())?);
        Ok(Some(decode_merkle_tree(tree_bytes.as_slice())?))
    }

    fn get_sub_tree(
        &self,
        tx_seq: u64,
        batch_start_index: usize,
    ) -> Result<Option<(ChunkArray, SubMerkleTree)>> {
        if batch_start_index % self.chunk_batch_size != 0 {
            bail!(Error::InvalidBatchBoundary);
        }
        let batch_end_index = batch_start_index + self.chunk_batch_size;
        let chunk_array = try_option!(self.chunk_store.get_chunks_by_tx_and_index_range(
            tx_seq,
            batch_start_index,
            batch_end_index,
        )?);
        let sub_tree = sub_merkle_tree(&chunk_array.data)?;
        Ok(Some((chunk_array, sub_tree)))
    }

    fn get_subtree_proof(&self, tx_seq: u64, index: usize) -> Result<Option<DataProof>> {
        let batch_start_index = index / self.chunk_batch_size * self.chunk_batch_size;
        let (_, sub_tree) = try_option!(self.get_sub_tree(tx_seq, batch_start_index)?);
        let offset = index % self.chunk_batch_size;
        let sub_proof = sub_tree.gen_proof(offset);
        Ok(Some(sub_proof))
    }
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
        if chunks.start_index as usize % self.batch_size != 0 || chunks.data.len() % CHUNK_SIZE != 0
        {
            bail!(Error::InvalidBatchBoundary);
        }
        let mut tx = self.kvdb.transaction();
        let end_index = chunks.start_index as usize + (chunks.data.len() / CHUNK_SIZE);
        for (index, end) in batch_iter(chunks.start_index as usize, end_index, self.batch_size) {
            let key = chunk_key(tx_seq, index);
            tx.put(
                COL_CHUNK,
                &key,
                &chunks.data[(index - chunks.start_index as usize) * CHUNK_SIZE
                    ..(end - chunks.start_index as usize) * CHUNK_SIZE],
            );
        }
        self.kvdb.write(tx)?;
        Ok(())
    }

    fn remove_all_chunks(&self, tx_seq: u64) -> Result<()> {
        self.kvdb
            .delete_with_prefix(COL_CHUNK, &tx_seq.to_be_bytes())
            .map_err(Into::into)
    }
}

impl LogStoreChunkRead for BatchChunkStore {
    fn get_chunk_by_tx_and_index(&self, tx_seq: u64, index: usize) -> Result<Option<Chunk>> {
        let maybe_chunk = self
            .get_chunks_by_tx_and_index_range(tx_seq, index, index + 1)?
            .map(|chunk_array| Chunk(chunk_array.data.try_into().expect("chunk data size match")));
        Ok(maybe_chunk)
    }

    fn get_chunks_by_tx_and_index_range(
        &self,
        tx_seq: u64,
        index_start: usize,
        index_end: usize,
    ) -> Result<Option<ChunkArray>> {
        if index_end <= index_start {
            bail!(Error::InvalidBatchBoundary);
        }
        // TODO: Use range iteration of rocksdb.
        let mut data = Vec::with_capacity((index_end - index_start) * CHUNK_SIZE);
        for (index, end_index) in batch_iter(index_start, index_end, self.batch_size) {
            let batch_start_index = index / self.batch_size * self.batch_size;
            let key = chunk_key(tx_seq, batch_start_index);
            let batch_data = try_option!(self.kvdb.get(COL_CHUNK, &key)?);
            let start_offset = (index % self.batch_size) * CHUNK_SIZE;
            let end_offset = cmp::min(
                batch_data.len(),
                ((end_index - 1) % self.batch_size + 1) * CHUNK_SIZE,
            );
            // If the loaded data of the last chunk is shorted than `batch_end`, we return the data
            // without error and leave the caller to check if there is any error.
            // TODO: Decide if this bahavior is what we need.
            if batch_data.len() != self.batch_size * CHUNK_SIZE {
                if index / self.batch_size == (index_end - 1) / self.batch_size {
                    trace!("read partial last batch");
                    if start_offset >= batch_data.len() {
                        return Ok(None);
                    }
                } else {
                    bail!(Error::Custom("incomplete chunk batch".to_string()));
                }
            }
            data.extend_from_slice(&batch_data[start_offset..end_offset]);
        }
        Ok(Some(ChunkArray {
            data,
            start_index: index_start as u32,
        }))
    }

    fn get_chunk_by_data_root_and_index(
        &self,
        _data_root: &DataRoot,
        _index: usize,
    ) -> Result<Option<Chunk>> {
        unreachable!("chunk store only index chunks by tx_seq")
    }

    fn get_chunks_by_data_root_and_index_range(
        &self,
        _data_root: &DataRoot,
        _index_start: usize,
        _index_end: usize,
    ) -> Result<Option<ChunkArray>> {
        unreachable!("chunk store only index chunks by tx_seq")
    }

    fn get_chunk_index_list(&self, tx_seq: u64) -> Result<Vec<usize>> {
        // TODO: Bench and compare with using rocksdb prefix_extractor.
        // TODO: Only iterate the key without reading the value.
        self.kvdb
            .iter_with_prefix(COL_CHUNK, &tx_seq.to_be_bytes())
            .map(|(k, _)| chunk_index(k.as_ref()))
            .collect()
    }
}

impl SimpleLogStore {
    #[allow(unused)]
    pub fn rocksdb(path: &Path) -> Result<Self> {
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

    #[allow(unused)]
    pub fn memorydb() -> Result<Self> {
        let kvdb = Arc::new(kvdb_memorydb::create(COL_NUM));

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
    fn get_chunk_by_tx_and_index(&self, tx_seq: u64, index: usize) -> Result<Option<Chunk>> {
        self.chunk_store.get_chunk_by_tx_and_index(tx_seq, index)
    }

    fn get_chunks_by_tx_and_index_range(
        &self,
        tx_seq: u64,
        index_start: usize,
        index_end: usize,
    ) -> Result<Option<ChunkArray>> {
        self.chunk_store
            .get_chunks_by_tx_and_index_range(tx_seq, index_start, index_end)
    }

    fn get_chunk_by_data_root_and_index(
        &self,
        data_root: &DataRoot,
        index: usize,
    ) -> Result<Option<Chunk>> {
        let tx_seq = try_option!(self.get_tx_seq_by_data_root(data_root)?);
        self.get_chunk_by_tx_and_index(tx_seq, index)
    }

    fn get_chunks_by_data_root_and_index_range(
        &self,
        data_root: &DataRoot,
        index_start: usize,
        index_end: usize,
    ) -> Result<Option<ChunkArray>> {
        let tx_seq = try_option!(self.get_tx_seq_by_data_root(data_root)?);
        self.get_chunks_by_tx_and_index_range(tx_seq, index_start, index_end)
    }

    fn get_chunk_index_list(&self, tx_seq: u64) -> Result<Vec<usize>> {
        // TODO: If the tx is completed, just read the top tree might be faster.
        self.chunk_store.get_chunk_index_list(tx_seq)
    }
}

impl LogStoreChunkWrite for SimpleLogStore {
    fn put_chunks(&self, tx_seq: u64, chunks: ChunkArray) -> Result<()> {
        self.chunk_store.put_chunks(tx_seq, chunks)
    }

    fn remove_all_chunks(&self, tx_seq: u64) -> Result<()> {
        self.chunk_store.remove_all_chunks(tx_seq)?;
        let mut tx = self.kvdb.transaction();
        let key = tx_seq.to_be_bytes();
        tx.delete(COL_TX_COMPLETED, &key);
        tx.delete(COL_TX_MERKLE, &key);
        self.kvdb.write(tx).map_err(Into::into)
    }
}

impl LogStoreWrite for SimpleLogStore {
    fn put_tx(&self, tx: Transaction) -> Result<()> {
        let mut db_tx = self.kvdb.transaction();
        db_tx.put(COL_TX, &tx.seq.to_be_bytes(), &tx.as_ssz_bytes());
        db_tx.put(COL_TX_HASH_INDEX, tx.hash.as_bytes(), &tx.seq.to_be_bytes());
        if self
            .get_tx_seq_by_data_root(&tx.data_merkle_root)?
            .is_none()
        {
            db_tx.put(
                COL_TX_DATA_ROOT_INDEX,
                tx.data_merkle_root.as_bytes(),
                &tx.seq.to_be_bytes(),
            );
        }
        self.kvdb.write(db_tx).map_err(Into::into)
    }

    fn finalize_tx(&self, tx_seq: u64) -> Result<()> {
        let maybe_tx = self.get_tx_by_seq_number(tx_seq)?;
        if maybe_tx.is_none() {
            bail!(Error::Custom(format!(
                "finalize_tx: tx not in db, tx_seq={}",
                tx_seq
            )));
        }
        let tx = maybe_tx.unwrap();
        if tx.size <= self.chunk_batch_size as u64 * CHUNK_SIZE as u64 {
            // Only one batch, so there is no need for a top tree.
            return Ok(());
        }
        let chunk_index_end = (tx.size / CHUNK_SIZE as u64) as usize;
        let mut chunk_batch_roots = Vec::with_capacity(chunk_index_end / self.chunk_batch_size + 1);
        for (batch_start_index, batch_end_index) in
            batch_iter(0, chunk_index_end, self.chunk_batch_size)
        {
            let maybe_chunks = self.chunk_store.get_chunks_by_tx_and_index_range(
                tx_seq,
                batch_start_index,
                batch_end_index,
            )?;
            if maybe_chunks.is_none() {
                bail!(anyhow!(
                    "finalize_tx: chunk batch not in db, start_index={}",
                    batch_start_index
                ));
            }
            let chunks = maybe_chunks.unwrap();
            if batch_end_index - batch_start_index != chunks.data.len() / CHUNK_SIZE {
                bail!(anyhow!(
                    "finalize_tx: data length unmatch: expected_chunks={}, data_len={}",
                    batch_end_index % self.chunk_batch_size,
                    chunks.data.len()
                ));
            }
            let merkle_tree = sub_merkle_tree(&chunks.data)?;
            let merkle_root = merkle_tree.root();
            chunk_batch_roots.push(merkle_root);
        }
        let merkle_tree = TopMerkleTree::new(chunk_batch_roots);
        if merkle_tree.root() != tx.data_merkle_root.0 {
            // TODO: Delete all chunks?
            bail!(Error::Custom(format!(
                "finalize_tx: data merkle root unmatch, found={:?} expected={:?}",
                DataRoot::from(merkle_tree.root()),
                tx.data_merkle_root,
            )));
        }
        // TODO: Bench and compare with storing the top_proof with the batch.
        self.kvdb.put(
            COL_TX_MERKLE,
            &tx_seq.to_be_bytes(),
            &encode_merkle_tree(&merkle_tree),
        )?;
        self.kvdb
            .put(COL_TX_COMPLETED, &tx_seq.to_be_bytes(), &[0])?;
        // TODO: Mark the tx as completed.
        Ok(())
    }
}

impl LogStoreRead for SimpleLogStore {
    fn get_tx_by_hash(&self, hash: &TransactionHash) -> Result<Option<Transaction>> {
        let value = try_option!(self.kvdb.get(COL_TX_HASH_INDEX, hash.as_bytes())?);
        let seq = decode_tx_seq(&value)?;
        self.get_tx_by_seq_number(seq)
    }

    fn get_tx_by_seq_number(&self, seq: u64) -> Result<Option<Transaction>> {
        let value = try_option!(self.kvdb.get(COL_TX, &seq.to_be_bytes())?);
        let tx = Transaction::from_ssz_bytes(&value).map_err(Error::from)?;
        Ok(Some(tx))
    }

    fn get_tx_seq_by_data_root(&self, data_root: &DataRoot) -> Result<Option<u64>> {
        let value = try_option!(self
            .kvdb
            .get(COL_TX_DATA_ROOT_INDEX, data_root.as_bytes())?);
        Ok(Some(decode_tx_seq(&value)?))
    }

    fn get_chunk_with_proof_by_tx_and_index(
        &self,
        tx_seq: u64,
        index: usize,
    ) -> Result<Option<ChunkWithProof>> {
        // TODO: It's possible to skip loading the tx in most cases. Optimize later.
        let batch_index = index / self.chunk_batch_size;
        let offset = index % self.chunk_batch_size;
        let (chunk_array, sub_tree) =
            try_option!(self.get_sub_tree(tx_seq, batch_index * self.chunk_batch_size)?);
        let sub_proof = sub_tree.gen_proof(offset);

        let tx = try_option!(self.get_tx_by_seq_number(tx_seq)?);
        let proof = if tx.size <= self.chunk_batch_size as u64 * CHUNK_SIZE as u64 {
            // The total tx size is less than a batch, so there is no top tree.
            ChunkProof::from_merkle_proof(&sub_proof)
        } else {
            let top_tree = try_option!(self.get_top_tree(tx_seq)?);
            let top_proof = top_tree.gen_proof(batch_index);
            chunk_proof(&top_proof, &sub_proof)?
        };
        Ok(Some(ChunkWithProof {
            chunk: try_option!(chunk_array.chunk_at(index)),
            proof,
        }))
    }

    fn get_chunks_with_proof_by_tx_and_index_range(
        &self,
        tx_seq: u64,
        index_start: usize,
        index_end: usize,
    ) -> Result<Option<ChunkArrayWithProof>> {
        if index_end <= index_start {
            bail!(Error::InvalidBatchBoundary);
        }
        let tx = try_option!(self.get_tx_by_seq_number(tx_seq)?);
        if index_end as u64 * CHUNK_SIZE as u64 > tx.size {
            bail!(Error::InvalidBatchBoundary);
        }
        if tx.size <= self.chunk_batch_size as u64 * CHUNK_SIZE as u64 {
            // The total tx size is less than a batch, so there is no top tree.
            let (chunk_array, sub_tree) = try_option!(self.get_sub_tree(tx_seq, 0)?);
            let start_proof = ChunkProof::from_merkle_proof(&sub_tree.gen_proof(index_start));
            let end_proof = ChunkProof::from_merkle_proof(&sub_tree.gen_proof(index_end - 1));
            Ok(Some(ChunkArrayWithProof {
                chunks: try_option!(chunk_array.sub_array(index_start, index_end)),
                start_proof,
                end_proof,
            }))
        } else {
            let top_tree = try_option!(self.get_top_tree(tx_seq)?);
            let left_batch_index = index_start / self.chunk_batch_size;
            let right_batch_index = (index_end - 1) / self.chunk_batch_size;
            let (start_proof, end_proof) = if left_batch_index == right_batch_index {
                let top_proof = top_tree.gen_proof(left_batch_index);
                let (_, sub_tree) = try_option!(
                    self.get_sub_tree(tx_seq, left_batch_index * self.chunk_batch_size)?
                );
                let left_offset = index_start % self.chunk_batch_size;
                let left_sub_proof = sub_tree.gen_proof(left_offset);
                let right_offset = (index_end - 1) % self.chunk_batch_size;
                let right_sub_proof = sub_tree.gen_proof(right_offset);
                (
                    chunk_proof(&top_proof, &left_sub_proof)?,
                    chunk_proof(&top_proof, &right_sub_proof)?,
                )
            } else {
                let left_top_proof = top_tree.gen_proof(left_batch_index);
                let right_top_proof = top_tree.gen_proof(right_batch_index);
                let left_sub_proof = try_option!(self.get_subtree_proof(tx_seq, index_start)?);
                let right_sub_proof = try_option!(self.get_subtree_proof(tx_seq, index_end - 1)?);
                (
                    chunk_proof(&left_top_proof, &left_sub_proof)?,
                    chunk_proof(&right_top_proof, &right_sub_proof)?,
                )
            };
            // TODO: The chunks may have been loaded from the proof generation process above.
            let chunks = try_option!(self.get_chunks_by_tx_and_index_range(
                tx_seq,
                index_start,
                index_end
            )?);
            Ok(Some(ChunkArrayWithProof {
                chunks,
                start_proof,
                end_proof,
            }))
        }
    }

    fn check_tx_completed(&self, tx_seq: u64) -> Result<bool> {
        self.kvdb
            .has_key(COL_TX_COMPLETED, &tx_seq.to_be_bytes())
            .map_err(Into::into)
    }
}

fn chunk_key(tx_seq: u64, index: usize) -> [u8; CHUNK_KEY_SIZE] {
    let mut key = [0u8; CHUNK_KEY_SIZE];
    key[0..8].copy_from_slice(&tx_seq.to_be_bytes());
    key[8..12].copy_from_slice(&(index as u32).to_be_bytes());
    key
}

fn chunk_index(chunk_key: &[u8]) -> Result<usize> {
    if chunk_key.len() != CHUNK_KEY_SIZE {
        bail!("invalid chunk key size, len={}", chunk_key.len());
    }
    let index = u32::from_be_bytes(chunk_key[8..12].try_into()?);
    Ok(index as usize)
}

fn chunk_proof(top_proof: &DataProof, sub_proof: &DataProof) -> Result<ChunkProof> {
    if top_proof.item() != sub_proof.root() {
        bail!(Error::Custom(format!(
            "top tree and sub tree mismatch: top_leaf={:?}, sub_root={:?}",
            top_proof.item(),
            sub_proof.root()
        )));
    }
    let mut lemma = sub_proof.lemma().to_vec();
    let mut path = sub_proof.path().to_vec();
    if lemma.len() == 1 {
        // Proof for a single-node tree.
        assert!(path.is_empty());
    } else {
        assert!(lemma.pop().is_some());
    }
    lemma.extend_from_slice(&top_proof.lemma()[1..]);
    path.extend_from_slice(top_proof.path());
    let proof = DataProof::new(lemma, path);
    Ok(ChunkProof::from_merkle_proof(&proof))
}

/// Return the batch boundaries `(batch_start_index, batch_end_index)` given the index range.
fn batch_iter(start: usize, end: usize, batch_size: usize) -> Vec<(usize, usize)> {
    let mut list = Vec::new();
    for i in (start / batch_size * batch_size..end).step_by(batch_size) {
        let batch_start = cmp::max(start, i);
        let batch_end = cmp::min(end, i + batch_size);
        list.push((batch_start, batch_end));
    }
    list
}

fn decode_tx_seq(data: &[u8]) -> Result<u64> {
    Ok(u64::from_be_bytes(
        data.try_into().map_err(|e| anyhow!("{:?}", e))?,
    ))
}
