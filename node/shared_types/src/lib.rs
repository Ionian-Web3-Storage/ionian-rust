use anyhow::{anyhow, bail, Result};
use ethereum_types::H256;
use merkle_light::hash::{Algorithm, Hashable};
use merkle_light::merkle::{log2_pow2, next_pow2};
use merkle_light::proof::Proof;
use merkle_tree::{RawLeafSha3Algorithm, LEAF};
use ssz_derive::{Decode as DeriveDecode, Encode as DeriveEncode};
use std::hash::Hasher;

/// Application level requests sent to the network.
#[derive(Debug, Clone, Copy)]
pub enum RequestId {
    Router,
}

/// Placeholder types for transactions and chunks.
pub type TransactionHash = H256;

pub type DataRoot = H256;

// Each chunk is 32 bytes.
pub const CHUNK_SIZE: usize = 256;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Chunk(pub [u8; CHUNK_SIZE]);

impl<H: Hasher> Hashable<H> for Chunk {
    fn hash(&self, state: &mut H) {
        let mut prepended = vec![LEAF];
        prepended.extend_from_slice(&self.0);
        state.write(&prepended)
    }
}

#[derive(Clone, Debug, PartialEq, DeriveEncode, DeriveDecode)]
pub struct ChunkProof {
    pub lemma: Vec<[u8; 32]>,
    pub path: Vec<bool>,
}
impl ChunkProof {
    pub fn new_empty() -> Self {
        Self {
            lemma: Vec::new(),
            path: Vec::new(),
        }
    }

    pub fn from_merkle_proof(proof: &Proof<[u8; 32]>) -> Self {
        ChunkProof {
            lemma: proof.lemma().to_vec(),
            path: proof.path().to_vec(),
        }
    }

    pub fn validate(
        &self,
        chunk: &Chunk,
        root: &DataRoot,
        position: usize,
        total_chunk_count: usize,
    ) -> Result<bool> {
        let tree_depth = log2_pow2(next_pow2(total_chunk_count));
        let mut proof_position = 0;
        for (i, is_left) in self.path.iter().rev().enumerate() {
            if !is_left {
                proof_position += 1 << (tree_depth - 1 - i);
            }
        }
        if proof_position != position {
            bail!(
                "wrong position: proof_pos={} provided={}",
                proof_position,
                position
            );
        }
        let proof = Proof::<[u8; 32]>::new(self.lemma.clone(), self.path.clone());
        if proof.root() != root.0 {
            bail!(
                "root mismatch, proof_root={:?} provided={:?}",
                proof.root(),
                root.0
            );
        }
        let mut h = RawLeafSha3Algorithm::default();
        chunk.hash(&mut h);
        let chunk_hash = h.hash();
        h.reset();
        let leaf_hash = h.leaf(chunk_hash);
        if proof.item() != leaf_hash {
            bail!(anyhow!(
                "data hash mismatch: \n leaf_hash={:?} \n proof_item={:?} \n chunk_hash={:?}",
                leaf_hash,
                proof.item(),
                chunk_hash
            ));
        }
        Ok(proof.validate::<RawLeafSha3Algorithm>())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, DeriveDecode, DeriveEncode)]
pub struct Transaction {
    pub hash: TransactionHash,
    pub size: u64,
    pub data_merkle_root: DataRoot,
    pub seq: u64,
}

pub struct ChunkWithProof {
    pub chunk: Chunk,
    pub proof: ChunkProof,
}

impl ChunkWithProof {
    pub fn validate(
        &self,
        root: &DataRoot,
        position: usize,
        total_chunk_count: usize,
    ) -> Result<bool> {
        self.proof
            .validate(&self.chunk, root, position, total_chunk_count)
    }
}

#[derive(Clone, PartialEq, DeriveEncode, DeriveDecode)]
pub struct ChunkArrayWithProof {
    pub chunks: ChunkArray,
    // TODO: The top levels of the two proofs can be merged.
    pub start_proof: ChunkProof,
    pub end_proof: ChunkProof,
}

impl ChunkArrayWithProof {
    pub fn validate(&self, root: &DataRoot, total_chunk_count: usize) -> Result<bool> {
        // FIXME: Validate `chunks.data`.
        let start_chunk = self
            .chunks
            .first_chunk()
            .ok_or_else(|| anyhow!("no start chunk"))?;
        if !self.start_proof.validate(
            &start_chunk,
            root,
            self.chunks.start_index as usize,
            total_chunk_count,
        )? {
            return Ok(false);
        }
        let end_chunk = self
            .chunks
            .last_chunk()
            .ok_or_else(|| anyhow!("no last chunk"))?;
        self.end_proof.validate(
            &end_chunk,
            root,
            self.chunks.start_index as usize + self.chunks.data.len() / CHUNK_SIZE - 1,
            total_chunk_count,
        )
    }
}

impl std::fmt::Debug for ChunkArrayWithProof {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // TODO(thegaram): replace this with something more meaningful
        f.write_str("ChunkArrayWithProof")
    }
}

#[derive(Clone, Debug, Eq, PartialEq, DeriveEncode, DeriveDecode)]
pub struct ChunkArray {
    // The length is exactly a multiple of `CHUNK_SIZE`
    pub data: Vec<u8>,
    pub start_index: u32,
}

impl ChunkArray {
    pub fn first_chunk(&self) -> Option<Chunk> {
        self.chunk_at(self.start_index as usize)
    }

    pub fn last_chunk(&self) -> Option<Chunk> {
        let last_index =
            (self.start_index as usize + self.data.len() / CHUNK_SIZE).checked_sub(1)?;
        self.chunk_at(last_index)
    }

    pub fn chunk_at(&self, index: usize) -> Option<Chunk> {
        if index >= self.data.len() / CHUNK_SIZE + self.start_index as usize
            || index < self.start_index as usize
        {
            return None;
        }
        let offset = (index - self.start_index as usize) * CHUNK_SIZE;
        Some(Chunk(
            self.data[offset..offset + CHUNK_SIZE]
                .try_into()
                .expect("length match"),
        ))
    }

    pub fn sub_array(&self, start: usize, end: usize) -> Option<ChunkArray> {
        if start >= self.data.len() / CHUNK_SIZE + self.start_index as usize
            || start < self.start_index as usize
            || end > self.data.len() / CHUNK_SIZE + self.start_index as usize
            || end <= self.start_index as usize
            || end <= start
        {
            return None;
        }
        let start_offset = (start - self.start_index as usize) * CHUNK_SIZE;
        let end_offset = (end - self.start_index as usize) * CHUNK_SIZE;
        Some(ChunkArray {
            data: self.data[start_offset..end_offset].to_vec(),
            start_index: start as u32,
        })
    }
}
