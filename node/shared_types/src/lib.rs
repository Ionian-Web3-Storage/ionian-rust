use anyhow::{anyhow, bail, Result};
use ethereum_types::H256;
use merkle_light::hash::{Algorithm, Hashable};
use merkle_light::merkle::next_pow2;
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
    pub lemma: Vec<H256>,
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
            lemma: proof.lemma().iter().map(|e| e.into()).collect(),
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
        let proof_position = self.position(total_chunk_count);
        if proof_position != position {
            bail!(
                "wrong position: proof_pos={} provided={}",
                proof_position,
                position
            );
        }
        let proof =
            Proof::<[u8; 32]>::new(self.lemma.iter().map(|e| e.0).collect(), self.path.clone());
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

    fn position(&self, total_chunk_count: usize) -> usize {
        let mut left_chunk_count = total_chunk_count;
        let mut proof_position = 0;
        // TODO: After the first `is_left == true`, the tree depth is fixed.
        for is_left in self.path.iter().rev() {
            let subtree_size = next_pow2(left_chunk_count) >> 1;
            if !is_left {
                proof_position += subtree_size;
                left_chunk_count -= subtree_size;
            } else {
                left_chunk_count = subtree_size;
            }
        }
        proof_position
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
        if !self.end_proof.validate(
            &end_chunk,
            root,
            self.chunks.start_index as usize + self.chunks.data.len() / CHUNK_SIZE - 1,
            total_chunk_count,
        )? {
            return Ok(false);
        }
        // The left subtree if full, so the start proof length is no less than the end proof.
        let tree_depth = self.start_proof.path.len();
        let mut children_layer: Vec<_> = self
            .chunks
            .data
            .chunks(CHUNK_SIZE)
            .map(|chunk_data| {
                let chunk = Chunk(chunk_data.try_into().unwrap());
                let mut a = RawLeafSha3Algorithm::default();
                chunk.hash(&mut a);
                a.hash()
            })
            .collect();
        let mut right_most_proof_index = 0;
        for depth in 0..tree_depth {
            let mut parent_layer = Vec::new();
            let start_index = if !self.start_proof.path[depth] {
                // If the left-most node is the right child, its sibling is not within the data range and should be retrieved from the proof.
                let mut a = RawLeafSha3Algorithm::default();
                let parent = a.node(self.start_proof.lemma[depth + 1].0, children_layer[0]);
                parent_layer.push(parent);
                1
            } else {
                // The left-most node is the left child, its sibling is just the next child.
                0
            };
            let mut iter = children_layer[start_index..].chunks_exact(2);
            while let Some([left, right]) = iter.next() {
                let mut a = RawLeafSha3Algorithm::default();
                parent_layer.push(a.node(*left, *right))
            }
            if let [right_most] = iter.remainder() {
                if self.end_proof.path[right_most_proof_index] {
                    let mut a = RawLeafSha3Algorithm::default();
                    parent_layer.push(a.node(
                        *right_most,
                        self.end_proof.lemma[right_most_proof_index + 1].0,
                    ));
                    right_most_proof_index += 1;
                } else {
                    parent_layer.push(*right_most);
                }
            } else {
                right_most_proof_index += 1;
            }
            children_layer = parent_layer;
        }
        assert_eq!(children_layer.len(), 1);
        if children_layer[0] != root.0 {
            return Ok(false);
        }

        Ok(true)
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
