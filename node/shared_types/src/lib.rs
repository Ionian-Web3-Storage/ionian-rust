#![allow(unused)]

use ethereum_types::H256;
use ssz::Encode;
use ssz_derive::{Decode as DeriveDecode, Encode as DeriveEncode};
use tiny_keccak::{Hasher, Keccak};

/// Application level requests sent to the network.
#[derive(Debug, Clone, Copy)]
pub enum RequestId {
    Router,
}

/// Placeholder types for transactions and chunks.
pub type TransactionHash = H256;

pub type DataRoot = H256;

// Each chunk is 32 bytes.
pub const CHUNK_SIZE: usize = 32;

pub struct Chunk(pub [u8; CHUNK_SIZE]);

#[derive(Clone, PartialEq, DeriveEncode, DeriveDecode)]
pub struct ChunkProof {}

#[derive(DeriveDecode, DeriveEncode)]
pub struct Transaction {
    #[ssz(skip_serializing, skip_deserializing)]
    hash: TransactionHash,
    pub size: u64,
    pub data_merkle_root: DataRoot,
    pub seq: u64,
}

impl Transaction {
    pub fn compute_hash(&mut self) -> TransactionHash {
        let ssz_bytes = self.as_ssz_bytes();
        let mut output = TransactionHash::default();
        let mut hasher = Keccak::v256();
        hasher.update(&ssz_bytes);
        hasher.finalize(output.as_bytes_mut());
        self.hash = output;
        output
    }

    pub fn hash(&self) -> &TransactionHash {
        &self.hash
    }
}

pub struct ChunkWithProof {
    chunk: Chunk,
    proof: ChunkProof,
}

#[derive(Clone, PartialEq, DeriveEncode, DeriveDecode)]
pub struct ChunkArrayWithProof {
    pub chunks: ChunkArray,
    pub start_proof: ChunkProof,
    pub end_proof: ChunkProof,
}

impl std::fmt::Debug for ChunkArrayWithProof {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // TODO(thegaram): replace this with something more meaningful
        f.write_str("ChunkArrayWithProof")
    }
}

#[derive(Clone, Debug, PartialEq, DeriveEncode, DeriveDecode)]
pub struct ChunkArray {
    // The length is exactly `(end_index - start_index) * CHUNK_SIZE`
    pub data: Vec<u8>,
    pub start_index: u32,
    pub end_index: u32,
}

impl ChunkArray {
    pub fn chunk_at(&self, index: u32) -> Option<Chunk> {
        if index >= self.end_index || index < self.start_index {
            return None;
        }
        let offset = (index - self.start_index) as usize * CHUNK_SIZE;
        Some(Chunk(
            self.data[offset..offset + CHUNK_SIZE]
                .try_into()
                .expect("length match"),
        ))
    }
}
