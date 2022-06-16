use anyhow::Result;
use ethereum_types::H256;
use merkle_tree::Sha3Algorithm;
use merkletree::proof::Proof;
use network::{
    rpc::{GoodbyeReason, RPCResponseErrorCode},
    PeerAction, PeerId, PeerRequestId, PubsubMessage, ReportSource, Request, Response,
};
use ssz_derive::{Decode as DeriveDecode, Encode as DeriveEncode};
use typenum::U0;

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

#[derive(Debug, Eq, PartialEq)]
pub struct Chunk(pub [u8; CHUNK_SIZE]);

#[derive(Clone, PartialEq, DeriveEncode, DeriveDecode)]
pub struct ChunkProof {
    pub lemma: Vec<[u8; 32]>,
    pub path: Vec<usize>,
}
impl ChunkProof {
    pub fn from_merkle_proof(proof: &Proof<[u8; 32]>) -> Self {
        ChunkProof {
            lemma: proof.lemma().clone(),
            path: proof.path().clone(),
        }
    }

    pub fn validate(&self, chunk: &Chunk, root: &DataRoot, position: usize) -> Result<bool> {
        let mut proof_position = 0;
        // TODO: Here we assume it's a full power of 2 Merkle tree.
        for (i, branch_index) in self.path.iter().enumerate() {
            proof_position += branch_index * (1 << i);
        }
        if proof_position != position {
            return Ok(false);
        }
        let proof = Proof::<[u8; 32]>::new::<U0, U0>(None, self.lemma.clone(), self.path.clone())?;
        if proof.root() != root.0 {
            return Ok(false);
        }
        proof.validate_with_data::<Sha3Algorithm>(&chunk.0)
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
    pub fn validate(&self, root: &DataRoot, position: usize) -> Result<bool> {
        self.proof.validate(&self.chunk, root, position)
    }
}

#[derive(Clone, PartialEq, DeriveEncode, DeriveDecode)]
pub struct ChunkArrayWithProof {
    pub chunks: ChunkArray,
    // TODO: The top levels of the two proofs can be merged.
    pub start_proof: ChunkProof,
    pub end_proof: ChunkProof,
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
    pub fn chunk_at(&self, index: u32) -> Option<Chunk> {
        if index >= (self.data.len() / CHUNK_SIZE) as u32 + self.start_index
            || index < self.start_index
        {
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
