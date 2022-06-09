#![allow(unused)]

use ssz_derive::{Decode as DeriveDecode, Encode as DeriveEncode};

pub const CHUNK_SIZE: usize = 32;

// TODO(Thegaram): Change back to newtype pattern
// "ssz_derive only supports named struct fields"
#[derive(DeriveEncode, DeriveDecode)]
pub struct Chunk {
    pub raw: [u8; CHUNK_SIZE],
}

#[derive(Clone, DeriveEncode, DeriveDecode, PartialEq)]
pub struct ChunkProof {}

pub struct ChunkWithProof {
    chunk: Chunk,
    proof: ChunkProof,
}

#[derive(Clone, DeriveEncode, DeriveDecode, PartialEq)]
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
        Some(Chunk {
            raw: self.data[offset..offset + CHUNK_SIZE]
                .try_into()
                .expect("length match"),
        })
    }
}

#[derive(Clone, DeriveEncode, DeriveDecode, PartialEq)]
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
