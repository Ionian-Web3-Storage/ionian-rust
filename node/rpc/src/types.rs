use crate::error;
use jsonrpsee::core::Error as RpcError;
use merkle_light::hash::Algorithm;
use merkle_light::merkle::MerkleTree;
use merkle_tree::{RawLeafSha3Algorithm, LEAF};
use serde::{Deserialize, Serialize};
use shared_types::{DataRoot, Proof, Transaction, CHUNK_SIZE};
use std::hash::Hasher;

pub(crate) type RpcResult<T> = Result<T, RpcError>;

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Status {
    pub connected_peers: usize,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileInfo {
    pub tx: Transaction,
    pub finalized: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Segment(#[serde(with = "base64")] pub Vec<u8>);

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SegmentWithProof {
    /// File merkle root.
    pub root: DataRoot,
    #[serde(with = "base64")]
    /// With fixed data size except the last segment.
    pub data: Vec<u8>,
    /// Segment index.
    pub index: u32,
    /// File merkle proof whose leaf node is segment root.
    pub proof: Proof,
}

impl SegmentWithProof {
    /// Splits file into segments and returns the total number of segments and the last segment size.
    fn split_file_into_segments(
        &self,
        file_size: usize,
        chunks_per_segment: usize,
    ) -> RpcResult<(u32, usize)> {
        if file_size == 0 {
            return Err(error::invalid_params("file_size", "file is empty"));
        }

        let segment_size = chunks_per_segment * CHUNK_SIZE;
        let remaining_size = file_size % segment_size;
        let mut num_segments = file_size / segment_size;

        if remaining_size == 0 {
            return Ok((num_segments as u32, segment_size));
        }

        // Otherwise, the last segment is not full.
        num_segments += 1;

        let last_chunk_size = remaining_size % CHUNK_SIZE;
        if last_chunk_size == 0 {
            Ok((num_segments as u32, remaining_size))
        } else {
            // Padding last chunk with zeros.
            let last_segment_size = remaining_size - last_chunk_size + CHUNK_SIZE;
            Ok((num_segments as u32, last_segment_size))
        }
    }

    fn validate_data_size_and_index(
        &self,
        file_size: usize,
        chunks_per_segment: usize,
    ) -> RpcResult<u32> {
        let (num_segments, last_segment_size) =
            self.split_file_into_segments(file_size, chunks_per_segment)?;

        if self.index >= num_segments {
            return Err(error::invalid_params("index", "index out of bound"));
        }

        let data_size = if self.index == num_segments - 1 {
            last_segment_size
        } else {
            chunks_per_segment * CHUNK_SIZE
        };

        if self.data.len() != data_size {
            return Err(error::invalid_params("data", "invalid data length"));
        }

        Ok(num_segments)
    }

    fn calculate_segment_merkle_root(&self) -> [u8; 32] {
        let mut a = RawLeafSha3Algorithm::default();
        let hashes = self.data.chunks_exact(CHUNK_SIZE).map(|x| {
            a.reset();
            a.write(&[LEAF]);
            a.write(x);
            a.hash()
        });
        MerkleTree::<_, RawLeafSha3Algorithm>::new(hashes).root()
    }

    fn validate_proof(&self, num_segments: usize) -> RpcResult<()> {
        // Validate proof data format at first.
        if self.proof.path.is_empty() {
            if self.proof.lemma.len() != 1 {
                return Err(error::invalid_params("proof", "invalid proof"));
            }
        } else if self.proof.lemma.len() != self.proof.path.len() + 2 {
            return Err(error::invalid_params("proof", "invalid proof"));
        }

        // Calculate segment merkle root to verify proof.
        let segment_root = self.calculate_segment_merkle_root();
        if !self
            .proof
            .validate(&segment_root, &self.root, self.index as usize, num_segments)?
        {
            return Err(error::invalid_params("proof", "validation failed"));
        }

        Ok(())
    }

    /// Validates the segment data size and proof.
    pub fn validate(&self, file_size: usize, chunks_per_segment: usize) -> RpcResult<()> {
        let num_segments = self.validate_data_size_and_index(file_size, chunks_per_segment)?;
        self.validate_proof(num_segments as usize)?;
        Ok(())
    }

    /// Returns the index of first chunk in the segment.
    pub fn chunk_index(&self, chunks_per_segment: usize) -> usize {
        self.index as usize * chunks_per_segment
    }
}

mod base64 {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S: Serializer>(v: &Vec<u8>, s: S) -> Result<S::Ok, S::Error> {
        let base64 = base64::encode(v);
        String::serialize(&base64, s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
        let base64 = String::deserialize(d)?;
        base64::decode(base64.as_bytes()).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::Segment;

    #[test]
    fn test_segment_serde() {
        let seg = Segment("hello, world".as_bytes().to_vec());
        let result = serde_json::to_string(&seg).unwrap();
        assert_eq!(result.as_str(), "\"aGVsbG8sIHdvcmxk\"");

        let seg2: Segment = serde_json::from_str("\"aGVsbG8sIHdvcmxk\"").unwrap();
        assert_eq!(String::from_utf8(seg2.0).unwrap().as_str(), "hello, world");
    }
}
