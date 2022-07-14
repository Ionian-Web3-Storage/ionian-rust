use merkle_light::hash::Algorithm;
use std::hash::Hasher;
use tiny_keccak::{Hasher as KeccakHasher, Keccak};

/// MT leaf hash prefix
pub const LEAF: u8 = 0x00;
/// MT interior node hash prefix
const INTERIOR: u8 = 0x01;

// TODO: Option here is only used for compatibility with `tiny_keccak` and `merkle_light`.
#[derive(Clone)]
pub struct RawLeafSha3Algorithm(Option<Keccak>);

impl RawLeafSha3Algorithm {
    fn new() -> RawLeafSha3Algorithm {
        RawLeafSha3Algorithm(Some(Keccak::v256()))
    }
}

impl Default for RawLeafSha3Algorithm {
    fn default() -> RawLeafSha3Algorithm {
        RawLeafSha3Algorithm::new()
    }
}

impl Hasher for RawLeafSha3Algorithm {
    #[inline]
    fn write(&mut self, msg: &[u8]) {
        self.0.as_mut().unwrap().update(msg)
    }

    #[inline]
    fn finish(&self) -> u64 {
        unimplemented!()
    }
}

pub type CryptoSHA256Hash = [u8; 32];

impl Algorithm<CryptoSHA256Hash> for RawLeafSha3Algorithm {
    #[inline]
    fn hash(&mut self) -> CryptoSHA256Hash {
        let mut h = [0u8; 32];
        self.0.take().unwrap().finalize(&mut h);
        h
    }

    fn leaf(&mut self, leaf: CryptoSHA256Hash) -> CryptoSHA256Hash {
        // Leave the leaf node untouched so we can save the subtree root
        // just as the leaf node for the top tree.
        // `LEAF` is prepended for `Chunk` hash computation.
        leaf
    }

    #[inline]
    fn node(&mut self, left: CryptoSHA256Hash, right: CryptoSHA256Hash) -> CryptoSHA256Hash {
        self.write(&[INTERIOR]);
        self.write(left.as_ref());
        self.write(right.as_ref());
        self.hash()
    }
}

#[cfg(test)]
mod tests {
    use crate::{RawLeafSha3Algorithm, LEAF};
    use merkle_light::{hash::Algorithm, merkle::MerkleTree};
    use std::hash::Hasher;

    #[test]
    fn test_root() {
        let results = [
            [
                241, 48, 193, 94, 101, 245, 240, 244, 161, 29, 60, 193, 132, 4, 58, 78, 37, 196,
                155, 133, 151, 104, 229, 103, 105, 91, 48, 189, 66, 90, 95, 116,
            ],
            [
                122, 137, 1, 255, 31, 110, 121, 53, 237, 46, 119, 179, 186, 109, 25, 47, 207, 184,
                83, 210, 235, 132, 9, 94, 252, 42, 77, 88, 169, 8, 80, 157,
            ],
        ];
        for (test_index, n_chunk) in [6, 7].into_iter().enumerate() {
            let mut data = Vec::with_capacity(n_chunk);
            for _ in 0..n_chunk {
                let mut a = RawLeafSha3Algorithm::default();
                a.write(&[LEAF]);
                a.write(&[0; 256]);
                data.push(a.hash());
            }
            let mt = MerkleTree::<_, RawLeafSha3Algorithm>::new(data);
            println!("{:?} {}", mt.root(), hex::encode(&mt.root()));
            assert_eq!(results[test_index], mt.root());
        }
    }
}
