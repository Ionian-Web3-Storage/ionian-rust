use crypto::digest::Digest;
use crypto::sha3::{Sha3, Sha3Mode};
use merkle_light::hash::Algorithm;
use std::hash::Hasher;

#[derive(Clone)]
pub struct Sha3Algorithm(Sha3);

impl Sha3Algorithm {
    fn new() -> Sha3Algorithm {
        Sha3Algorithm(Sha3::new(Sha3Mode::Sha3_256))
    }
}

impl Default for Sha3Algorithm {
    fn default() -> Sha3Algorithm {
        Sha3Algorithm::new()
    }
}

impl Hasher for Sha3Algorithm {
    #[inline]
    fn write(&mut self, msg: &[u8]) {
        self.0.input(msg)
    }

    #[inline]
    fn finish(&self) -> u64 {
        unimplemented!()
    }
}

/// The only difference with `Sha3Algorithm` is that the leaf node is not hashed again.
pub struct TopTreeSha3Algorithm(Sha3);

impl TopTreeSha3Algorithm {
    fn new() -> TopTreeSha3Algorithm {
        TopTreeSha3Algorithm(Sha3::new(Sha3Mode::Sha3_256))
    }
}

impl Default for TopTreeSha3Algorithm {
    fn default() -> TopTreeSha3Algorithm {
        TopTreeSha3Algorithm::new()
    }
}

impl Hasher for TopTreeSha3Algorithm {
    #[inline]
    fn write(&mut self, msg: &[u8]) {
        self.0.input(msg)
    }

    #[inline]
    fn finish(&self) -> u64 {
        unimplemented!()
    }
}

pub type CryptoSHA256Hash = [u8; 32];

impl Algorithm<CryptoSHA256Hash> for Sha3Algorithm {
    #[inline]
    fn hash(&mut self) -> CryptoSHA256Hash {
        let mut h = [0u8; 32];
        self.0.result(&mut h);
        h
    }

    #[inline]
    fn reset(&mut self) {
        self.0.reset();
    }
}

impl Algorithm<CryptoSHA256Hash> for TopTreeSha3Algorithm {
    #[inline]
    fn hash(&mut self) -> CryptoSHA256Hash {
        let mut h = [0u8; 32];
        self.0.result(&mut h);
        h
    }

    #[inline]
    fn reset(&mut self) {
        self.0.reset();
    }

    fn leaf(&mut self, leaf: CryptoSHA256Hash) -> CryptoSHA256Hash {
        // Leave the leaf node untouched so we can save the subtree root
        // just as the leaf node for the top tree.
        // This also saves half hash computations for building the merkle tree.
        leaf
    }
}
