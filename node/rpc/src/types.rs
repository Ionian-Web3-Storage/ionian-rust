use jsonrpsee::core::Error as RpcError;
use serde::{Deserialize, Serialize};
use shared_types::Transaction;

pub(crate) type RpcResult<T> = Result<T, RpcError>;

#[derive(Serialize, Deserialize)]
pub struct Status {
    pub connected_peers: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FileInfo {
    pub tx: Transaction,
    pub finalized: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Segment(#[serde(with = "base64")] pub Vec<u8>);

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
