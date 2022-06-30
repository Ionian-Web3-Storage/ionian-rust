use crate::types::RpcAddress;
use ethereum_types::{U256, U64};
use rustc_hex::{FromHex, ToHex};
use serde::de::{Error, Visitor};
use serde::{Serialize, Serializer, *};
use std::fmt;

#[derive(Debug, Default, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CallRequest {
    /// From
    pub from: Option<RpcAddress>,
    /// To
    pub to: Option<RpcAddress>,
    /// Gas Price
    pub gas_price: Option<U256>,
    /// Gas
    pub gas: Option<U256>,
    /// Value
    pub value: Option<U256>,
    /// Data
    pub data: Option<Bytes>,
    /// Nonce
    pub nonce: Option<U256>,
    /// StorageLimit
    pub storage_limit: Option<U64>,
}

/// Wrapper structure around vector of bytes.
#[derive(Debug, PartialEq, Eq, Default, Hash, Clone)]
#[allow(dead_code)]
pub struct Bytes(pub Vec<u8>);

impl Bytes {
    /// Simple constructor.
    pub fn new(bytes: Vec<u8>) -> Bytes {
        Bytes(bytes)
    }

    /// Convert back to vector
    #[allow(dead_code)]
    pub fn into_vec(self) -> Vec<u8> {
        self.0
    }
}

impl From<Vec<u8>> for Bytes {
    fn from(bytes: Vec<u8>) -> Bytes {
        Bytes(bytes)
    }
}

impl Into<Vec<u8>> for Bytes {
    fn into(self) -> Vec<u8> {
        self.0
    }
}

impl Serialize for Bytes {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut serialized = "0x".to_owned();
        serialized.push_str(self.0.to_hex::<String>().as_ref());
        serializer.serialize_str(serialized.as_ref())
    }
}

impl<'a> Deserialize<'a> for Bytes {
    fn deserialize<D>(deserializer: D) -> Result<Bytes, D::Error>
    where
        D: Deserializer<'a>,
    {
        deserializer.deserialize_any(BytesVisitor)
    }
}

struct BytesVisitor;

impl<'a> Visitor<'a> for BytesVisitor {
    type Value = Bytes;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(formatter, "a 0x-prefixed, hex-encoded vector of bytes")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: Error,
    {
        if value.len() >= 2 && &value[0..2] == "0x" && value.len() & 1 == 0 {
            Ok(Bytes::new(FromHex::from_hex(&value[2..]).map_err(|e| {
                Error::custom(format!("Invalid hex: {}", e))
            })?))
        } else {
            Err(Error::custom(
                "Invalid bytes format. Expected a 0x-prefixed hex string with even length",
            ))
        }
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
    where
        E: Error,
    {
        self.visit_str(value.as_ref())
    }
}
