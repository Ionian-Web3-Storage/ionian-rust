use crate::types::variadic_value::VariadicValue;
use crate::types::RpcAddress;
use ethereum_types::{H256, U256, U64};
use ethers::prelude::Bytes;
use serde::de::{Error, Visitor};
use serde::{self, Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::str::FromStr;

/// Subscription result.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(untagged, rename_all = "camelCase")]
// NOTE: rename_all does not apply to enum member fields
// see: https://github.com/serde-rs/serde/issues/1061
pub enum PubsubResult {
    /// Log
    Log(Log),
}

/// Subscription kind.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub enum Kind {
    /// Logs subscription.
    Logs,
}

/// Subscription kind.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize)]
pub enum Params {
    /// No parameters passed.
    None,
    /// Log parameters.
    #[serde()]
    Logs(CfxRpcLogFilter),
}

impl Default for Params {
    fn default() -> Self {
        Params::None
    }
}

#[derive(PartialEq, Debug, Serialize, Deserialize, Eq, Hash, Clone)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CfxRpcLogFilter {
    /// Search will be applied from this epoch number.
    pub from_epoch: Option<EpochNumber>,

    /// Till this epoch number.
    pub to_epoch: Option<EpochNumber>,

    /// Search will be applied from this block number.
    pub from_block: Option<U64>,

    /// Till this block number.
    pub to_block: Option<U64>,

    /// Search will be applied in these blocks if given.
    /// This will override from/to_epoch fields.
    pub block_hashes: Option<Vec<H256>>,

    /// Search addresses.
    ///
    /// If None, match all.
    /// If specified, log must be produced by one of these addresses.
    pub address: Option<VariadicValue<RpcAddress>>,

    /// Search topics.
    ///
    /// Logs can have 4 topics: the function signature and up to 3 indexed
    /// event arguments. The elements of `topics` match the corresponding
    /// log topics. Example: ["0xA", null, ["0xB", "0xC"], null] matches
    /// logs with "0xA" as the 1st topic AND ("0xB" OR "0xC") as the 3rd
    /// topic. If None, match all.
    pub topics: Option<Vec<VariadicValue<H256>>>,

    /// Logs offset
    ///
    /// If None, return all logs
    /// If specified, should skip the *last* `n` logs.
    pub offset: Option<U64>,

    /// Logs limit
    ///
    /// If None, return all logs
    /// If specified, should only return *last* `n` logs
    /// after the offset has been applied.
    pub limit: Option<U64>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Hash, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Log {
    /// Address
    pub address: RpcAddress,

    /// Topics
    pub topics: Vec<H256>,

    /// Data
    pub data: Bytes,

    /// Block Hash
    #[serde(skip_serializing_if = "Option::is_none")]
    pub block_hash: Option<H256>,

    /// Epoch Number
    #[serde(skip_serializing_if = "Option::is_none")]
    pub epoch_number: Option<U256>,

    /// Transaction Hash
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transaction_hash: Option<H256>,

    /// Transaction Index
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transaction_index: Option<U256>,

    /// Log Index in Block
    #[serde(skip_serializing_if = "Option::is_none")]
    pub log_index: Option<U256>,

    /// Log Index in Transaction
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transaction_log_index: Option<U256>,
}

/// Represents rpc api epoch number param.
#[derive(Debug, PartialEq, Clone, Hash, Eq)]
pub enum EpochNumber {
    /// Number
    Num(U64),
    /// Earliest epoch (true genesis)
    Earliest,
    /// The latest checkpoint (cur_era_genesis)
    LatestCheckpoint,
    ///
    LatestFinalized,
    /// The latest confirmed (with the estimation of the confirmation meter)
    LatestConfirmed,
    /// Latest block with state.
    LatestState,
    /// Latest mined block.
    LatestMined,
}

//impl Default for EpochNumber {
//    fn default() -> Self { EpochNumber::Latest }
//}

impl<'a> Deserialize<'a> for EpochNumber {
    fn deserialize<D>(deserializer: D) -> Result<EpochNumber, D::Error>
    where
        D: Deserializer<'a>,
    {
        deserializer.deserialize_any(EpochNumberVisitor)
    }
}

impl Serialize for EpochNumber {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match *self {
            EpochNumber::Num(ref x) => serializer.serialize_str(&format!("0x{:x}", x)),
            EpochNumber::LatestMined => serializer.serialize_str("latest_mined"),
            EpochNumber::LatestFinalized => serializer.serialize_str("latest_finalized"),
            EpochNumber::LatestState => serializer.serialize_str("latest_state"),
            EpochNumber::Earliest => serializer.serialize_str("earliest"),
            EpochNumber::LatestCheckpoint => serializer.serialize_str("latest_checkpoint"),
            EpochNumber::LatestConfirmed => serializer.serialize_str("latest_confirmed"),
        }
    }
}

struct EpochNumberVisitor;

impl<'a> Visitor<'a> for EpochNumberVisitor {
    type Value = EpochNumber;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(
            formatter,
            "an epoch number or 'latest_mined', 'latest_state', 'latest_checkpoint', 'latest_finalized', 'latest_confirmed' or 'earliest'"
        )
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: Error,
    {
        value.parse().map_err(Error::custom)
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
    where
        E: Error,
    {
        self.visit_str(value.as_ref())
    }
}

impl FromStr for EpochNumber {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "latest_mined" => Ok(EpochNumber::LatestMined),
            "latest_state" => Ok(EpochNumber::LatestState),
            "latest_finalized" => Ok(EpochNumber::LatestFinalized),
            "latest_confirmed" => Ok(EpochNumber::LatestConfirmed),
            "earliest" => Ok(EpochNumber::Earliest),
            "latest_checkpoint" => Ok(EpochNumber::LatestCheckpoint),
            _ if s.starts_with("0x") => u64::from_str_radix(&s[2..], 16)
                .map(U64::from)
                .map(EpochNumber::Num)
                .map_err(|e| format!("Invalid epoch number: {}", e)),
            _ => Err("Invalid epoch number: missing 0x prefix".to_string()),
        }
    }
}
