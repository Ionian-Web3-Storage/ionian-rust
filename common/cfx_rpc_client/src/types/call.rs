use crate::types::RpcAddress;
use ethereum_types::{U256, U64};
use ethers::prelude::Bytes;
use serde::Serialize;

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
