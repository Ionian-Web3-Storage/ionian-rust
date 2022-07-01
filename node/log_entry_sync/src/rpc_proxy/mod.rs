use anyhow::Result;
use async_trait::async_trait;
use ethereum_types::{H160, H256};
use ethers::prelude::Bytes;
use jsonrpsee::core::client::Subscription;

// TODO: Define accounts/filter/events as associated types?
// TODO: Define an abstraction suitable for other chains.
#[async_trait]
pub trait EvmRpcProxy {
    async fn call(&self, to: Address, data: Bytes) -> Result<Bytes>;

    async fn sub_events(&self, filter: SubFilter) -> Subscription<SubEvent>;
}

pub type Address = H160;

pub type Topic = H256;

pub struct SubFilter {
    to: Option<Address>,
    topics: Vec<Topic>,
}

pub struct SubEvent {
    /// Address
    pub address: Address,

    /// Topics
    pub topics: Vec<Topic>,

    /// Data
    pub data: Bytes,
}

pub(crate) mod cfx;
pub(crate) mod eth;
