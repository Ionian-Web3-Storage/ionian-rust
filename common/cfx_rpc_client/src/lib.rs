mod types;

use ethers::prelude::Bytes;
use jsonrpsee::async_client::Client;

use crate::types::pubsub;
use jsonrpsee::core::Error;
use jsonrpsee::proc_macros::rpc;
use jsonrpsee::ws_client::WsClientBuilder;

pub use types::call::CallRequest;

#[rpc(client, namespace = "cfx")]
pub trait CfxRpc {
    /// Async method call example.
    #[method(name = "call")]
    async fn call(&self, request: CallRequest) -> Result<Bytes, Error>;

    #[subscription(name = "subscribe", unsubscribe = "unsubscribe", item = pubsub::PubsubResult)]
    fn sub(&self, kind: pubsub::Kind, params: pubsub::Params);
}

pub async fn new_cfx_client(url: &str) -> anyhow::Result<Client> {
    let client = WsClientBuilder::default().build(url).await?;
    Ok(client)
}
