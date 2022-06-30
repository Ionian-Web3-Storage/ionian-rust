mod types;

use anyhow;
use jsonrpsee::async_client::Client;

use crate::types::call::{Bytes, CallRequest};
use crate::types::pubsub;
use jsonrpsee::core::Error;
use jsonrpsee::proc_macros::rpc;
use jsonrpsee::ws_client::WsClientBuilder;

#[rpc(client, namespace = "cfx")]
pub trait CfxRpc {
    /// Async method call example.
    #[method(name = "call")]
    async fn call(&self, request: CallRequest) -> Result<Bytes, Error>;

    #[subscription(name = "subscribe", unsubscribe = "unsubscribe", item = pubsub::PubsubResult)]
    fn sub(&self, kind: pubsub::Kind, params: pubsub::Params);
}

pub async fn new_cfx_client(addr: &str) -> anyhow::Result<Client> {
    let uri = format!("wss://{}", addr);
    let client = WsClientBuilder::default().build(&uri).await?;
    Ok(client)
}

#[tokio::test]
async fn test_call() {
    let client = new_cfx_client("main.confluxrpc.com:443/ws").await.unwrap();
    let mut request = CallRequest::default();
    request.to = Some("cfx:acdk6ruuw4fmks9dsjsan4unnu5x4cnhdpnfbwmt68".to_string());
    let r = client.call(request).await.unwrap();
    println!("{:?}", r);
}
