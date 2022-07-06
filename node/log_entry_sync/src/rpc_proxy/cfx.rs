use crate::rpc_proxy::{ContractAddress, EvmRpcProxy, SubEvent, SubFilter};
use anyhow::Result;
use async_trait::async_trait;
use cfx_addr::{cfx_addr_encode, EncodingOptions, Network};
use cfx_rpc_client::{new_cfx_client, CallRequest, CfxRpcClient};
use ethers::prelude::Bytes;
use jsonrpsee::async_client::Client;
use jsonrpsee::core::client::Subscription;

pub struct CfxClient {
    client: Client,
    network: Network,
}

impl CfxClient {
    #[allow(unused)]
    pub async fn new(addr: &str, network: Network) -> Result<Self> {
        let client = new_cfx_client(addr).await?;
        Ok(Self { client, network })
    }
}

#[async_trait]
impl EvmRpcProxy for CfxClient {
    async fn call(&self, to: ContractAddress, data: Bytes) -> Result<Bytes> {
        let to_addr = cfx_addr_encode(to.as_bytes(), self.network, EncodingOptions::Simple)?;
        let request = CallRequest {
            to: Some(to_addr),
            data: Some(data),
            ..Default::default()
        };
        self.client.call(request).await.map_err(Into::into)
    }

    async fn sub_events(&self, _filter: SubFilter) -> Subscription<SubEvent> {
        todo!()
    }
}
