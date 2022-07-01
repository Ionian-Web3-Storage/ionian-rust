use crate::rpc_proxy::{Address, EvmRpcProxy, SubEvent, SubFilter};
use async_trait::async_trait;
use ethers::prelude::{Bytes, Filter, Middleware, Provider, Ws};
use ethers::types::TransactionRequest;
use jsonrpsee::core::client::Subscription;

pub struct EthRpcProxy {
    client: Provider<Ws>,
}

impl EthRpcProxy {
    pub async fn new(url: &str) -> anyhow::Result<EthRpcProxy> {
        let client = Provider::new(Ws::connect(url).await?);
        Ok(Self { client })
    }
}

#[async_trait]
impl EvmRpcProxy for EthRpcProxy {
    async fn call(&self, to: Address, data: Bytes) -> anyhow::Result<Bytes> {
        let request = TransactionRequest::new().to(to).data(data);
        Ok(self.client.call(&request.into(), None).await?)
    }

    async fn sub_events(&self, filter: SubFilter) -> Subscription<SubEvent> {
        todo!()
        // self.client.subscribe_logs(Filter::new())
    }
}
