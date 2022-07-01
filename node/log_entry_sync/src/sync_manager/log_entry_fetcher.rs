use crate::contracts::IonianLogContract;
use crate::rpc_proxy::cfx::CfxRpcProxy;
use crate::rpc_proxy::eth::EthRpcProxy;
use crate::rpc_proxy::{Address, EvmRpcProxy};
use anyhow::{anyhow, Result};
use cfx_addr::Network;
use ethereum_types::U256;
use ethers::contract::Contract;
use ethers::prelude::{abigen, Middleware, Provider, Ws};
use std::fs::File;
use std::pin::Pin;
use std::sync::Arc;

pub struct LogEntryFetcher {
    contract: IonianLogContract<Provider<Ws>>,
    contract_address: Address,
}

impl LogEntryFetcher {
    pub async fn new(url: &str, contract_address: Address) -> Result<Self> {
        let contract = IonianLogContract::new(
            contract_address,
            Arc::new(Provider::<Ws>::new(Ws::connect(url).await?)),
        );
        // TODO: `error` types are removed from the ABI json file.
        Ok(Self {
            contract,
            contract_address,
        })
    }

    async fn num_log_entries(&self) -> Result<u128> {
        let func = self.contract.num_log_entries();
        let response = func.call().await.map_err(|e| anyhow!("{:?}", e))?;
        Ok(response.as_u128())
    }
}

#[tokio::test]
async fn test_fetch() {
    let fetcher = LogEntryFetcher::new(
        "wss://evmtestnet.confluxrpc.com:443/ws",
        "b42a30f4ba37ba033bb918f028561785bd8aa34c".parse().unwrap(),
    )
    .await
    .unwrap();
    println!("{}", fetcher.num_log_entries().await.unwrap());
}
