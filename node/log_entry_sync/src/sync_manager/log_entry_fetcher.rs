use crate::contracts::IonianLogContract;
use crate::rpc_proxy::ContractAddress;
use anyhow::{anyhow, Result};
use ethers::prelude::{Http, Provider};
use shared_types::Transaction;
use std::sync::Arc;

pub struct LogEntryFetcher {
    contract: IonianLogContract<Provider<Http>>,
}

impl LogEntryFetcher {
    pub async fn new(url: &str, contract_address: ContractAddress) -> Result<Self> {
        let contract = IonianLogContract::new(contract_address, Arc::new(Provider::try_from(url)?));
        // TODO: `error` types are removed from the ABI json file.
        Ok(Self { contract })
    }

    pub async fn num_log_entries(&self) -> Result<u64> {
        let response = self
            .contract
            .num_log_entries()
            .call()
            .await
            .map_err(|e| anyhow!("{:?}", e))?;
        Ok(response.as_u64())
    }

    pub async fn entry_at(&self, offset: u64, limit: Option<usize>) -> Result<Vec<Transaction>> {
        let response = self
            .contract
            .get_log_entries(offset.into(), limit.unwrap_or(1).into())
            .call()
            .await
            .map_err(|e| anyhow!("{:?}", e))?;
        Ok(response
            .into_iter()
            .enumerate()
            .map(|(i, e)| Transaction {
                stream_ids: e.stream_ids,
                data: e.data.to_vec(),
                size: e.size_bytes.as_u64(),
                data_merkle_root: e.data_root.into(),
                seq: offset + i as u64,
            })
            .collect())
    }
}
