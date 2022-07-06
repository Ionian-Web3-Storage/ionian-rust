use crate::rpc_proxy::ContractAddress;
use std::time::Duration;

const DEFAULT_FETCH_BATCH_SIZE: usize = 10;
const DEFAULT_SYNC_PERIOD_MS: u64 = 500;

pub struct LogSyncConfig {
    pub rpc_endpoint_url: String,
    pub contract_address: ContractAddress,

    pub fetch_batch_size: usize,
    pub sync_period: Duration,
}

impl LogSyncConfig {
    pub fn new(rpc_endpoint_url: String, contract_address: ContractAddress) -> Self {
        Self {
            rpc_endpoint_url,
            contract_address,
            fetch_batch_size: DEFAULT_FETCH_BATCH_SIZE,
            sync_period: Duration::from_millis(DEFAULT_SYNC_PERIOD_MS),
        }
    }
}
