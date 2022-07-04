use crate::rpc_proxy::Address;
use cfx_addr::Network;
use std::time::Duration;

pub struct SyncManagerConfig {
    pub rpc_endpoint_url: String,
    pub cfx_network: Option<Network>,
    pub contract_address: Address,

    pub fetch_batch_size: usize,
    pub sync_period: Duration,
}
