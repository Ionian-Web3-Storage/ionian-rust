use crate::rpc_proxy::Address;
use cfx_addr::Network;

pub struct SyncManagerConfig {
    pub rpc_endpoint_url: String,
    pub cfx_network: Option<Network>,
    pub contract_address: Address,
}
