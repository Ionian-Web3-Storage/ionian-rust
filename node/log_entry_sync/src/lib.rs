pub(crate) mod contracts;
pub(crate) mod rpc_proxy;
mod sync_manager;

pub use rpc_proxy::ContractAddress;
pub use sync_manager::{config::LogSyncConfig, LogSyncManager};
