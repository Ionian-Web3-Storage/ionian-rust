use crate::types::RpcResult;
use jsonrpsee::proc_macros::rpc;

#[rpc(server, client, namespace = "admin")]
pub trait Rpc {
    #[method(name = "shutdown")]
    async fn shutdown(&self) -> RpcResult<()>;

    #[method(name = "startSyncFile")]
    async fn start_sync_file(&self, tx_seq: u64) -> RpcResult<()>;

    #[method(name = "getSyncStatus")]
    async fn get_sync_status(&self, tx_seq: u64) -> RpcResult<String>;
}
