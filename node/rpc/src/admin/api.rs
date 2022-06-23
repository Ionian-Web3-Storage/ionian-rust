use jsonrpsee::proc_macros::rpc;

#[rpc(server, client, namespace = "admin")]
pub trait Rpc {
    #[method(name = "shutdown")]
    async fn shutdown(&self) -> Result<(), jsonrpsee::core::Error>;

    #[method(name = "startSyncFile")]
    async fn start_sync_file(
        &self,
        tx_seq: u64,
        num_chunks: usize,
    ) -> Result<(), jsonrpsee::core::Error>;

    #[method(name = "getSyncStatus")]
    async fn get_sync_status(&self, tx_seq: u64) -> Result<String, jsonrpsee::core::Error>;
}
