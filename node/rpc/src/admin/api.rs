use jsonrpsee::proc_macros::rpc;

#[rpc(server, client, namespace = "admin")]
pub trait Rpc {
    #[method(name = "shutdown")]
    async fn shutdown(&self) -> Result<(), jsonrpsee::core::Error>;
}
