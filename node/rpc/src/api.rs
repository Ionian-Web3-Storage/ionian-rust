use crate::types::Status;
use jsonrpsee::proc_macros::rpc;

#[rpc(server, client, namespace = "ionian")]
pub trait Rpc {
    #[method(name = "getStatus")]
    async fn get_status(&self) -> Result<Status, jsonrpsee::core::Error>;

    #[method(name = "sendStatus")]
    async fn send_status(&self) -> Result<String, jsonrpsee::core::Error>;
}
