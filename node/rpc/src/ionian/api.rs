use crate::types::Status;
use jsonrpsee::proc_macros::rpc;
use shared_types::DataRoot;

#[rpc(server, client, namespace = "ionian")]
pub trait Rpc {
    #[method(name = "getStatus")]
    async fn get_status(&self) -> Result<Status, jsonrpsee::core::Error>;

    #[method(name = "sendStatus")]
    async fn send_status(&self, data: u64) -> Result<(), jsonrpsee::core::Error>;

    #[method(name = "uploadSegment")]
    async fn upload_segment(
        &self,
        data_root: DataRoot,
        data_segment: Vec<u8>,
        start_index: u32,
        proof: Option<Vec<u8>>,
    ) -> Result<(), jsonrpsee::core::Error>;

    #[method(name = "downloadSegment")]
    async fn download_segment(
        &self,
        data_root: DataRoot,
        start_index: u32,
        end_index: u32,
    ) -> Result<Option<Vec<u8>>, jsonrpsee::core::Error>;
}
