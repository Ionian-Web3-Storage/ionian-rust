use crate::types::{FileInfo, RpcResult, Segment, Status};
use jsonrpsee::proc_macros::rpc;
use shared_types::DataRoot;

#[rpc(server, client, namespace = "ionian")]
pub trait Rpc {
    #[method(name = "getStatus")]
    async fn get_status(&self) -> RpcResult<Status>;

    #[method(name = "sendStatus")]
    async fn send_status(&self, data: u64) -> RpcResult<()>;

    #[method(name = "uploadSegment")]
    async fn upload_segment(
        &self,
        data_root: DataRoot,
        data_segment: Segment,
        start_index: u32,
        proof: Option<Vec<u8>>,
    ) -> RpcResult<()>;

    #[method(name = "downloadSegment")]
    async fn download_segment(
        &self,
        data_root: DataRoot,
        start_index: u32,
        end_index: u32,
    ) -> RpcResult<Option<Segment>>;

    #[method(name = "getFileInfo")]
    async fn get_file_info(&self, data_root: DataRoot) -> RpcResult<Option<FileInfo>>;
}
