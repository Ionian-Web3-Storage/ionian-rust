use crate::types::{FileInfo, RpcResult, Segment, SegmentWithProof, Status};
use jsonrpsee::proc_macros::rpc;
use shared_types::DataRoot;

#[rpc(server, client, namespace = "ionian")]
pub trait Rpc {
    #[method(name = "getStatus")]
    async fn get_status(&self) -> RpcResult<Status>;

    #[method(name = "uploadSegment")]
    async fn upload_segment(&self, segment: SegmentWithProof) -> RpcResult<()>;

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
