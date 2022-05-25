use super::api::RpcServer;
use crate::Context;
use futures::prelude::*;
use jsonrpsee::core::async_trait;
use task_executor::ShutdownReason;

pub struct RpcServerImpl {
    pub ctx: Context,
}

#[async_trait]
impl RpcServer for RpcServerImpl {
    async fn shutdown(&self) -> Result<(), jsonrpsee::core::Error> {
        let res = self
            .ctx
            .shutdown_sender
            .clone()
            .send(ShutdownReason::Success("Shutdown by admin"))
            .await;

        if let Err(e) = res {
            warn!(
                error = %e,
                "failed to send a shutdown signal",
            )
        }

        Ok(())
    }
}
