#[macro_use]
extern crate tracing;

mod admin;
mod config;
mod error;
mod ionian;
mod types;

use futures::channel::mpsc::Sender;
use jsonrpsee::http_server::{HttpServerBuilder, HttpServerHandle};
use network::NetworkGlobals;
use service::NetworkMessage;
use std::error::Error;
use std::sync::Arc;
use task_executor::ShutdownReason;
use tokio::sync::mpsc::UnboundedSender;

use admin::RpcServer as AdminRpcServer;
use ionian::RpcServer as IonianRpcServer;

pub use config::Config as RPCConfig;

/// A wrapper around all the items required to spawn the HTTP server.
///
/// The server will gracefully handle the case where any fields are `None`.
#[derive(Clone)]
pub struct Context {
    pub config: RPCConfig,
    pub network_tx: Option<UnboundedSender<NetworkMessage>>,
    pub network_globals: Option<Arc<NetworkGlobals>>,
    pub shutdown_sender: Sender<ShutdownReason>,
}

pub async fn run_server(ctx: Context) -> Result<HttpServerHandle, Box<dyn Error>> {
    let server = HttpServerBuilder::default()
        .build(ctx.config.listen_address)
        .await?;

    let mut ionian = (ionian::RpcServerImpl { ctx: ctx.clone() }).into_rpc();
    let admin = (admin::RpcServerImpl { ctx }).into_rpc();
    ionian.merge(admin)?;

    let addr = server.local_addr()?;
    let handle = server.start(ionian)?;
    info!("Server started http://{}", addr);

    Ok(handle)
}
