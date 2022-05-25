use super::{Client, RuntimeContext};
use network::{NetworkConfig, NetworkGlobals};
use rpc::RPCConfig;
use service::{NetworkMessage, NetworkService};
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedSender;

/// Builds a `Client` instance.
///
/// ## Notes
///
/// The builder may start some services (e.g.., libp2p, http server) immediately after they are
/// initialized, _before_ the `self.build(..)` method has been called.
pub struct ClientBuilder {
    runtime_context: Option<RuntimeContext>,
    network_globals: Option<Arc<NetworkGlobals>>,
    network_send: Option<UnboundedSender<NetworkMessage>>,
}

impl ClientBuilder {
    /// Instantiates a new, empty builder.
    pub fn new() -> Self {
        Self {
            runtime_context: None,
            network_globals: None,
            network_send: None,
        }
    }

    /// Specifies the runtime context (tokio executor, logger, etc) for client services.
    pub fn runtime_context(mut self, context: RuntimeContext) -> Self {
        self.runtime_context = Some(context);
        self
    }

    /// Starts the networking stack.
    pub async fn network(mut self, config: &NetworkConfig) -> Result<Self, String> {
        let context = self
            .runtime_context
            .as_ref()
            .ok_or("network requires a runtime_context")?
            .clone();

        let (network_globals, network_send) = NetworkService::start(config, context.executor)
            .await
            .map_err(|e| format!("Failed to start network: {:?}", e))?;

        self.network_globals = Some(network_globals);
        self.network_send = Some(network_send);

        Ok(self)
    }

    pub async fn rpc(self, config: RPCConfig) -> Result<Self, String> {
        if !config.enabled {
            return Ok(self);
        }

        let executor = self
            .runtime_context
            .as_ref()
            .ok_or("build requires a runtime context")?
            .executor
            .clone();

        let shutdown_sender = executor.shutdown_sender();

        let ctx = rpc::Context {
            config,
            network_tx: self.network_send.clone(),
            network_globals: self.network_globals.clone(),
            shutdown_sender,
        };

        let rpc_handle = rpc::run_server(ctx)
            .await
            .map_err(|e| format!("Unable to start HTTP RPC server: {:?}", e))?;

        executor.spawn(rpc_handle, "rpc");

        Ok(self)
    }

    /// Consumes the builder, returning a `Client` if all necessary components have been
    /// specified.
    pub fn build(self) -> Result<Client, String> {
        self.runtime_context
            .as_ref()
            .ok_or("build requires a runtime context")?;

        Ok(Client {
            network_globals: self.network_globals,
        })
    }
}
