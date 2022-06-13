use super::{Client, RuntimeContext};
use network::{NetworkConfig, NetworkGlobals, ServiceMessage};
use rpc::RPCConfig;
use service::NetworkService;
use std::sync::Arc;
use storage::log_store::{SimpleLogStore, Store};
use tokio::sync::mpsc::UnboundedSender;

/// Builds a `Client` instance.
///
/// ## Notes
///
/// The builder may start some services (e.g.., libp2p, http server) immediately after they are
/// initialized, _before_ the `self.build(..)` method has been called.
pub struct ClientBuilder {
    runtime_context: Option<RuntimeContext>,
    store: Option<Arc<dyn Store>>,
    network_globals: Option<Arc<NetworkGlobals>>,
    network_send: Option<UnboundedSender<ServiceMessage>>,
}

impl ClientBuilder {
    /// Instantiates a new, empty builder.
    pub fn new() -> Self {
        Self {
            runtime_context: None,
            store: None,
            network_globals: None,
            network_send: None,
        }
    }

    /// Specifies the runtime context (tokio executor, logger, etc) for client services.
    pub fn with_runtime_context(mut self, context: RuntimeContext) -> Self {
        self.runtime_context = Some(context);
        self
    }

    /// Initializes in-memory storage.
    pub fn with_memory_store(mut self) -> Result<Self, String> {
        let store = SimpleLogStore::memorydb()
            .map_err(|e| format!("Unable to start in-memory store: {:?}", e))?;

        self.store = Some(Arc::new(store));
        Ok(self)
    }

    /// Starts the networking stack.
    pub async fn with_network(mut self, config: &NetworkConfig) -> Result<Self, String> {
        let context = self
            .runtime_context
            .as_ref()
            .ok_or("network requires a runtime_context")?
            .clone();

        let store = self
            .store
            .as_ref()
            .ok_or("network requires a store")?
            .clone();

        let (network_globals, network_send) =
            NetworkService::start(config, context.executor, store)
                .await
                .map_err(|e| format!("Failed to start network: {:?}", e))?;

        self.network_globals = Some(network_globals);
        self.network_send = Some(network_send);

        Ok(self)
    }

    pub async fn with_rpc(self, config: RPCConfig) -> Result<Self, String> {
        if !config.enabled {
            return Ok(self);
        }

        let executor = self
            .runtime_context
            .as_ref()
            .ok_or("rpc requires a runtime context")?
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
