use super::{Client, RuntimeContext};
use gossip_cache::GossipCache;
use log_entry_sync::{LogSyncConfig, LogSyncManager};
use miner::{MinerMessage, MinerService};
use network::{
    self, NetworkConfig, NetworkGlobals, NetworkMessage, RequestId, Service as LibP2PService,
};
use router::RouterService;
use rpc::RPCConfig;
use std::sync::Arc;
use storage::log_store::{SimpleLogStore, Store};
use sync::{SyncSender, SyncService};
use tokio::sync::mpsc;

macro_rules! require {
    ($component:expr, $self:ident, $e:ident) => {
        $self
            .$e
            .as_ref()
            .ok_or(format!("{} requires {}", $component, std::stringify!($e)))?
    };
}

struct NetworkComponents {
    send: mpsc::UnboundedSender<NetworkMessage>,
    globals: Arc<NetworkGlobals>,

    // note: these will be owned by the router service
    owned: Option<(
        LibP2PService<RequestId>,
        mpsc::UnboundedReceiver<NetworkMessage>,
    )>,
}

struct SyncComponents {
    send: SyncSender,
}

struct MinerComponents {
    send: mpsc::UnboundedSender<MinerMessage>,
}

/// Builds a `Client` instance.
///
/// ## Notes
///
/// The builder may start some services (e.g.., libp2p, http server) immediately after they are
/// initialized, _before_ the `self.build(..)` method has been called.
pub struct ClientBuilder {
    runtime_context: Option<RuntimeContext>,
    store: Option<Arc<dyn Store>>,
    async_store: Option<storage_async::Store>,
    gossip_cache: Option<Arc<GossipCache>>,
    network: Option<NetworkComponents>,
    sync: Option<SyncComponents>,
    miner: Option<MinerComponents>,
}

impl ClientBuilder {
    /// Instantiates a new, empty builder.
    pub fn new() -> Self {
        Self {
            runtime_context: None,
            store: None,
            async_store: None,
            gossip_cache: None,
            network: None,
            sync: None,
            miner: None,
        }
    }

    /// Specifies the runtime context (tokio executor, logger, etc) for client services.
    pub fn with_runtime_context(mut self, context: RuntimeContext) -> Self {
        self.runtime_context = Some(context);
        self
    }

    /// Initializes in-memory storage.
    pub fn with_memory_store(mut self) -> Result<Self, String> {
        let store = Arc::new(
            SimpleLogStore::memorydb()
                .map_err(|e| format!("Unable to start in-memory store: {:?}", e))?,
        );

        self.store = Some(store.clone());

        if let Some(ctx) = self.runtime_context.as_ref() {
            self.async_store = Some(storage_async::Store::new(store, ctx.executor.clone()));
        }

        Ok(self)
    }

    pub fn with_gossip_cache(mut self) -> Self {
        let gossip_cache = Default::default();
        self.gossip_cache = Some(Arc::new(gossip_cache));
        self
    }

    /// Starts the networking stack.
    pub async fn with_network(mut self, config: &NetworkConfig) -> Result<Self, String> {
        let executor = require!("network", self, runtime_context).clone().executor;

        // construct the libp2p service context
        let service_context = network::Context { config };

        // launch libp2p service
        let (globals, libp2p) = LibP2PService::new(executor, service_context)
            .await
            .map_err(|e| format!("Failed to start network service: {:?}", e))?;

        // construct communication channel
        let (send, recv) = mpsc::unbounded_channel::<NetworkMessage>();

        self.network = Some(NetworkComponents {
            send,
            globals,
            owned: Some((libp2p, recv)),
        });

        Ok(self)
    }

    pub fn with_sync(mut self) -> Result<Self, String> {
        let executor = require!("sync", self, runtime_context).clone().executor;
        let store = require!("sync", self, store).clone();
        let gossip_cache = require!("sync", self, gossip_cache).clone();
        let network_send = require!("sync", self, network).send.clone();
        let network_globals = require!("sync", self, network).globals.clone();

        let send = SyncService::spawn(executor, network_send, network_globals, store, gossip_cache);
        self.sync = Some(SyncComponents { send });

        Ok(self)
    }

    pub fn with_miner(mut self) -> Result<Self, String> {
        let executor = require!("miner", self, runtime_context).clone().executor;
        let network_send = require!("miner", self, network).send.clone();

        let send = MinerService::spawn(executor, network_send);
        self.miner = Some(MinerComponents { send });

        Ok(self)
    }

    /// Starts the networking stack.
    pub fn with_router(mut self) -> Result<Self, String> {
        let executor = require!("router", self, runtime_context).clone().executor;
        let sync_send = require!("router", self, sync).send.clone(); // note: we can make this optional in the future
        let miner_send = require!("router", self, miner).send.clone(); // note: we can make this optional in the future
        let store = require!("router", self, store).clone();
        let gossip_cache = require!("router", self, gossip_cache).clone();

        let network = self.network.as_mut().ok_or("router requires a network")?;

        let (libp2p, netowork_recv) = network
            .owned
            .take() // router takes ownership of libp2p and network_recv
            .ok_or("router requires a network")?;

        RouterService::spawn(
            executor,
            libp2p,
            network.globals.clone(),
            netowork_recv,
            network.send.clone(),
            sync_send,
            miner_send,
            store,
            gossip_cache,
        );

        Ok(self)
    }

    pub async fn with_rpc(self, config: RPCConfig) -> Result<Self, String> {
        if !config.enabled {
            return Ok(self);
        }

        let executor = require!("rpc", self, runtime_context).clone().executor;
        let async_store = require!("rpc", self, async_store).clone();

        let (chunk_pool, chunk_pool_handler) = chunk_pool::unbounded(async_store.clone());

        let ctx = rpc::Context {
            config,
            network_globals: self.network.as_ref().map(|network| network.globals.clone()),
            network_send: self.network.as_ref().map(|network| network.send.clone()),
            sync_send: self.sync.as_ref().map(|sync| sync.send.clone()),
            log_store: async_store,
            chunk_pool,
            shutdown_sender: executor.shutdown_sender(),
        };

        let rpc_handle = rpc::run_server(ctx)
            .await
            .map_err(|e| format!("Unable to start HTTP RPC server: {:?}", e))?;

        executor.spawn(rpc_handle, "rpc");
        executor.spawn(chunk_pool_handler.run(), "chunk_pool_handler");

        Ok(self)
    }

    pub fn with_log_sync(self, config: LogSyncConfig) -> Result<Self, String> {
        let executor = require!("log_sync", self, runtime_context).clone().executor;
        let store = require!("log_sync", self, store).clone();
        LogSyncManager::spawn(config, executor, store).map_err(|e| e.to_string())?;
        Ok(self)
    }

    /// Consumes the builder, returning a `Client` if all necessary components have been
    /// specified.
    pub fn build(self) -> Result<Client, String> {
        require!("client", self, runtime_context);

        Ok(Client {
            network_globals: self.network.as_ref().map(|network| network.globals.clone()),
        })
    }
}
