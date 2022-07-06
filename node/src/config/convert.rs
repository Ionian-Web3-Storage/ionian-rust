#![allow(clippy::field_reassign_with_default)]

use crate::IonianConfig;
use log_entry_sync::{ContractAddress, LogSyncConfig};
use network::NetworkConfig;
use rpc::RPCConfig;

impl IonianConfig {
    pub fn network_config(&self) -> Result<NetworkConfig, String> {
        let mut network_config = NetworkConfig::default();

        network_config.listen_address = self
            .network_listen_address
            .parse::<std::net::IpAddr>()
            .map_err(|e| format!("Unable to parse network_listen_address: {:?}", e))?;

        network_config.network_dir = self.network_dir.clone().into();
        network_config.libp2p_port = self.network_libp2p_port;
        network_config.disable_discovery = self.network_disable_discovery;
        network_config.discovery_port = self.network_libp2p_port;
        network_config.enr_tcp_port = Some(self.network_libp2p_port);
        network_config.enr_udp_port = Some(self.network_libp2p_port);

        network_config.boot_nodes_multiaddr = self
            .network_boot_nodes
            .iter()
            .map(|addr| addr.parse::<libp2p::Multiaddr>())
            .collect::<Result<_, _>>()
            .map_err(|e| format!("Unable to parse network_boot_nodes: {:?}", e))?;

        network_config.libp2p_nodes = self
            .network_libp2p_nodes
            .iter()
            .map(|addr| addr.parse::<libp2p::Multiaddr>())
            .collect::<Result<_, _>>()
            .map_err(|e| format!("Unable to parse network_libp2p_nodes: {:?}", e))?;

        network_config.discv5_config.table_filter = |_| true;

        // TODO
        network_config.enr_address = Some("127.0.0.1".parse::<std::net::IpAddr>().unwrap());
        network_config.target_peers = self.network_target_peers;
        network_config.private = self.network_private;

        Ok(network_config)
    }

    pub fn rpc_config(&self) -> Result<RPCConfig, String> {
        let listen_address = self
            .rpc_listen_address
            .parse::<std::net::SocketAddr>()
            .map_err(|e| format!("Unable to parse rpc_listen_address: {:?}", e))?;

        Ok(RPCConfig {
            enabled: self.rpc_enabled,
            listen_address,
        })
    }

    pub fn log_sync_config(&self) -> Result<LogSyncConfig, String> {
        let contract_address = self
            .log_contract_address
            .parse::<ContractAddress>()
            .map_err(|e| format!("Unable to parse log_contract_address: {:?}", e))?;
        Ok(LogSyncConfig::new(
            self.blockchain_rpc_endpoint.clone(),
            contract_address,
        ))
    }
}
