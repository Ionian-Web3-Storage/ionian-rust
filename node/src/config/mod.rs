mod config_macro;

mod convert;
use config_macro::*;
use std::ops::Deref;

build_config! {
    // network
    (network_dir, (String), "network".to_string())
    (network_listen_address, (String), "0.0.0.0".to_string())
    (network_libp2p_port, (u16), 1234)
    (network_target_peers, (usize), 3)
    (network_boot_nodes, (Vec<String>), vec![])
    (network_libp2p_nodes, (Vec<String>), vec![])
    (network_private, (bool), false)
    (network_disable_discovery, (bool), false)

    // log sync
    (blockchain_rpc_endpoint, (String), "http://127.0.0.1:8545".to_string())
    (log_contract_address, (String), "".to_string())

    // rpc
    (rpc_enabled, (bool), true)
    (rpc_listen_address, (String), "127.0.0.1:5678".to_string())

    // db
    (db_dir, (String), "db".to_string())

    // misc
    (log_config_file, (String), "log_config".to_string())
}

#[derive(Debug)]
pub struct IonianConfig {
    pub raw_conf: RawConfiguration,
}

impl Deref for IonianConfig {
    type Target = RawConfiguration;

    fn deref(&self) -> &Self::Target {
        &self.raw_conf
    }
}

impl IonianConfig {
    pub fn parse(matches: &clap::ArgMatches) -> Result<IonianConfig, String> {
        Ok(IonianConfig {
            raw_conf: RawConfiguration::parse(matches)?,
        })
    }
}
