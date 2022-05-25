use std::net::SocketAddr;

#[derive(Clone)]
pub struct Config {
    pub enabled: bool,
    pub listen_address: SocketAddr,
}
