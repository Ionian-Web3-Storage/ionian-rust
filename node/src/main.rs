mod cli;
mod config;
mod log;

#[macro_use]
extern crate tracing;

use config::IonianConfig;
use futures::channel::mpsc;
use futures::StreamExt;
use network::behaviour::{BehaviourEvent, Request, Response};
use network::rpc::StatusMessage;
use network::Context;
use network::{Libp2pEvent, Service as NetworkService};
use rpc::Command as RPCCommand;
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let matches = cli::cli_app().get_matches();
    let config = IonianConfig::parse(&matches)?;
    log::configure(&config.log_config_file);

    // start RPC
    let (rpc_sender, mut rpc_receiver) = mpsc::channel(0);
    let rpc_config = config.rpc_config()?;
    let _rpc_handle = rpc::run_server(&rpc_config, rpc_sender).await?;

    // start network
    let network_config = config.network_config()?;

    let service_context = Context {
        config: &network_config,
    };

    let (_network_globals, mut network) = NetworkService::<usize>::new(service_context).await?;

    // event loop
    loop {
        tokio::select! {
            event = network.next_event() => match event {
                Libp2pEvent::Behaviour(BehaviourEvent::PeerConnectedIncoming(peer_id)) => {
                    info!(%peer_id, "peer connected incoming");
                    network.send_request(peer_id, 1, Request::Status(StatusMessage{ data: 2 }));
                }
                Libp2pEvent::Behaviour(BehaviourEvent::RequestReceived{ peer_id, id, request }) => {
                    info!(%peer_id, "request = {:?}", request);
                    network.send_response(peer_id, id, Response::Status(StatusMessage{ data: 3 }));
                }
                Libp2pEvent::Behaviour(BehaviourEvent::ResponseReceived{ peer_id, id: _, response }) => {
                    info!(%peer_id, "response = {:?}", response);
                }
                e => {
                    info!("event = {:?}", e);
                }
            },
            command = rpc_receiver.next() => match command {
                Some(RPCCommand::SendStatus) => {
                    info!("Hello!");
                },
                None => break,
            },
        }
    }

    Ok(())
}
