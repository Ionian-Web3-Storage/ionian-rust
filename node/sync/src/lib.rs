#[macro_use]
extern crate tracing;

mod context;
mod controllers;
mod service;

pub(crate) use context::SyncNetworkContext;
pub use service::{SyncMessage, SyncRequest, SyncResponse, SyncSender, SyncService};

pub(crate) fn timestamp_now() -> u32 {
    let timestamp = chrono::Utc::now().timestamp();
    u32::try_from(timestamp).expect("The year is between 1970 and 2106")
}
