#[macro_use]
extern crate tracing;

mod error;
mod memory;

pub use error::Error;
pub use memory::MemoryStore;

pub trait Store {
    fn read(key: &[u8]) -> Result<Option<Vec<u8>>, Error>;

    fn write(key: &[u8], value: &[u8]) -> Result<(), Error>;
}
