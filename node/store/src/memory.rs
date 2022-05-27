#![allow(dead_code)]

use crate::{Error, Store};

pub struct MemoryStore {}

impl Store for MemoryStore {
    fn read(key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        trace!(?key, "MemoryStore read key");
        todo!()
    }

    fn write(key: &[u8], value: &[u8]) -> Result<(), Error> {
        trace!(?key, ?value, "MemoryStore write key");
        todo!()
    }
}
