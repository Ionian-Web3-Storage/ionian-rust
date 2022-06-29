#[macro_use]
extern crate tracing;
use kvdb::KeyValueDB;

pub mod error;
pub mod log_store;

trait IonianKeyValueDB: KeyValueDB {
    fn put(&self, col: u32, key: &[u8], value: &[u8]) -> std::io::Result<()> {
        let mut tx = self.transaction();
        tx.put(col, key, value);
        self.write(tx)
    }

    fn delete(&self, col: u32, key: &[u8]) -> std::io::Result<()> {
        let mut tx = self.transaction();
        tx.delete(col, key);
        self.write(tx)
    }

    fn delete_with_prefix(&self, col: u32, key_prefix: &[u8]) -> std::io::Result<()> {
        let mut tx = self.transaction();
        tx.delete_prefix(col, key_prefix);
        self.write(tx)
    }
}

impl<T: KeyValueDB> IonianKeyValueDB for T {}
