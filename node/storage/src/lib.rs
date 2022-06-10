use kvdb::KeyValueDB;

pub mod error;
pub mod log_store;

trait IonianKeyValueDB: KeyValueDB {
    fn put(&self, col: u32, key: &[u8], value: &[u8]) -> std::io::Result<()> {
        let mut tx = self.transaction();
        tx.put(col, key, value);
        self.write(tx)
    }
}

impl<T: KeyValueDB> IonianKeyValueDB for T {}
