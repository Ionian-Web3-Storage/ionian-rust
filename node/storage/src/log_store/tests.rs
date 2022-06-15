use crate::log_store::simple_log_store::SimpleLogStore;
use crate::log_store::{LogStoreChunkRead, LogStoreChunkWrite, LogStoreRead, LogStoreWrite};
use ethereum_types::{H256, U256};
use shared_types::{ChunkArray, Transaction, TransactionHash, CHUNK_SIZE};
use std::ops::Deref;
use tempdir::TempDir;

struct TempSimpleLogStore {
    // Keep this so that the directory will be automatically deleted.
    _temp_dir: TempDir,
    pub store: SimpleLogStore,
}

impl AsRef<SimpleLogStore> for TempSimpleLogStore {
    fn as_ref(&self) -> &SimpleLogStore {
        &self.store
    }
}

impl Deref for TempSimpleLogStore {
    type Target = SimpleLogStore;

    fn deref(&self) -> &Self::Target {
        &self.store
    }
}

fn create_temp_log_store() -> TempSimpleLogStore {
    let temp_dir = TempDir::new("test_ionian_storage").unwrap();
    let store = SimpleLogStore::open(&temp_dir.path()).unwrap();
    TempSimpleLogStore {
        _temp_dir: temp_dir,
        store,
    }
}

#[test]
fn test_put_get() {
    let store = create_temp_log_store();
    let data_size = CHUNK_SIZE * store.store.chunk_batch_size + 1;
    let data = vec![1; data_size];
    let chunk_array = ChunkArray {
        data,
        start_index: 0,
    };
    let tx_hash = TransactionHash::random();
    let tx = Transaction {
        hash: tx_hash,
        size: data_size as u64,
        data_merkle_root: Default::default(),
        seq: 0,
    };
    store.put_tx(tx.clone()).unwrap();
    store.put_chunks(tx.seq, chunk_array.clone()).unwrap();
    assert_eq!(store.get_tx_by_seq_number(0).unwrap().unwrap(), tx);
    assert_eq!(store.get_tx_by_hash(&tx_hash).unwrap().unwrap(), tx);
    assert_eq!(
        store.get_chunk_by_tx_and_index(tx.seq, 0).unwrap().unwrap(),
        chunk_array.chunk_at(0).unwrap()
    );
    assert_eq!(
        store.get_chunk_by_tx_and_index(tx.seq, 1).unwrap().unwrap(),
        chunk_array.chunk_at(1).unwrap()
    );
    assert_eq!(
        store
            .get_chunks_by_tx_and_index_range(tx.seq, 0, 2)
            .unwrap()
            .unwrap(),
        chunk_array
    );
}
