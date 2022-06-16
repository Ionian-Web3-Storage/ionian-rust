use crate::log_store::simple_log_store::{merkle_tree, SimpleLogStore};
use crate::log_store::{LogStoreChunkRead, LogStoreChunkWrite, LogStoreRead, LogStoreWrite};
use rand::random;
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
    let chunk_count = store.store.chunk_batch_size + 1;
    let data_size = CHUNK_SIZE * chunk_count;
    let mut data = vec![0u8; data_size];
    for i in 0..chunk_count {
        data[i * CHUNK_SIZE] = random();
    }
    let merkle = merkle_tree(&data, CHUNK_SIZE, None).unwrap();
    let chunk_array = ChunkArray {
        data,
        start_index: 0,
    };
    let tx_hash = TransactionHash::random();
    let tx = Transaction {
        hash: tx_hash,
        size: data_size as u64,
        data_merkle_root: merkle.root().into(),
        seq: 0,
    };
    store.put_tx(tx.clone()).unwrap();
    store.put_chunks(tx.seq, chunk_array.clone()).unwrap();
    store.finalize_tx(tx.seq).unwrap();
    assert_eq!(store.get_tx_by_seq_number(0).unwrap().unwrap(), tx);
    assert_eq!(store.get_tx_by_hash(&tx_hash).unwrap().unwrap(), tx);
    for i in 0..chunk_count {
        assert_eq!(
            store
                .get_chunk_by_tx_and_index(tx.seq, i as u32)
                .unwrap()
                .unwrap(),
            chunk_array.chunk_at(i as u32).unwrap()
        );
    }
    assert_eq!(
        store
            .get_chunk_by_tx_and_index(tx.seq, chunk_count as u32)
            .unwrap(),
        None
    );

    assert_eq!(
        store
            .get_chunks_by_tx_and_index_range(tx.seq, 0, chunk_count as u32)
            .unwrap()
            .unwrap(),
        chunk_array
    );
    for i in 0..chunk_count {
        let chunk_with_proof = store
            .get_chunk_with_proof_by_tx_and_index(tx.seq, i as u32)
            .unwrap()
            .unwrap();
        assert_eq!(
            chunk_with_proof.chunk,
            chunk_array.chunk_at(i as u32).unwrap()
        );
        assert!(chunk_with_proof.validate(&tx.data_merkle_root, i).unwrap());
    }
}
