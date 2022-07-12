use crate::log_store::simple_log_store::{sub_merkle_tree, SimpleLogStore};
use crate::log_store::{LogStoreChunkRead, LogStoreChunkWrite, LogStoreRead, LogStoreWrite};
use rand::random;
use shared_types::{ChunkArray, Proof, Transaction, CHUNK_SIZE};
use std::cmp;
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

#[allow(unused)]
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
    let store = SimpleLogStore::memorydb().unwrap();
    let chunk_count = store.chunk_batch_size + store.chunk_batch_size / 2 - 1;
    let data_size = CHUNK_SIZE * chunk_count;
    let mut data = vec![0u8; data_size];
    for i in 0..chunk_count {
        data[i * CHUNK_SIZE] = random();
    }
    let merkle = sub_merkle_tree(&data).unwrap();
    let tx = Transaction {
        stream_ids: vec![],
        size: data_size as u64,
        data_merkle_root: merkle.root().into(),
        seq: 0,
        data: vec![],
    };
    store.put_tx(tx.clone()).unwrap();
    for start_index in (0..chunk_count).step_by(store.chunk_batch_size) {
        let end = cmp::min(
            (start_index + store.chunk_batch_size) * CHUNK_SIZE,
            data.len(),
        );
        let chunk_array = ChunkArray {
            data: data[start_index * CHUNK_SIZE..end].to_vec(),
            start_index: start_index as u32,
        };
        store.put_chunks(tx.seq, chunk_array.clone()).unwrap();
    }
    store.finalize_tx(tx.seq).unwrap();
    let chunk_array = ChunkArray {
        data,
        start_index: 0,
    };
    assert_eq!(store.get_tx_by_seq_number(0).unwrap().unwrap(), tx);
    for i in 0..chunk_count {
        assert_eq!(
            store.get_chunk_by_tx_and_index(tx.seq, i).unwrap().unwrap(),
            chunk_array.chunk_at(i).unwrap()
        );
    }
    assert_eq!(
        store
            .get_chunk_by_tx_and_index(tx.seq, chunk_count)
            .unwrap(),
        None
    );

    assert_eq!(
        store
            .get_chunks_by_tx_and_index_range(tx.seq, 0, chunk_count)
            .unwrap()
            .unwrap(),
        chunk_array
    );
    for i in 0..chunk_count {
        let chunk_with_proof = store
            .get_chunk_with_proof_by_tx_and_index(tx.seq, i)
            .unwrap()
            .unwrap();
        assert_eq!(chunk_with_proof.chunk, chunk_array.chunk_at(i).unwrap());
        assert_eq!(
            chunk_with_proof.proof,
            Proof::from_merkle_proof(&merkle.gen_proof(i))
        );
        assert!(
            chunk_with_proof
                .validate(&tx.data_merkle_root, i, chunk_count)
                .unwrap(),
            "proof={:?}",
            chunk_with_proof.proof
        );
    }
    for i in (0..chunk_count).step_by(store.chunk_batch_size / 3) {
        let end = std::cmp::min(i + store.chunk_batch_size, chunk_count);
        let chunk_array_with_proof = store
            .get_chunks_with_proof_by_tx_and_index_range(tx.seq, i, end)
            .unwrap()
            .unwrap();
        assert_eq!(
            chunk_array_with_proof.chunks,
            chunk_array.sub_array(i, end).unwrap()
        );
        assert!(chunk_array_with_proof
            .validate(&tx.data_merkle_root, chunk_count)
            .unwrap());
    }
}

#[test]
fn test_root() {
    let results = [
        [
            241, 48, 193, 94, 101, 245, 240, 244, 161, 29, 60, 193, 132, 4, 58, 78, 37, 196, 155,
            133, 151, 104, 229, 103, 105, 91, 48, 189, 66, 90, 95, 116,
        ],
        [
            122, 137, 1, 255, 31, 110, 121, 53, 237, 46, 119, 179, 186, 109, 25, 47, 207, 184, 83,
            210, 235, 132, 9, 94, 252, 42, 77, 88, 169, 8, 80, 157,
        ],
    ];
    for (test_index, n_chunk) in [6, 7].into_iter().enumerate() {
        let data = vec![0; n_chunk * CHUNK_SIZE];
        let mt = sub_merkle_tree(&data).unwrap();
        println!("{:?} {}", mt.root(), hex::encode(&mt.root()));
        assert_eq!(results[test_index], mt.root());
    }
}
