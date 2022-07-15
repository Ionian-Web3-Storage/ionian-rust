use hashlink::{LinkedHashMap, LinkedHashSet};
use network::types::AnnounceFile;
use parking_lot::Mutex;
use rand::seq::IteratorRandom;

const MAX_FILES: usize = 1000;
const MAX_ENTRIES_PER_FILE: usize = 3;

#[derive(Default)]
pub struct GossipCache {
    /// TODO(ionian-dev): using `LinkedHashSet` as an entry is probably an overkill
    cache: Mutex<LinkedHashMap<u64, LinkedHashSet<AnnounceFile>>>,
}

impl GossipCache {
    pub fn insert(&self, announcement: AnnounceFile) {
        let mut cache = self.cache.lock();

        let entry = cache
            .entry(announcement.tx_seq)
            .or_insert_with(Default::default);

        entry.insert(announcement);

        if entry.len() > MAX_ENTRIES_PER_FILE {
            entry.pop_front();
        }

        if cache.len() > MAX_FILES {
            cache.pop_front();
        }
    }

    pub fn get_one(&self, tx_seq: u64) -> Option<AnnounceFile> {
        let mut cache = self.cache.lock();

        cache
            .to_back(&tx_seq)
            .and_then(|a| a.iter().choose(&mut rand::thread_rng()))
            .cloned()
    }

    pub fn get_all(&self, tx_seq: u64) -> Vec<AnnounceFile> {
        let mut cache = self.cache.lock();

        match cache.to_back(&tx_seq) {
            Some(entries) => entries.iter().cloned().collect(),
            None => vec![],
        }
    }
}
