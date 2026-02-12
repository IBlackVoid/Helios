use crate::network::Command;
use std::collections::HashMap;
use std::sync::Mutex;

#[derive(Clone, Debug, PartialEq)]
pub struct LogEntry {
    pub term: u64,
    pub command: Command,
}

pub struct Store {
    data: Mutex<HashMap<String, String>>,
    log: Mutex<Vec<LogEntry>>, // The Raft log
}

impl Store {
    pub fn new() -> Self {
        Store {
            data: Mutex::new(HashMap::new()),
            log: Mutex::new(Vec::new()),
        }
    }
    
    pub fn append_log(&self, term: u64, command: Command) -> usize {
        let mut log = self.log.lock().unwrap();
        log.push(LogEntry { term, command });
        log.len() - 1
    }
    
    pub fn get_log_entry(&self, index: usize) -> Option<LogEntry> {
        self.log.lock().unwrap().get(index).cloned()
    }

    pub fn get_last_log_index_term(&self) -> (usize, u64) {
        let log = self.log.lock().unwrap();
        if log.is_empty() {
            (0, 0)
        } else {
            (log.len() - 1, log.last().unwrap().term)
        }
    }

    pub fn get_log_term_at(&self, index: usize) -> Option<u64> {
        let log = self.log.lock().unwrap();
        log.get(index).map(|e| e.term)
    }

    pub fn truncate_log_from(&self, start_index: usize) {
        let mut log = self.log.lock().unwrap();
        if start_index < log.len() {
            log.truncate(start_index);
        }
    }

    pub fn apply_log_entry(&self, entry: &LogEntry) {
        let mut data = self.data.lock().unwrap();
        match &entry.command {
            Command::Set(key, value) => { data.insert(key.clone(), value.clone()); }
            Command::Delete(key) => { data.remove(key); }
            _ => {}
        }
    }

    pub fn apply_range_inclusive(&self, from_index: usize, to_index: usize) {
        for idx in from_index..=to_index {
            if let Some(entry) = self.get_log_entry(idx) {
                self.apply_log_entry(&entry);
            }
        }
    }

    pub fn set(&self, key: String, value: String) { self.data.lock().unwrap().insert(key, value); }
    pub fn get(&self, key: &str) -> Option<String> { self.data.lock().unwrap().get(key).cloned() }
    pub fn delete(&self, key: &str) -> Option<String> { self.data.lock().unwrap().remove(key) }
    pub fn get_all_data(&self) -> HashMap<String, String> { self.data.lock().unwrap().clone() }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::network::Command;

    #[test]
    fn set_get_delete_roundtrip() {
        let store = Store::new();
        assert_eq!(store.get("k"), None);
        store.set("k".into(), "v".into());
        assert_eq!(store.get("k"), Some("v".into()));
        assert_eq!(store.delete("k"), Some("v".into()));
        assert_eq!(store.get("k"), None);
    }

    #[test]
    fn log_append_and_apply() {
        let store = Store::new();
        let idx = store.append_log(1, Command::Set("a".into(), "b".into()));
        assert_eq!(idx, 0);
        let entry = store.get_log_entry(idx).unwrap();
        store.apply_log_entry(&entry);
        assert_eq!(store.get("a"), Some("b".into()));
        // Truncate should keep nothing if start is 0
        store.truncate_log_from(0);
        assert!(store.get_log_entry(0).is_none());
    }
}