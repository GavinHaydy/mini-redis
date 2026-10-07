use std::collections::{HashMap, HashSet, VecDeque};
use std::time::Instant;

pub enum Value {
    String(String),
    List(VecDeque<String>),
    Set(HashSet<String>),
}

pub struct Entry {
    pub value: Value,
    pub expires_at: Option<Instant>,
}

pub type Db = HashMap<String, Entry>;

pub fn is_expired(entry: &Entry) -> bool {
    match entry.expires_at {
        Some(expires_at) => Instant::now() >= expires_at,
        None => false,
    }
}

pub fn remove_expired(db: &mut Db, key: &str) -> bool {
    let expired = match db.get(key) {
        Some(entry) => is_expired(entry),
        None => false,
    };
    if expired {
        db.remove(key);
    }
    expired
}

pub fn cleanup_expired(db: &mut Db) -> usize {
    let now = Instant::now();
    let mut removed = 0;

    db.retain(|_, entry|{
        let expired = match entry.expires_at {
            Some(expired_at) => now >= expired_at,
            None => false,
        };

        if expired {
            removed += 1;
        }
        !expired
    });

    removed
}
