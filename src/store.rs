use std::collections::HashMap;
use std::time::Instant;

pub struct Entry {
    pub value: String,
    pub expires_at: Option<Instant>,
}

pub type Db = HashMap<String, Entry>;