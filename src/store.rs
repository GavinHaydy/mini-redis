use std::collections::{HashMap, VecDeque};
use std::time::Instant;

pub enum Value {
    String(String),
    List(VecDeque<String>)
}

pub struct Entry {
    pub value: Value,
    pub expires_at: Option<Instant>,
}

pub type Db = HashMap<String, Entry>;