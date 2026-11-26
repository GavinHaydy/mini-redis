use crate::resp::RespValue;
use crate::store::{Db, Entry, Value, remove_expired, cleanup_expired};
use rand::seq::IteratorRandom;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

pub enum Command {
    Set(String, String, Option<u64>),
    Get(String),
    MSet(Vec<(String, String)>),
    MGet(Vec<String>),
    Del(String),
    Ping,
    Info(Option<String>),
    Hello(u8),
    ClientSetInfo(String, String),
    ClientSetName(String),
    ClientGetName,
    ClientInfo,
    ClientId,
    ClientList,
    Expire(String, u64),
    Incr(String),
    Ttl(String),
    LPush(String, Vec<String>),
    RPush(String, Vec<String>),
    LPop(String),
    RPop(String),
    LRange(String, i64, i64),
    LLen(String),
    LIndex(String, i64),
    LSet(String, i64, String),
    SAdd(String, Vec<String>),
    SIsMember(String, String),
    SMembers(String),
    SRem(String, String),
    SCard(String),
    SInter(String, String),
    SUnion(String, String),
    SDiff(String, String),
    SMove(String, String, String),
    SPop(String),
    SRandMember(String),
    SInterStore(String, String, String),
    SUnionStore(String, String, String),
    SDiffStore(String, String, String),
    Keys(String),
    FlushDB,
    HSet(String, Vec<(String, String)>),
    HGet(String, String),
    HDel(String, Vec<String>),
    HExists(String, String),
    HGetAll(String),
    HKeys(String),
    HVals(String),
    HLen(String),
    HIncrBy(String, String, i64),

}

#[derive(Clone)]
pub struct ClientState {
    pub id: u64,
    pub addr: Option<String>,
    pub laddr: Option<String>,
    pub name: Option<String>,
    pub lib_name: Option<String>,
    pub lib_ver: Option<String>,
    pub connected_at: Instant,
    pub last_active_at: Instant,
}

impl ClientState {
    pub fn new() -> Self {
        static NEXT_CLIENT_ID: AtomicU64 = AtomicU64::new(1);

        let now = Instant::now();

        Self {
            id: NEXT_CLIENT_ID.fetch_add(1, Ordering::Relaxed),
            name: None,
            addr: None,
            laddr: None,
            lib_name: None,
            lib_ver: None,
            connected_at: now,
            last_active_at: now
        }
    }
}

fn key_matches(key: &str, pattern: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    if let Some(prefix) = pattern.strip_suffix("*") {
        return key.starts_with(prefix);
    }

    key == pattern
}

fn normalize_index(index: i64, len: usize) -> Option<usize> {
    let len = len as i64;

    let index = if index < 0 {
        len + index
    } else {
        index
    };

    if index < 0 || index >= len {
        None
    }else {
        Some(index as usize)
    }
}


fn get_hash<'a>(
    db: &'a Db,
    key: &str,
) -> Result<Option<&'a HashMap<String, String>>, String> {
    match db.get(key) {
        None => Ok(None),
        Some(entry) => match &entry.value {
            Value::Hash(hash) => Ok(Some(hash)),
            _ => Err("WrongType".to_string()),
        },
    }
}

fn get_list<'a>(
    db: &'a Db,
    key: &str,
) -> Result<Option<&'a VecDeque<String>>, String> {
    match db.get(key) {
        None => Ok(None),
        Some(entry) => match &entry.value {
            Value::List(list) => Ok(Some(list)),
            _ => Err("WrongType".to_string()),
        },
    }
}

fn get_set<'a>(
    db: &'a Db,
    key: &str,
) -> Result<Option<&'a HashSet<String>>, String> {
    match db.get(key) {
        None => Ok(None),
        Some(entry) => match &entry.value {
            Value::Set(set) => Ok(Some(set)),
            _ => Err("WrongType".to_string()),
        },
    }
}

fn get_sets<'a>(
    db: &'a Db,
    keys: &[String],
) -> Result<Vec<Option<&'a HashSet<String>>>, String> {
    keys.iter()
    .map(|key| get_set(db, key))
    .collect()
}

fn remove_empty_set(db: &mut Db, key: &str) {
    let is_empty = match db.get(key) {
        Some(entry) => match &entry.value {
            Value::Set(set) => set.is_empty(),
            _ => false,
        },
        None => false,
    };

    if is_empty {
        db.remove(key);
    }
}

fn remove_empty_list(db: &mut Db, key: &str) {
    let is_empty = match db.get(key) {
        Some(entry) => match &entry.value {
            Value::List(list) => list.is_empty(),
            _ => false,
        },
        None => false,
    };

    if is_empty {
        db.remove(key);
    }
}

fn pop_list(
    db: &mut Db,
    key: &str,
    from_front: bool,
) -> Result<Option<String>, String> {
    let result = match get_list_mut(db, key)? {
        Some(list) => {
            if from_front {
                list.pop_front()
            } else {
                list.pop_back()
            }
        }
        None => None,
    };

    remove_empty_list(db, key);

    Ok(result)
}

fn set_to_resp_array(set: &HashSet<String>) -> RespValue {
    let values = set
        .iter()
        .map(|member| {
            RespValue::BulkString(Some(member.as_bytes().to_vec()))
        })
        .collect();

    RespValue::Array(values)
}

fn string_to_bulk_string(value: Option<String>) -> RespValue {
    match value {
        Some(value) => RespValue::BulkString(Some(value.into_bytes())),
        None => RespValue::BulkString(None),
    }
}

fn get_set_mut<'a>(
    db: &'a mut Db,
    key: &str,
) -> Result<Option<&'a mut HashSet<String>>, String> {
    match db.get_mut(key) {
        None => Ok(None),
        Some(entry) => match &mut entry.value {
            Value::Set(set) => Ok(Some(set)),
            _ => Err("WrongType".to_string()),
        },
    }
}

fn add_set(
    db: &mut Db,
    key: String,
    member: String,
) -> Result<bool, String> {
    match db.get_mut(&key) {
        Some(entry) => match &mut entry.value {
            Value::Set(set) => Ok(set.insert(member)),
            _ => Err("WrongType".to_string()),
        },
        None => {
            let mut set = HashSet::new();
            set.insert(member);

            db.insert(
                key,
                Entry {
                    value: Value::Set(set),
                    expires_at: None,
                },
            );

            Ok(true)
        }
    }
}


fn get_list_mut<'a>(
    db: &'a mut Db,
    key: &str,
) -> Result<Option<&'a mut VecDeque<String>>, String> {
    match db.get_mut(key) {
        None => Ok(None),
        Some(entry) => match &mut entry.value {
            Value::List(list) => Ok(Some(list)),
            _ => Err("WrongType".to_string()),
        },
    }
}

fn push_list(
    db: &mut Db,
    key: String,
    value: String,
    push_front: bool,
) -> Result<usize, String> {
    match db.get_mut(&key) {
        Some(entry) => match &mut entry.value {
            Value::List(list) => {
                if push_front {
                    list.push_front(value);
                } else {
                    list.push_back(value);
                }

                Ok(list.len())
            }
            _ => Err("WrongType".to_string()),
        },
        None => {
            let mut list = VecDeque::new();

            if push_front {
                list.push_front(value);
            } else {
                list.push_back(value);
            }

            let len = list.len();

            db.insert(
                key,
                Entry {
                    value: Value::List(list),
                    expires_at: None,
                },
            );

            Ok(len)
        }
    }
}


impl Command {
    pub fn parse(values: Vec<RespValue>) -> Result<Self, String> {
        if values.is_empty() {
            return Err("empty command".to_string());
        }

        let name = match &values[0] {
            RespValue::BulkString(Some(data)) => String::from_utf8_lossy(data).to_uppercase(),
            _ => return Err("invalid command".to_string()),
        };

        match name.as_str() {
            "PING" => {
                if values.len() != 1 {
                    return Err("ERR wrong number of arguments for 'ping' command".to_string());
                }

                Ok(Command::Ping)
            }

            "INFO" => {
                if values.len() > 2 {
                    return Err(
                        "ERR wrong number of arguments for 'info' command".to_string()
                    );
                }

                let section = if values.len() == 2 {
                    Some(value_to_string(&values[1])?)
                } else {
                    None
                };

                Ok(Command::Info(section))
            }

            "HELLO" => {
                if values.len() != 2 {
                    return Err(
                        "ERR wrong number of arguments for 'hello' command".to_string()
                    );
                }

                let version = value_to_string(&values[1])?
                    .parse::<u8>()
                    .map_err(|_| {
                        "ERR Protocol version is not an integer".to_string()
                    })?;

                if version != 2 {
                    return Err("ERR unsupported protocol version".to_string());
                }

                Ok(Command::Hello(version))
            }

            "CLIENT" => {
                if values.len() < 2 {
                    return Err("ERR wrong number of arguments for 'client' command".to_string());
                }

                let subcommand = value_to_string(&values[1])?.to_uppercase();

                match subcommand.as_str() {
                    "SETINFO" => {
                        if values.len() != 4 {
                            return Err(
                                "ERR wrong number of arguments for 'client|setinfo' command".to_string()
                            );
                        }

                        let info_type = value_to_string(&values[2])?.to_uppercase();
                        let info_value = value_to_string(&values[3])?;

                        if info_type != "LIB-NAME" && info_type != "LIB-VER" {
                            return Err("ERR syntax error".to_string());
                        }

                        Ok(Command::ClientSetInfo(info_type, info_value))
                    }

                    "SETNAME" => {
                        if values.len() != 3 {
                            return Err(
                                "ERR wrong number of arguments for 'client|setname' command".to_string()
                            );
                        }

                        let name = value_to_string(&values[2])?;

                        Ok(Command::ClientSetName(name))
                    }

                    "GETNAME" => {
                        if values.len() != 2 {
                            return Err(
                                "ERR wrong number of arguments for 'client|getname' command".to_string()
                            );
                        }

                        Ok(Command::ClientGetName)
                    }

                    "INFO" => {
                        if values.len() != 2 {
                            return Err("ERR wrong number of arguments for 'client|info' command".to_string());
                        }

                        Ok(Command::ClientInfo)
                    }

                    "ID" => {
                        if values.len() != 2 {
                            return Err("ERR wrong number of arguments for 'client|id' command".to_string());
                        }

                        Ok(Command::ClientId)
                    }

                    "LIST" => {
                        if values.len() != 2 {
                            return Err(
                                "ERR wrong number of arguments for 'client|list' command".to_string()
                            );
                        }

                        Ok(Command::ClientList)
                    }

                    _ => Err("ERR unknown CLIENT subcommand".to_string()),
                }
            }

            "GET" => {
                if values.len() != 2 {
                    return Err("ERR wrong number of arguments for 'get' command".to_string());
                }

                let key = value_to_string(&values[1])?;

                Ok(Command::Get(key))
            }

            "SET" => {
                if values.len() != 3 && values.len() != 5 {
                    return Err("ERR wrong number of arguments for 'set' command".to_string());
                }

                let key = value_to_string(&values[1])?;
                let value = value_to_string(&values[2])?;

                let expires_in = if values.len() == 5 {
                    let option = value_to_string(&values[3])?;

                    if option.to_uppercase() != "EX" {
                        return Err("ERR syntax err".to_string());
                    }

                    let seconds = value_to_string(&values[4])?
                        .parse::<u64>()
                        .map_err(|_| "ERR invalid expire time".to_string())?;
                    Some(seconds)
                } else {
                    None
                };
                Ok(Command::Set(key, value, expires_in))
            }

            "MSET" => {
                if values.len() < 3 || values.len() % 2 == 0 {
                    return Err(
                        "ERR wrong number of arguments for 'mset' command".to_string()
                    );
                }

                let mut pairs = Vec::new();

                for i in (1..values.len()).step_by(2) {
                    let key = value_to_string(&values[i])?;
                    let value = value_to_string(&values[i + 1])?;

                    pairs.push((key, value));
                }

                Ok(Command::MSet(pairs))
            }

            "MGET" => {
                if values.len() < 2 {
                    return Err(
                        "ERR wrong number of arguments for 'mget' command".to_string()
                    );
                }

                let keys = values[1..]
                    .iter()
                    .map(value_to_string)
                    .collect::<Result<Vec<String>, String>>()?;

                Ok(Command::MGet(keys))
            }

            "DEL" => {
                if values.len() != 2 {
                    return Err("ERR wrong number of arguments for 'del' command".to_string());
                }

                let key = value_to_string(&values[1])?;

                Ok(Command::Del(key))
            }

            "EXPIRE" => {
                if values.len() != 3 {
                    return Err("Err wrong number of arguments for 'expire' command".to_string());
                }
                let key = value_to_string(&values[1])?;

                let seconds = value_to_string(&values[2])?
                    .parse::<u64>()
                    .map_err(|_| "ERR invalid expire time".to_string())?;

                Ok(Command::Expire(key, seconds))
            }

            "INCR" => {
                if values.len() != 2 {
                    return Err("ERR wrong number of arguments for 'incr' command".to_string());
                }

                let key = value_to_string(&values[1])?;

                Ok(Command::Incr(key))
            }

            "TTL" => {
                if values.len() != 2 {
                    return Err("ERR wrong number of arguments for 'ttl' command".to_string());
                }
                let key = value_to_string(&values[1])?;
                Ok(Command::Ttl(key))
            }

            "LPUSH" => {
                if values.len() < 3 {
                    return Err("ERR wrong number of arguments for 'lpush' command".to_string());
                }

                let key = value_to_string(&values[1])?;

                let items = values[2..]
                    .iter()
                    .map(value_to_string)
                    .collect::<Result<Vec<String>, String>>()?;

                Ok(Command::LPush(key, items))
            }

            "RPUSH" => {
                if values.len() < 3 {
                    return Err("ERR wrong number of arguments for 'rpush' command".to_string());
                }

                let key = value_to_string(&values[1])?;

                let items = values[2..]
                    .iter()
                    .map(value_to_string)
                    .collect::<Result<Vec<String>, String>>()?;

                Ok(Command::RPush(key, items))
            }

            "LPOP" => {
                if values.len() != 2 {
                    return Err("ERR wrong number of arguments for 'lPop' command".to_string());
                }

                let key = value_to_string(&values[1])?;

                Ok(Command::LPop(key))
            }

            "RPOP" => {
                if values.len() != 2 {
                    return Err("ERR wrong number of arguments for 'rPop' command".to_string());
                }

                let key = value_to_string(&values[1])?;

                Ok(Command::RPop(key))
            }

            "LRANGE" => {
                if values.len() != 4 {
                    return Err("ERR wrong number of arguments for 'lRange' command".to_string());
                }

                let key = value_to_string(&values[1])?;

                let start = value_to_string(&values[2])?
                    .parse::<i64>()
                    .map_err(|_| "ERR value is not an integer or out of range".to_string())?;

                let stop = value_to_string(&values[3])?
                    .parse::<i64>()
                    .map_err(|_| "ERR value is not an integer or out of range".to_string())?;

                Ok(Command::LRange(key, start, stop))
            }

            "LLEN" => {
                if values.len() != 2 {
                    return Err(
                        "ERR wrong number of arguments for 'lLen' command"
                            .to_string(),
                    );
                }

                let key = value_to_string(&values[1])?;

                Ok(Command::LLen(key))
            }

            "LINDEX" => {
                if values.len() != 3 {
                    return Err(
                        "ERR wrong number of arguments for 'lIndex' command"
                            .to_string(),
                    );
                }

                let key = value_to_string(&values[1])?;

                let index = value_to_string(&values[2])?
                    .parse::<i64>()
                    .map_err(|_| {
                        "ERR value is not an integer or out of range".to_string()
                    })?;

                Ok(Command::LIndex(key, index))
            }

            "LSET" => {
                if values.len() != 4 {
                    return Err(
                        "ERR wrong number of arguments for 'lSet' command".to_string(),
                    )
                }

                let key = value_to_string(&values[1])?;

                let index = value_to_string(&values[2])?
                    .parse::<i64>()
                    .map_err(|_| "ERR invalid index".to_string())?;

                let value = value_to_string(&values[3])?;

                Ok(Command::LSet(key, index, value))
            }

            "SADD" => {
                if values.len() < 3 {
                    return Err("ERR wrong number of arguments for 'sadd' command".to_string());
                }

                let key = value_to_string(&values[1])?;

                let members = values[2..]
                    .iter()
                    .map(value_to_string)
                    .collect::<Result<Vec<String>, String>>()?;

                Ok(Command::SAdd(key, members))
            }

            "SISMEMBER" => {
                if values.len() != 3 {
                    return Err(
                        "ERR wrong number of arguments for 'sismember' command"
                            .to_string(),
                    );
                }

                let key = value_to_string(&values[1])?;

                let member = value_to_string(&values[2])?;

                Ok(Command::SIsMember(key, member))
            }

            "SMEMBERS" => {
                if values.len() != 2 {
                    return Err(
                        "ERR wrong number of arguments for 'sMembers' command"
                            .to_string(),
                    );
                }

                let key = value_to_string(&values[1])?;

                Ok(Command::SMembers(key))
            }

            "SREM" => {
                if values.len() != 3 {
                    return Err(
                        "ERR wrong number of arguments for 'srem' command".to_string(),
                    );
                }

                let key = value_to_string(&values[1])?;
                let member = value_to_string(&values[2])?;

                Ok(Command::SRem(key, member))
            }

            "SCARD" => {
                if values.len() != 2 {
                    return Err(
                        "ERR wrong number of arguments for 'scard' command".to_string(),
                    );
                }

                let key = value_to_string(&values[1])?;

                Ok(Command::SCard(key))
            }

            "SINTER" => {
                if values.len() != 3 {
                    return Err(
                        "ERR wrong number of arguments for 'sinter' command".to_string(),
                    );
                }

                let key1 = value_to_string(&values[1])?;
                let key2 = value_to_string(&values[2])?;

                Ok(Command::SInter(key1, key2))
            }

            "SUNION" => {
                if values.len() != 3 {
                    return Err(
                        "ERR wrong number of arguments for 'sunion' command".to_string(),
                    );
                }

                let key1 = value_to_string(&values[1])?;
                let key2 = value_to_string(&values[2])?;

                Ok(Command::SUnion(key1, key2))
            }

            "SDIFF" => {
                if values.len() != 3 {
                    return Err("wrong number of arguments for 'sdiff' command".to_string());
                }

                let key1 = value_to_string(&values[1])?;
                let key2 = value_to_string(&values[2])?;

                Ok(Command::SDiff(key1, key2))
            }

            "SMOVE" => {
                if values.len() != 4 {
                    return Err(
                        "wrong number of arguments for 'smove' command".to_string()
                    );
                }

                let source = value_to_string(&values[1])?;
                let destination = value_to_string(&values[2])?;
                let member = value_to_string(&values[3])?;

                Ok(Command::SMove(source, destination, member))
            }

            "SPOP" => {
                if values.len() != 2 {
                    return Err(
                        "wrong number of arguments for 'spop' command".to_string()
                    );
                }

                let key = value_to_string(&values[1])?;

                Ok(Command::SPop(key))
            }

            "SRANDMEMBER" => {
                if values.len() != 2 {
                    return Err(
                        "wrong number of arguments for 'srandmember' command".to_string()
                    );
                }

                let key = value_to_string(&values[1])?;

                Ok(Command::SRandMember(key))
            }

            "SINTERSTORE" => {
                if values.len() != 4 {
                    return Err(
                        "wrong number of arguments for 'sinterstore' command".to_string()
                    );
                }

                let destination = value_to_string(&values[1])?;
                let key1 = value_to_string(&values[2])?;
                let key2 = value_to_string(&values[3])?;

                Ok(Command::SInterStore(destination, key1, key2))
            }

            "SUNIONSTORE" => {
                if values.len() != 4 {
                    return Err(
                        "wrong number of arguments for 'sunionstore' command".to_string()
                    );
                }

                let destination = value_to_string(&values[1])?;
                let key1 = value_to_string(&values[2])?;
                let key2 = value_to_string(&values[3])?;

                Ok(Command::SUnionStore(destination, key1, key2))
            }

            "SDIFFSTORE" => {
                if values.len() != 4 {
                    return Err(
                        "wrong number of arguments for 'sdiffstore' command".to_string()
                    );
                }

                let destination = value_to_string(&values[1])?;
                let key1 = value_to_string(&values[2])?;
                let key2 = value_to_string(&values[3])?;

                Ok(Command::SDiffStore(destination, key1, key2))
            }

            "KEYS" => {
                if values.len() != 2 {
                    return Err(
                        "ERR wrong number of arguments for 'keys' command".to_string()
                    )
                }
                let pattern = value_to_string(&values[1])?;
                Ok(Command::Keys(pattern))
            }

            "FLUSHDB" => {
                if values.len() != 1 {
                    return Err(
                        "ERR wrong number of arguments for 'flushdb' command".to_string()
                    );
                }
                Ok(Command::FlushDB)
            }

            "HSET" => {
                if values.len() < 4 || values.len() % 2 != 0 {
                    return Err("ERR wrong number of arguments for 'hset' command".to_string());
                }

                let key = value_to_string(&values[1])?;

                let mut fields = Vec::new();

                for i in (2..values.len()).step_by(2) {
                    let field = value_to_string(&values[i])?;
                    let value = value_to_string(&values[i + 1])?;

                    fields.push((field, value));
                }

                Ok(Command::HSet(key, fields))
            }

            "HGET" => {
                if values.len() != 3 {
                    return Err(
                        "wrong number of arguments for 'hget' command".to_string()
                    );
                }

                let key = value_to_string(&values[1])?;
                let field = value_to_string(&values[2])?;

                Ok(Command::HGet(key, field))
            }

            "HDEL" => {
                if values.len() < 3 {
                    return Err(
                        "wrong number of arguments for 'hdel' command".to_string()
                    );
                }

                let key = value_to_string(&values[1])?;

                let mut fields = Vec::new();

                for value in &values[2..] {
                    fields.push(value_to_string(value)?);
                }

                Ok(Command::HDel(key, fields))
            }

            "HEXISTS" => {
                if values.len() != 3 {
                    return Err(
                        "wrong number of arguments for 'hexists' command".to_string()
                    );
                }

                let key = value_to_string(&values[1])?;
                let field = value_to_string(&values[2])?;

                Ok(Command::HExists(key, field))
            }

            "HGETALL" => {
                if values.len() != 2 {
                    return Err(
                        "wrong number of arguments for 'hgetall' command".to_string()
                    );
                }

                let key = value_to_string(&values[1])?;

                Ok(Command::HGetAll(key))
            }

            "HKEYS" => {
                if values.len() != 2 {
                    return Err(
                        "wrong number of arguments for 'hkeys' command".to_string()
                    );
                }

                let key = value_to_string(&values[1])?;

                Ok(Command::HKeys(key))
            }

            "HVALS" => {
                if values.len() != 2 {
                    return Err(
                        "wrong number of arguments for 'hvals' command".to_string()
                    );
                }

                let key = value_to_string(&values[1])?;

                Ok(Command::HVals(key))
            }

            "HLEN" => {
                if values.len() != 2 {
                    return Err(
                        "wrong number of arguments for 'hlen' command".to_string()
                    );
                }

                let key = value_to_string(&values[1])?;

                Ok(Command::HLen(key))
            }

            "HINCRBY" => {
                if values.len() != 4 {
                    return Err(
                        "wrong number of arguments for 'hincrby' command".to_string()
                    );
                }

                let key = value_to_string(&values[1])?;
                let field = value_to_string(&values[2])?;
                let increment = value_to_string(&values[3])?
                    .parse::<i64>()
                    .map_err(|_| "ERR increment is not an integer".to_string())?;

                Ok(Command::HIncrBy(key, field, increment))
            }

            _ => Err(format!("ERR unknown command '{}'", name)),
        }
    }
    pub fn execute(self, db: &mut Db,  client: &mut ClientState, clients: Option<&HashMap<u64, ClientState>>) -> Result<RespValue, String> {
        match self {
            Command::Ping => Ok(RespValue::SimpleString("PONG".to_string())),
            Command::Info(section) => {
                let info = match section.as_deref() {
                    None | Some("default") | Some("all") => {
                        "# Server\r\n\
            redis_version:0.1.0\r\n\
            redis_mode:standalone\r\n\
            \r\n\
            # Clients\r\n\
            connected_clients:1\r\n"
                    }

                    Some("server") => {
                        "# Server\r\n\
            redis_version:0.1.0\r\n\
            redis_mode:standalone\r\n"
                    }

                    Some("clients") => {
                        "# Clients\r\n\
            connected_clients:1\r\n"
                    }

                    Some(_) => "",
                };

                Ok(RespValue::BulkString(Some(info.as_bytes().to_vec())))
            }

            Command::Hello(version) => {
                if version != 2 {
                    return Err("ERR unsupported protocol version".to_string());
                }

                Ok(RespValue::Array(vec![
                    RespValue::BulkString(Some(b"server".to_vec())),
                    RespValue::BulkString(Some(b"mini-redis".to_vec())),

                    RespValue::BulkString(Some(b"version".to_vec())),
                    RespValue::BulkString(Some(b"0.1.0".to_vec())),

                    RespValue::BulkString(Some(b"proto".to_vec())),
                    RespValue::Integer(2),

                    RespValue::BulkString(Some(b"mode".to_vec())),
                    RespValue::BulkString(Some(b"standalone".to_vec())),

                    RespValue::BulkString(Some(b"role".to_vec())),
                    RespValue::BulkString(Some(b"master".to_vec())),

                    RespValue::BulkString(Some(b"modules".to_vec())),
                    RespValue::Array(vec![]),
                ]))
            }
            
            Command::Set(key, value, expires_in) => {
                let expires_at =
                    expires_in.map(|seconds| Instant::now() + Duration::from_secs(seconds));
                db.insert(
                    key,
                    Entry {
                        value: Value::String(value),
                        expires_at,
                    },
                );
                Ok(RespValue::SimpleString("OK".to_string()))
            }
            Command::Get(key) => {
                remove_expired(db, &key);

                match db.get(&key) {
                    Some(entry) => match &entry.value {
                        Value::String(value) => {
                            Ok(RespValue::BulkString(Some(value.as_bytes().to_vec())))
                        }
                        Value::List(_)| Value::Set(_) | Value::Hash(_) => Err(
                            "WrongType Operation against a key holding the wrong kind of value"
                                .to_string(),
                        ),
                    },
                    None => Ok(RespValue::BulkString(None)),
                }
            }


            Command::MSet(pairs) => {
                for (key, value) in pairs {
                    db.insert(
                        key,
                        Entry {
                            value: Value::String(value),
                            expires_at: None,
                        },
                    );
                }

                Ok(RespValue::SimpleString("OK".to_string()))
            }

            Command::MGet(keys) => {
                let mut values = Vec::new();

                for key in keys {
                    remove_expired(db, &key);

                    let value = match db.get(&key) {
                        Some(entry) => match &entry.value {
                            Value::String(value) => {
                                RespValue::BulkString(Some(value.as_bytes().to_vec()))
                            }
                            Value::List(_) | Value::Set(_) | Value::Hash(_) => {
                                RespValue::BulkString(None)
                            }
                        },
                        None => RespValue::BulkString(None),
                    };

                    values.push(value);
                }

                Ok(RespValue::Array(values))
            }

            Command::Del(key) => {
                let expired = match db.get(&key) {
                    Some(entry) => match entry.expires_at {
                        Some(expires_at) => Instant::now() >= expires_at,
                        None => false,
                    },
                    None => false,
                };

                if expired {
                    db.remove(&key);
                    Ok(RespValue::Integer(0))
                } else {
                    let deleted = db.remove(&key).is_some();
                    Ok(RespValue::Integer(if deleted { 1 } else { 0 }))
                }
            }

            Command::ClientSetInfo(key, value) => {
                match key.as_str() {
                    "LIB-NAME" => client.lib_name = Some(value),
                    "LIB-VER" => client.lib_ver = Some(value),
                    _ => return Err("ERR unknown CLIENT SETINFO option".to_string()),
                }

                Ok(RespValue::SimpleString("OK".to_string()))
            }

            Command::ClientInfo => {
                let name = client.name.as_deref().unwrap_or("");
                let lib_name = client.lib_name.as_deref().unwrap_or("");
                let lib_ver = client.lib_ver.as_deref().unwrap_or("");

                let info = format!(
                    "name={} lib-name={} lib-ver={}",
                    name, lib_name, lib_ver
                );

                Ok(RespValue::BulkString(Some(info.into_bytes())))
            }

            Command::ClientId => {
                Ok(RespValue::Integer(client.id as i64))
            }

            Command::ClientList => {
                let clients = clients.ok_or_else(|| {
                    "CLIENT LIST requires client registry".to_string()
                })?;
                let result = clients
                    .values()
                    .map(|client| {

                        let age = client.connected_at.elapsed().as_secs();
                        let idle = client.last_active_at.elapsed().as_secs();

                        format!(
                            "id={} addr={}  laddr={} age={} idle={} name={} lib-name={} lib-ver={}",
                            client.id,
                            client.addr.as_deref().unwrap_or(""),
                            client.laddr.as_deref().unwrap_or(""),
                            age,
                            idle,
                            client.name.as_deref().unwrap_or(""),
                            client.lib_name.as_deref().unwrap_or(""),
                            client.lib_ver.as_deref().unwrap_or("")
                        )
                    })
                    .collect::<Vec<String>>()
                    .join("\n");

                Ok(RespValue::BulkString(Some(result.into_bytes())))
            }

            Command::Expire(key, seconds) => Ok(match db.get_mut(&key) {
                Some(entry) => {
                    entry.expires_at = Some(Instant::now() + Duration::from_secs(seconds));

                    RespValue::Integer(1)
                }
                None => RespValue::Integer(0),
            }),

            Command::ClientSetName(name) => {
                client.name = Some(name);

                Ok(RespValue::SimpleString("OK".to_string()))
            }

            Command::ClientGetName => {
                match &client.name {
                    Some(name) => {
                        Ok(RespValue::BulkString(Some(name.as_bytes().to_vec())))
                    }
                    None => {
                        Ok(RespValue::BulkString(None))
                    }
                }
            }

            Command::Incr(key) => {
                remove_expired(db, &key);

                let entry = db.entry(key).or_insert_with(|| Entry {
                    value: Value::String("0".to_string()),
                    expires_at: None,
                });

                let number = match &entry.value {
                    Value::String(value) => value
                        .parse::<i64>()
                        .map_err(|_| "ERR value is not an integer or out of range".to_string())?,
                    Value::List(_) | Value::Set(_) | Value::Hash(_) => {
                        return Err(
                            "WrongType Operation against a key holding the wrong kind of value"
                                .to_string(),
                        );
                    }
                };

                let number = number + 1;

                entry.value = Value::String(number.to_string());

                Ok(RespValue::Integer(number))
            }

            Command::Ttl(key) => {
                if let Some(entry) = db.get(&key) {
                    println!("Value: {:?}", entry.expires_at);

                    match entry.expires_at {
                        Some(expires_at) => {
                            println!(
                                "Remaining: {:?}",
                                expires_at.saturating_duration_since(Instant::now())
                            );
                        }
                        None => println!("No expiration"),
                    }
                } else {
                    println!("Key does not exist");
                }

                remove_expired(db, &key);

                match db.get(&key) {
                    None => Ok(RespValue::Integer(-2)),

                    Some(entry) => match entry.expires_at {
                        None => Ok(RespValue::Integer(-1)),

                        Some(expires_at) => {
                            let seconds = expires_at
                                .saturating_duration_since(Instant::now())
                                .as_secs() as i64;

                            Ok(RespValue::Integer(seconds))
                        }
                    },
                }
            }

            Command::LPush(key, values) => {
                remove_expired(db, &key);

                let mut len = 0;

                for value in values {
                    len = push_list(db, key.clone(), value, true)?;
                }

                Ok(RespValue::Integer(len as i64))
            }

            Command::RPush(key, values) => {
                remove_expired(db, &key);

                let mut len = 0;

                for value in values {
                    len = push_list(db, key.clone(), value, false)?;
                }

                Ok(RespValue::Integer(len as i64))
            }

            Command::LPop(key) => {
                remove_expired(db, &key);

                let result =pop_list(db,& key, true)?;

                Ok(string_to_bulk_string(result))
            }

            Command::RPop(key) => {
                remove_expired(db, &key);

                let result = pop_list(db, &key, false)?;

                Ok(string_to_bulk_string(result))
            }

            Command::LRange(key, start, stop) => {
                remove_expired(db, &key);

                match get_list(db, &key)? {
                    Some(list) => {
                        let len = list.len() as i64;

                        let start = if start < 0 {
                            (len + start).max(0)
                        } else {
                            start
                        };

                        let stop = if stop < 0 {
                            len + stop
                        } else {
                            stop
                        };

                        let values = if start >= len || start > stop || stop < 0 {
                            vec![]
                        } else {
                            list.iter()
                                .skip(start as usize)
                                .take((stop.min(len - 1) - start + 1) as usize)
                                .map(|value| {
                                    RespValue::BulkString(Some(value.as_bytes().to_vec()))
                                })
                                .collect()
                        };

                        Ok(RespValue::Array(values))
                    }
                    None => Ok(RespValue::Array(vec![])),
                }
            }

            Command::LLen(key) => {
                remove_expired(db, &key);

                match get_list(db, &key)? {
                    Some(list) => Ok(RespValue::Integer(list.len() as i64)),
                    None => Ok(RespValue::Integer(0)),
                }

            }

            Command::LIndex(key, index) => {
                remove_expired(db, &key);

                match get_list(db, &key)? {
                    Some(list) => {
                        let index = if index < 0 {
                            list.len() as i64 + index
                        } else {
                            index
                        };

                        match usize::try_from(index).ok().and_then(|i| list.get(i)) {
                            Some(value) => Ok(RespValue::BulkString(Some(
                                value.as_bytes().to_vec(),
                            ))),
                            None => Ok(RespValue::BulkString(None)),
                        }
                    }
                    None => Ok(RespValue::BulkString(None)),
                }

            }

            Command::LSet(key, index, value) => {
                remove_expired(db, &key);

                let Some(entry) = db.get_mut(&key) else {
                    return Err("ERR no such key".to_string());
                };

                match &mut entry.value {
                    Value::List(list) => {
                        let Some(index) = normalize_index(index, list.len()) else {
                            return Err("ERR index out of range".to_string());
                        };

                        list[index] = value;

                        Ok(RespValue::SimpleString("OK".to_string()))
                    }

                    Value::String(_) | Value::Set(_)  | Value::Hash(_) => Err(
                        "WrongType Operation against a key holding the wrong kind of value".to_string()
                    )
                }

            }

            Command::SAdd(key, members) => {
                remove_expired(db, &key);

                let mut added_count = 0;

                for member in members {
                    if add_set(db, key.clone(), member)? {
                        added_count += 1;
                    }
                }

                Ok(RespValue::Integer(added_count))
            }
            Command::SIsMember(key, member) => {
                remove_expired(db, &key);

                match get_set(db, &key)? {
                    Some(set) => {
                        let exists = set.contains(&member);

                        Ok(RespValue::Integer(if exists { 1 } else { 0 }))
                    }
                    None => Ok(RespValue::Integer(0)),
                }
            }

            Command::SMembers(key) => {
                remove_expired(db, &key);

                match get_set(db, &key)? {
                    Some(set) => Ok(set_to_resp_array(set)),
                    None => Ok(set_to_resp_array(&HashSet::new())),
                }
            }

            Command::SRem(key, member) => {
                remove_expired(db, &key);

                let removed = match get_set_mut(db, &key)? {
                    Some(set) => set.remove(&member),
                    None => false,
                };

                remove_empty_set(db, &key);

                Ok(RespValue::Integer(if removed { 1 } else { 0 }))
            }

            Command::SCard(key) => {
                remove_expired(db, &key);

                match get_set(db, &key)? {
                    Some(set) => Ok(RespValue::Integer(set.len() as i64)),
                    None => Ok(RespValue::Integer(0)),
                }
            }

            Command::SInter(key1, key2) => {
                remove_expired(db, &key1);
                remove_expired(db, &key2);

                let keys = vec![key1, key2];
                let sets = get_sets(db, &keys)?;

                let mut result = HashSet::new();

                if let (Some(Some(first)), Some(Some(second))) =
                    (sets.first(), sets.get(1))
                {
                    for member in first.iter() {
                        if second.contains(member) {
                            result.insert(member.clone());
                        }
                    }
                }

                Ok(set_to_resp_array(&result))
            }

            Command::SUnion(key1, key2) => {
                remove_expired(db, &key1);
                remove_expired(db, &key2);

                let keys = vec![key1, key2];
                let sets = get_sets(db, &keys)?;

                let mut result = HashSet::new();

                for set in sets.iter().flatten() {
                    result.extend(set.iter().cloned());
                }

                Ok(set_to_resp_array(&result))
            }

            Command::SDiff(key1, key2) => {
                // 1. 删除已经过期的 key
                remove_expired(db, &key1);
                remove_expired(db, &key2);

                // 2. 获取第一个集合，复制它的元素
                let mut result = get_set(db, &key1)?
                .cloned()
                .unwrap_or_default();

                // 3. 从结果中移除第二个集合包含的元素
                if let Some(set) = get_set(db, &key2)? {
                    for member in set {
                        result.remove(member);
                    }
                }

                // 4. 将集合元素转换为 RESP 数组
                Ok(set_to_resp_array(&result))
            }

            Command::SMove(source, destination, member) => {
                remove_expired(db, &source);
                remove_expired(db, &destination);

                // 同一个 key：只检查元素是否存在，不实际移动
                if source == destination {
                    let exists = get_set(db, &source)?
                        .is_some_and(|set| set.contains(&member));

                    return Ok(RespValue::Integer(if exists { 1 } else { 0 }));
                }

                // 1. 检查源 key 和目标 key 的类型
                for key in [&source, &destination] {
                    let _ = get_set(db, key)?;
                }

                // 2. 从源集合中删除元素
                let removed = match get_set_mut(db, &source)? {
                    Some(set) => set.remove(&member),
                    None => false,
                };

                if !removed {
                    return  Ok(RespValue::Integer(0));
                }

                // 3. 如果源集合已经空了，删除源 key
                remove_empty_set(db, &source);

                // 4. 将元素添加到目标集合
                add_set(db, destination, member)?;

                Ok(RespValue::Integer(1))
            }

            Command::SPop(key) => {
                remove_expired(db, &key);

                let result = match get_set_mut(db, &key)? {
                    Some(set) => {
                        let member = set
                            .iter()
                            .choose(&mut rand::thread_rng())
                            .cloned();

                        if let Some(member) = &member {
                            set.remove(member);
                        }

                        member
                    }
                    None => None,
                };

                remove_empty_set(db, &key);

                Ok(string_to_bulk_string(result))
            }

            Command::SRandMember(key) => {
                remove_expired(db, &key);

                let member = get_set(db, &key)?
                    .and_then(|set| {
                        set.iter()
                        .choose(&mut rand::thread_rng())
                        .cloned()
                    });

                Ok(string_to_bulk_string(member))

                
            }

            Command::SInterStore(destination, key1, key2) => {
                // 1. 清理已经过期的 key
                remove_expired(db, &destination);
                remove_expired(db, &key1);
                remove_expired(db, &key2);

                // 2. 获取第一个集合的副本
                let mut result = match db.get(&key1) {
                    Some(entry) => match &entry.value {
                        Value::Set(set) => set.clone(),
                        Value::String(_) | Value::List(_) | Value::Hash(_) => {
                            return Err(
                                "WrongType Operation against a key holding the wrong kind of value"
                                    .to_string(),
                            );
                        }
                    },
                    None => HashSet::new(),
                };

                // 3. 从副本中删除不属于第二个集合的元素
                match db.get(&key2) {
                    Some(entry) => match &entry.value {
                        Value::Set(set) => {
                            result.retain(|member| set.contains(member));
                        }
                        Value::String(_) | Value::List(_) | Value::Hash(_) => {
                            return Err(
                                "WrongType Operation against a key holding the wrong kind of value"
                                    .to_string(),
                            );
                        }
                    },
                    None => {
                        result.clear();
                    }
                }

                // 4. 计算结果数量
                let count = result.len() as i64;

                // 5. 替换目标 key
                // 即使 destination 与某个源 key 相同也没有关系，
                // 因为交集已经计算完毕。
                db.remove(&destination);

                if !result.is_empty() {
                    db.insert(
                        destination,
                        Entry {
                            value: Value::Set(result),
                            expires_at: None,
                        },
                    );
                }

                // 6. 返回交集的元素数量
                Ok(RespValue::Integer(count))
            }

            Command::SUnionStore(destination, key1, key2) => {
                // 1. 清理过期 key
                remove_expired(db, &destination);
                remove_expired(db, &key1);
                remove_expired(db, &key2);

                // 2. 复制第一个集合
                let mut result = match db.get(&key1) {
                    Some(entry) => match &entry.value {
                        Value::Set(set) => set.clone(),
                        Value::String(_) | Value::List(_) | Value::Hash(_) => {
                            return Err(
                                "WrongType Operation against a key holding the wrong kind of value"
                                    .to_string(),
                            );
                        }
                    },
                    None => HashSet::new(),
                };

                // 3. 把第二个集合的元素合并进来
                match db.get(&key2) {
                    Some(entry) => match &entry.value {
                        Value::Set(set) => {
                            result.extend(set.iter().cloned());
                        }
                        Value::String(_) | Value::List(_) | Value::Hash(_) => {
                            return Err(
                                "WrongType Operation against a key holding the wrong kind of value"
                                    .to_string(),
                            );
                        }
                    },
                    None => {}
                }

                // 4. 记录结果数量
                let count = result.len() as i64;

                // 5. 替换目标 key
                db.remove(&destination);

                if !result.is_empty() {
                    db.insert(
                        destination,
                        Entry {
                            value: Value::Set(result),
                            expires_at: None,
                        },
                    );
                }

                // 6. 返回并集的元素数量
                Ok(RespValue::Integer(count))
            }

            Command::SDiffStore(destination, key1, key2) => {
                // 1. 清理过期 key
                remove_expired(db, &destination);
                remove_expired(db, &key1);
                remove_expired(db, &key2);

                // 2. 复制第一个集合
                let mut result = match db.get(&key1) {
                    Some(entry) => match &entry.value {
                        Value::Set(set) => set.clone(),
                        Value::String(_) | Value::List(_) | Value::Hash(_) => {
                            return Err(
                                "WrongType Operation against a key holding the wrong kind of value"
                                    .to_string(),
                            );
                        }
                    },
                    None => HashSet::new(),
                };

                // 3. 移除第二个集合中存在的元素
                match db.get(&key2) {
                    Some(entry) => match &entry.value {
                        Value::Set(set) => {
                            for member in set {
                                result.remove(member);
                            }
                        }
                        Value::String(_) | Value::List(_) | Value::Hash(_) => {
                            return Err(
                                "WrongType Operation against a key holding the wrong kind of value"
                                    .to_string(),
                            );
                        }
                    },
                    None => {}
                }

                // 4. 计算结果数量
                let count = result.len() as i64;

                // 5. 替换目标 key
                db.remove(&destination);

                if !result.is_empty() {
                    db.insert(
                        destination,
                        Entry {
                            value: Value::Set(result),
                            expires_at: None,
                        },
                    );
                }

                // 6. 返回差集的元素数量
                Ok(RespValue::Integer(count))
            }

            Command::Keys(pattern) => {
                cleanup_expired(db);

                let mut keys: Vec<RespValue> = db
                    .keys()
                    .filter(|key| key_matches(key, &pattern))
                    .map(|key| {
                        RespValue::BulkString(Some(key.as_bytes().to_vec()))
                    })
                    .collect();

                keys.sort_by(|a, b| {
                    match (a,b) {
                        (
                        RespValue::BulkString(Some(a)),
                        RespValue::BulkString(Some(b)),
                        ) => a.cmp(b),
                        _ => std::cmp::Ordering::Equal,
                    }
                });
                Ok(RespValue::Array(keys))
            }

            Command::FlushDB => {
                db.clear();

                Ok(RespValue::SimpleString("OK".to_string()))
            }

            Command::HSet(key, fields) => {
                remove_expired(db, &key);

                let mut added_count = 0;

                match db.get_mut(&key) {
                    Some(entry) => match &mut entry.value {
                        Value::Hash(hash) => {
                            for (field, value) in fields {
                                if !hash.contains_key(&field) {
                                    added_count += 1;
                                }

                                hash.insert(field, value);
                            }
                        }
                        Value::String(_) | Value::List(_) | Value::Set(_) => {
                            return Err(
                                "WRONGTYPE Operation against a key holding the wrong kind of value"
                                .to_string(),
                            );
                        }
                    },
                    None => {
                        let mut hash = HashMap::new();

                        for (field, value) in fields {
                            hash.insert(field, value);
                            added_count += 1;
                        }

                        db.insert(
                            key,
                            Entry {
                                value: Value::Hash(hash),
                                expires_at: None,
                            },
                        );
                    }
                }

                Ok(RespValue::Integer(added_count))
            }

            Command::HGet(key, field) => {
                remove_expired(db, &key);

                match get_hash(db, &key)? {
                    Some(hash) => match hash.get(&field) {
                        Some(value) => Ok(RespValue::BulkString(Some(
                            value.as_bytes().to_vec(),
                        ))),
                        None => Ok(RespValue::BulkString(None)),
                    },
                    None => Ok(RespValue::BulkString(None)),
                }
            }

            Command::HDel(key, fields) => {
                remove_expired(db, &key);

                let mut deleted = 0;
                let mut should_remove_key = false;

                match db.get_mut(&key) {
                    Some(entry) => match &mut entry.value {
                        Value::Hash(hash) => {
                            for field in fields {
                                if hash.remove(&field).is_some() {
                                    deleted += 1;
                                }
                            }

                            should_remove_key = hash.is_empty();
                        }
                        Value::String(_)
                        | Value::List(_)
                        | Value::Set(_) => {
                            return Err("WrongType".to_string());
                        }
                    },
                    None => {}
                }

                if should_remove_key {
                    db.remove(&key);
                }

                Ok(RespValue::Integer(deleted))
            }


            Command::HExists(key, field) => {
                remove_expired(db, &key);

                match get_hash(db, &key)? {
                    Some(hash) => {
                        let exists = hash.contains_key(&field);
                        Ok(RespValue::Integer(if exists { 1 } else { 0 }))
                    }
                    None => Ok(RespValue::Integer(0))
                }

            }

            Command::HGetAll(key) => {
                remove_expired(db, &key);

                match db.get(&key) {
                    Some(entry) => match &entry.value {
                        Value::Hash(hash) => {
                            let mut result = Vec::with_capacity(hash.len() * 2);

                            for (field, value) in hash {
                                result.push(RespValue::BulkString(
                                    Some(field.as_bytes().to_vec()),
                                ));

                                result.push(RespValue::BulkString(
                                    Some(value.as_bytes().to_vec()),
                                ));
                            }

                            Ok(RespValue::Array(result))
                        }
                        Value::String(_)
                        | Value::List(_)
                        | Value::Set(_) => Err("WrongType".to_string()),
                    },
                    None => Ok(RespValue::Array(vec![])),
                }
            }

            Command::HKeys(key) => {
                remove_expired(db, &key);

                match db.get(&key) {
                    Some(entry) => match &entry.value {
                        Value::Hash(hash) => {
                            let mut result = Vec::with_capacity(hash.len());

                            for field in hash.keys() {
                                result.push(RespValue::BulkString(
                                    Some(field.as_bytes().to_vec()),
                                ));
                            }

                            Ok(RespValue::Array(result))
                        }
                        Value::String(_)
                        | Value::List(_)
                        | Value::Set(_) => Err("WrongType".to_string()),
                    },
                    None => Ok(RespValue::Array(vec![])),
                }
            }


            Command::HVals(key) => {
                remove_expired(db, &key);

                match get_hash(db, &key)? {
                    Some(hash) => {
                        let values = hash
                            .values()
                            .map(|value| {
                                RespValue::BulkString(Some(value.as_bytes().to_vec()))
                            })
                            .collect();

                        Ok(RespValue::Array(values))
                    }

                    None => Ok(RespValue::Array(vec![])),
                }

            }

            Command::HLen(key) => {
                remove_expired(db, &key);

                match get_hash(db, &key)? {
                    Some(hash) => Ok(RespValue::Integer(hash.len() as i64)),
                    None => Ok(RespValue::Integer(0)),
                }

            }

            Command::HIncrBy(key, field, increment) => {
                remove_expired(db, &key);

                match db.get_mut(&key) {
                    Some(entry) => match &mut entry.value {
                        Value::Hash(hash) => {
                            let current = match hash.get(&field) {
                                Some(value) => value
                                    .parse::<i64>()
                                    .map_err(|_| {
                                        "ERR hash value is not an integer".to_string()
                                    })?,
                                None => 0,
                            };

                            let new_value = current.checked_add(increment).ok_or_else(|| {
                                "ERR increment or decrement would overflow".to_string()
                            })?;

                            hash.insert(field, new_value.to_string());

                            Ok(RespValue::Integer(new_value))
                        }
                        Value::String(_)
                        | Value::List(_)
                        | Value::Set(_) => Err("WrongType".to_string()),
                    },
                    None => {
                        let new_value = increment;

                        let mut hash = HashMap::new();
                        hash.insert(field, new_value.to_string());

                        db.insert(
                            key,
                            Entry {
                                value: Value::Hash(hash),
                                expires_at: None,
                            },
                        );

                        Ok(RespValue::Integer(new_value))
                    }
                }
            }


        }
    }
}

fn value_to_string(value: &RespValue) -> Result<String, String> {
    match value {
        RespValue::BulkString(Some(data)) => {
            String::from_utf8(data.clone()).map_err(|_| "invalid utf8".to_string())
        }
        RespValue::BulkString(None) => {
            Err("nil bulk string is not valid command argument".to_string())
        }
        _ => Err("expected bulk string".to_string()),
    }
}
