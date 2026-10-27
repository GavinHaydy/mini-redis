use crate::resp::RespValue;
use crate::store::{Db, Entry, Value, remove_expired, cleanup_expired};
use rand::seq::IteratorRandom;
use std::collections::{HashMap, HashSet, VecDeque};
use std::time::{Duration, Instant};

pub enum Command {
    Set(String, String, Option<u64>),
    Get(String),
    Del(String),
    Ping,
    Expire(String, u64),
    Incr(String),
    Ttl(String),
    LPush(String, String),
    RPush(String, String),
    LPop(String),
    RPop(String),
    LRange(String, i64, i64),
    LLen(String),
    LIndex(String, i64),
    LSet(String, i64, String),
    SAdd(String, String),
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
    HSet(String, String, String),
    HGet(String, String),
    HDel(String, Vec<String>),
    HExists(String, String),
    HGetAll(String),
    HKeys(String),
    HVals(String),

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

fn normalize_range_index(index: i64, len: usize) -> i64 {
    let len = len as i64;

    if index < 0 {
        len + index
    } else {
        index
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
                if values.len() != 3 {
                    return Err("ERR wrong number of arguments for 'LPush' command".to_string());
                }
                let key = value_to_string(&values[1])?;
                let value = value_to_string(&values[2])?;
                Ok(Command::LPush(key, value))
            }

            "RPUSH" => {
                if values.len() != 3 {
                    return Err("ERR wrong number of arguments for 'RPush' command".to_string());
                }
                let key = value_to_string(&values[1])?;
                let value = value_to_string(&values[2])?;
                Ok(Command::RPush(key, value))
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
                if values.len() != 3 {
                    return Err(
                        "ERR wrong number of arguments for 'sadd' command".to_string()
                    );
                }

                let key = value_to_string(&values[1])?;

                let member = value_to_string(&values[2])?;

                Ok(Command::SAdd(key, member))
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
                if values.len() != 4 {
                    return Err(
                        "wrong number of arguments for 'hset' command".to_string()
                    );
                }

                let key = value_to_string(&values[1])?;
                let field = value_to_string(&values[2])?;
                let value = value_to_string(&values[3])?;

                Ok(Command::HSet(key, field, value))
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

            _ => Err(format!("ERR unknown command '{}'", name)),
        }
    }
    pub fn execute(self, db: &mut Db) -> Result<RespValue, String> {
        match self {
            Command::Ping => Ok(RespValue::SimpleString("PONG".to_string())),
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
            Command::Expire(key, seconds) => Ok(match db.get_mut(&key) {
                Some(entry) => {
                    entry.expires_at = Some(Instant::now() + Duration::from_secs(seconds));

                    RespValue::Integer(1)
                }
                None => RespValue::Integer(0),
            }),

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

            Command::LPush(key, value) => {
                remove_expired(db, &key);

                let entry = db.entry(key).or_insert_with(|| Entry {
                    value: Value::List(VecDeque::new()),
                    expires_at: None,
                });

                match &mut entry.value {
                    Value::List(list) => {
                        list.push_front(value);

                        Ok(RespValue::Integer(list.len() as i64))
                    }
                    Value::String(_)  | Value::Set(_) | Value::Hash(_) => Err(
                        "WrongType Operation against a key holding the wrong kind of value "
                            .to_string(),
                    ),
                }
            }

            Command::RPush(key, value) => {
                remove_expired(db, &key);

                let entry = db.entry(key).or_insert_with(|| Entry {
                    value: Value::List(VecDeque::new()),
                    expires_at: None,
                });

                match &mut entry.value {
                    Value::List(list) => {
                        list.push_back(value);

                        Ok(RespValue::Integer(list.len() as i64))
                    }
                    Value::String(_)  | Value::Set(_) | Value::Hash(_) => Err(
                        "WrongType Operation against a key holding the wrong kind of value "
                            .to_string(),
                    ),
                }
            }

            Command::LPop(key) => {
                remove_expired(db, &key);

                let result = match db.get_mut(&key) {
                    Some(entry) => match &mut entry.value {
                        Value::List(list) => list.pop_front(),
                        Value::String(_) | Value::Set(_)  | Value::Hash(_) => {
                            return Err(
                                "WrongType Operation against a key holding the wrong kind of value"
                                    .to_string(),
                            );
                        }
                    },
                    None => None,
                };

                let is_empty = matches!(
                    db.get(&key),
                    Some(Entry {
                        value: Value::List(list),
                        ..
                    }) if list.is_empty()
                );

                if is_empty {
                    db.remove(&key);
                }

                Ok(RespValue::BulkString(
                    result.map(|value| value.into_bytes()),
                ))
            }

            Command::RPop(key) => {
                remove_expired(db, &key);

                let result = match db.get_mut(&key) {
                    Some(entry) => match &mut entry.value {
                        Value::List(list) => list.pop_back(),
                        Value::String(_)  | Value::Set(_) | Value::Hash(_) => {
                            return Err(
                                "WrongType Operation against a key holding the wrong kind of value"
                                    .to_string(),
                            );
                        }
                    },
                    None => None,
                };

                let is_empty = matches!(
                    db.get(&key),
                    Some(Entry {
                        value: Value::List(list),
                        ..
                    }) if list.is_empty()
                );

                if is_empty {
                    db.remove(&key);
                }

                Ok(RespValue::BulkString(
                    result.map(|value| value.into_bytes()),
                ))
            }

            Command::LRange(key, start, stop) => {
                remove_expired(db, &key);

                let Some(entry) = db.get(&key) else {
                    return Ok(RespValue::Array(Vec::new()));
                };

                match &entry.value {
                    Value::List(list) => {
                        let len = list.len();

                        if len == 0 {
                            return Ok(RespValue::Array(Vec::new()));
                        }

                        let mut start = normalize_range_index(start, len);
                        let mut stop = normalize_range_index(stop, len);

                        // 小于 0 的 start，从 0 开始
                        if start < 0 {
                            start = 0;
                        }

                        // 大于等于 len 的 stop，限制到最后一个元素
                        if stop >= len as i64 {
                            stop = len as i64 - 1;
                        }

                        // start > stop，没有结果
                        if start > stop {
                            return Ok(RespValue::Array(Vec::new()));
                        }

                        let result = list
                            .iter()
                            .skip(start as usize)
                            .take((stop - start + 1) as usize)
                            .map(|value| {
                                RespValue::BulkString(
                                    Some(value.as_bytes().to_vec())
                                )
                            })
                            .collect();

                        Ok(RespValue::Array(result))
                    }

                    Value::String(_) | Value::Set(_)  | Value::Hash(_) => Err(
                        "WrongType Operation against a key holding the wrong kind of value"
                            .to_string()
                    ),
                }
            }

            Command::LLen(key) => {
                remove_expired(db, &key);

                match db.get(&key) {
                    None => Ok(RespValue::Integer(0)),

                    Some(entry) => match &entry.value {
                        Value::List(list) => {
                            Ok(RespValue::Integer(list.len() as i64))
                        }

                        Value::String(_) | Value::Set(_)  | Value::Hash(_) => {
                            Err(
                                "WrongType Operation against a key holding the wrong kind of value"
                                    .to_string()
                            )
                        }
                    },
                }
            }

            Command::LIndex(key, index) => {
                remove_expired(db, &key);

                let Some(entry) = db.get(&key) else {
                    return Ok(RespValue::BulkString(None));
                };


                match &entry.value {
                    Value::List(list) => {
                        let Some(index) = normalize_index(index, list.len()) else {
                            return Ok(RespValue::BulkString(None));
                        };

                        let value = &list[index];

                        Ok(RespValue::BulkString(
                            Some(value.as_bytes().to_vec())
                        ))
                    }
                    Value::String(_) | Value::Set(_)  | Value::Hash(_) => Err(
                        "WrongType Operation against a key holding the wrong kind of value".to_string()
                    )

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

            Command::SAdd(key, member) => {
                remove_expired(db, &key);

                match db.get_mut(&key) {
                    Some(entry) => {
                        match &mut entry.value {
                            Value::Set(set) => {
                                let added = set.insert(member);

                                if added {
                                    Ok(RespValue::Integer(1))
                                } else {
                                    Ok(RespValue::Integer(0))
                                }
                            }

                            Value::String(_) | Value::List(_) | Value::Hash(_) => {
                                Err(
                                    "WrongType Operation against a key holding the wrong kind of value"
                                        .to_string()
                                )
                            }
                        }
                    }

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

                        Ok(RespValue::Integer(1))
                    }
                }
            }

            Command::SIsMember(key, member) => {
                remove_expired(db, &key);

                match db.get(&key) {
                    Some(entry) => {
                        match &entry.value {
                            Value::Set(set) => {
                                if set.contains(&member) {
                                    Ok(RespValue::Integer(1))
                                } else {
                                    Ok(RespValue::Integer(0))
                                }
                            }

                            Value::String(_) | Value::List(_) | Value::Hash(_) => {
                                Err(
                                    "WrongType Operation against a key holding the wrong kind of value"
                                        .to_string(),
                                )
                            }
                        }
                    }

                    None => Ok(RespValue::Integer(0)),
                }
            }

            Command::SMembers(key) => {
                remove_expired(db, &key);

                match db.get(&key) {
                    Some(entry) => {
                        match &entry.value {
                            Value::Set(set) => {
                                let members = set
                                    .iter()
                                    .map(|member| {
                                        RespValue::BulkString(
                                            Some(member.as_bytes().to_vec())
                                        )
                                    })
                                    .collect();

                                Ok(RespValue::Array(members))
                            }

                            Value::String(_) | Value::List(_) | Value::Hash(_) => {
                                Err(
                                    "WrongType Operation against a key holding the wrong kind of value"
                                        .to_string(),
                                )
                            }
                        }
                    }

                    None => Ok(RespValue::Array(Vec::new())),
                }
            }

            Command::SRem(key, member) => {
                remove_expired(db, &key);

                match db.get_mut(&key) {
                    Some(entry) => {
                        match &mut entry.value {
                            Value::Set(set) => {
                                if set.remove(&member) {
                                    Ok(RespValue::Integer(1))
                                } else {
                                    Ok(RespValue::Integer(0))
                                }
                            }

                            Value::String(_) | Value::List(_) | Value::Hash(_) => {
                                Err(
                                    "WrongType Operation against a key holding the wrong kind of value"
                                        .to_string(),
                                )
                            }
                        }
                    }

                    None => Ok(RespValue::Integer(0)),
                }
            }

            Command::SCard(key) => {
                remove_expired(db, &key);

                match db.get(&key) {
                    Some(entry) => {
                        match &entry.value {
                            Value::Set(set) => {
                                Ok(RespValue::Integer(set.len() as i64))
                            }

                            Value::String(_) | Value::List(_) | Value::Hash(_) => {
                                Err(
                                    "WrongType Operation against a key holding the wrong kind of value"
                                        .to_string(),
                                )
                            }
                        }
                    }

                    None => Ok(RespValue::Integer(0)),
                }
            }

            Command::SInter(key1, key2) => {
                remove_expired(db, &key1);
                remove_expired(db, &key2);

                let set1 = match db.get(&key1) {
                    Some(entry) => match &entry.value {
                        Value::Set(set) => set,
                        Value::String(_) | Value::List(_) | Value::Hash(_) => {
                            return Err(
                                "WrongType Operation against a key holding the wrong kind of value"
                                    .to_string(),
                            );
                        }
                    },
                    None => return Ok(RespValue::Array(Vec::new())),
                };

                let set2 = match db.get(&key2) {
                    Some(entry) => match &entry.value {
                        Value::Set(set) => set,
                        Value::String(_) | Value::List(_) | Value::Hash(_) => {
                            return Err(
                                "WrongType Operation against a key holding the wrong kind of value"
                                    .to_string(),
                            );
                        }
                    },
                    None => return Ok(RespValue::Array(Vec::new())),
                };

                let members = set1
                    .intersection(set2)
                    .map(|member| {
                        RespValue::BulkString(Some(member.as_bytes().to_vec()))
                    })
                    .collect();

                Ok(RespValue::Array(members))
            }

            Command::SUnion(key1, key2) => {
                remove_expired(db, &key1);
                remove_expired(db, &key2);

                let set1 = match db.get(&key1) {
                    Some(entry) => match &entry.value {
                        Value::Set(set) => set,
                        Value::String(_) | Value::List(_) | Value::Hash(_) => {
                            return Err(
                                "WrongType Operation against a key holding the wrong kind of value"
                                    .to_string(),
                            );
                        }
                    },
                    None => {
                        return match db.get(&key2) {
                            Some(entry) => match &entry.value {
                                Value::Set(set) => {
                                    let members = set
                                        .iter()
                                        .map(|member| {
                                            RespValue::BulkString(Some(
                                                member.as_bytes().to_vec(),
                                            ))
                                        })
                                        .collect();

                                    Ok(RespValue::Array(members))
                                }
                                Value::String(_) | Value::List(_) | Value::Hash(_) => Err(
                                    "WrongType Operation against a key holding the wrong kind of value"
                                        .to_string(),
                                ),
                            },
                            None => Ok(RespValue::Array(Vec::new())),
                        };
                    }
                };

                let set2 = match db.get(&key2) {
                    Some(entry) => match &entry.value {
                        Value::Set(set) => set,
                        Value::String(_) | Value::List(_) | Value::Hash(_) => {
                            return Err(
                                "WrongType Operation against a key holding the wrong kind of value"
                                    .to_string(),
                            );
                        }
                    },
                    None => {
                        let members = set1
                            .iter()
                            .map(|member| {
                                RespValue::BulkString(Some(member.as_bytes().to_vec()))
                            })
                            .collect();

                        return Ok(RespValue::Array(members));
                    }
                };

                let members = set1
                    .union(set2)
                    .map(|member| {
                        RespValue::BulkString(Some(member.as_bytes().to_vec()))
                    })
                    .collect();

                Ok(RespValue::Array(members))
            }

            Command::SDiff(key1, key2) => {
                // 1. 删除已经过期的 key
                remove_expired(db, &key1);
                remove_expired(db, &key2);

                // 2. 获取第一个集合，复制它的元素
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

                // 3. 从结果中移除第二个集合包含的元素
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

                // 4. 将集合元素转换为 RESP 数组
                let members = result
                    .into_iter()
                    .map(|member| {
                        RespValue::BulkString(Some(member.into_bytes()))
                    })
                    .collect();

                Ok(RespValue::Array(members))
            }

            Command::SMove(source, destination, member) => {
                remove_expired(db, &source);
                remove_expired(db, &destination);

                // 同一个 key：只检查元素是否存在，不实际移动
                if source == destination {
                    return match db.get(&source) {
                        Some(entry) => match &entry.value {
                            Value::Set(set) => {
                                Ok(RespValue::Integer(
                                    if set.contains(&member) { 1 } else { 0 }
                                ))
                            }
                            Value::String(_) | Value::List(_) | Value::Hash(_) => Err(
                                "WrongType Operation against a key holding the wrong kind of value"
                                    .to_string()
                            ),
                        },
                        None => Ok(RespValue::Integer(0)),
                    };
                }

                // 1. 检查源 key 和目标 key 的类型
                for key in [&source, &destination] {
                    if let Some(entry) = db.get(key) {
                        match &entry.value {
                            Value::Set(_) => {}
                            Value::String(_) | Value::List(_) | Value::Hash(_) => {
                                return Err(
                                    "WrongType Operation against a key holding the wrong kind of value"
                                        .to_string()
                                );
                            }
                        }
                    }
                }

                // 2. 从源集合中删除元素
                let removed = match db.get_mut(&source) {
                    Some(entry) => match &mut entry.value {
                        Value::Set(set) => set.remove(&member),
                        Value::String(_) | Value::List(_) | Value::Hash(_) => unreachable!(),
                    },
                    None => false,
                };

                // 源集合没有该元素，移动失败
                if !removed {
                    return Ok(RespValue::Integer(0));
                }

                // 3. 如果源集合已经空了，删除源 key
                let source_is_empty = match db.get(&source) {
                    Some(entry) => match &entry.value {
                        Value::Set(set) => set.is_empty(),
                        Value::String(_) | Value::List(_) | Value::Hash(_) => false,
                    },
                    None => false,
                };

                if source_is_empty {
                    db.remove(&source);
                }

                // 4. 将元素添加到目标集合
                match db.get_mut(&destination) {
                    Some(entry) => match &mut entry.value {
                        Value::Set(set) => {
                            set.insert(member);
                        }
                        Value::String(_) | Value::List(_) | Value::Hash(_) => unreachable!(),
                    },
                    None => {
                        let mut set = HashSet::new();
                        set.insert(member);

                        db.insert(
                            destination,
                            Entry {
                                value: Value::Set(set),
                                expires_at: None,
                            },
                        );
                    }
                }

                Ok(RespValue::Integer(1))
            }

            Command::SPop(key) => {
                remove_expired(db, &key);

                // 1. 检查 key 是否存在，以及类型是否正确
                match db.get(&key) {
                    Some(entry) => match &entry.value {
                        Value::Set(_) => {}
                        Value::String(_) | Value::List(_) | Value::Hash(_) => {
                            return Err(
                                "WrongType Operation against a key holding the wrong kind of value"
                                    .to_string(),
                            );
                        }
                    },
                    None => {
                        return Ok(RespValue::BulkString(None));
                    }
                }

                // 2. 随机选取一个元素
                let member = match db.get(&key) {
                    Some(entry) => match &entry.value {
                        Value::Set(set) => {
                            set.iter().choose(&mut rand::thread_rng()).cloned()
                        }
                        Value::String(_) | Value::List(_) | Value::Hash(_) => unreachable!(),
                    },
                    None => None,
                };

                // 3. 没有元素时返回 nil
                let Some(member) = member else {
                    db.remove(&key);
                    return Ok(RespValue::BulkString(None));
                };

                // 4. 删除选中的元素
                if let Some(entry) = db.get_mut(&key) {
                    if let Value::Set(set) = &mut entry.value {
                        set.remove(&member);
                    }
                }

                // 5. 如果集合已经空了，删除整个 key
                let is_empty = match db.get(&key) {
                    Some(entry) => match &entry.value {
                        Value::Set(set) => set.is_empty(),
                        Value::String(_) | Value::List(_) | Value::Hash(_) => false,
                    },
                    None => true,
                };

                if is_empty {
                    db.remove(&key);
                }

                // 6. 返回被移除的元素
                Ok(RespValue::BulkString(Some(member.into_bytes())))
            }

            Command::SRandMember(key) => {
                remove_expired(db, &key);

                match db.get(&key) {
                    Some(entry) => match &entry.value {
                        Value::Set(set) => {
                            let member = set
                                .iter()
                                .choose(&mut rand::thread_rng())
                                .cloned();

                            match member {
                                Some(member) => {
                                    Ok(RespValue::BulkString(Some(member.into_bytes())))
                                }
                                None => Ok(RespValue::BulkString(None)),
                            }
                        }
                        Value::String(_) | Value::List(_) | Value::Hash(_) => Err(
                            "WrongType Operation against a key holding the wrong kind of value"
                                .to_string()
                        ),
                    },
                    None => Ok(RespValue::BulkString(None)),
                }
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

            Command::HSet(key, field, value) => {
                remove_expired(db, &key);

                match db.get_mut(&key) {
                    Some(entry) => match &mut entry.value {
                        Value::Hash(hash) => {
                            let is_new = !hash.contains_key(&field);

                            hash.insert(field, value);

                            Ok(RespValue::Integer(if is_new { 1 } else { 0 }))
                        }
                        Value::String(_) | Value::List(_) | Value::Set(_) => {
                            Err(
                                "WrongType Operation against a key holding the wrong kind of value"
                                    .to_string()
                            )
                        }
                    },
                    None => {
                        let mut hash = HashMap::new();
                        hash.insert(field, value);

                        db.insert(
                            key,
                            Entry {
                                value: Value::Hash(hash),
                                expires_at: None,
                            },
                        );

                        Ok(RespValue::Integer(1))
                    }
                }
            }

            Command::HGet(key, field) => {
                remove_expired(db, &key);

                match db.get(&key) {
                    Some(entry) => match &entry.value {
                        Value::Hash(hash) => {
                            match hash.get(&field) {
                                Some(value) => Ok(
                                    RespValue::BulkString(
                                        Some(value.as_bytes().to_vec())
                                    )
                                ),
                                None => Ok(RespValue::BulkString(None)),
                            }
                        }
                        Value::String(_)
                        | Value::List(_)
                        | Value::Set(_) => Err("WrongType".to_string()),
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

                match db.get(&key) {
                    Some(entry) => match &entry.value {
                        Value::Hash(hash) => {
                            Ok(RespValue::Integer(
                                if hash.contains_key(&field) { 1 } else { 0 }
                            ))
                        }
                        Value::String(_)
                        | Value::List(_)
                        | Value::Set(_) => Err("WrongType".to_string()),
                    },
                    None => Ok(RespValue::Integer(0)),
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

                match db.get(&key) {
                    Some(entry) => match &entry.value {
                        Value::Hash(hash) => {
                            let mut result = Vec::with_capacity(hash.len());

                            for value in hash.values() {
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
