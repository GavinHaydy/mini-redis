use std::collections::VecDeque;
use std::time::{Duration, Instant};
use crate::resp::RespValue;
use crate::store::{remove_expired, Db, Entry, Value};

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
                        return Err(
                            "ERR syntax err".to_string()
                        );
                    }

                    let seconds = value_to_string(&values[4])?
                        .parse::<u64>()
                        .map_err(|_|{
                            "ERR invalid expire time".to_string()
                        })?;
                    Some(seconds)
                }else { None };
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
                    return Err(
                        "Err wrong number of arguments for 'expire' command"
                            .to_string(),
                    );
                }
                let key = value_to_string(&values[1])?;

                let seconds = value_to_string(&values[2])?
                    .parse::<u64>()
                    .map_err(|_| "ERR invalid expire time".to_string())?;

                Ok(Command::Expire(key, seconds))
            }

            "INCR" => {
                if values.len() != 2 {
                    return Err(
                        "ERR wrong number of arguments for 'incr' command"
                            .to_string(),
                    );
                }

                let key = value_to_string(&values[1])?;

                Ok(Command::Incr(key))
            }

            "TTL" => {
                if values.len() != 2 {
                    return Err(
                        "ERR wrong number of arguments for 'ttl' command".to_string(),
                    );
                }
                let key = value_to_string(&values[1])?;
                Ok(Command::Ttl(key))
            }

            "LPUSH" => {
                if values.len() != 3 {
                    return Err(
                        "ERR wrong number of arguments for 'LPush' command".to_string(),
                    );
                }
                let key = value_to_string(&values[1])?;
                let value = value_to_string(&values[2])?;
                Ok(Command::LPush(key, value))
            }

            "RPUSH" => {
                if values.len() != 3 {
                    return Err(
                        "ERR wrong number of arguments for 'RPush' command".to_string(),
                    );
                }
                let key = value_to_string(&values[1])?;
                let value = value_to_string(&values[2])?;
                Ok(Command::LPush(key, value))
            }

            _ => Err(format!("ERR unknown command '{}'", name)),
        }
    }
    pub fn execute(self, db: &mut Db) -> Result<RespValue, String> {
        match self {
            Command::Ping => Ok(RespValue::SimpleString("PONG".to_string())),
            Command::Set(key, value,expires_in) => {
                let expires_at = expires_in.map(|seconds| {
                   Instant::now() + Duration::from_secs(seconds)
                });
                db.insert(
                    key,
                    Entry{
                        value: Value::String(value),
                        expires_at
                    }
                );
                Ok(RespValue::SimpleString("OK".to_string()))
            }
            Command::Get(key) => {
                remove_expired(db, &key);

                match db.get(&key) {
                    Some(entry) => match &entry.value {
                        Value::String(value) => {
                            Ok(RespValue::BulkString(
                                Some(value.as_bytes().to_vec())
                            ))
                        }
                        Value::List(_) => {
                            Err("WrongType Operation against a key holding the wrong kind of value".to_string())
                        }
                    },
                    None => Ok(RespValue::BulkString(None)),
                }
            },
            Command::Del(key) => {
                let expired = match db.get(&key) {
                    Some(entry) => {
                        match entry.expires_at {
                            Some(expires_at) => Instant::now() >= expires_at,
                            None => false,
                        }
                    }
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
            Command::Expire(key, seconds) => {
                Ok(match db.get_mut(&key) {
                    Some(entry) => {
                        entry.expires_at =
                            Some(Instant::now() + Duration::from_secs(seconds));

                        RespValue::Integer(1)
                    }
                    None => {
                        RespValue::Integer(0)
                    }
                })
            }

            Command::Incr(key) => {
                remove_expired(db, &key);

                let entry = db
                    .entry(key)
                    .or_insert_with(|| Entry {
                        value: Value::String("0".to_string()),
                        expires_at: None
                    });

                let number = match &entry.value {
                    Value::String(value) => {
                        value.parse::<i64>().map_err(|_| {
                            "ERR value is not an integer or out of range".to_string()
                        })?
                    }
                    Value::List(_) => {
                        return Err(
                            "WrongType Operation against a key holding the wrong kind of value".to_string()
                        )
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

                    Some(entry) => {
                        match entry.expires_at {
                            None => Ok(RespValue::Integer(-1)),

                            Some(expires_at) => {
                                let seconds = expires_at
                                    .saturating_duration_since(Instant::now())
                                    .as_secs() as i64;

                                Ok(RespValue::Integer(seconds))
                            }
                        }
                    }
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
                    Value::String(_) => {
                        Err(
                            "WrongType Operation against a key holding the wrong kind of value ".to_string()
                        )
                    }
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
                    Value::String(_) => {
                        Err(
                            "WrongType Operation against a key holding the wrong kind of value ".to_string()
                        )
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
