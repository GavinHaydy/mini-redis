use std::time::{Duration, Instant};
use crate::resp::RespValue;
use crate::store::{Db, Entry};

pub enum Command {
    Set(String, String),
    Get(String),
    Del(String),
    Ping,
    Expire(String, u64),
    Incr(String)
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
                if values.len() != 3 {
                    return Err("ERR wrong number of arguments for 'set' command".to_string());
                }

                let key = value_to_string(&values[1])?;
                let value = value_to_string(&values[2])?;

                Ok(Command::Set(key, value))
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

            _ => Err(format!("ERR unknown command '{}'", name)),
        }
    }
    pub fn execute(self, db: &mut Db) -> Result<RespValue, String> {
        match self {
            Command::Ping => Ok(RespValue::SimpleString("PONG".to_string())),
            Command::Set(key, value) => {
                db.insert(
                    key,
                    Entry{
                        value,
                        expires_at: None
                    }
                );
                Ok(RespValue::SimpleString("OK".to_string()))
            }
            Command::Get(key) => match db.get(&key) {
                Some(val) => Ok(RespValue::BulkString(Some(val.value.as_bytes().to_vec()))),
                None => Ok(RespValue::BulkString(None)),
            },
            Command::Del(key) => {
                let deleted = db.remove(&key).is_some();
                Ok(RespValue::Integer(if deleted { 1 } else { 0 }))
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
                let entry = db
                    .entry(key)
                    .or_insert_with(|| Entry {
                        value: "0".to_string(),
                        expires_at: None
                    });

                let number = entry
                    .value
                    .parse::<i64>()
                    .map_err(|_| {
                        "ERR value is not an integer or out of range".to_string()
                    })?;

                let number = number + 1;

                entry.value = number.to_string();

                Ok(RespValue::Integer(number))
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
