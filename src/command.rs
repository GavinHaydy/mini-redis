use std::time::{Duration, Instant};
use crate::resp::RespValue;
use crate::store::{Db, Entry};

pub enum Command {
    Set(String, String),
    Get(String),
    Del(String),
    Ping,
    Expire(String, u64),
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

            _ => Err(format!("ERR unknown command '{}'", name)),
        }
    }
    pub fn execute(self, db: &mut Db) -> RespValue {
        match self {
            Command::Ping => RespValue::SimpleString("PONG".to_string()),
            Command::Set(key, value) => {
                db.insert(
                    key,
                    Entry{
                        value,
                        expires_at: None
                    }
                );
                RespValue::SimpleString("OK".to_string())
            }
            Command::Get(key) => match db.get(&key) {
                Some(val) => RespValue::BulkString(Some(val.value.as_bytes().to_vec())),
                None => RespValue::BulkString(None),
            },
            Command::Del(key) => {
                let deleted = db.remove(&key).is_some();
                RespValue::Integer(if deleted { 1 } else { 0 })
            }
            Command::Expire(key, seconds) => {
                match db.get_mut(&key) {
                    Some(entry) => {
                        entry.expires_at =
                            Some(Instant::now() + Duration::from_secs(seconds));

                        RespValue::Integer(1)
                    }
                    None => {
                        RespValue::Integer(0)
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
