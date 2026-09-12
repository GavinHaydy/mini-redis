use crate::resp::RespValue;
use std::collections::HashMap;

pub enum Command {
    Set { key: String, value: String },
    Get { key: String },
    Ping,
}

impl Command {
    pub fn parse(values: Vec<RespValue>) -> Result<Self, String> {
        if values.is_empty() {
            return Err("empty command".to_string());
        }

        let name = match &values[0] {
            RespValue::BulkString(Some(data)) => {
                String::from_utf8_lossy(data).to_uppercase()
            }
            _ => return Err("invalid command".to_string()),
        };

        match name.as_str() {
            "PING" => {
                if values.len() != 1 {
                    return Err(
                        "ERR wrong number of arguments for 'ping' command"
                            .to_string(),
                    );
                }

                Ok(Command::Ping)
            }

            "GET" => {
                if values.len() != 2 {
                    return Err(
                        "ERR wrong number of arguments for 'get' command"
                            .to_string(),
                    );
                }

                let key = value_to_string(&values[1])?;

                Ok(Command::Get { key })
            }

            "SET" => {
                if values.len() != 3 {
                    return Err(
                        "ERR wrong number of arguments for 'set' command"
                            .to_string(),
                    );
                }

                let key = value_to_string(&values[1])?;
                let value = value_to_string(&values[2])?;

                Ok(Command::Set { key, value })
            }

            _ => Err(format!("ERR unknown command '{}'", name)),
        }
    }
}

pub fn execute(
    command: Command,
    db: &mut HashMap<String, String>,
) -> RespValue {
    match command {
        Command::Ping => RespValue::SimpleString(String::from("PONG")),
        Command::Set { key, value } => {
            db.insert(key, value);
            RespValue::SimpleString("OK".to_string())
        }
        Command::Get { key } => {
            match db.get(&key) {
                Some(val) => {
                    RespValue::BulkString(
                        Some(val.as_bytes().to_vec())
                    )
                }
                None => {
                    RespValue::BulkString(None)
                }
            }
        }
    }
}


fn value_to_string(value: &RespValue) -> Result<String, String> {
    match value {
        RespValue::BulkString(Some(data)) => {
            String::from_utf8(data.clone())
                .map_err(|_| "invalid utf8".to_string())
        }
        RespValue::BulkString(None) => {
            Err("nil bulk string is not valid command argument".to_string())
        }
        _ => Err("expected bulk string".to_string()),
    }
}