use crate::resp::RespValue;
use std::collections::HashMap;

pub fn execute(
    command: Vec<RespValue>,
    db: &mut HashMap<String, String>,
) -> RespValue {
    if command.is_empty() {
        return RespValue::Error("empty command".to_string());
    }

    let command_name = match &command[0] {
        RespValue::BulkString(Some(value)) => {
            String::from_utf8_lossy(value).to_uppercase()
        }
        _ => {
            return RespValue::Error("invalid command".to_string());
        }
    };

    match command_name.as_str() {
        "SET" => set(&command, db),
        "GET" => get(&command, db),
        "PING" => RespValue::SimpleString("PONG".to_string()),
        _ => RespValue::Error(format!("unknown command '{}'", command_name)),
    }
}

fn set(
    command: &[RespValue],
    db: &mut HashMap<String, String>,
) -> RespValue {
    if command.len() != 3 {
        return RespValue::Error(
            "ERR wrong number of arguments for 'set' command".to_string(),
        );
    }

    let key = match value_to_string(&command[1]) {
        Ok(value) => value,
        Err(error) => return RespValue::Error(error),
    };

    let value = match value_to_string(&command[2]) {
        Ok(value) => value,
        Err(error) => return RespValue::Error(error),
    };

    db.insert(key, value);

    RespValue::SimpleString("OK".to_string())
}

fn get(
    command: &[RespValue],
    db: &HashMap<String, String>,
) -> RespValue {
    if command.len() != 2 {
        return RespValue::Error(
            "ERR wrong number of arguments for 'get' command".to_string(),
        );
    }

    let key = match value_to_string(&command[1]) {
        Ok(value) => value,
        Err(error) => return RespValue::Error(error),
    };

    match db.get(&key) {
        Some(value) => {
            RespValue::BulkString(Some(value.as_bytes().to_vec()))
        }
        None => RespValue::BulkString(None),
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