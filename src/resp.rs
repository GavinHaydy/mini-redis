#[derive(Debug)]
pub enum RespValue {
    BulkString(Vec<u8>),
    Array(Vec<RespValue>),
}

pub fn parse_bulk_string(input: &[u8]) -> Result<Option<(Vec<u8>, usize)>, String> {
    if !input.starts_with(b"$") {
        return Err("not a bulk string".to_string());
    }

    let Some(pos) = input.windows(2).position(|w| w == b"\r\n") else {
        return Ok(None);
    };

    let len_str = std::str::from_utf8(&input[1..pos])
        .map_err(|_| "invalid length".to_string())?;

    let len: usize = len_str
        .parse()
        .map_err(|_| "invalid length".to_string())?;

    let data_start = pos + 2;
    let data_end = data_start + len;

    if input.len() < data_end + 2 {
        return Ok(None);
    }

    if &input[data_end..data_end + 2] != b"\r\n" {
        return Err("missing CRLF".to_string());
    }

    let data = input[data_start..data_end].to_vec();

    let consumed = data_end + 2;

    Ok(Some((data, consumed)))
}

pub fn parse_array(
    input: &[u8],
) -> Result<Option<(Vec<RespValue>, usize)>, String> {
    if !input.starts_with(b"*") {
        return Err("not an array".to_string());
    }

    let Some(pos) = input.windows(2).position(|w| w == b"\r\n") else {
        return Ok(None);
    };

    let count_str = std::str::from_utf8(&input[1..pos])
        .map_err(|_| "invalid array length".to_string())?;

    let count: usize = count_str
        .parse()
        .map_err(|_| "invalid array length".to_string())?;

    let mut offset = pos + 2;
    let mut values = Vec::with_capacity(count);

    for _ in 0..count {
        let remaining = &input[offset..];

        let Some((value, consumed)) = parse_bulk_string(remaining)? else {
            return Ok(None);
        };

        values.push(RespValue::BulkString(value));

        offset += consumed;
    }

    Ok(Some((values, offset)))
}