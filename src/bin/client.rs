use mini_redis::resp;
use std::io::{Read, Write};
use std::net::TcpStream;
use mini_redis::resp::RespValue;

struct Client {
    stream: TcpStream,
    input: Vec<u8>,
}

impl Client {
    fn expect_simple_string(
        response: resp::RespValue,
    ) -> Result<String, String> {
        match response {
            resp::RespValue::SimpleString(s) => Ok(s),
            resp::RespValue::Error(e) => Err(e),
            _ => Err("unexpected response".to_string()),
        }
    }

    fn connect(addr: &str) -> Self {
        let stream = TcpStream::connect(addr).expect("failed to connect");

        Self {
            stream,
            input: Vec::new(),
        }
    }

    fn send(&mut self, args: &[&str]) -> Result<resp::RespValue, String> {
        let request = encode_command(args);

        self.stream.write_all(&request).expect("failed to write");

        let mut buffer = [0; 1024];
        // let mut input = Vec::new();

        loop {
            let n = self.stream.read(&mut buffer).expect("failed to read");

            if n == 0 {
                panic!("server closed connection");
            }

            self.input.extend_from_slice(&buffer[..n]);

            match resp::parse(&self.input) {
                Ok(Some((response, consumed))) => {
                    self.input.drain(..consumed);
                    return Ok(response);
                }

                Ok(None) => {
                    continue;
                }

                Err(e) => {
                    panic!("failed to parse response: {}", e);
                }
            }
        }
    }

    fn set(&mut self, key: &str, value: &str) -> Result<(), String> {
        let response = self.send(&["SET", key, value])?;

        match response {
            resp::RespValue::SimpleString(value) if value == "OK" => {
                Ok(())
            }
            resp::RespValue::Error(err) => {Err(err)}
            _ => Err("unexpected response ".to_string())
        }
    }

    fn get(&mut self, key: &str) -> Result<Option<String>, String> {
        let response = self.send(&["GET", key])?;

        match response {
            resp::RespValue::BulkString(Some(value)) => {
                let value = String::from_utf8(value)
                    .map_err(|_| "invalid utf8".to_string())?;

                Ok(Some(value))
            }
            resp::RespValue::BulkString(None) => Ok(None),
            resp::RespValue::Error(e) => Err(e),
            _ => Err("unexpected response".to_string()),
        }
    }

    fn del(&mut self, key: &str) -> Result<i64, String> {
        let response = self.send(&["DEL", key])?;

        match response {
            resp::RespValue::Integer(value) => Ok(value),
            resp::RespValue::Error(e) => Err(e),
            _ => Err("unexpected response".to_string()),
        }
    }

    fn ping(&mut self) -> Result<String, String> {
        let response = self.send(&["PING"])?;

        Self::expect_simple_string(response)
    }
}

fn encode_command(args: &[&str]) -> Vec<u8> {
    let mut output = Vec::new();

    output.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());

    for arg in args {
        output.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());

        output.extend_from_slice(arg.as_bytes());
        output.extend_from_slice(b"\r\n");
    }
    output
}

fn main() {
    let mut client = Client::connect("127.0.0.1:6379");

    println!("{:?}", client.ping());

    println!("{:?}", client.set("name", "Gavin"));
    match client.set("name", "Gavin") {
        Ok(()) => println!("set success"),
        Err(error) => println!("set error: {}", error),
    }

    println!("{:?}", client.get("name"));
    match client.get("name") {
        Ok(Some(resp)) => println!("{:?}", resp),
        Ok(None) => {}
        Err(e) => println!("{:?}", e),
    }

    println!("{:?}", client.del("name"));

    println!("{:?}", client.get("name"));
    match client.ping() {
        Ok(value) => println!("{}", value),
        Err(error) => println!("error: {}", error),
    }
}
