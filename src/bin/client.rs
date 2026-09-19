use mini_redis::resp;
use std::io::{Read, Write};
use std::net::TcpStream;

struct Client {
    stream: TcpStream,
}

impl Client {
    fn connect(addr: &str) -> Self {
        let stream = TcpStream::connect(addr).expect("failed to connect");

        Self { stream }
    }

    fn send(&mut self, args: &[&str]) -> Result<resp::RespValue, String> {
        let request = encode_command(args);

        self.stream.write_all(&request).expect("failed to write");

        let mut buffer = [0; 1024];
        let mut input = Vec::new();

        loop {
            let n = self.stream.read(&mut buffer).expect("failed to read");

            if n == 0 {
                panic!("server closed connection");
            }

            input.extend_from_slice(&buffer[..n]);

            match resp::parse(&input) {
                Ok(Some((response, consumed))) => {
                    input.drain(..consumed);
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

    fn set(&mut self, key: &str, value: &str) -> Result<resp::RespValue, String> {
        self.send(&["SET", key, value])
    }

    fn get(&mut self, key: &str) -> Result<resp::RespValue, String> {
        self.send(&["GET", key])
    }

    fn del(&mut self, key: &str) -> Result<resp::RespValue, String> {
        self.send(&["DEL", key])
    }

    fn ping(&mut self) -> Result<String, String> {
        let response = self.send(&["PING"])?;

        match response {
            resp::RespValue::SimpleString(value) => {
                Ok(value)
            }
            resp::RespValue::Error(err) => {
                Err(err)
            },
            _ => {
                Err("unexpected response".to_string())
            }
        }
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

    println!("{:?}", client.get("name"));

    println!("{:?}", client.del("name"));

    println!("{:?}", client.get("name"));
    match client.ping() {
        Ok(value) => println!("{}", value),
        Err(error) => println!("error: {}", error),
    }
}
