use std::io::{Read, Write};
use std::net::TcpStream;
use mini_redis::resp;

fn encode_command(args: &[&str]) -> Vec<u8> {
    let mut output = Vec::new();

    output.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());

    for arg in args {
        output.extend_from_slice(
            format!("${}\r\n", arg.len()).as_bytes()
        );

        output.extend_from_slice(arg.as_bytes());
        output.extend_from_slice(b"\r\n");

    }
    output
}

fn main() {
    let mut stream = TcpStream::connect("127.0.0.1:6379")
        .expect("failed to connect");

    let request = encode_command(&["PING"]);

    stream
        .write_all(&request)
        .expect("failed to write");

    let mut buffer = [0; 1024];

    let n = stream
        .read(&mut buffer)
        .expect("failed to read");

    let response = resp::parse(&buffer[..n]);

    println!("{:?}", response);
}