use std::io::Read;
use std::net::TcpListener;

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    println!("Mini Redis listening on 127.0.0.1:6379");

    for stream in listener.incoming() {
        let mut stream = stream.unwrap();

        println!("Client connected: {:?}", stream.peer_addr());

        let mut buffer = [0; 1024];
        let n = stream.read(&mut buffer).unwrap();

        let msg = String::from_utf8_lossy(&buffer[..n]);

        println!("Client received: {}", msg);
    }
}
