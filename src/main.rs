use std::io::Read;
use std::net::{TcpListener, TcpStream};
use std::thread;

fn handle_client(mut stream: TcpStream) {
    let mut buffer = [0; 1024];

    loop {
        let n = match stream.read(&mut buffer) {
            Ok(n) => n,
            Err(e) => {
                println!("Read error: {}", e);
                break;
            }
        };

        if n == 0 {
            break;
        }

        let msg = String::from_utf8_lossy(&buffer[..n]);

        println!("Received: {}", msg);
    }
}
fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    println!("Mini Redis listening on 127.0.0.1:6379");

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {

                println!("Client connected: {:?}", stream.peer_addr());

                thread::spawn(move || {
                    handle_client(stream);
                });

                println!("Client disconnected");
            }
            Err(e) => {
                println!("connection error: {}", e);
            }
        }
        
    }
}
