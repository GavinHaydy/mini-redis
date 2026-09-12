mod resp;
mod command;

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;


fn handle_client(
    mut stream: TcpStream,
    db: Arc<Mutex<HashMap<String, String>>>,
) {
    let mut buffer = [0; 1024];
    let mut input = Vec::new();

    loop {
        let n = match stream.read(&mut buffer) {
            Ok(0) => break,
            Ok(n) => n,
            Err(e) => {
                println!("read error: {}", e);
                break;
            }
        };

        input.extend_from_slice(&buffer[..n]);

        loop {
            match resp::parse_array(&input) {
                Ok(Some((command, consumed))) => {
                    input.drain(..consumed);

                    let response = {
                        let mut db = db.lock().unwrap();

                        match command::Command::parse(command) {
                            Ok(command) => command::execute(command, &mut db),
                            Err(error) => resp::RespValue::Error(error)
                        }
                    };

                    let output = resp::encode(&response);

                    if let Err(e) = stream.write_all(&output) {
                        println!("write error: {}", e);
                        return;
                    }
                }

                Ok(None) => break,

                Err(e) => {
                    let response =
                        resp::RespValue::Error(e);

                    let output = resp::encode(&response);

                    let _ = stream.write_all(&output);

                    return;
                }
            }
        }
    }
}

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    let db = Arc::new(Mutex::new(HashMap::new()));
    println!("Mini Redis listening on 127.0.0.1:6379");

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("Client connected: {:?}", stream.peer_addr());

                let db = Arc::clone(&db);
                thread::spawn(move || {
                    handle_client(stream, db);
                });

                println!("Client disconnected");
            }
            Err(e) => {
                println!("connection error: {}", e);
            }
        }
    }
}
