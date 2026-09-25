mod store;
mod resp;
mod command;

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;
use crate::resp::RespValue;

fn handle_client(
    mut stream: TcpStream,
    db: Arc<Mutex<store::Db>>,
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

                    let command = match command::Command::parse(command) {
                        Ok(command) => command,
                        Err(error) => {
                            let response = RespValue::Error(error);
                            let output = resp::encode(&response);
                            let _ = stream.write_all(&output);
                            continue;
                        }
                    };

                    let mut db = db.lock().unwrap();
                    let response = match command.execute(&mut db) {
                        Ok(response) => response,
                        Err(e) => RespValue::Error(e),
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
                        RespValue::Error(e);

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

    let db = Arc::new(Mutex::new(store::Db::new()));
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
