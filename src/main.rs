mod store;
mod resp;
mod command;

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread::{sleep, spawn};
use std::time::Duration;
use crate::resp::RespValue;
use crate::store::cleanup_expired;

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
    println!("Client disconnected");

}

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    let db = Arc::new(Mutex::new(store::Db::new()));

    // clean expired keys
    let cleanup_db = Arc::clone(&db);
    spawn(move || {
        loop {
            sleep(Duration::from_secs(1));

            let mut db = match cleanup_db.lock(){
                Ok(db) => db,
                Err(err) => {
                    eprintln!("Cleanup thread: failed to lock DB: {err}");
                    break;
                }
            };

            let removed = cleanup_expired(&mut db);

            if removed > 0 {
                println!("Cleaned up {} expired keys", removed);
            }
        }
    });

    println!("Mini Redis listening on 127.0.0.1:6379");

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("Client connected: {:?}", stream.peer_addr());

                let db = Arc::clone(&db);
                spawn(move || {
                    handle_client(stream, db);
                });

            }
            Err(e) => {
                println!("connection error: {}", e);
            }
        }
    }
}
