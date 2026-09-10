mod resp;

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;

fn value_to_string(value: &resp::RespValue) -> Result<String, String> {
    match value {
        resp::RespValue::BulkString(Some(data)) => {
            String::from_utf8(data.clone())
                .map_err(|_| "invalid utf8".to_string())
        }

        resp::RespValue::BulkString(None) => {
            Err("nil bulk string is not valid command argument".to_string())
        }

        _ => {
            Err("expected bulk string".to_string())
        }
    }
}

fn handle_client(
    mut stream: TcpStream,
    db: Arc<Mutex<HashMap<String, String>>>,
) {
    let mut buffer = [0; 1024];
    let mut input = Vec::new();

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

        input.extend_from_slice(&buffer[..n]);

        loop {
            let result = resp::parse_array(&input);

            match result {
                Ok(Some((command, consumed))) => {
                    input.drain(..consumed);

                    let args: Result<Vec<String>, String> = command
                        .iter()
                        .map(value_to_string)
                        .collect();

                    let args = match args {
                        Ok(args) => args,
                        Err(e) => {
                            println!("Command error: {}", e);
                            continue;
                        }
                    };

                    println!("Command: {:?}", args);

                    if args.len() == 3 && args[0] == "SET" {
                        {
                            let mut db = db.lock().unwrap();

                            db.insert(
                                args[1].clone(),
                                args[2].clone(),
                            );
                        }

                        let response = resp::RespValue::SimpleString(
                            "OK".to_string()
                        );

                        let output = resp::encode(&response);

                        stream.write_all(&output).unwrap();
                    } else if args.len() == 2 && args[0] == "GET" {
                        let value = {
                            let db = db.lock().unwrap();

                            db.get(&args[1]).cloned()
                        };

                        match value {
                            Some(value) => {
                                let response = resp::RespValue::BulkString(
                                    Some(value.into_bytes())
                                );

                                let output = resp::encode(&response);

                                stream.write_all(&output).unwrap();
                            }

                            None => {
                                let response = resp::RespValue::BulkString(None);

                                let output = resp::encode(&response);

                                stream.write_all(&output).unwrap();
                            }
                        }
                    }
                }

                Ok(None) => {
                    break;
                }

                Err(e) => {
                    println!("RESP error: {}", e);
                    break;
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
