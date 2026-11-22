mod store;
mod resp;
mod command;

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread::{sleep, spawn};
use std::time::Duration;
use crate::resp::RespValue;
use crate::store::cleanup_expired;

type ClientRegistry = Arc<Mutex<HashMap<u64, command::ClientState>>>;

fn handle_client(
    mut stream: TcpStream,
    db: Arc<Mutex<store::Db>>,
    clients: ClientRegistry,
) {
    let mut client_state = command::ClientState::new();

    let client_id = client_state.id;

    {
        let mut clients = clients.lock().unwrap();
        clients.insert(client_id, client_state.clone());
    }
    
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

                    let clients_snapshot = {
                        let clients = clients.lock().unwrap();
                        clients.clone()
                    };

                    let mut db = db.lock().unwrap();
                    let response = match command.execute(&mut db, &mut client_state, &clients_snapshot) {
                        Ok(response) => response,
                        Err(e) => RespValue::Error(e),
                    };

                    {
                        let mut clients = clients.lock().unwrap();
                        clients.insert(client_id, client_state.clone());
                    }

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
    {
        let mut clients = clients.lock().unwrap();
        clients.remove(&client_id);
    }
    
    println!("Client disconnected");

}

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    let db = Arc::new(Mutex::new(store::Db::new()));
    let clients: ClientRegistry = Arc::new(Mutex::new(HashMap::new()));

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
                let clients = Arc::clone(&clients);

                spawn(move || {
                    handle_client(stream, db, clients);
                });
            }
            Err(e) => {
                println!("connection error: {}", e);
            }
        }
    }
}
