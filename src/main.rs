mod store;
mod resp;
mod command;
mod client_registry;

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread::{sleep, spawn};
use std::time::{Duration, Instant};
use crate::client_registry::{ClientCleanup, ClientManager};
use crate::resp::RespValue;
use crate::store::cleanup_expired;


fn write_response(
    stream: &mut TcpStream,
    response: &RespValue,
) -> Result<(), String> {
    let output = resp::encode(response);

    stream
    .write_all(&output)
    .map_err(|e| format!("write error: {}", e))
}

fn read_from_client(
    stream: &mut TcpStream,
    buffer: &mut [u8],
    input: &mut Vec<u8>,
) -> std::io::Result<usize> {
    let n = stream.read(buffer)?;

    if n > 0 {
        input.extend_from_slice(&buffer[..n]);
    }

    Ok(n)
}

fn handle_client(
    mut stream: TcpStream,
    db: Arc<Mutex<store::Db>>,
    clients: ClientManager,
) {
    let mut client_state = command::ClientState::new();

    client_state.addr  =  stream.peer_addr().ok().map(|addr| addr.to_string());
    client_state.laddr = stream.local_addr().ok().map(|addr| addr.to_string());


    let client_id = client_state.id;

    clients.register(client_state.clone());

    let _cleanup = ClientCleanup::new(
        client_id,
        clients.clone(),
        );
    
    let mut buffer = [0; 1024];
    let mut input = Vec::new();

    loop {
        let n = match read_from_client(
            &mut stream,
            &mut buffer,
            &mut input
        ) {
            Ok(0) => break,
            Ok(n) => n,
            Err(e) => {
                println!("read error: {}", e);
                break;
            }
        };

        loop {
            match resp::parse_array(&input) {
                Ok(Some((command, consumed))) => {
                    input.drain(..consumed);

                    client_state.last_active_at = Instant::now();

                    let command = match command::Command::parse(command) {
                        Ok(command) => command,
                        Err(error) => {
                            let response = RespValue::Error(error);
                            let _ = write_response(&mut stream, &response);
                            continue;
                        }
                    };

                    clients.register(client_state.clone());
                    
                    let need_clients = matches!(
                        &command,
                        command::Command::ClientList
                    );

                    let clients_snapshot = if need_clients {
                        clients.update( &client_state);

                        Some(clients.snapshot())
                    } else {
                        None
                    };

                    let mut db = db.lock().unwrap();

                    let response = match command.execute(
                        &mut db,
                        &mut client_state,
                        clients_snapshot.as_ref(),
                    ) {
                        Ok(response) => response,
                        Err(e) => RespValue::Error(e),
                    };

                    {
                        clients.update(&client_state);
                    }

                    if let Err(e) = write_response(&mut stream, &response) {
                        println!("{}", e);
                        return;
                    }
                }

                Ok(None) => break,

                Err(e) => {
                    let response =
                        RespValue::Error(e);

                    let _ = write_response(&mut stream, &response);

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
    let clients = ClientManager::new();

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
                let clients =clients.clone();

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
