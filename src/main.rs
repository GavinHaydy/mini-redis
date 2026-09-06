use std::collections::HashMap;
use std::io::Read;
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;

fn handle_client(mut stream: TcpStream, db: Arc<Mutex<HashMap<String, String>>>) {

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
        let parts: Vec<&str> = msg.trim().split_whitespace().collect();
        if parts.len() == 3 && parts[0] == "SET" {
            let mut db = db.lock().unwrap();
            db.insert(parts[1].to_string(), parts[2].to_string());

            println!("SET {} = {}", parts[1], parts[2]);
        }else if parts.len() == 2 && parts[0] == "GET" {
            let value = {
                let db = db.lock().unwrap();
                db.get(parts[1]).cloned()
            };

            match value {
                Some(value) => println!("GET {} = {}", parts[1], value),
                None => println!("GET {} = nil", parts[1]),
            }
        }

        println!("Received: {}", msg);
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
                    handle_client(stream,db);
                });

                println!("Client disconnected");
            }
            Err(e) => {
                println!("connection error: {}", e);
            }
        }
        
    }
}
