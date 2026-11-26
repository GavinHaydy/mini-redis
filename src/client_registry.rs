use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::command::ClientState;

pub type ClientRegistry = Arc<Mutex<HashMap<u64, ClientState>>>;

pub fn update_client_registry(
    clients: &ClientRegistry,
    client_state: &ClientState,
) {
    let mut clients = clients.lock().unwrap();
    clients.insert(client_state.id, client_state.clone());
}

pub fn get_client_snapshot(
    clients: &ClientRegistry,
) -> HashMap<u64, ClientState> {
    let clients = clients.lock().unwrap();
    clients.clone()
}

pub struct ClientCleanup {
    client_id: u64,
    clients: ClientRegistry,
}

impl ClientCleanup {
    pub fn new(client_id: u64, clients: ClientRegistry) -> Self {
        Self {
            client_id,
            clients,
        }
    }
}

impl Drop for ClientCleanup {
    fn drop(&mut self) {
        match self.clients.lock() {
            Ok(mut clients) => {
                clients.remove(&self.client_id);
                println!("Client {} removed from registry", self.client_id);
            }
            Err(e) => {
                eprintln!(
                    "Failed to lock client registry while removing client {}: {}",
                    self.client_id,
                    e
                );
            }
        }
    }
}
