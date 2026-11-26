use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::command::ClientState;

// pub type ClientRegistry = Arc<Mutex<HashMap<u64, ClientState>>>;

#[derive(Clone)]
pub struct ClientManager {
    clients: Arc<Mutex<HashMap<u64, ClientState>>>,
}

impl ClientManager {
    pub fn new() -> Self {
        Self {
            clients: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn register(&self, client: ClientState) {
        let mut clients = self.clients.lock().unwrap();
        clients.insert(client.id, client);
    }

    pub fn update(&self, client: &ClientState) {
        let mut clients = self.clients.lock().unwrap();
        clients.insert(client.id, client.clone());
    }

    pub fn snapshot(&self) -> HashMap<u64, ClientState> {
        let clients = self.clients.lock().unwrap();
        clients.clone()
    }

    pub fn remove(&self, client_id: u64) {
        let mut clients = self.clients.lock().unwrap();
        clients.remove(&client_id);
    }
}

// pub fn update_client_registry(
//     clients: &ClientRegistry,
//     client_state: &ClientState,
// ) {
//     let mut clients = clients.lock().unwrap();
//     clients.insert(client_state.id, client_state.clone());
// }
//
// pub fn get_client_snapshot(
//     clients: &ClientRegistry,
// ) -> HashMap<u64, ClientState> {
//     let clients = clients.lock().unwrap();
//     clients.clone()
// }

pub struct ClientCleanup {
    client_id: u64,
    manager: ClientManager,
}

impl ClientCleanup {
    pub fn new(client_id: u64, manager: ClientManager) -> Self {
        Self {
            client_id,
            manager,
        }
    }
}

impl Drop for ClientCleanup {
    fn drop(&mut self) {
        self.manager.remove(self.client_id);
        println!("Client {} removed from registry", self.client_id);
    }
}
