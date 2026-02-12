// Helios - Minimal distributed KV with primary-replica sync and simplified Raft-like election
// Binary entrypoint only. All modules live in the library (src/lib.rs and submodules).

use std::sync::{Arc, Mutex};
use tokio::net::TcpListener;
use std::env;

use helios::store::Store;
use helios::{consensus, replication, network, persistence};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("[HELIOS] Igniting the core...");

    let store = Arc::new(Store::new());
    println!("[HELIOS] In-memory storage engine initialized.");

    persistence::load_from_disk(Arc::clone(&store))?;
    println!("[PERSISTENCE] AOF data reloaded into memory.");

    // --- Enhanced Argument Parsing for Cluster Awareness ---
    let args: Vec<String> = env::args().collect();
    let mut self_addr = "127.0.0.1:6380".to_string(); // Default address
    let mut peers = Vec::new();
    let mut replica_of: Option<String> = None;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--replicaof" => {
                replica_of = Some(args[i + 1].clone());
                i += 2;
            }
            "--addr" => {
                self_addr = args[i + 1].clone();
                i += 2;
            }
            "--peer" => {
                peers.push(args[i + 1].clone());
                i += 2;
            }
            _ => i += 1,
        }
    }
    
    let self_addr_arc = Arc::new(self_addr.clone());
    let peers_arc = Arc::new(peers);

    let role = if let Some(primary_addr) = replica_of {
        println!("[HELIOS] Operating as REPLICA. Primary target: {}", primary_addr);
        let store_clone = Arc::clone(&store);
        let role_clone = Arc::new(Mutex::new(replication::Role::default_replica(primary_addr.clone())));
        let election_role_clone = Arc::clone(&role_clone);
        let self_addr_clone = Arc::clone(&self_addr_arc);
        let peers_clone = Arc::clone(&peers_arc);

        tokio::spawn(async move {
            replication::connect_to_primary(&primary_addr, store_clone, election_role_clone, self_addr_clone, peers_clone).await;
        });
        role_clone
    } else {
        println!("[HELIOS] Operating as PRIMARY.");
        let (last_log_index, last_log_term) = store.get_last_log_index_term();
        // Initialize primary term from last_log_term if available; otherwise default to 1
        let init_term = if last_log_term == 0 { 1 } else { last_log_term };
        let primary_role = replication::Role::new_primary(init_term, Arc::clone(&peers_arc), last_log_index);
        let role_arc = Arc::new(Mutex::new(primary_role));
        consensus::start_heartbeat_loop(Arc::clone(&role_arc), Arc::clone(&self_addr_arc), Arc::clone(&peers_arc), Arc::clone(&store));
        role_arc
    };

    let listener = TcpListener::bind(&self_addr).await?;
    println!("[HELIOS] Network listener bound to {}. Awaiting connections.", self_addr);

    loop {
        let (socket, addr) = listener.accept().await?;
        let store_clone = Arc::clone(&store);
        let role_clone = Arc::clone(&role);
        let self_addr_clone = Arc::clone(&self_addr_arc);
        let peers_clone = Arc::clone(&peers_arc);

        tokio::spawn(async move {
            println!("[NETWORK] Accepted new connection from: {}", addr);
            if let Err(e) = network::handle_connection(socket, addr, store_clone, role_clone, self_addr_clone, peers_clone).await {
                println!("[ERROR] Connection with {} terminated: {}", addr, e);
            }
        });
    }
}