use crate::{store::Store, network::{Command, RespValue, decode_resp}, consensus};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use tokio::net::TcpStream;
use tokio::io::{AsyncWriteExt, AsyncReadExt};
use bytes::BytesMut;
use std::time::{Duration, Instant};

#[derive(Debug)]
pub enum Role {
    Primary {
        term: u64,
        peers: Arc<Vec<String>>,
        commit_index: usize,
        last_applied: usize,
        // Raft leader state: track next log index to send to each follower
        next_index: Arc<Mutex<HashMap<String, usize>>>,
        // Raft leader state: track highest log index replicated on each follower
        match_index: Arc<Mutex<HashMap<String, usize>>>,
        // Performance metrics
        last_heartbeat: Arc<Mutex<Instant>>,
    },
    Replica {
        primary_addr: String,
        term: u64,
        voted_for: Option<String>,
        commit_index: usize,
        last_applied: usize,
        // Track when last append entries was received to detect leader failure
        last_append_received: Arc<Mutex<Instant>>,
    },
    Candidate {
        term: u64,
        votes_received: Arc<Mutex<HashSet<String>>>,
        voted_for: Option<String>,
        // Pre-vote optimization state
        pre_vote_votes: Arc<Mutex<HashSet<String>>>,
        election_start: Instant,
        // Exponential backoff for failed elections
        election_timeout_base: Duration,
    },
}

impl Role {
    pub fn get_term(&self) -> u64 {
        match self { 
            Role::Primary { term, .. } | Role::Replica { term, .. } | Role::Candidate { term, .. } => *term 
        }
    }
    
    pub fn get_commit_index(&self) -> usize {
        match self {
            Role::Primary { commit_index, .. } | Role::Replica { commit_index, .. } => *commit_index,
            Role::Candidate { .. } => 0,
        }
    }

    pub fn get_last_applied(&self) -> usize {
        match self {
            Role::Primary { last_applied, .. } | Role::Replica { last_applied, .. } => *last_applied,
            Role::Candidate { .. } => 0,
        }
    }

    pub fn default_replica(primary_addr: String) -> Self {
        Role::Replica { 
            primary_addr, 
            term: 0, 
            voted_for: None, 
            commit_index: 0, 
            last_applied: 0,
            last_append_received: Arc::new(Mutex::new(Instant::now())),
        }
    }

    pub fn new_candidate(current_term: u64, self_addr: &str) -> Self {
        let mut votes = HashSet::new();
        votes.insert(self_addr.to_string());
        
        Role::Candidate {
            term: current_term + 1,
            votes_received: Arc::new(Mutex::new(votes)),
            voted_for: Some(self_addr.to_string()),
            pre_vote_votes: Arc::new(Mutex::new(HashSet::new())),
            election_start: Instant::now(),
            election_timeout_base: Duration::from_millis(150),
        }
    }

    pub fn new_primary(term: u64, peers: Arc<Vec<String>>, last_log_index: usize) -> Self {
        let mut next_index = HashMap::new();
        let mut match_index = HashMap::new();
        
        // Initialize next_index to leader last log index + 1
        // Initialize match_index to 0 for all followers
        for peer in peers.iter() {
            next_index.insert(peer.clone(), last_log_index + 1);
            match_index.insert(peer.clone(), 0);
        }

        Role::Primary {
            term,
            peers,
            commit_index: 0,
            last_applied: 0,
            next_index: Arc::new(Mutex::new(next_index)),
            match_index: Arc::new(Mutex::new(match_index)),
            last_heartbeat: Arc::new(Mutex::new(Instant::now())),
        }
    }

    pub fn transition_to_replica(&mut self, primary_addr: String, term: u64, voted_for: Option<String>) {
        *self = Role::Replica {
            primary_addr,
            term,
            voted_for,
            commit_index: self.get_commit_index(),
            last_applied: self.get_last_applied(),
            last_append_received: Arc::new(Mutex::new(Instant::now())),
        };
    }

    pub fn should_start_election(&self, election_timeout: Duration) -> bool {
        match self {
            Role::Replica { last_append_received, .. } => {
                last_append_received.lock().unwrap().elapsed() > election_timeout
            }
            Role::Candidate { election_start, election_timeout_base, .. } => {
                election_start.elapsed() > *election_timeout_base
            }
            Role::Primary { .. } => false,
        }
    }
}

pub async fn connect_to_primary(primary_addr: &str, store: Arc<Store>, role: Arc<Mutex<Role>>, self_addr: Arc<String>, peers: Arc<Vec<String>>) {
    println!("[REPLICATION] Attempting to connect to primary at {}", primary_addr);
    match TcpStream::connect(primary_addr).await {
        Ok(mut stream) => {
            println!("[REPLICATION] Connected. Sending PSYNC for full synchronization.");
            let psync_cmd = RespValue::Array(vec![RespValue::BulkString("PSYNC".to_string())]);
            if let Err(e) = stream.write_all(&psync_cmd.encode()).await {
                println!("[ERROR] Failed sending PSYNC: {}", e);
                return;
            }

            let mut buffer = BytesMut::with_capacity(4096);
            if let Err(e) = stream.read_buf(&mut buffer).await {
                println!("[ERROR] Failed reading PSYNC response: {}", e);
                return;
            }
            if let Some(RespValue::SimpleString(s)) = decode_resp(&mut buffer) {
                if s.starts_with("FULLRESYNC") {
                    println!("[REPLICATION] FULLRESYNC received. Applying dataset.");
                    if let Some(RespValue::Array(data)) = decode_resp(&mut buffer) {
                        for chunk in data.chunks_exact(2) {
                            if let (RespValue::BulkString(k), RespValue::BulkString(v)) = (&chunk[0], &chunk[1]) {
                                store.set(k.clone(), v.clone());
                            }
                        }
                        println!("[REPLICATION] Full sync complete. Now streaming live commands.");
                    }
                }
            } else {
                println!("[ERROR] Invalid PSYNC response from primary.");
                return;
            }

            loop {
                // Adaptive timeout based on network conditions
                let base_timeout = Duration::from_millis(300);
                let election_timeout = base_timeout + Duration::from_millis(rand::random::<u64>() % 300);
                
                match tokio::time::timeout(election_timeout, stream.read_buf(&mut buffer)).await {
                    Ok(Ok(0)) | Err(_) => {
                        println!("[REPLICATION] Connection to primary lost or timed out.");
                        let should_elect = {
                            let guard = role.lock().unwrap();
                            guard.should_start_election(election_timeout)
                        };
                        if should_elect {
                            println!("[REPLICATION] Election timeout exceeded based on heartbeats. Starting election.");
                            consensus::start_election(role, self_addr, peers, Arc::clone(&store)).await;
                        } else {
                            println!("[REPLICATION] Skipping election: recent heartbeats observed.");
                        }
                        break;
                    }
                    Ok(Ok(_)) => {
                        // Update last append received timestamp
                        {
                            let mut role_guard = role.lock().unwrap();
                            if let Role::Replica { last_append_received, .. } = &mut *role_guard {
                                *last_append_received.lock().unwrap() = Instant::now();
                            }
                        }
                        
                        while let Some(value) = decode_resp(&mut buffer) {
                            if let Ok(command) = Command::from_resp(value) {
                                match command {
                                    Command::Set(key, value) => store.set(key, value),
                                    Command::Delete(key) => { store.delete(&key); },
                                    Command::Ping => { /* Heartbeat */ },
                                    _ => {}
                                }
                            }
                        }
                    }
                    Ok(Err(e)) => {
                        println!("[ERROR] Failed reading from primary stream: {}", e);
                        let should_elect = {
                            let guard = role.lock().unwrap();
                            // Reuse the same computed election timeout window
                            guard.should_start_election(election_timeout)
                        };
                        if should_elect {
                            println!("[REPLICATION] Election timeout exceeded based on heartbeats. Starting election.");
                            consensus::start_election(role, self_addr, peers, Arc::clone(&store)).await;
                        } else {
                            println!("[REPLICATION] Skipping election despite read error: recent heartbeats observed.");
                        }
                        break;
                    }
                }
            }
        }
        Err(e) => {
            println!("[ERROR] Failed to connect to primary: {}.", e);
            // Only start election if we've actually exceeded the heartbeat-based timeout
            let should_elect = {
                let guard = role.lock().unwrap();
                // Use a conservative timeout window when connection couldn't be established
                guard.should_start_election(Duration::from_millis(600))
            };
            if should_elect {
                println!("[REPLICATION] No recent heartbeats detected. Starting election.");
                consensus::start_election(role, self_addr, peers, Arc::clone(&store)).await;
            } else {
                println!("[REPLICATION] Skipping election after connect failure: recent heartbeats observed.");
            }
        }
    }
}

pub async fn propagate_command(role_arc: &Arc<Mutex<Role>>, command: Command) {
    // Build RESP bytes for the command once
    let command_resp = match command {
        Command::Set(key, value) => RespValue::Array(vec![
            RespValue::BulkString("SET".to_string()), RespValue::BulkString(key), RespValue::BulkString(value),
        ]),
        Command::Delete(key) => RespValue::Array(vec![
            RespValue::BulkString("DELETE".to_string()), RespValue::BulkString(key),
        ]),
        _ => return,
    };
    let command_bytes = command_resp.encode();

    // Get the peer list if we are primary
    let peers_arc = {
        let role_guard = role_arc.lock().unwrap();
        if let Role::Primary { peers, .. } = &*role_guard {
            Arc::clone(peers)
        } else { return; }
    };

    // Send to each peer by connecting on the fly
    for peer in peers_arc.iter() {
        match TcpStream::connect(peer).await {
            Ok(mut stream) => {
                if let Err(e) = stream.write_all(&command_bytes).await {
                    println!("[ERROR] Failed to propagate to {}: {}", peer, e);
                }
            }
            Err(e) => {
                println!("[ERROR] Failed to connect to {}: {}", peer, e);
            }
        }
    }
}
