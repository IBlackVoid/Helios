use crate::replication::Role;
use crate::network::RespValue;
use crate::store::{Store, LogEntry};
use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use tokio::net::TcpStream;
use tokio::io::{AsyncWriteExt, AsyncReadExt};
use bytes::BytesMut;
use std::time::{Duration, Instant};

/// Enhanced Raft election with sophisticated timeout management and pre-vote optimization
pub async fn start_election(
    role_arc: Arc<Mutex<Role>>,
    self_addr: Arc<String>,
    peers: Arc<Vec<String>>,
    store: Arc<Store>,
) {
    let mut election_attempts = 0u32;
    
    loop {
        election_attempts += 1;
        let (new_term, majority, last_log_index, last_log_term) = {
            let mut role_guard = role_arc.lock().unwrap();
            let current_term = role_guard.get_term();
            let new_term = current_term + 1;
            
            let total_nodes = peers.len() + 1;
            let majority = (total_nodes / 2) + 1;
            let (last_log_index, last_log_term) = store.get_last_log_index_term();

            println!("[CONSENSUS] Starting election #{} for term {}. Majority is {} votes.", 
                     election_attempts, new_term, majority);

            // Use the new Role constructor
            *role_guard = Role::new_candidate(current_term, &self_addr);
            (new_term, majority, last_log_index, last_log_term)
        };

        // Pre-vote phase: Check if we could win an election before incrementing term
        let mut pre_vote_responses = Vec::new();
        for peer in peers.iter() {
            let peer_addr = peer.clone();
            let self_addr_clone = Arc::clone(&self_addr);
            
            let response = tokio::spawn(async move {
                let pre_vote_request = RespValue::Array(vec![
                    RespValue::BulkString("PREVOTE".to_string()),
                    RespValue::BulkString(new_term.to_string()),
                    RespValue::BulkString(self_addr_clone.to_string()),
                    RespValue::Integer(last_log_index as i64),
                    RespValue::Integer(last_log_term as i64),
                ]);

                if let Ok(mut stream) = TcpStream::connect(&peer_addr).await {
                    if stream.write_all(&pre_vote_request.encode()).await.is_ok() {
                        let mut buffer = BytesMut::with_capacity(256);
                        if let Ok(Ok(_)) = tokio::time::timeout(Duration::from_millis(100), stream.read_buf(&mut buffer)).await {
                            if let Some(RespValue::Array(resp)) = crate::network::decode_resp(&mut buffer) {
                                if let Some(RespValue::Integer(granted)) = resp.get(1) {
                                    return *granted == 1;
                                }
                            }
                        }
                    }
                }
                false
            });
            pre_vote_responses.push(response);
        }

        // Collect pre-vote results
        let mut pre_votes = 1; // Vote for self
        for response in pre_vote_responses {
            if let Ok(granted) = response.await {
                if granted { pre_votes += 1; }
            }
        }

        // Skip actual election if pre-vote didn't get majority
        if pre_votes < majority {
            let backoff_duration = Duration::from_millis(150 + (election_attempts as u64 * 50).min(1000));
            println!("[CONSENSUS] Pre-vote failed ({}/{} votes). Backing off for {:?}.", 
                     pre_votes, majority, backoff_duration);
            tokio::time::sleep(backoff_duration).await;
            continue;
        }

        // Actual RequestVote phase
        let mut vote_tasks = Vec::new();
        for peer in peers.iter() {
            let peer_addr = peer.clone();
            let role_clone = Arc::clone(&role_arc);
            let self_addr_clone = Arc::clone(&self_addr);
            let peers_clone = Arc::clone(&peers);
            let store_clone = Arc::clone(&store);

            let task = tokio::spawn(async move {
                let vote_request = RespValue::Array(vec![
                    RespValue::BulkString("REQUESTVOTE".to_string()),
                    RespValue::BulkString(new_term.to_string()),
                    RespValue::BulkString(self_addr_clone.to_string()),
                    RespValue::Integer(last_log_index as i64),
                    RespValue::Integer(last_log_term as i64),
                ]);

                if let Ok(mut stream) = TcpStream::connect(&peer_addr).await {
                    if stream.write_all(&vote_request.encode()).await.is_ok() {
                        let mut buffer = BytesMut::with_capacity(256);
                        if let Ok(Ok(_)) = tokio::time::timeout(Duration::from_millis(200), stream.read_buf(&mut buffer)).await {
                            if let Some(RespValue::Array(resp)) = crate::network::decode_resp(&mut buffer) {
                                if let (Some(RespValue::Integer(_)), Some(RespValue::Integer(granted))) = (resp.get(0), resp.get(1)) {
                                    if *granted == 1 {
                                        println!("[CONSENSUS] Vote granted by {} for term {}", peer_addr, new_term);
                                        let mut role_guard = role_clone.lock().unwrap();
                                        if let Role::Candidate { votes_received, .. } = &mut *role_guard {
                                            let won = {
                                                let mut votes = votes_received.lock().unwrap();
                                                votes.insert(peer_addr.clone());
                                                votes.len() >= majority
                                            };
                                            if won {
                                                println!(
                                                    "[CONSENSUS] MAJORITY REACHED. Elected PRIMARY for term {}.",
                                                    new_term
                                                );
                                                let (last_log_index, _) = store_clone.get_last_log_index_term();
                                                *role_guard = Role::new_primary(new_term, Arc::clone(&peers_clone), last_log_index);
                                                start_heartbeat_loop(Arc::clone(&role_clone), self_addr_clone, peers_clone, store_clone);
                                                 return true;
                                             }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                false
            });
            vote_tasks.push(task);
        }

        // Wait for all vote requests with adaptive timeout
        let election_timeout = Duration::from_millis(150 + (rand::random::<u64>() % 300));
        tokio::time::sleep(election_timeout).await;

        // Clean up any remaining tasks
        for task in vote_tasks {
            task.abort();
        }

        // Check leadership result without holding the mutex across an await
        let is_primary = {
            let role_guard = role_arc.lock().unwrap();
            matches!(&*role_guard, Role::Primary { .. })
        };
        if is_primary {
            println!(
                "[CONSENSUS] Election succeeded. Primary established for term {}.",
                new_term
            );
            break;
        } else {
            println!("[CONSENSUS] Election timeout for term {}. Preparing new attempt...", new_term);
            // Exponential backoff with jitter (drop lock before sleeping)
            let backoff = Duration::from_millis(
                (150 * (1 << election_attempts.min(4))) + (rand::random::<u64>() % 200)
            );
            tokio::time::sleep(backoff).await;
        }
    }
}

/// PreVote RPC: non-disruptive probe to check if an election is likely to succeed
pub fn handle_prevote(
    role_arc: Arc<Mutex<Role>>, 
    store: Arc<Store>,
    candidate_term: u64, 
    _candidate_id: String,
    candidate_last_log_index: usize,
    candidate_last_log_term: u64
) -> RespValue {
    let role_guard = role_arc.lock().unwrap();
    let my_term = role_guard.get_term();
    let (my_last_log_index, my_last_log_term) = store.get_last_log_index_term();

    // If the candidate's term is behind, immediately reject
    if candidate_term < my_term {
        return RespValue::Array(vec![
            RespValue::Integer(my_term as i64),
            RespValue::Integer(0),
        ]);
    }

    // Check if candidate's log is at least as up-to-date as ours
    let log_up_to_date = candidate_last_log_term > my_last_log_term || 
                        (candidate_last_log_term == my_last_log_term && candidate_last_log_index >= my_last_log_index);

    // If we've recently heard from a leader, be conservative and reject pre-vote
    let leader_recent = match &*role_guard {
        Role::Replica { last_append_received, .. } => last_append_received.lock().unwrap().elapsed() < Duration::from_millis(150),
        _ => false,
    };

    let granted = log_up_to_date && !leader_recent;

    RespValue::Array(vec![
        RespValue::Integer(my_term as i64),
        RespValue::Integer(if granted { 1 } else { 0 }),
    ])
}

/// Production-grade RequestVote RPC with log up-to-date checks
pub fn handle_request_vote(
    role_arc: Arc<Mutex<Role>>, 
    store: Arc<Store>,
    candidate_term: u64, 
    candidate_id: String,
    candidate_last_log_index: usize,
    candidate_last_log_term: u64
) -> RespValue {
    let mut role_guard = role_arc.lock().unwrap();
    let my_term = role_guard.get_term();
    let (my_last_log_index, my_last_log_term) = store.get_last_log_index_term();
    
    // Check if candidate's log is at least as up-to-date as ours
    let log_up_to_date = candidate_last_log_term > my_last_log_term || 
                        (candidate_last_log_term == my_last_log_term && candidate_last_log_index >= my_last_log_index);
    
    let vote_granted = match &*role_guard {
        Role::Replica { voted_for, .. } | Role::Candidate { voted_for, .. } => {
            if candidate_term > my_term && log_up_to_date && 
               (voted_for.is_none() || voted_for.as_ref() == Some(&candidate_id)) {
                println!("[CONSENSUS] Vote granted to {} for term {} (log up-to-date: {}/{})", 
                         candidate_id, candidate_term, candidate_last_log_index, candidate_last_log_term);
                role_guard.transition_to_replica(candidate_id.clone(), candidate_term, Some(candidate_id));
                true
            } else {
                let reason = if candidate_term <= my_term { "term not greater" }
                           else if !log_up_to_date { "log not up-to-date" }
                           else { "already voted" };
                println!("[CONSENSUS] Vote denied to {} for term {} (reason: {})", 
                         candidate_id, candidate_term, reason);
                false
            }
        }
        Role::Primary { term, .. } => {
            if candidate_term > *term {
                println!("[CONSENSUS] Primary stepping down, vote granted to {} for term {}", 
                         candidate_id, candidate_term);
                role_guard.transition_to_replica(candidate_id.clone(), candidate_term, Some(candidate_id));
                true
            } else {
                println!("[CONSENSUS] Primary denying vote to {} for term {} (not greater than my term {})", 
                         candidate_id, candidate_term, term);
                false
            }
        }
    };

    RespValue::Array(vec![
        RespValue::Integer(role_guard.get_term() as i64),
        RespValue::Integer(if vote_granted { 1 } else { 0 }),
    ])
}

/// Full Raft AppendEntries RPC with consistency checks and log replication
pub fn handle_append_entries(
    role_arc: Arc<Mutex<Role>>,
    store: Arc<Store>,
    leader_term: u64,
    leader_id: String,
    prev_log_index: usize,
    prev_log_term: u64,
    entries: Vec<LogEntry>,
    leader_commit: usize,
) -> RespValue {
    let mut role_guard = role_arc.lock().unwrap();
    let my_term = role_guard.get_term();
    
    // Reply false if term < currentTerm
    if leader_term < my_term {
        println!("[APPEND_ENTRIES] Rejecting AppendEntries from {} (term {} < current term {})", 
                 leader_id, leader_term, my_term);
        return RespValue::Array(vec![
            RespValue::Integer(my_term as i64),
            RespValue::Integer(0), // success = false
        ]);
    }

    // Update term and convert to follower if necessary
    if leader_term > my_term {
        println!("[APPEND_ENTRIES] Updating term from {} to {} and becoming follower of {}", 
                 my_term, leader_term, leader_id);
        role_guard.transition_to_replica(leader_id.clone(), leader_term, None);
    }

    // Reset election timeout (heartbeat received)
    if let Role::Replica { last_append_received, .. } = &mut *role_guard {
        *last_append_received.lock().unwrap() = Instant::now();
    }

    // Check log consistency: Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
    if prev_log_index > 0 {
        if let Some(prev_entry_term) = store.get_log_term_at(prev_log_index - 1) {
            if prev_entry_term != prev_log_term {
                println!("[APPEND_ENTRIES] Log consistency check failed at index {} (expected term {}, got {})", 
                         prev_log_index, prev_log_term, prev_entry_term);
                return RespValue::Array(vec![
                    RespValue::Integer(leader_term as i64),
                    RespValue::Integer(0), // success = false
                ]);
            }
        } else {
            println!("[APPEND_ENTRIES] Log consistency check failed: no entry at prev_log_index {}", prev_log_index);
            return RespValue::Array(vec![
                RespValue::Integer(leader_term as i64),
                RespValue::Integer(0), // success = false
            ]);
        }
    }

    // If an existing entry conflicts with a new one (same index but different terms),
    // delete the existing entry and all that follow it
    for (i, entry) in entries.iter().enumerate() {
        let log_index = prev_log_index + i;
        if let Some(existing_term) = store.get_log_term_at(log_index) {
            if existing_term != entry.term {
                println!("[APPEND_ENTRIES] Conflict detected at index {}, truncating from here", log_index);
                store.truncate_log_from(log_index);
                break;
            }
        }
    }

    // Append any new entries not already in the log
    let (current_log_len, _) = store.get_last_log_index_term();
    for (i, entry) in entries.iter().enumerate() {
        let log_index = prev_log_index + i;
        if log_index >= current_log_len {
            store.append_log(entry.term, entry.command.clone());
            println!("[APPEND_ENTRIES] Appended entry at index {} (term {})", log_index, entry.term);
        }
    }

    // If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
    let old_commit_index = role_guard.get_commit_index();
    if leader_commit > old_commit_index {
        let (last_log_index, _) = store.get_last_log_index_term();
        let new_commit_index = leader_commit.min(last_log_index);
        
        if let Role::Replica { commit_index, last_applied, .. } = &mut *role_guard {
            *commit_index = new_commit_index;
            
            // Apply newly committed entries to state machine
            if new_commit_index > *last_applied {
                println!("[APPEND_ENTRIES] Applying committed entries from {} to {}", 
                         *last_applied + 1, new_commit_index);
                store.apply_range_inclusive(*last_applied, new_commit_index - 1);
                *last_applied = new_commit_index;
            }
        }
    }

    println!("[APPEND_ENTRIES] AppendEntries successful from {} (term {}, {} entries)", 
             leader_id, leader_term, entries.len());
    
    RespValue::Array(vec![
        RespValue::Integer(leader_term as i64),
        RespValue::Integer(1), // success = true
    ])
}

/// Enhanced heartbeat loop with adaptive timing and failure detection
pub fn start_heartbeat_loop(role_arc: Arc<Mutex<Role>>, self_addr: Arc<String>, peers: Arc<Vec<String>>, store: Arc<Store>) {
    tokio::spawn(async move {
        println!("[HEARTBEAT] Starting heartbeat loop with {} peers", peers.len());
        let mut heartbeat_interval = Duration::from_millis(50); // Start aggressive
        let mut consecutive_failures = HashMap::new();
        
        loop {
            tokio::time::sleep(heartbeat_interval).await;
            let heartbeat_start = Instant::now();
            let (last_log_index, last_log_term) = store.get_last_log_index_term();

            // Fetch current term and commit index from role; stop if no longer primary
            let (current_term, leader_commit, still_primary) = {
                let guard = role_arc.lock().unwrap();
                let term = guard.get_term();
                let commit = guard.get_commit_index();
                let is_primary = matches!(*guard, Role::Primary { .. });
                (term, commit, is_primary)
            };

            if !still_primary {
                println!("[HEARTBEAT] Role changed from PRIMARY; stopping heartbeat loop.");
                break;
            }

            let append_cmd = RespValue::Array(vec![
                RespValue::BulkString("APPENDENTRIES".to_string()),
                RespValue::Integer(current_term as i64), // term
                RespValue::BulkString(self_addr.to_string()), // leader_id
                RespValue::Integer(last_log_index as i64),
                RespValue::Integer(last_log_term as i64),
                RespValue::Array(vec![]), // empty entries (heartbeat)
                RespValue::Integer(leader_commit as i64), // leader_commit
            ]);
            let append_bytes = append_cmd.encode();

            let mut tasks = Vec::new();
            for peer in peers.iter() {
                let peer_addr = peer.clone();
                let append_bytes_clone = append_bytes.clone();
                
                let task = tokio::spawn(async move {
                    let start = Instant::now();
                    match TcpStream::connect(&peer_addr).await {
                        Ok(mut stream) => {
                            match stream.write_all(&append_bytes_clone).await {
                                Ok(_) => {
                                    let latency = start.elapsed();
                                    println!("[HEARTBEAT] Heartbeat to {} succeeded ({:?})", peer_addr, latency);
                                    (peer_addr, Ok(latency))
                                }
                                Err(e) => {
                                    println!("[HEARTBEAT] Failed to send heartbeat to {}: {}", peer_addr, e);
                                    (peer_addr, Err(e))
                                }
                            }
                        }
                        Err(e) => {
                            println!("[HEARTBEAT] Failed to connect to {}: {}", peer_addr, e);
                            (peer_addr, Err(e))
                        }
                    }
                });
                tasks.push(task);
            }

            // Collect heartbeat results and adapt timing
            let mut successful_heartbeats = 0;
            for task in tasks {
                if let Ok((peer_addr, result)) = task.await {
                    match result {
                        Ok(_latency) => {
                            consecutive_failures.remove(&peer_addr);
                            successful_heartbeats += 1;
                        }
                        Err(_) => {
                            *consecutive_failures.entry(peer_addr).or_insert(0) += 1;
                        }
                    }
                }
            }

            // Adaptive heartbeat timing based on cluster health
            let failure_ratio = 1.0 - (successful_heartbeats as f64 / peers.len() as f64);
            heartbeat_interval = if failure_ratio > 0.5 {
                Duration::from_millis(200) // Slow down if many failures
            } else {
                Duration::from_millis(50)  // Stay aggressive if healthy
            };

            let heartbeat_duration = heartbeat_start.elapsed();
            if heartbeat_duration > Duration::from_millis(100) {
                println!("[HEARTBEAT] Slow heartbeat cycle: {:?}", heartbeat_duration);
            }
        }
    });
}

