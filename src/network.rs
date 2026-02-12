use crate::{store::{Store, LogEntry}, replication::{self, Role}, persistence, consensus};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use bytes::{BytesMut, Buf};
use std::time::Instant;

#[derive(Debug, Clone, PartialEq)]
pub enum RespValue {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(String),
    Array(Vec<RespValue>),
    Nil,
}

impl RespValue {
    pub fn encode(self) -> Vec<u8> {
        match self {
            RespValue::SimpleString(s) => format!("+{}\r\n", s).into_bytes(),
            RespValue::Error(s) => format!("-{}\r\n", s).into_bytes(),
            RespValue::Integer(i) => format!(":{}\r\n", i).into_bytes(),
            RespValue::BulkString(s) => format!("${}\r\n{}\r\n", s.len(), s).into_bytes(),
            RespValue::Array(arr) => {
                let mut bytes = format!("*{}\r\n", arr.len()).into_bytes();
                for val in arr { bytes.extend(val.encode()); }
                bytes
            }
            RespValue::Nil => b"$-1\r\n".to_vec(),
        }
    }
}

pub fn decode_resp(buffer: &mut BytesMut) -> Option<RespValue> {
    if buffer.is_empty() { return None; }
    let prefix = buffer[0];

    // Parse header line and compute header_total from header length
    let header_str = {
        let slice = &buffer[1..];
        let pos = slice.windows(2).position(|w| w == b"\r\n")?;
        std::str::from_utf8(&slice[..pos]).ok()?.to_string()
    };
    let header_total = 1 + header_str.len() + 2; // prefix + data + CRLF

    match prefix {
        b'+' => {
            buffer.advance(header_total);
            Some(RespValue::SimpleString(header_str))
        }
        b'-' => {
            buffer.advance(header_total);
            Some(RespValue::Error(header_str))
        }
        b':' => {
            let i = header_str.parse().ok()?;
            buffer.advance(header_total);
            Some(RespValue::Integer(i))
        }
        b'$' => {
            let len: isize = header_str.parse().ok()?;
            if len == -1 { // Null bulk string
                buffer.advance(header_total);
                return Some(RespValue::Nil);
            }
            let len = len as usize;
            if buffer.len() < header_total + len + 2 { return None; }
            let s = {
                let slice = &buffer[header_total..header_total + len];
                std::str::from_utf8(slice).ok()?.to_string()
            };
            // consume header + content + trailing CRLF
            buffer.advance(header_total + len + 2);
            Some(RespValue::BulkString(s))
        }
        b'*' => {
            let count: isize = header_str.parse().ok()?;
            if count <= 0 {
                buffer.advance(header_total);
                return Some(RespValue::Array(vec![]));
            }
            let count = count as usize;
            // advance past header before decoding elements
            buffer.advance(header_total);
            let mut items = Vec::with_capacity(count);
            for _ in 0..count {
                let item = decode_resp(buffer)?;
                items.push(item);
            }
            Some(RespValue::Array(items))
        }
        _ => None,
    }
}

pub async fn handle_connection(
    stream: TcpStream,
    addr: SocketAddr,
    store: Arc<Store>,
    role: Arc<Mutex<Role>>, 
    self_addr: Arc<String>,
    _peers: Arc<Vec<String>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut reader = BufReader::new(stream);
    let mut buffer = BytesMut::with_capacity(1024);

    // Helper to check if the remote address exactly matches our configured primary.
    let is_from_primary = || -> bool {
        let local_addr = match self_addr.parse::<SocketAddr>() {
            Ok(a) => a,
            Err(_) => return false,
        };
        if let Ok(guard) = role.lock() {
            if let Role::Replica { primary_addr, .. } = &*guard {
                if let Ok(primary_sa) = primary_addr.parse::<SocketAddr>() {
                    // If local node and primary are configured loopback, strict tuple match.
                    // Otherwise still require full socket address match to prevent same-IP spoofing.
                    if local_addr.ip().is_loopback() && primary_sa.ip().is_loopback() {
                        return primary_sa == addr;
                    }
                    return primary_sa == addr;
                }
            }
        }
        false
    };

    loop {
        let n = reader.read_buf(&mut buffer).await?;
        if n == 0 { return Ok(()); }

        while let Some(command_value) = decode_resp(&mut buffer) {
            let response = match Command::from_resp(command_value.clone()) {
                Ok(Command::Get(key)) => store.get(&key).map_or(RespValue::Nil, |v| RespValue::BulkString(v)),
                Ok(Command::Set(key, value)) => {
                    // Determine behavior based on role and source of the connection
                    let mut as_primary_term: Option<u64> = None;
                    let is_replication_from_primary = is_from_primary();
                    {
                        let role_guard = role.lock().unwrap();
                        match &*role_guard {
                            Role::Primary { term, .. } => as_primary_term = Some(*term),
                            Role::Replica { .. } if is_replication_from_primary => { /* accept below */ }
                            _ => {}
                        }
                    }

                    if let Some(term) = as_primary_term {
                        // Primary: accept client write, replicate to peers
                        let command = Command::Set(key.clone(), value.clone());
                        store.append_log(term, command.clone());
                        store.set(key, value);
                        persistence::log_command(&command)?;
                        replication::propagate_command(&role, command).await;
                        RespValue::SimpleString("OK".to_string())
                    } else if is_replication_from_primary {
                        // Replica receiving replication from primary: apply locally
                        store.set(key, value);
                        // update last append timestamp to avoid starting election
                        if let Ok(guard) = role.lock() {
                            if let Role::Replica { last_append_received, .. } = &*guard {
                                *last_append_received.lock().unwrap() = Instant::now();
                            }
                        }
                        RespValue::SimpleString("OK".to_string())
                    } else {
                        RespValue::Error("REPLICA/CANDIDATE can't accept writes".to_string())
                    }
                }
                Ok(Command::Delete(key)) => {
                    let mut as_primary_term: Option<u64> = None;
                    let is_replication_from_primary = is_from_primary();
                    {
                        let role_guard = role.lock().unwrap();
                        match &*role_guard {
                            Role::Primary { term, .. } => as_primary_term = Some(*term),
                            Role::Replica { .. } if is_replication_from_primary => { /* accept below */ }
                            _ => {}
                        }
                    }

                    if let Some(term) = as_primary_term {
                        let command = Command::Delete(key.clone());
                        store.append_log(term, command.clone());
                        let existed = store.delete(&key).is_some();
                        if existed { persistence::log_command(&command)?; }
                        replication::propagate_command(&role, command).await;
                        RespValue::Integer(if existed { 1 } else { 0 })
                    } else if is_replication_from_primary {
                        let existed = store.delete(&key).is_some();
                        if let Ok(guard) = role.lock() {
                            if let Role::Replica { last_append_received, .. } = &*guard {
                                *last_append_received.lock().unwrap() = Instant::now();
                            }
                        }
                        RespValue::Integer(if existed { 1 } else { 0 })
                    } else {
                        RespValue::Error("REPLICA/CANDIDATE can't accept writes".to_string())
                    }
                }
                Ok(Command::Psync) => {
                    let is_primary = {
                        let role_guard = role.lock().unwrap();
                        matches!(&*role_guard, Role::Primary { .. })
                    };
                    if is_primary {
                        println!("[REPLICATION] Received PSYNC from {}. Starting full resync.", addr);
                        let response = RespValue::SimpleString("FULLRESYNC".to_string());
                        reader.get_mut().write_all(&response.encode()).await?;

                        let all_data = store.get_all_data();
                        let mut data_as_resp = Vec::with_capacity(all_data.len() * 2);
                        for (k, v) in all_data {
                            data_as_resp.push(RespValue::BulkString(k));
                            data_as_resp.push(RespValue::BulkString(v));
                        }
                        reader.get_mut().write_all(&RespValue::Array(data_as_resp).encode()).await?;
                        // Close connection after full resync; leader will push updates by dialing peers on demand
                        return Ok(());
                    } else {
                        RespValue::Error("Not a primary node".to_string())
                    }
                }
                // --- Leader Election Commands ---
                Ok(Command::PreVote { term, candidate_id, last_log_index, last_log_term }) => {
                    consensus::handle_prevote(Arc::clone(&role), Arc::clone(&store), term, candidate_id, last_log_index, last_log_term)
                }
                Ok(Command::RequestVote { term, candidate_id, last_log_index, last_log_term }) => {
                    consensus::handle_request_vote(Arc::clone(&role), Arc::clone(&store), term, candidate_id, last_log_index, last_log_term)
                }
                Ok(Command::AppendEntries { term, leader_id, prev_log_index, prev_log_term, entries, leader_commit }) => {
                    consensus::handle_append_entries(Arc::clone(&role), Arc::clone(&store), term, leader_id, prev_log_index, prev_log_term, entries, leader_commit)
                }
                Ok(Command::Ping) => {
                    // Treat PING from primary as heartbeat and update last append timestamp on replica
                    if is_from_primary() {
                        if let Ok(guard) = role.lock() {
                            if let Role::Replica { last_append_received, .. } = &*guard {
                                *last_append_received.lock().unwrap() = Instant::now();
                            }
                        }
                    }
                    RespValue::SimpleString("PONG".to_string())
                }
                Err(e) => RespValue::Error(e),
            };
            reader.get_mut().write_all(&response.encode()).await?;
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum Command {
    Get(String),
    Set(String, String),
    Delete(String),
    Psync,
    PreVote { term: u64, candidate_id: String, last_log_index: usize, last_log_term: u64 },
    RequestVote { term: u64, candidate_id: String, last_log_index: usize, last_log_term: u64 },
    AppendEntries { term: u64, leader_id: String, prev_log_index: usize, prev_log_term: u64, entries: Vec<LogEntry>, leader_commit: usize },
    Ping,
}

impl Command {
    pub fn from_resp(value: RespValue) -> Result<Self, String> {
        match value {
            RespValue::Array(arr) => {
                let command_name = match arr.get(0) {
                    Some(RespValue::BulkString(s)) => s.to_uppercase(),
                    _ => return Err("Invalid command format".to_string()),
                };
                match command_name.as_str() {
                    "GET" if arr.len() == 2 => match &arr[1] {
                        RespValue::BulkString(k) => Ok(Command::Get(k.clone())),
                        _ => Err("GET expects a bulk string key".to_string()),
                    },
                    "SET" if arr.len() == 3 => match (&arr[1], &arr[2]) {
                        (RespValue::BulkString(k), RespValue::BulkString(v)) => Ok(Command::Set(k.clone(), v.clone())),
                        _ => Err("SET expects two bulk string arguments".to_string()),
                    },
                    "DELETE" if arr.len() == 2 => match &arr[1] {
                        RespValue::BulkString(k) => Ok(Command::Delete(k.clone())),
                        _ => Err("DELETE expects a bulk string key".to_string()),
                    },
                    "PSYNC" => Ok(Command::Psync),
                    "PREVOTE" if arr.len() == 5 => {
                        let term = parse_int(&arr[1], "term")? as u64;
                        let candidate_id = match &arr[2] { RespValue::BulkString(s) => s.clone(), _ => return Err("PREVOTE expects candidate_id".to_string()) };
                        let last_log_index = parse_int(&arr[3], "last_log_index")? as usize;
                        let last_log_term = parse_int(&arr[4], "last_log_term")? as u64;
                        Ok(Command::PreVote { term, candidate_id, last_log_index, last_log_term })
                    }
                    "REQUESTVOTE" if arr.len() == 5 => {
                        let term = parse_int(&arr[1], "term")? as u64;
                        let candidate_id = match &arr[2] { RespValue::BulkString(s) => s.clone(), _ => return Err("REQUESTVOTE expects candidate_id".to_string()) };
                        let last_log_index = parse_int(&arr[3], "last_log_index")? as usize;
                        let last_log_term = parse_int(&arr[4], "last_log_term")? as u64;
                        Ok(Command::RequestVote { term, candidate_id, last_log_index, last_log_term })
                    }
                    "APPENDENTRIES" if arr.len() == 7 => {
                        let term = parse_int(&arr[1], "term")? as u64;
                        let leader_id = match &arr[2] { RespValue::BulkString(s) => s.clone(), _ => return Err("APPENDENTRIES expects leader_id".to_string()) };
                        let prev_log_index = parse_int(&arr[3], "prev_log_index")? as usize;
                        let prev_log_term = parse_int(&arr[4], "prev_log_term")? as u64;
                        let entries = match &arr[5] {
                            RespValue::Array(items) => parse_log_entries(items)?,
                            _ => return Err("APPENDENTRIES expects entries array".to_string()),
                        };
                        let leader_commit = parse_int(&arr[6], "leader_commit")? as usize;
                        Ok(Command::AppendEntries { term, leader_id, prev_log_index, prev_log_term, entries, leader_commit })
                    }
                    "PING" => Ok(Command::Ping),
                    _ => Err(format!("Unknown command '{}'", command_name)),
                }
            }
            _ => Err("Invalid RESP format: expected Array".to_string()),
        }
    }
}

impl LogEntry {
    pub fn get_key(&self) -> String {
        match &self.command {
            Command::Set(k, _) => k.clone(),
            Command::Delete(k) => k.clone(),
            _ => String::new(),
        }
    }

    pub fn get_value(&self) -> Option<String> {
        match &self.command {
            Command::Set(_, v) => Some(v.clone()),
            _ => None,
        }
    }
}

fn parse_int(val: &RespValue, name: &str) -> Result<i64, String> {
    match val {
        RespValue::Integer(i) => Ok(*i),
        RespValue::BulkString(s) => s.parse::<i64>().map_err(|_| format!("{} must be integer", name)),
        _ => Err(format!("{} must be integer", name)),
    }
}

fn parse_log_entries(items: &Vec<RespValue>) -> Result<Vec<LogEntry>, String> {
    let mut entries = Vec::new();
    for item in items {
        match item {
            RespValue::Array(e) if e.len() >= 2 => {
                let term = parse_int(&e[0], "term")? as u64;
                let cmd_array = match &e[1] { RespValue::Array(a) => a.clone(), _ => return Err("entry command must be array".to_string()) };
                let command = Command::from_resp(RespValue::Array(cmd_array))?;
                // Optional third element (index) is ignored to match Store::LogEntry (term, command)
                entries.push(LogEntry { term, command });
            }
            _ => return Err("invalid log entry format".to_string()),
        }
    }
    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resp_encode_decode_roundtrip() {
        let arr = RespValue::Array(vec![RespValue::BulkString("SET".to_string()), RespValue::BulkString("k".to_string()), RespValue::BulkString("v".to_string())]);
        let mut buf = BytesMut::from(&arr.clone().encode()[..]);
        let decoded = decode_resp(&mut buf).unwrap();
        assert_eq!(arr, decoded);
    }

    #[test]
    fn command_parse_set_get_delete() {
        let set = RespValue::Array(vec![RespValue::BulkString("SET".to_string()), RespValue::BulkString("k".to_string()), RespValue::BulkString("v".to_string())]);
        let get = RespValue::Array(vec![RespValue::BulkString("GET".to_string()), RespValue::BulkString("k".to_string())]);
        let del = RespValue::Array(vec![RespValue::BulkString("DELETE".to_string()), RespValue::BulkString("k".to_string())]);
        assert!(matches!(Command::from_resp(set), Ok(Command::Set(_, _))));
        assert!(matches!(Command::from_resp(get), Ok(Command::Get(_))));
        assert!(matches!(Command::from_resp(del), Ok(Command::Delete(_))));
    }
}
