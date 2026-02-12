use helios::network::{self, RespValue};
use helios::replication::Role;
use helios::store::Store;
use bytes::BytesMut;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[tokio::test]
async fn kv_set_get_delete_roundtrip() {
    // Start a minimal server harness
    let store = Arc::new(Store::new());
    let role = Arc::new(Mutex::new(Role::Primary {
        replicas: Arc::new(Mutex::new(HashMap::new())),
        term: 0,
        peers: Arc::new(vec![]),
        commit_index: 0,
        last_applied: 0,
    }));
    let self_addr = Arc::new("127.0.0.1:0".to_string());
    let peers = Arc::new(vec![]);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = {
        let store_clone = Arc::clone(&store);
        let role_clone = Arc::clone(&role);
        let self_addr_clone = Arc::clone(&self_addr);
        let peers_clone = Arc::clone(&peers);
        tokio::spawn(async move {
            let (socket, peer_addr) = listener.accept().await.unwrap();
            network::handle_connection(socket, peer_addr, store_clone, role_clone, self_addr_clone, peers_clone)
                .await
                .unwrap();
        })
    };

    // Client
    let mut stream = TcpStream::connect(addr).await.unwrap();

    // SET k v
    let set = RespValue::Array(vec![
        RespValue::BulkString("SET".into()),
        RespValue::BulkString("k".into()),
        RespValue::BulkString("v".into()),
    ]);
    stream.write_all(&set.encode()).await.unwrap();
    let mut buf = BytesMut::with_capacity(256);
    stream.read_buf(&mut buf).await.unwrap();
    let resp = network::decode_resp(&mut buf).unwrap();
    assert_eq!(resp, RespValue::SimpleString("OK".into()));

    // GET k
    let get = RespValue::Array(vec![
        RespValue::BulkString("GET".into()),
        RespValue::BulkString("k".into()),
    ]);
    stream.write_all(&get.encode()).await.unwrap();
    let mut buf2 = BytesMut::with_capacity(256);
    stream.read_buf(&mut buf2).await.unwrap();
    let resp2 = network::decode_resp(&mut buf2).unwrap();
    assert_eq!(resp2, RespValue::BulkString("v".into()));

    // DELETE k
    let del = RespValue::Array(vec![
        RespValue::BulkString("DELETE".into()),
        RespValue::BulkString("k".into()),
    ]);
    stream.write_all(&del.encode()).await.unwrap();
    let mut buf3 = BytesMut::with_capacity(256);
    stream.read_buf(&mut buf3).await.unwrap();
    let resp3 = network::decode_resp(&mut buf3).unwrap();
    assert_eq!(resp3, RespValue::Integer(1));

    // Cleanup
    drop(stream);
    let _ = std::fs::remove_file("helios.aof");
    let _ = server.await;
}