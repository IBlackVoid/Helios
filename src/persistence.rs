use crate::{store::Store, network::{Command, RespValue}};
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::sync::Arc;
use bytes::BytesMut;

const AOF_FILE: &str = "helios.aof";

pub fn log_command(command: &Command) -> io::Result<()> {
    let mut file = OpenOptions::new().create(true).append(true).open(AOF_FILE)?;
    let bytes = match command {
        Command::Set(k, v) => RespValue::Array(vec![
            RespValue::BulkString("SET".to_string()),
            RespValue::BulkString(k.clone()),
            RespValue::BulkString(v.clone()),
        ]).encode(),
        Command::Delete(k) => RespValue::Array(vec![
            RespValue::BulkString("DELETE".to_string()),
            RespValue::BulkString(k.clone()),
        ]).encode(),
        _ => vec![],
    };
    if !bytes.is_empty() { file.write_all(&bytes)?; }
    Ok(())
}

pub fn load_from_disk(store: Arc<Store>) -> io::Result<()> {
    if let Ok(mut file) = File::open(AOF_FILE) {
        let mut content = Vec::new();
        file.read_to_end(&mut content)?;
        let mut buf = BytesMut::from(&content[..]);
        while let Some(value) = crate::network::decode_resp(&mut buf) {
            if let Ok(cmd) = crate::network::Command::from_resp(value) {
                match cmd {
                    crate::network::Command::Set(k, v) => store.set(k, v),
                    crate::network::Command::Delete(k) => { store.delete(&k); },
                    _ => {}
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn aof_roundtrip() {
        let _ = fs::remove_file(AOF_FILE);
        let store = Arc::new(Store::new());
        log_command(&crate::network::Command::Set("a".into(), "b".into())).unwrap();
        log_command(&crate::network::Command::Delete("a".into())).unwrap();
        load_from_disk(Arc::clone(&store)).unwrap();
        assert_eq!(store.get("a"), None);
    }
}