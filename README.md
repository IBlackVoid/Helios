# Helios

Helios is a Rust distributed key-value store project built to understand replication, leader election, and failure handling from first principles.

This is not production software. It is a systems-learning codebase with explicit tradeoffs and known gaps.

## What It Does

- In-memory key-value store with RESP-like protocol support.
- Primary/replica mode with replication commands.
- Raft-inspired election flow (`PREVOTE`, `REQUESTVOTE`, `APPENDENTRIES`).
- Persistent append-only command log.

## Why It Exists

Most tutorials hide hard parts behind libraries. This project does the opposite:
- implement the wire protocol,
- implement role state transitions,
- debug timeouts, retries, and split-brain edge cases in code you can read.

## Build

```bash
cargo build
```

## Run

```bash
# Primary
cargo run -- --addr 127.0.0.1:6380 --peer 127.0.0.1:6381

# Replica
cargo run -- --addr 127.0.0.1:6381 --replicaof 127.0.0.1:6380 --peer 127.0.0.1:6380
```

## Test

```bash
cargo test
```

## Current Limits

- Consensus is Raft-inspired, not full Raft compliance.
- No authentication/TLS on network traffic.
- Operational tooling and observability are minimal.

If you want a production system, use one. If you want to learn by building one, this project is for that.
