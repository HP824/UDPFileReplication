# CSE156 Lab 4 — UDP File Replication

A custom UDP file replication protocol with Go-Back-N reliability on the client. The client reads a local file and uploads it to one or more replica servers; each server stores the file under a configurable root directory.

## Build

Requires a Linux environment with `g++` and `pthread` (WSL works on Windows).

```bash
make
```

Binaries are written to `bin/myclient` and `bin/myserver`.

```bash
make clean   # remove bin/*
```

## Usage

### Server

```bash
./bin/myserver <port> <droppc> <root_folder_path>
```

| Argument | Description |
|----------|-------------|
| `port` | UDP port to listen on |
| `droppc` | Packet drop probability 0–100 (testing only) |
| `root_folder_path` | Directory where uploaded files are stored |

Example:

```bash
./bin/myserver 9100 0 test/roots/replica1
```

With `droppc > 0`, the server randomly drops incoming DATA packets and outgoing ACKs to exercise client retransmissions. Server logs include timestamps, client address, and event type (`DATA`, `ACK`, `DROP DATA`, `DROP ACK`).

### Client

```bash
./bin/myclient <servn> <servaddr.conf> <mss> <winsz> <in_file_path> <out_file_path>
```

| Argument | Description |
|----------|-------------|
| `servn` | Number of replica servers to use (≤ entries in config) |
| `servaddr.conf` | Server list file (`IP port` per line) |
| `mss` | Max segment size in bytes (must be > 6) |
| `winsz` | Go-Back-N send window size |
| `in_file_path` | Local file to upload |
| `out_file_path` | Remote path relative to each server's root |

Example:

```bash
./bin/myclient 2 servaddr.conf 1400 8 test/input/sample.txt uploads/sample.txt
```

### Server config (`servaddr.conf`)

One server per line. Lines starting with `#` are comments.

```
127.0.0.1 9100
127.0.0.1 9101
```

## Protocol (brief)

1. Client sends `CMD_PUT` with the remote filename; server replies with `CMD_ACK`.
2. Client sends `CMD_DATA` packets with sequence numbers and file chunks.
3. Server ACKs each DATA packet. An empty DATA packet marks end-of-file.
4. Client uses Go-Back-N: at most `winsz` packets in flight; unacked packets in the current window are retransmitted.

## Testing

Test fixtures and scripts are under `test/` and `scripts/`.

```bash
# Build and run automated tests (from project root)
bash scripts/run_basic_test.sh      # 2 replicas, no packet loss
bash scripts/run_lossy_test.sh      # 1 replica, droppc=5
bash scripts/run_lossy_test.sh 10   # custom drop rate
```

### Manual two-replica test

Terminal 1:

```bash
mkdir -p test/roots/replica1
./bin/myserver 9100 0 test/roots/replica1
```

Terminal 2:

```bash
mkdir -p test/roots/replica2
./bin/myserver 9101 0 test/roots/replica2
```

Terminal 3:

```bash
./bin/myclient 2 servaddr.conf 1400 8 test/input/sample.txt uploads/sample.txt
```

Verify:

```bash
cmp test/input/sample.txt test/roots/replica1/uploads/sample.txt
cmp test/input/sample.txt test/roots/replica2/uploads/sample.txt
```

For loss testing, use a single server with a low `droppc` (5–10). Higher drop rates may cause ACK timeouts.

## Project layout

```
├── Makefile
├── README.md
├── servaddr.conf          # example server list
├── bin/                   # built binaries (gitignored)
├── scripts/
│   ├── run_basic_test.sh
│   └── run_lossy_test.sh
├── src/
│   ├── myclient.cpp       # Go-Back-N client with multi-replica threads
│   └── myserver.cpp       # UDP receiver with optional packet drops
└── test/
    ├── input/             # sample upload files
    ├── roots/             # per-replica storage roots
    └── logs/              # server logs from test scripts
```
