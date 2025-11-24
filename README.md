# Stable Leader Paxos

Deterministic, stable-leader Multi-Paxos for a replicated banking ledger. The system keeps a single leader in steady state, executes transfers through Paxos rounds, persists balances in BoltDB, and exposes an interactive client driver for scripted fault-injection scenarios.

## Overview

- **Protocol** – Multi-Paxos with a stable leader. The leader assigns monotonically increasing sequence numbers, drives accept/commit, and only triggers elections when timers or explicit faults fire.
- **Workload** – Simple banking transfers between accounts A–J. Each node stores balances locally (`bbolt`) and applies committed transactions in order.
- **Networking** – All replicas communicate over gRPC using the service defined in `proto/paxos.proto`. The generated bindings live under `pb/`.
- **Default topology** – 5 nodes (`n1`–`n5`) and 10 clients. With 5 replicas the system tolerates 2 simultaneous failures.

## System Architecture

### Paxos node (`cmd/server`, `internal/paxos`)
- **Composition** – Every node wires together a `Proposer`, `Acceptor`, `Executor`, `LeaderElector`, `CheckpointManager`, `Logger`, and a `SafeTimer`. Components share the `ServerState` (ballots, log, checkpoints) and `ServerConfig` (n, f, checkpoint interval `K=10`).
- **Lifecycle** – Nodes start a gRPC server, initialize BoltDB, connect to peers, elect a leader (`InitializeSystem`), and spawn routers for execution, leader election, and checkpoint purging.
- **Leader behaviour** – The leader owns the proposer context, assigns sequence numbers, runs accept/commit phases, broadcasts commits, and periodically emits checkpoints. If timers expire or `KillLeader` is called, the elector runs a prepare/new-view cycle to pick a new leader.
- **Failure handling** – Checkpoints, catch-up RPCs, and install-checkpoint routines allow lagging replicas to regain state before executing new work. Nodes can also be toggled alive/dead at runtime via the client tool.

### Client driver (`cmd/client`, `internal/clientapp`)
- Parses CSV workloads into per-client queues grouped by *set number* and optional *subset* markers (`LF` rows).
- Spins one goroutine per logical client plus a queue coordinator. Uses channels to feed sequence sets to the replicas.
- Offers an interactive REPL for stepping through sets, printing state, reconfiguring node liveness, or injecting failures (e.g., `kill leader`).

### Data layer (`internal/database`)
- Thin wrapper over BoltDB with operations to initialize balances, update accounts atomically, dump state, and reset the store.
- Each node keeps its own `data/<node>.db`. Databases are recreated by the launch script for clean runs.

### gRPC surface (`proto/paxos.proto`)
- **Consensus RPCs** – `PrepareRequest`, `AcceptRequest`, `CommitRequest`, `NewViewRequest`, `CheckpointRequest`, `CatchupRequest`, `GetCheckpoint`.
- **Client RPCs** – `TransferRequest` for the happy path, `ForwardRequest` so backups can redirect traffic to the leader.
- **Diagnostics & control** – `PrintLog`, `PrintDB`, `PrintStatus`, `PrintView`, `ReconfigureNode`, `KillLeader`, `ResetNode`.

## Request lifecycle
1. A client sends `TransferRequest` to what it believes is the leader.
2. The leader assigns a sequence number, logs the request, and multicasts `AcceptRequest` to peers.
3. Once it gathers `f+1` accepts (including itself), it marks the log entry committed and issues `CommitRequest`.
4. `Executor` processes committed entries in order, mutates BoltDB, and acknowledges the trigger channel so the proposer can proceed.
5. Every 10 commits the leader snapshots balances, distributes `CheckpointMessage`, and purges stale log entries.

## Configuration (`configs/config.yaml`)

```yaml
nodes:
  n1:
    id: "n1"
    address: "localhost:5001"
  n2:
    id: "n2"
    address: "localhost:5002"
  n3:
    id: "n3"
    address: "localhost:5003"
  n4:
    id: "n4"
    address: "localhost:5004"
  n5:
    id: "n5"
    address: "localhost:5005"

clients: [A, B, C, D, E, F, G, H, I, J]

db_dir: ./data
init_balance: 10
```

- Add/remove nodes by editing the `nodes` map; each entry must include `id` and `address`.
- `clients` drives both the database initialization and the goroutines the client app creates.
- `init_balance` controls the starting balance of every account across all nodes.

## Running locally

### Prerequisites
- Go `1.25.1+`
- `protoc`, `protoc-gen-go`, `protoc-gen-go-grpc` on `PATH`
- `yq` (launch script parses YAML): `brew install yq`
- Python 3 (optional) with `pandas`/`numpy` for workload generation

### Install deps
```bash
cd /Users/mavleo/Repositories/stable-leader-paxos
go mod download
```

### Generate protobuf stubs (only after editing `proto/paxos.proto`)
   ```bash
./scripts/generate_stubs.sh
```

### Start replicas
```bash
./scripts/launch.sh
```
- Drops `logs/` and `data/`, recreates directories, and runs `go run cmd/server/main.go` per node with stdout/err redirected to `logs/out` and `logs/err`.
- Requires `yq` to iterate over the node list.
- To stop everything: `./scripts/kill.sh` or send `Ctrl+C` in each server terminal.

To run a single node manually:
```bash
go run cmd/server/main.go --id n1 --config ./configs/config.yaml
```

### Run the interactive client
```bash
go run cmd/client/main.go --file testdata/single.csv
```
- Loads the CSV, spawns client goroutines, and opens a REPL. Use `next` to start the next set, `exit` to stop. The process shuts down cleanly by cancelling all goroutines and closing node channels.

## Client REPL commands
- `next` – Dispatch the next set/subset of transactions.
- `print log` – Ask nodes to dump their Paxos log for the current test set.
- `print db` – Collect the balance snapshot from every node.
- `print status [N|all]` – Show accept/commit/execution status for a particular sequence number or the whole log.
- `print view` – Show the leader and ballot.
- `kill leader` – Invoke `KillLeader` RPC on the currently known leader to simulate a crash.
- `reset` – Reset both clients and replicas (database balances revert to `init_balance`).
- `exit` – Stop the client program.

## Test data format (`testdata/*.csv`)
Each file follows the schema produced by `scripts/create_synth_data.py`:

| Column       | Meaning                                                                 |
|--------------|-------------------------------------------------------------------------|
| `Set Number` | Integer label for a batch of transactions.                              |
| `Transactions` | Either `(Sender, Receiver, Amount)` or the literal `LF`.              |
| `Live Nodes` | `[n1, n2, ...]` for the first row of a set to declare active replicas.  |

- Transactions are queued per client in the order they appear.
- When the parser sees `LF`, it closes the current subset and starts a new one under the same `Set Number` (handy for liveness/partition experiments).
- `scripts/create_synth_data.py` can regenerate `synth_small.csv`, `synth.csv`, and `synth_big.csv` with random traffic.

## Logs & troubleshooting
- Runtime logs sit in `logs/out/*.out` and `logs/err/*.err`. The server uses `logrus` with ms precision timestamps.
- Use `print log` / `print db` before digging into the files; most debugging endpoints stream through gRPC.
- Checkpoint digests and catch-up progress are logged by the `CheckpointManager`.

## Scripts
- `scripts/launch.sh` – start every node, wiping old state.
- `scripts/kill.sh` – kill all `go run cmd/server` processes.
- `scripts/generate_stubs.sh` – regenerate Go protobuf/grpc code and run `go mod tidy`.
- `scripts/create_synth_data.py` – regenerate synthetic workloads.

## Repository layout

```
stable-leader-paxos/
├── cmd/
│   ├── client/           # Interactive workload driver
│   └── server/           # Paxos replica entrypoint
├── configs/              # Cluster + workload configuration
├── internal/
│   ├── clientapp/        # CSV parsing, queues, REPL helpers
│   ├── config/           # YAML loader
│   ├── database/         # BoltDB wrapper
│   ├── models/           # Node/client definitions + dialers
│   ├── paxos/            # Proposer/Acceptor/Elector/etc.
│   └── utils/            # Generic helpers (connections, parsing)
├── logs/                 # stdout/stderr per node
├── pb/                   # Generated protobuf bindings
├── proto/                # `paxos.proto`
├── scripts/              # Tooling and launch helpers
├── testdata/             # Sample workloads
├── data/                 # Node-local BoltDB files (gitignored)
└── README.md
```

## Go & system dependencies
- `google.golang.org/grpc` – RPC transport.
- `google.golang.org/protobuf` – message types.
- `go.etcd.io/bbolt` – embedded storage for balances.
- `github.com/sirupsen/logrus` – structured logging with leveled output.
- `gopkg.in/yaml.v3` – configuration file parsing.
- `yq` – required by the launch script to iterate over YAML nodes.

Run `go mod download` after cloning to pull these modules.

## Development notes
- After editing proto files run `scripts/generate_stubs.sh` (requires `protoc`, `protoc-gen-go`, `protoc-gen-go-grpc`).
- `TODO.md` and `REFACTOR.md` capture outstanding cleanup ideas; keep them up to date when touching core protocol code.
- When changing the node list, wipe `data/` and `logs/` (the launch script already does this).

## AI usage
Cursor + open-source LLMs assisted with incremental refactors (Paxos logging utilities, client reset plumbing) and with this documentation refresh. All protocol logic remains reviewed and tested by the maintainers.