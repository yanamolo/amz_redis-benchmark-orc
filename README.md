# Valkey Server Benchmark

Benchmarking framework for comparing valkey-server binary performance using `amz_valkey-benchmark` as the load generator. Designed for regression analysis between different server builds (e.g., Valkey 8.1 OSS vs 9.0 ElastiCache).

## Architecture

Three-layer architecture:

```
┌───────────────────────────────────────────────────────┐
│  Layer 3: generate_server_graphs.py                   │
│  Interactive Plotly.js HTML graphs                    │
│  (RPS, latency, CPU, delta, scatter)                  │
└───────────────────────┬───────────────────────────────┘
                        │ reads aggregate.json + _manifest.json
┌───────────────────────┴───────────────────────────────┐
│  Layer 2: run_server_matrix.py                        │
│  Sweeps scale multiplier over base connections        │
│  (set_clients × scale, get_clients × scale)           │
└───────────────────────┬───────────────────────────────┘
                        │ invokes per data point
┌───────────────────────┴───────────────────────────────┐
│  Layer 1: orchestrator.py                             │
│  Single experiment: server lifecycle, FIFO barrier,   │
│  CSV collection, mpstat, outlier filtering            │
└───────────────────────────────────────────────────────┘
```

## Quick Start

### Single Experiment

```bash
# Edit the config with your server binary paths
cp configs/examples/matrix-scalability.json my-experiment.json
vim my-experiment.json

# Dry run — see what would happen
python orchestrator.py --config my-experiment.json --dry-run

# Run the experiment
python orchestrator.py --config my-experiment.json

# Generate graphs from a single experiment
python generate_server_graphs.py results/server-benchmark/2026-03-23T20:00:00/
```

### Matrix Sweep (Scalability)

```bash
# Edit the matrix config
cp configs/examples/matrix-scalability.json my-matrix.json
vim my-matrix.json

# Dry run
python run_server_matrix.py --matrix my-matrix.json --dry-run

# Run the sweep
python run_server_matrix.py --matrix my-matrix.json

# Generate graphs
python generate_server_graphs.py results/server-matrix/2026-03-23T20:00:00/
```

## Configuration

### Experiment Config (Layer 1)

Used directly by `orchestrator.py`. Defines a complete benchmark experiment.

```json
{
    "description": "Valkey 8.1 vs 9.0 regression test",
    "iterations": 5,

    "servers": [
        {
            "name": "valkey-8.1-oss",
            "binary": "/opt/valkey/8.1/bin/valkey-server",
            "args": ["--save", "", "--appendonly", "no", "--protected-mode", "no"]
        },
        {
            "name": "valkey-9.0-elasticache",
            "binary": "/opt/valkey/9.0-ec/bin/valkey-server",
            "args": ["--save", "", "--appendonly", "no", "--protected-mode", "no"]
        }
    ],

    "servers_directory": "/tmp/valkey-bench-servers",
    "amz_valkey_benchmark_binary": "/usr/local/bin/amz_valkey-benchmark",
    "output_directory": "./results/server-benchmark",

    "experiment_duration_seconds": 120,
    "warmup_skip_seconds": 5,
    "port": 6379,

    "keyspace": {
        "key_count": 100000,
        "data_size_bytes": 512,
        "seed": 100000
    },

    "clients": {
        "max_connections_per_benchmark_process": 50,
        "set_clients": 4,
        "get_clients": 12
    },

    "numa": {
        "enabled": true,
        "server_node": 0,
        "benchmark_node": 1
    }
}
```

**Key fields:**

| Field | Description |
|-------|-------------|
| `servers[].name` | Unique label for the server binary (used in output and graphs) |
| `servers[].binary` | Absolute path to the valkey-server binary |
| `servers[].args` | Additional CLI args (port is added automatically) |
| `servers_directory` | Temp directory for isolated server copies |
| `amz_valkey_benchmark_binary` | Absolute path to the amz_valkey-benchmark binary |
| `experiment_duration_seconds` | How long to run the benchmark load |
| `warmup_skip_seconds` | Seconds of initial data to discard (cold-start filtering) |
| `clients.max_connections_per_benchmark_process` | Max `-c` per amz_valkey-benchmark process. Orchestrator splits connections across processes automatically. |
| `clients.set_clients` | Total TCP connections for SET workload |
| `clients.get_clients` | Total TCP connections for GET workload |

**Total connections** = `set_clients + get_clients`

**Process computation**: `ceil(set_clients / max_connections_per_benchmark_process)` SET processes, each with `ceil(set_clients / num_procs)` connections. Same for GET.

Example: 160 SET + 640 GET = 800 total connections. With max 50 conns/proc → 4 SET procs (×40c) + 13 GET procs (×50c).

### Matrix Config (Layer 2)

Used by `run_server_matrix.py`. Sweeps a `scale` multiplier across values.

```json
{
    "description": "Scalability sweep (base: 10 SET + 40 GET, scaled ×1..×16)",

    "experiment_template": {
        "clients": {
            "set_clients": 10,
            "get_clients": 40,
            "max_connections_per_benchmark_process": 50
        },
        "...other experiment fields..."
    },

    "dimensions": {
        "scale": [1, 2, 4, 8, 16]
    }
}
```

The matrix runner multiplies `set_clients` and `get_clients` by each scale value:
- ×1: 10 SET + 40 GET = 50 connections
- ×2: 20 SET + 80 GET = 100 connections
- ×16: 160 SET + 640 GET = 800 connections

## How It Works

### Orchestrator Execution Flow

For each **server binary** × **iteration**:

1. **Prepare isolation** — Copy server binary to a unique temp directory
2. **Start server** — Launch with NUMA pinning (if enabled), wait for PING
3. **Pre-populate** — Run `amz_valkey-benchmark -t set -n <key_count>` to fill the keyspace
4. **Create benchmark processes** — All held at a FIFO barrier (named pipe)
5. **Start mpstat** — Per-second CPU monitoring
6. **Release barrier** — All processes start simultaneously (zero skew)
7. **Wait** — Monitor for server crashes and benchmark failures
8. **Benchmark processes self-exit** — `--test-duration` ensures clean exit after the configured duration (timer starts after all connections are established)
9. **Stop mpstat** — Collect CPU data
10. **Validate** — Check exit codes, CSV data, stderr logs
11. **Compute summary** — Parse interval-metrics CSVs, aggregate RPS and latency per command type
12. **Cleanup** — Stop server, remove temp directory

After all iterations for a server, compute **aggregate** with outlier filtering.

### Server Lifecycle & Error Detection

**Server startup**: After launching valkey-server, the orchestrator polls with `redis-cli PING` for up to 30 seconds. If the server does not respond with `PONG` within this window, the run **fails immediately** with a `RuntimeError` — the error is logged, saved to `error.txt` in the iteration directory, and the exception propagates up to abort the experiment.

**Server shutdown between iterations**: At the end of each iteration (in the `finally` block, so it runs even on failure):
1. All benchmark processes are force-killed (`SIGKILL` via process group) if still running
2. mpstat is stopped
3. The server is stopped via `SIGTERM` with a 10-second grace period
4. If the server doesn't exit within 10 seconds, `SIGKILL` is sent
5. The barrier FIFO and isolated server home directory are cleaned up

This ensures no zombie processes or port conflicts between iterations.

### FIFO Barrier (Zero-Skew Launch)

All amz_valkey-benchmark processes are created first, each blocking on `read < /tmp/barrier_fifo`. Once all processes are created, the orchestrator writes to the FIFO, unblocking them simultaneously. This prevents load skew where early processes generate traffic while later ones are still starting.

### NUMA Isolation

On multi-NUMA machines (e.g., metal instances), the server and benchmarks are pinned to separate NUMA nodes:

```
NUMA Node 0: valkey-server
NUMA Node 1: amz_valkey-benchmark × N
```

This provides:
- No cross-NUMA memory access noise
- Consistent cache behavior
- Low coefficient of variance between iterations

### Outlier Filtering

4-method consensus detection (same as resp-bench):

1. **Modified Z-Score** (MAD-based, threshold=3.5)
2. **IQR** (box-plot, multiplier=1.5)
3. **% Deviation from Median** (threshold=15%)
4. **Grubbs' Test** (alpha=0.05)

A run is discarded when flagged by **≥2 methods**.

### Warmup Skip

The first `warmup_skip_seconds` of benchmark data is discarded to filter cold-start effects (JIT, connection establishment, cache warming).

## Output Structure

### Single Experiment

```
results/server-benchmark/2026-03-23T20:00:00/
├── experiment-config.json          # Copy of input config
├── orchestrator.log                # Script log
├── orchestrator_sha256.txt         # Script hash for reproducibility
├── binary_checksums.json           # SHA256 of all binaries
├── numa_topology.txt               # numactl --hardware output
├── comparison.json                 # Side-by-side summary
├── valkey-8.1-oss/
│   ├── aggregate.json              # Cross-iteration averages
│   ├── iteration-1/
│   │   ├── server.log              # valkey-server output
│   │   ├── benchmark-set-00.csv    # Interval-metrics CSV (1s intervals)
│   │   ├── benchmark-set-00.log    # stderr
│   │   ├── benchmark-get-00.csv
│   │   ├── ...
│   │   ├── mpstat.log              # Per-second CPU data
│   │   └── summary.json            # Iteration summary
│   └── iteration-2/
│       └── ...
└── valkey-9.0-elasticache/
    └── ...
```

### Matrix Sweep

```
results/server-matrix/2026-03-23T20:00:00/
├── _manifest.json                  # Matrix metadata
├── matrix-config.json              # Copy of input config
├── 4-clients/
│   ├── valkey-8.1-oss/
│   │   ├── aggregate.json
│   │   └── iteration-N/
│   └── valkey-9.0-elasticache/
│       └── ...
├── 8-clients/
│   └── ...
├── 16-clients/
│   └── ...
└── server_benchmark.html           # Generated graph (after running graph generator)
```

## Generated Graphs

The graph generator produces a self-contained HTML file with interactive Plotly.js charts:

| Chart | Description |
|-------|-------------|
| **RPS Scalability** | Total RPS (SET+GET) vs total_clients per server |
| **SET RPS** | SET throughput vs total_clients |
| **GET RPS** | GET throughput vs total_clients |
| **RPS Delta** | % difference vs reference server (positive = faster) |
| **SET Latency p50/p95/p99** | SET latency percentiles per server |
| **GET Latency p50/p95/p99** | GET latency percentiles per server |
| **CPU Usage** | System CPU% during experiment |
| **CPU Efficiency** | RPS per CPU% (throughput efficiency) |
| **Per-Iteration Scatter** | Individual iteration RPS values with outliers marked (✕) |

## Prerequisites

- **Python 3.8+** (no external dependencies)
- **redis-cli** — must be available on `PATH` (used for server readiness checks and FLUSHALL)
- **amz_valkey-benchmark** — the benchmark load-generator binary; must be provided by the user and referenced via `amz_valkey_benchmark_binary` in the config
- **Redis / Valkey server binaries** — the server binaries under test; must be provided by the user and referenced via `servers[].binary` in the config
- **mpstat** (from `sysstat` package) — for CPU monitoring
- **numactl** (optional) — for NUMA pinning

```bash
# Install prerequisites (Ubuntu/Debian)
sudo apt install -y sysstat numactl
```

## CLI Reference

### orchestrator.py

```
python orchestrator.py --config <config.json> [--dry-run] [--verbose]

Options:
  --config, -c    Path to experiment config JSON (required)
  --dry-run       Show plan without executing
  --verbose, -v   Enable DEBUG logging
```

### run_server_matrix.py

```
python run_server_matrix.py --matrix <matrix.json> [--output-dir <dir>] [--iterations N] [--dry-run]

Options:
  --matrix, -m      Path to matrix config JSON (required)
  --output-dir, -o  Override output directory
  --iterations      Override iteration count
  --dry-run         Show plan without executing
```

### generate_server_graphs.py

```
python generate_server_graphs.py <results_dir> [--output <path>] [--title <str>] [--reference <server>] [--warmup-skip N]

Options:
  results_dir       Directory with benchmark results (required)
  --output, -o      Output HTML path (default: <results_dir>/server_benchmark.html)
  --title, -t       Title prefix for the report
  --reference, -r   Reference server for delta charts (default: first server)
  --warmup-skip     Seconds already skipped (display only, default: 5)
```

## Design Decisions

### Why amz_valkey-benchmark?

- Custom fork with `--test-duration` for clean timed exits (no SIGINT needed)
- `--interval-metrics` for 1-second CSV output independent of throughput — solves the zero-RPS reporting issue at high client counts
- `--interval-metrics-path` writes CSV directly to file, keeping stdout clean
- Timer starts after all connections are established, ensuring accurate duration measurement

### Why separate SET and GET processes?

`amz_valkey-benchmark` doesn't support mixed SET/GET ratios natively. Running separate processes for each command type gives us:
- Exact control over the SET/GET ratio
- Per-command latency reporting (not mixed)
- Clean scaling of each workload type independently

### Why FIFO barrier instead of SIGSTOP/SIGCONT?

- Cleaner: processes block in userspace on a `read` syscall
- No signal-related race conditions
- Works reliably across shells and environments
- Each process starts from a clean state (no suspended/resumed quirks)

### Why copy the binary?

Copying the server binary to an isolated directory per run ensures:
- No accidental file locking between runs
- Clean `--dir` working directory for each server instance
- Binary integrity can be verified via SHA256 checksums
