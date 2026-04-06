#!/usr/bin/env python3
"""
orchestrator.py — Single-experiment orchestrator for valkey-server benchmarking.

Runs a complete benchmark experiment: for each server binary × iteration,
starts valkey-server, spawns amz_valkey-benchmark processes (barrier-synchronized),
collects interval-metrics CSV data, mpstat CPU data, and produces per-iteration
and aggregate summaries with outlier filtering.

Key design:
  - FIFO-barrier launch: all amz_valkey-benchmark processes are created first,
    each blocking on a named pipe. Once all are ready, writing to the pipe
    unblocks them simultaneously — no skew.
  - --test-duration: benchmark processes exit cleanly after the configured
    duration (timer starts AFTER all connections are established). No SIGTERM
    needed.
  - --interval-metrics: CSV output at 1-second intervals, independent of
    throughput. Solves the zero-RPS issue at high client counts.
  - NUMA pinning: server and benchmarks can be pinned to separate NUMA nodes.
  - Reliable error detection: non-zero exit codes, server crashes, zero-RPS,
    connection errors in stderr.
  - Outlier filtering: 4-method consensus (Modified Z-Score, IQR,
    % Deviation, Grubbs' Test) — same as resp-bench.

Usage:
    python orchestrator.py --config configs/examples/experiment-2servers.json

    python orchestrator.py --config experiment.json --dry-run
"""

import argparse
import csv
import hashlib
import io
import json
import logging
import math
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from collections import defaultdict
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, median, stdev
from typing import List, Optional, Dict, Set, Tuple


# ═══════════════════════════════════════════════════════════════════════════════
# Logging
# ═══════════════════════════════════════════════════════════════════════════════

LOG_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

logger = logging.getLogger("orchestrator")


def setup_logging(log_file: Optional[Path] = None, verbose: bool = False):
    """Configure logging to both console and optional file."""
    level = logging.DEBUG if verbose else logging.INFO
    logger.setLevel(level)

    console = logging.StreamHandler(sys.stdout)
    console.setLevel(level)
    console.setFormatter(logging.Formatter(LOG_FORMAT, LOG_DATE_FORMAT))
    logger.addHandler(console)

    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        fh = logging.FileHandler(str(log_file))
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(logging.Formatter(LOG_FORMAT, LOG_DATE_FORMAT))
        logger.addHandler(fh)


# ═══════════════════════════════════════════════════════════════════════════════
# Data Classes
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class BenchmarkSample:
    """A single metric row from amz_valkey-benchmark interval-metrics CSV."""
    timestamp: int          # unix epoch seconds
    rps: float              # request_sec for this interval
    avg_latency_usec: float
    p50_latency_usec: float
    p90_latency_usec: float
    p95_latency_usec: float
    p99_latency_usec: float
    p99_9_latency_usec: float
    p99_99_latency_usec: float
    p99_999_latency_usec: float
    max_latency_usec: float
    request_finished: int
    requests_total_failed: int
    requests_moved: int
    requests_clusterdown: int
    client_disconnects: int


@dataclass
class IterationSummary:
    """Summary of one iteration for one server binary."""
    iteration: int
    server_name: str
    total_rps: float
    set_rps: float
    get_rps: float
    set_latency: Dict[str, float] = field(default_factory=dict)  # p50, p95, p99, avg (usec)
    get_latency: Dict[str, float] = field(default_factory=dict)
    avg_cpu_percent: float = 0.0
    duration_seconds: float = 0.0
    benchmark_process_count: int = 0
    total_connections: int = 0
    errors: List[str] = field(default_factory=list)


@dataclass
class AggregateSummary:
    """Cross-iteration aggregate for one server binary."""
    server_name: str
    iterations_total: int
    iterations_kept: int
    outliers_removed: int
    total_rps_mean: float
    total_rps_stdev: float
    total_rps_cov: float
    set_rps_mean: float
    get_rps_mean: float
    set_latency: Dict[str, float] = field(default_factory=dict)
    get_latency: Dict[str, float] = field(default_factory=dict)
    avg_cpu_percent: float = 0.0
    per_iteration_rps: List[float] = field(default_factory=list)
    outlier_indices: List[int] = field(default_factory=list)


# ═══════════════════════════════════════════════════════════════════════════════
# Config Loading & Validation
# ═══════════════════════════════════════════════════════════════════════════════

def load_config(config_path: str) -> dict:
    """Load and validate experiment configuration JSON."""
    with open(config_path) as f:
        config = json.load(f)

    # Required fields (servers_directory is conditionally required below)
    required = [
        "servers", "amz_valkey_benchmark_binary",
        "output_directory", "experiment_duration_seconds", "port",
        "keyspace", "clients", "iterations"
    ]
    for field_name in required:
        if field_name not in config:
            raise ValueError(f"Missing required config field: '{field_name}'")

    # Defaults
    config.setdefault("warmup_skip_seconds", 5)
    config.setdefault("description", "")
    config.setdefault("numa", {"enabled": False})

    # Validate servers — each must have exactly one of local-server-info or remote-server-info
    names = set()
    has_local_server = False
    for srv in config["servers"]:
        if "name" not in srv:
            raise ValueError("Each server must have 'name'")
        if srv["name"] in names:
            raise ValueError(f"Duplicate server name: '{srv['name']}'")
        names.add(srv["name"])

        has_local = "local-server-info" in srv
        has_remote = "remote-server-info" in srv

        # Backward compatibility: if 'binary' is present at top level, convert to local-server-info
        if not has_local and not has_remote and "binary" in srv:
            srv["local-server-info"] = {
                "binary": srv.pop("binary"),
                "args": srv.pop("args", []),
            }
            has_local = True

        if has_local and has_remote:
            raise ValueError(
                f"Server '{srv['name']}' has both 'local-server-info' and "
                f"'remote-server-info' — must have exactly one"
            )
        if not has_local and not has_remote:
            raise ValueError(
                f"Server '{srv['name']}' must have either 'local-server-info' "
                f"(with 'binary') or 'remote-server-info' (with 'endpoint')"
            )

        if has_local:
            has_local_server = True
            local = srv["local-server-info"]
            if "binary" not in local:
                raise ValueError(
                    f"Server '{srv['name']}': local-server-info must have 'binary'"
                )
            local.setdefault("args", [])

        if has_remote:
            remote = srv["remote-server-info"]
            if "endpoint" not in remote:
                raise ValueError(
                    f"Server '{srv['name']}': remote-server-info must have 'endpoint'"
                )
            remote.setdefault("port", 6379)

    # servers_directory is required only if there are local servers
    if has_local_server and "servers_directory" not in config:
        raise ValueError(
            "Missing required config field: 'servers_directory' "
            "(required when using local-server-info servers)"
        )

    # Validate clients
    clients = config["clients"]
    for req in ["max_connections_per_benchmark_process", "set_clients", "get_clients"]:
        if req not in clients:
            raise ValueError(f"Missing clients field: '{req}'")
    if clients["set_clients"] + clients["get_clients"] < 1:
        raise ValueError("set_clients + get_clients must be >= 1")
    if clients["max_connections_per_benchmark_process"] < 1:
        raise ValueError("max_connections_per_benchmark_process must be >= 1")

    return config


def is_remote_server(server: dict) -> bool:
    """Check if a server entry is a remote/external server."""
    return "remote-server-info" in server


def get_server_host(server: dict) -> Optional[str]:
    """Get the host/endpoint for a server. Returns None for local servers (localhost)."""
    if is_remote_server(server):
        return server["remote-server-info"]["endpoint"]
    return None


def get_server_port(server: dict, default_port: int) -> int:
    """Get the port for a server."""
    if is_remote_server(server):
        return server["remote-server-info"].get("port", 6379)
    return default_port


def get_server_binary(server: dict) -> Optional[str]:
    """Get the binary path for a local server. Returns None for remote servers."""
    if "local-server-info" in server:
        return server["local-server-info"]["binary"]
    return None


def get_server_args(server: dict) -> List[str]:
    """Get the extra args for a local server. Returns [] for remote servers."""
    if "local-server-info" in server:
        return server["local-server-info"].get("args", [])
    return []


# ═══════════════════════════════════════════════════════════════════════════════
# Hashing
# ═══════════════════════════════════════════════════════════════════════════════

def sha256_file(path: str) -> str:
    """Compute SHA256 hex digest of a file."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


# ═══════════════════════════════════════════════════════════════════════════════
# NUMA
# ═══════════════════════════════════════════════════════════════════════════════

def detect_numa_nodes() -> List[int]:
    """Detect available NUMA nodes from /sys."""
    node_dir = Path("/sys/devices/system/node")
    if not node_dir.exists():
        return [0]
    nodes = []
    for entry in node_dir.iterdir():
        if entry.name.startswith("node") and entry.name[4:].isdigit():
            nodes.append(int(entry.name[4:]))
    return sorted(nodes) if nodes else [0]


def build_numa_prefix(node: int) -> List[str]:
    """Build numactl command prefix for a NUMA node."""
    return ["numactl", f"--cpunodebind={node}", f"--membind={node}"]


def validate_numa(config: dict):
    """Validate NUMA configuration against available topology."""
    numa = config.get("numa", {})
    if not numa.get("enabled", False):
        return

    nodes = detect_numa_nodes()
    server_node = numa.get("server_node", 0)
    bench_node = numa.get("benchmark_node", 1)

    if server_node not in nodes:
        raise ValueError(f"NUMA server_node {server_node} not available. Available: {nodes}")
    if bench_node not in nodes:
        raise ValueError(f"NUMA benchmark_node {bench_node} not available. Available: {nodes}")
    if server_node == bench_node:
        logger.warning("NUMA server_node and benchmark_node are the same (%d) — "
                       "this defeats the purpose of NUMA isolation", server_node)
    if len(nodes) < 2:
        logger.warning("Only 1 NUMA node detected — NUMA pinning will not provide isolation")


def get_numa_topology_text() -> str:
    """Capture numactl --hardware output."""
    try:
        result = subprocess.run(
            ["numactl", "--hardware"],
            capture_output=True, text=True, timeout=10
        )
        return result.stdout
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return "numactl not available"


# ═══════════════════════════════════════════════════════════════════════════════
# Server Management
# ═══════════════════════════════════════════════════════════════════════════════

def prepare_server_directory(server: dict, servers_dir: str, run_id: str) -> Path:
    """Create an isolated directory for a server run and copy the binary there."""
    server_home = Path(servers_dir) / f"{server['name']}_{run_id}"
    server_home.mkdir(parents=True, exist_ok=True)

    binary_path = get_server_binary(server)
    src_binary = Path(binary_path)
    if not src_binary.exists():
        raise FileNotFoundError(f"Server binary not found: {src_binary}")

    dst_binary = server_home / src_binary.name
    shutil.copy2(str(src_binary), str(dst_binary))
    dst_binary.chmod(0o755)

    return server_home


def start_server(server: dict, server_home: Path, port: int, numa_config: dict,
                 log_path: Path) -> subprocess.Popen:
    """Start valkey-server and return the Popen handle."""
    binary_path = get_server_binary(server)
    binary = server_home / Path(binary_path).name
    cmd = []

    if numa_config.get("enabled", False):
        cmd.extend(build_numa_prefix(numa_config.get("server_node", 0)))

    cmd.append(str(binary))
    cmd.extend(["--port", str(port)])
    cmd.extend(["--dir", str(server_home)])
    cmd.extend(get_server_args(server))

    # Log with proper quoting so empty args are visible in the log
    cmd_display = [f'"{c}"' if c == "" else c for c in cmd]
    logger.info("Starting server: %s", " ".join(cmd_display))

    log_fh = open(str(log_path), "w")
    proc = subprocess.Popen(
        cmd,
        stdout=log_fh,
        stderr=subprocess.STDOUT,
        cwd=str(server_home),
    )
    # Store file handle on proc so we can close it later
    proc._log_fh = log_fh
    return proc


def _redis_cli_cmd(host: Optional[str], port: int, cli_path: str) -> List[str]:
    """Build base redis-cli command with host and port."""
    cmd = [cli_path]
    if host:
        cmd.extend(["-h", host])
    cmd.extend(["-p", str(port)])
    return cmd


def wait_for_server_ready(cli_path: str, port: int, host: Optional[str] = None,
                          timeout: float = 30.0, interval: float = 0.5) -> bool:
    """Wait for the server to respond to PING."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            cmd = _redis_cli_cmd(host, port, cli_path) + ["PING"]
            result = subprocess.run(
                cmd, capture_output=True, text=True, timeout=5
            )
            if result.returncode == 0 and "PONG" in result.stdout:
                return True
        except (subprocess.TimeoutExpired, FileNotFoundError):
            pass
        time.sleep(interval)
    return False


def validate_standalone_server(host: str, port: int, cli_path: str):
    """Validate that a remote server is reachable and running in standalone mode.

    Runs 'redis-cli -h <host> -p <port> INFO cluster' and checks:
    - Server is reachable (command succeeds)
    - cluster_enabled:0 (standalone mode, not cluster)

    Raises RuntimeError if validation fails.
    """
    cmd = _redis_cli_cmd(host, port, cli_path) + ["INFO", "cluster"]
    display_addr = f"{host}:{port}"
    logger.info("Validating remote server %s (standalone mode check)...", display_addr)

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
    except subprocess.TimeoutExpired:
        raise RuntimeError(
            f"Remote server {display_addr} did not respond within 10s. "
            f"Check that the server is running and reachable."
        )

    if result.returncode != 0:
        raise RuntimeError(
            f"Failed to connect to remote server {display_addr}: "
            f"{result.stderr.strip() or result.stdout.strip()}"
        )

    # Parse cluster_enabled from INFO cluster output - disabled for serverless, TODO: make configurable
    # output = result.stdout
    # for line in output.splitlines():
    #     line = line.strip()
    #     if line.startswith("cluster_enabled:"):
    #         value = line.split(":")[1].strip()
    #         if value == "0":
    #             logger.info("Remote server %s validated: standalone mode, reachable",
    #                         display_addr)
    #             return
    #         else:
    #             raise RuntimeError(
    #                 f"Remote server {display_addr} is running in cluster mode "
    #                 f"(cluster_enabled:{value}). Only standalone mode is supported."
    #             )

    # raise RuntimeError(
    #     f"Could not determine cluster mode for remote server {display_addr}. "
    #     f"'cluster_enabled' not found in INFO cluster output."
    # )


def stop_server(proc: subprocess.Popen, timeout: float = 10.0):
    """Gracefully stop the server process."""
    if proc.poll() is not None:
        return
    try:
        proc.terminate()
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        logger.warning("Server did not stop gracefully, killing...")
        proc.kill()
        proc.wait(timeout=5)
    finally:
        if hasattr(proc, "_log_fh"):
            proc._log_fh.close()


def flush_server(cli_path, port: int, host: Optional[str] = None):
    """Run FLUSHALL on the server."""
    cmd = _redis_cli_cmd(host, port, cli_path) + ["FLUSHALL"]
    logger.info("Running FLUSHALL on %s:%d", host or "localhost", port)
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
    if result.returncode != 0:
        logger.warning("FLUSHALL failed: %s", result.stderr.strip() or result.stdout.strip())
    else:
        logger.info("FLUSHALL complete")

def find_cli_path():
    which_redis_cli = shutil.which("redis-cli")
    which_valkey_cli = shutil.which("valkey-cli")
    if which_redis_cli is None and which_valkey_cli is None:
        raise RuntimeError("redis-cli/valkey-cli not found on PATH — required for server validation")
    return which_redis_cli or which_valkey_cli


# ═══════════════════════════════════════════════════════════════════════════════
# Pre-population
# ═══════════════════════════════════════════════════════════════════════════════

def prepopulate_keys(benchmark_binary: str, port: int, keyspace: dict,
                     numa_config: dict, host: Optional[str] = None):
    """Pre-populate the keyspace with SET commands so GETs don't all miss."""
    key_count = keyspace["key_count"]
    data_size = keyspace["data_size_bytes"]
    seed = keyspace.get("seed", key_count)

    cmd = []
    if numa_config.get("enabled", False):
        cmd.extend(build_numa_prefix(numa_config.get("benchmark_node", 1)))

    cmd.append(benchmark_binary)
    if host:
        cmd.extend(["-h", host])
    cmd.extend([
        "-p", str(port),
        "-t", "set",
        "-n", str(key_count),
        "-r", str(seed),
        "-d", str(data_size),
        "-c", "50",
    ])

    logger.info("Pre-populating %d keys (%d bytes) on %s:%d...",
                key_count, data_size, host or "localhost", port)
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
    if result.returncode != 0:
        raise RuntimeError(f"Pre-population failed: {result.stderr}")
    logger.info("Pre-population complete")


# ═══════════════════════════════════════════════════════════════════════════════
# amz_valkey-benchmark Process Management (FIFO Barrier)
# ═══════════════════════════════════════════════════════════════════════════════

def create_benchmark_process(benchmark_binary: str, port: int, command: str,
                             connections: int, keyspace: dict,
                             duration_seconds: int,
                             numa_config: dict, barrier_fifo: str,
                             csv_path: Path, log_path: Path,
                             host: Optional[str] = None) -> subprocess.Popen:
    """Create a single amz_valkey-benchmark process held at a FIFO barrier.

    The process blocks on `read < barrier_fifo` until the orchestrator
    writes to the FIFO, then exec's amz_valkey-benchmark with --test-duration
    (clean self-exit) and --interval-metrics (1-second CSV output).

    Returns the Popen handle.
    """
    seed = keyspace.get("seed", keyspace["key_count"])
    data_size = keyspace["data_size_bytes"]

    stdout_log_path = csv_path.with_suffix(".stdout.log")

    bench_args = [benchmark_binary]
    if host:
        bench_args.extend(["-h", host])
    bench_args.extend([
        "-p", str(port),
        "-t", command,
        "-c", str(connections),
        "-r", str(seed),
        "-d", str(data_size),
        "--test-duration", str(duration_seconds),
        "--interval-metrics-interval-duration-sec", "1",
        "--interval-metrics-path", str(csv_path),
    ])
    bench_cmd_str = " ".join(bench_args)

    # The full shell command:
    # 1. Wait on barrier FIFO
    # 2. Run amz_valkey-benchmark (writes CSV via --interval-metrics-path,
    #    stdout gets human-readable summary, stderr to log)
    numa_prefix = ""
    if numa_config.get("enabled", False):
        node = numa_config.get("benchmark_node", 1)
        numa_prefix = f"numactl --cpunodebind={node} --membind={node} "

    shell_cmd = (
        f'read < {barrier_fifo} && '
        f'{numa_prefix}{bench_cmd_str} '
        f'>{stdout_log_path} 2>{log_path}'
    )

    proc = subprocess.Popen(
        ["bash", "-c", shell_cmd],
        stdin=subprocess.DEVNULL,
        start_new_session=True,  # new process group for clean kill
    )
    logger.info("  amz_valkey-benchmark PID %d → %s%s (csv: %s)",
                proc.pid, numa_prefix, bench_cmd_str, csv_path.name)
    return proc


def release_barrier(barrier_fifo: str, process_count: int):
    """Release all processes waiting on the FIFO barrier.

    Opening the FIFO for writing unblocks all readers.
    We write one line per process to ensure each `read` gets data.
    """
    logger.info("Releasing barrier for %d amz_valkey-benchmark processes...", process_count)
    with open(barrier_fifo, "w") as f:
        for _ in range(process_count):
            f.write("GO\n")


def wait_for_benchmark_processes(processes: List[subprocess.Popen],
                                 timeout: float = 120.0):
    """Wait for all benchmark processes to exit naturally.

    amz_valkey-benchmark with --test-duration exits cleanly on its own.
    We wait up to timeout seconds for all processes to finish. If any
    process hangs, we force-kill it.
    """
    logger.info("Waiting for %d amz_valkey-benchmark processes to finish...",
                len(processes))
    deadline = time.time() + timeout

    for proc in processes:
        remaining = max(0.1, deadline - time.time())
        try:
            proc.wait(timeout=remaining)
        except subprocess.TimeoutExpired:
            logger.warning("amz_valkey-benchmark PID %d did not exit within timeout, "
                           "sending SIGKILL...", proc.pid)
            try:
                pgid = os.getpgid(proc.pid)
                os.killpg(pgid, signal.SIGKILL)
            except (ProcessLookupError, OSError):
                pass
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                pass

    # Log exit codes
    for proc in processes:
        rc = proc.returncode
        if rc is not None and rc != 0:
            logger.warning("amz_valkey-benchmark PID %d exited with code %d",
                           proc.pid, rc)


# ═══════════════════════════════════════════════════════════════════════════════
# mpstat Integration
# ═══════════════════════════════════════════════════════════════════════════════

def start_mpstat(output_path: Path) -> subprocess.Popen:
    """Start mpstat logging at 1-second intervals."""
    fh = open(str(output_path), "w")
    proc = subprocess.Popen(
        ["mpstat", "-P", "ALL", "1"],
        stdout=fh,
        stderr=subprocess.STDOUT,
    )
    proc._log_fh = fh
    return proc


def stop_mpstat(proc: subprocess.Popen):
    """Stop mpstat process."""
    if proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=3)
    if hasattr(proc, "_log_fh"):
        proc._log_fh.close()


def parse_mpstat_avg_cpu(mpstat_path: Path) -> float:
    """Parse mpstat output and return average all-CPU idle complement (= usage %).

    Handles both 12h (AM/PM) and 24h mpstat time formats by searching for 'all'
    anywhere in the first few columns. The last column is always %idle.
    Raises RuntimeError if no CPU data could be parsed.
    """
    idle_values = []
    try:
        with open(mpstat_path) as f:
            for line in f:
                parts = line.split()
                # "all" can be at parts[1] (24h: "HH:MM:SS all ...") or
                # parts[2] (12h: "HH:MM:SS AM all ..." or "HH:MM:SS PM all ...")
                if len(parts) >= 12 and "all" in parts[:4]:
                    try:
                        idle = float(parts[-1])
                        idle_values.append(idle)
                    except ValueError:
                        continue
    except FileNotFoundError:
        raise RuntimeError(f"mpstat output file not found: {mpstat_path}")

    if not idle_values:
        raise RuntimeError(
            f"No CPU data parsed from mpstat output: {mpstat_path}. "
            f"File may be empty or in an unexpected format."
        )

    avg_idle = mean(idle_values)
    return round(100.0 - avg_idle, 2)


# ═══════════════════════════════════════════════════════════════════════════════
# CSV Parsing (interval-metrics format)
# ═══════════════════════════════════════════════════════════════════════════════

def parse_benchmark_csv(csv_path: Path, warmup_skip_seconds: float = 0) -> List[BenchmarkSample]:
    """Parse an amz_valkey-benchmark interval-metrics CSV file.

    Format (from --interval-metrics-path):
        timestamp,request_sec,p50_usec,p90_usec,p95_usec,p99_usec,
        p99_9_usec,p99_99_usec,p99_999_usec,p100_usec,avg_usec,
        request_finished,requests_total_failed,requests_moved,
        requests_clusterdown,client_disconnects

    Returns list of BenchmarkSample, skipping the first warmup_skip_seconds.
    """
    samples = []
    first_ts = None

    try:
        with open(csv_path, newline="") as f:
            reader = csv.reader(f)
            header = next(reader, None)
            if header is None:
                return samples

            for row in reader:
                if len(row) < 16:
                    continue
                try:
                    ts = int(row[0])
                    rps = float(row[1])
                    p50 = float(row[2])
                    p90 = float(row[3])
                    p95 = float(row[4])
                    p99 = float(row[5])
                    p99_9 = float(row[6])
                    p99_99 = float(row[7])
                    p99_999 = float(row[8])
                    p100 = float(row[9])
                    avg = float(row[10])
                    req_finished = int(row[11])
                    req_failed = int(row[12])
                    req_moved = int(row[13])
                    req_clusterdown = int(row[14])
                    disconnects = int(row[15])
                except (ValueError, IndexError):
                    continue

                # Apply warmup skip
                if warmup_skip_seconds > 0:
                    if first_ts is None:
                        first_ts = ts
                    elapsed = ts - first_ts
                    if elapsed < warmup_skip_seconds:
                        continue

                samples.append(BenchmarkSample(
                    timestamp=ts,
                    rps=rps,
                    avg_latency_usec=avg,
                    p50_latency_usec=p50,
                    p90_latency_usec=p90,
                    p95_latency_usec=p95,
                    p99_latency_usec=p99,
                    p99_9_latency_usec=p99_9,
                    p99_99_latency_usec=p99_99,
                    p99_999_latency_usec=p99_999,
                    max_latency_usec=p100,
                    request_finished=req_finished,
                    requests_total_failed=req_failed,
                    requests_moved=req_moved,
                    requests_clusterdown=req_clusterdown,
                    client_disconnects=disconnects,
                ))
    except FileNotFoundError:
        logger.warning("CSV file not found: %s", csv_path)

    return samples


def compute_iteration_summary(csv_paths: List[Tuple[str, Path]],
                              warmup_skip_seconds: float,
                              iteration: int,
                              server_name: str,
                              mpstat_path: Optional[Path],
                              config: dict) -> IterationSummary:
    """Compute iteration summary from all benchmark CSV files.

    csv_paths: list of (command_type, csv_path) tuples
    """
    set_rps_values = []
    get_rps_values = []
    set_latencies = defaultdict(list)  # metric -> [values]
    get_latencies = defaultdict(list)

    for cmd_type, csv_path in csv_paths:
        samples = parse_benchmark_csv(csv_path, warmup_skip_seconds)
        if not samples:
            logger.warning("No samples after warmup skip in %s", csv_path)
            continue

        for s in samples:
            if cmd_type == "set":
                set_rps_values.append(s.rps)
                set_latencies["avg"].append(s.avg_latency_usec)
                set_latencies["p50"].append(s.p50_latency_usec)
                set_latencies["p90"].append(s.p90_latency_usec)
                set_latencies["p95"].append(s.p95_latency_usec)
                set_latencies["p99"].append(s.p99_latency_usec)
            elif cmd_type == "get":
                get_rps_values.append(s.rps)
                get_latencies["avg"].append(s.avg_latency_usec)
                get_latencies["p50"].append(s.p50_latency_usec)
                get_latencies["p90"].append(s.p90_latency_usec)
                get_latencies["p95"].append(s.p95_latency_usec)
                get_latencies["p99"].append(s.p99_latency_usec)

    clients = config["clients"]
    max_c = clients["max_connections_per_benchmark_process"]
    set_conns = clients["set_clients"]
    get_conns = clients["get_clients"]
    # Process counts (computed from connection counts)
    set_procs = max(1, math.ceil(set_conns / max_c)) if set_conns > 0 else 0
    get_procs = max(1, math.ceil(get_conns / max_c)) if get_conns > 0 else 0

    # Sum RPS across all processes for each sample window.
    # Each process reports its own RPS independently; we take the mean per-process
    # and multiply by process count to estimate total.
    set_rps = mean(set_rps_values) * set_procs if set_rps_values else 0
    get_rps = mean(get_rps_values) * get_procs if get_rps_values else 0
    total_rps = set_rps + get_rps

    set_lat = {k: mean(v) for k, v in set_latencies.items()} if set_latencies else {}
    get_lat = {k: mean(v) for k, v in get_latencies.items()} if get_latencies else {}

    avg_cpu = 0.0
    if mpstat_path and mpstat_path.exists():
        avg_cpu = parse_mpstat_avg_cpu(mpstat_path)

    return IterationSummary(
        iteration=iteration,
        server_name=server_name,
        total_rps=total_rps,
        set_rps=set_rps,
        get_rps=get_rps,
        set_latency=set_lat,
        get_latency=get_lat,
        avg_cpu_percent=avg_cpu,
        duration_seconds=config["experiment_duration_seconds"],
        benchmark_process_count=set_procs + get_procs,
        total_connections=set_conns + get_conns,
    )


# ═══════════════════════════════════════════════════════════════════════════════
# Outlier Detection (4-method consensus, same as resp-bench)
# ═══════════════════════════════════════════════════════════════════════════════

MODIFIED_Z_THRESHOLD = 3.5
IQR_MULTIPLIER = 1.5
PCT_DEVIATION_THRESHOLD = 15.0
GRUBBS_ALPHA = 0.05
CONSENSUS_MIN_METHODS = 2


def _norm_ppf(p):
    """Approximate inverse normal CDF (Beasley-Springer-Moro algorithm)."""
    a = [0, -3.969683028665376e+01, 2.209460984245205e+02,
         -2.759285104469687e+02, 1.383577518672690e+02,
         -3.066479806614716e+01, 2.506628277459239e+00]
    b = [0, -5.447609879822406e+01, 1.615858368580409e+02,
         -1.556989798598866e+02, 6.680131188771972e+01,
         -1.328068155288572e+01]
    c = [0, -7.784894002430293e-03, -3.223964580411365e-01,
         -2.400758277161838e+00, -2.549732539343734e+00,
         4.374664141464968e+00, 2.938163982698783e+00]
    d = [0, 7.784695709041462e-03, 3.224671290700398e-01,
         2.445134137142996e+00, 3.754408661907416e+00]
    p_low = 0.02425
    p_high = 1.0 - p_low
    if p < p_low:
        q = math.sqrt(-2.0 * math.log(p))
        return (((((c[1]*q+c[2])*q+c[3])*q+c[4])*q+c[5])*q+c[6]) / \
               ((((d[1]*q+d[2])*q+d[3])*q+d[4])*q+1.0)
    elif p <= p_high:
        q = p - 0.5
        r = q * q
        return (((((a[1]*r+a[2])*r+a[3])*r+a[4])*r+a[5])*r+a[6])*q / \
               (((((b[1]*r+b[2])*r+b[3])*r+b[4])*r+b[5])*r+1.0)
    else:
        q = math.sqrt(-2.0 * math.log(1.0 - p))
        return -(((((c[1]*q+c[2])*q+c[3])*q+c[4])*q+c[5])*q+c[6]) / \
                ((((d[1]*q+d[2])*q+d[3])*q+d[4])*q+1.0)


def _t_critical(df, alpha=0.05):
    p = 1.0 - alpha / 2.0
    z = _norm_ppf(p)
    g1 = (z**3 + z) / 4.0
    g2 = (5*z**5 + 16*z**3 + 3*z) / 96.0
    g3 = (3*z**7 + 19*z**5 + 17*z**3 - 15*z) / 384.0
    return z + g1/df + g2/df**2 + g3/df**3


def _grubbs_critical(n, alpha=0.05):
    if n < 3:
        return float("inf")
    t = _t_critical(n - 2, alpha / (2.0 * n))
    return ((n - 1) / math.sqrt(n)) * math.sqrt(t**2 / (n - 2 + t**2))


def find_consensus_outliers(values: List[float]) -> Set[int]:
    """Return set of indices flagged by ≥2 outlier detection methods."""
    if len(values) < 3:
        return set()

    # Modified Z-Score
    med = median(values)
    mad = median([abs(v - med) for v in values])
    if mad == 0:
        mad = mean([abs(v - med) for v in values])
    flags_mz = set()
    if mad > 0:
        for i, v in enumerate(values):
            if abs(0.6745 * (v - med) / mad) > MODIFIED_Z_THRESHOLD:
                flags_mz.add(i)

    # IQR
    sorted_v = sorted(values)
    n = len(sorted_v)
    q1 = sorted_v[n // 4]
    q3 = sorted_v[(3 * n) // 4]
    iqr = q3 - q1
    flags_iqr = {i for i, v in enumerate(values) if v < q1 - IQR_MULTIPLIER * iqr or v > q3 + IQR_MULTIPLIER * iqr}

    # % Deviation
    flags_pct = set()
    if med > 0:
        flags_pct = {i for i, v in enumerate(values) if abs(v - med) / med * 100 > PCT_DEVIATION_THRESHOLD}

    # Grubbs
    flags_grubbs = set()
    if n >= 3:
        m = mean(values)
        s = stdev(values)
        if s > 0:
            g_vals = [(abs(v - m) / s, i) for i, v in enumerate(values)]
            g_stat, idx = max(g_vals, key=lambda x: x[0])
            if g_stat > _grubbs_critical(n, GRUBBS_ALPHA):
                flags_grubbs.add(idx)

    # Consensus
    all_indices = flags_mz | flags_iqr | flags_pct | flags_grubbs
    return {
        idx for idx in all_indices
        if sum(1 for s in [flags_mz, flags_iqr, flags_pct, flags_grubbs] if idx in s) >= CONSENSUS_MIN_METHODS
    }


def compute_aggregate(summaries: List[IterationSummary]) -> AggregateSummary:
    """Compute aggregate summary with outlier filtering."""
    server_name = summaries[0].server_name
    rps_values = [s.total_rps for s in summaries]

    outlier_indices = find_consensus_outliers(rps_values)
    clean = [(i, s) for i, s in enumerate(summaries) if i not in outlier_indices]

    clean_rps = [s.total_rps for _, s in clean]
    clean_set_rps = [s.set_rps for _, s in clean]
    clean_get_rps = [s.get_rps for _, s in clean]
    clean_cpu = [s.avg_cpu_percent for _, s in clean]

    # Average latencies across clean iterations
    set_lat_agg = defaultdict(list)
    get_lat_agg = defaultdict(list)
    for _, s in clean:
        for k, v in s.set_latency.items():
            set_lat_agg[k].append(v)
        for k, v in s.get_latency.items():
            get_lat_agg[k].append(v)

    rps_mean = mean(clean_rps) if clean_rps else 0
    rps_std = stdev(clean_rps) if len(clean_rps) > 1 else 0
    rps_cov = (rps_std / rps_mean * 100) if rps_mean > 0 else 0

    return AggregateSummary(
        server_name=server_name,
        iterations_total=len(summaries),
        iterations_kept=len(clean),
        outliers_removed=len(outlier_indices),
        total_rps_mean=round(rps_mean, 2),
        total_rps_stdev=round(rps_std, 2),
        total_rps_cov=round(rps_cov, 2),
        set_rps_mean=round(mean(clean_set_rps), 2) if clean_set_rps else 0,
        get_rps_mean=round(mean(clean_get_rps), 2) if clean_get_rps else 0,
        set_latency={k: round(mean(v), 4) for k, v in set_lat_agg.items()},
        get_latency={k: round(mean(v), 4) for k, v in get_lat_agg.items()},
        avg_cpu_percent=round(mean(clean_cpu), 2) if clean_cpu else 0,
        per_iteration_rps=rps_values,
        outlier_indices=sorted(outlier_indices),
    )


# ═══════════════════════════════════════════════════════════════════════════════
# Validation
# ═══════════════════════════════════════════════════════════════════════════════

def validate_benchmark_logs(log_paths: List[Path]) -> List[str]:
    """Check benchmark stderr logs for errors."""
    errors = []
    error_patterns = ["ERR", "Connection refused", "Could not connect", "DENIED"]
    for log_path in log_paths:
        try:
            content = log_path.read_text()
            for pattern in error_patterns:
                if pattern in content:
                    errors.append(f"{log_path.name}: found '{pattern}' in stderr")
        except FileNotFoundError:
            pass
    return errors


def validate_csv_output(csv_paths: List[Path]) -> List[str]:
    """Check CSV files have non-zero data."""
    errors = []
    for csv_path in csv_paths:
        try:
            lines = csv_path.read_text().strip().split("\n")
            if len(lines) < 2:
                errors.append(f"{csv_path.name}: no data rows (only {len(lines)} lines)")
        except FileNotFoundError:
            errors.append(f"{csv_path.name}: file not found")
    return errors


# ═══════════════════════════════════════════════════════════════════════════════
# Main Orchestration
# ═══════════════════════════════════════════════════════════════════════════════

def run_experiment(config: dict, output_root: Path):
    """Run the full experiment: for each server × iteration.

    Supports both local servers (started/stopped by the orchestrator) and
    remote/external servers (pre-existing, validated for standalone mode).
    """
    servers = config["servers"]
    iterations = config["iterations"]
    duration = config["experiment_duration_seconds"]
    warmup_skip = config.get("warmup_skip_seconds", 5)
    default_port = config["port"]
    keyspace = config["keyspace"]
    clients = config["clients"]
    numa = config.get("numa", {"enabled": False})
    benchmark_binary = config["amz_valkey_benchmark_binary"]
    servers_dir = config.get("servers_directory")
    max_c = clients["max_connections_per_benchmark_process"]
    set_conns = clients["set_clients"]  # total SET connections
    get_conns = clients["get_clients"]  # total GET connections
    # Compute process counts and per-process connection counts
    set_procs = max(1, math.ceil(set_conns / max_c)) if set_conns > 0 else 0
    get_procs = max(1, math.ceil(get_conns / max_c)) if get_conns > 0 else 0
    total_procs = set_procs + get_procs
    total_conns = set_conns + get_conns
    # Distribute connections evenly across processes
    set_conns_per_proc = math.ceil(set_conns / set_procs) if set_procs > 0 else 0
    get_conns_per_proc = math.ceil(get_conns / get_procs) if get_procs > 0 else 0

    # Save config and checksums
    config_copy = output_root / "experiment-config.json"
    config_copy.write_text(json.dumps(config, indent=2) + "\n")

    # Binary checksums (only for local servers)
    checksums = {}
    for srv in servers:
        binary = get_server_binary(srv)
        if binary:
            checksums[srv["name"]] = sha256_file(binary)
        else:
            checksums[srv["name"]] = f"remote:{get_server_host(srv)}:{get_server_port(srv, default_port)}"
    checksums["amz_valkey-benchmark"] = sha256_file(benchmark_binary)
    checksums_path = output_root / "binary_checksums.json"
    checksums_path.write_text(json.dumps(checksums, indent=2) + "\n")

    # Orchestrator SHA256
    script_path = Path(__file__).resolve()
    orch_sha = output_root / "orchestrator_sha256.txt"
    orch_sha.write_text(sha256_file(str(script_path)) + "\n")

    # NUMA topology
    if numa.get("enabled", False):
        validate_numa(config)
        numa_txt = output_root / "numa_topology.txt"
        numa_txt.write_text(get_numa_topology_text())

    # Build server description strings for logging
    server_descs = []
    for s in servers:
        if is_remote_server(s):
            host = get_server_host(s)
            p = get_server_port(s, default_port)
            server_descs.append(f"{s['name']} (remote: {host}:{p})")
        else:
            server_descs.append(f"{s['name']} (local: {get_server_binary(s)})")

    logger.info("=" * 70)
    logger.info("Valkey Server Benchmark Experiment")
    logger.info("  Description: %s", config.get("description", ""))
    logger.info("  Servers: %s", ", ".join(server_descs))
    logger.info("  Iterations: %d", iterations)
    logger.info("  Duration: %ds per iteration", duration)
    logger.info("  Connections: %d SET + %d GET = %d total", set_conns, get_conns, total_conns)
    logger.info("  Processes: %d SET (×%d conns) + %d GET (×%d conns) = %d total",
                set_procs, set_conns_per_proc, get_procs, get_conns_per_proc, total_procs)
    logger.info("  Benchmark binary: %s", benchmark_binary)
    logger.info("  Output: %s", output_root)
    logger.info("=" * 70)

    all_aggregates = {}
    cli_path = find_cli_path()

    for server in servers:
        server_name = server["name"]
        remote = is_remote_server(server)
        srv_host = get_server_host(server)       # None for local
        srv_port = get_server_port(server, default_port)

        server_output = output_root / server_name
        server_output.mkdir(parents=True, exist_ok=True)

        # For remote servers: validate standalone mode once before all iterations
        if remote:
            logger.info("")
            logger.info("Validating remote server '%s' at %s:%d...",
                        server_name, srv_host, srv_port)
            validate_standalone_server(srv_host, srv_port, cli_path)

        iteration_summaries = []

        for iteration in range(1, iterations + 1):
            run_id = f"iter{iteration}_{datetime.now().strftime('%H%M%S')}"
            iter_dir = server_output / f"iteration-{iteration}"
            iter_dir.mkdir(parents=True, exist_ok=True)

            logger.info("")
            if remote:
                logger.info("═══ %s (%s:%d) — iteration %d/%d ═══",
                            server_name, srv_host, srv_port, iteration, iterations)
            else:
                logger.info("═══ %s — iteration %d/%d ═══",
                            server_name, iteration, iterations)

            server_home = None
            server_proc = None
            mpstat_proc = None
            bench_procs = []
            barrier_dir = None

            try:
                if remote:
                    # Remote server: no start/stop, just validate it's still up
                    if not wait_for_server_ready(cli_path, srv_port, host=srv_host, timeout=10):
                        raise RuntimeError(
                            f"Remote server {server_name} ({srv_host}:{srv_port}) "
                            f"is not responding at iteration {iteration}"
                        )
                    logger.info("Remote server %s:%d is ready", srv_host, srv_port)
                else:
                    # Local server: prepare directory, start, wait for ready
                    server_home = prepare_server_directory(server, servers_dir, run_id)
                    server_log = iter_dir / "server.log"
                    server_proc = start_server(server, server_home, srv_port, numa, server_log)
                    if not wait_for_server_ready(cli_path, srv_port):
                        raise RuntimeError(
                            f"Server {server_name} failed to start within 30s"
                        )
                    logger.info("Server ready on port %d (PID %d)", srv_port, server_proc.pid)

                # Flush and pre-populate (for both local and remote)
                flush_server(cli_path, srv_port, host=srv_host)
                prepopulate_keys(benchmark_binary, srv_port, keyspace, numa,
                                 host=srv_host)

                # Create FIFO barrier
                barrier_dir = tempfile.mkdtemp(prefix="vbench-barrier-")
                barrier_fifo = os.path.join(barrier_dir, "barrier")
                os.mkfifo(barrier_fifo)

                # Create all benchmark processes (held at barrier)
                csv_paths = []  # (command_type, csv_path)
                log_paths = []

                for proc_idx in range(set_procs):
                    csv_path = iter_dir / f"benchmark-set-{proc_idx:02d}.csv"
                    log_path = iter_dir / f"benchmark-set-{proc_idx:02d}.log"
                    proc = create_benchmark_process(
                        benchmark_binary, srv_port, "set", set_conns_per_proc,
                        keyspace, duration, numa, barrier_fifo, csv_path, log_path,
                        host=srv_host
                    )
                    bench_procs.append(proc)
                    csv_paths.append(("set", csv_path))
                    log_paths.append(log_path)

                for proc_idx in range(get_procs):
                    csv_path = iter_dir / f"benchmark-get-{proc_idx:02d}.csv"
                    log_path = iter_dir / f"benchmark-get-{proc_idx:02d}.log"
                    proc = create_benchmark_process(
                        benchmark_binary, srv_port, "get", get_conns_per_proc,
                        keyspace, duration, numa, barrier_fifo, csv_path, log_path,
                        host=srv_host
                    )
                    bench_procs.append(proc)
                    csv_paths.append(("get", csv_path))
                    log_paths.append(log_path)

                logger.info("Created %d amz_valkey-benchmark processes, all waiting at barrier",
                            total_procs)

                # Small delay to let all processes reach the barrier read
                time.sleep(0.5)

                # Start mpstat
                mpstat_path = iter_dir / "mpstat.log"
                mpstat_proc = start_mpstat(mpstat_path)

                # Release barrier — all benchmarks start simultaneously
                release_barrier(barrier_fifo, total_procs)
                start_time = time.time()
                logger.info("Benchmark load started — running for %ds "
                            "(processes will self-exit via --test-duration)...", duration)

                # Wait for all benchmark processes to exit naturally
                wait_timeout = duration + 120  # extra time for connection setup + cleanup
                wait_for_benchmark_processes(bench_procs, timeout=wait_timeout)

                elapsed = time.time() - start_time
                logger.info("All benchmark processes finished (%.1fs elapsed)", elapsed)

                # Stop mpstat
                stop_mpstat(mpstat_proc)
                mpstat_proc = None

                # Validate
                all_csv_paths = [p for _, p in csv_paths]
                csv_errors = validate_csv_output(all_csv_paths)
                log_errors = validate_benchmark_logs(log_paths)
                all_errors = csv_errors + log_errors

                if all_errors:
                    for err in all_errors:
                        logger.warning("Validation warning: %s", err)

                # Check exit codes
                for bp in bench_procs:
                    if bp.returncode not in (0, None):
                        all_errors.append(
                            f"Process {bp.pid} exited with code {bp.returncode}"
                        )

                # Compute iteration summary
                summary = compute_iteration_summary(
                    csv_paths, warmup_skip, iteration, server_name,
                    mpstat_path, config
                )
                summary.errors = all_errors

                # Save iteration summary
                summary_path = iter_dir / "summary.json"
                summary_path.write_text(json.dumps(asdict(summary), indent=2) + "\n")

                iteration_summaries.append(summary)

                logger.info(
                    "Iteration %d complete: total_rps=%.0f "
                    "(SET=%.0f GET=%.0f) CPU=%.1f%%",
                    iteration, summary.total_rps,
                    summary.set_rps, summary.get_rps,
                    summary.avg_cpu_percent
                )

                if summary.total_rps == 0:
                    logger.error("ZERO RPS detected in iteration %d! "
                                 "Check benchmark logs and CSV files in %s",
                                 iteration, iter_dir)

            except Exception as e:
                logger.error("Iteration %d FAILED: %s", iteration, e)
                # Save error info
                error_path = iter_dir / "error.txt"
                error_path.write_text(str(e) + "\n")
                raise

            finally:
                # Cleanup benchmark processes
                for bp in bench_procs:
                    if bp.poll() is None:
                        try:
                            pgid = os.getpgid(bp.pid)
                            os.killpg(pgid, signal.SIGKILL)
                        except (ProcessLookupError, OSError):
                            pass
                        try:
                            bp.wait(timeout=5)
                        except subprocess.TimeoutExpired:
                            pass

                if mpstat_proc and mpstat_proc.poll() is None:
                    stop_mpstat(mpstat_proc)

                # Stop local server (remote servers are not managed)
                if server_proc:
                    stop_server(server_proc)

                # Clean up barrier
                if barrier_dir:
                    shutil.rmtree(barrier_dir, ignore_errors=True)

                # Clean up local server home directory
                if server_home:
                    shutil.rmtree(str(server_home), ignore_errors=True)

        # Compute aggregate for this server
        if iteration_summaries:
            aggregate = compute_aggregate(iteration_summaries)
            agg_path = server_output / "aggregate.json"
            agg_path.write_text(json.dumps(asdict(aggregate), indent=2) + "\n")
            all_aggregates[server_name] = aggregate

            logger.info("")
            logger.info("═══ %s AGGREGATE ═══", server_name)
            logger.info("  RPS: %.0f ± %.0f (CoV: %.1f%%)",
                        aggregate.total_rps_mean, aggregate.total_rps_stdev,
                        aggregate.total_rps_cov)
            logger.info("  SET RPS: %.0f  GET RPS: %.0f",
                        aggregate.set_rps_mean, aggregate.get_rps_mean)
            logger.info("  CPU: %.1f%%", aggregate.avg_cpu_percent)
            logger.info("  Iterations: %d kept / %d total (%d outliers)",
                        aggregate.iterations_kept, aggregate.iterations_total,
                        aggregate.outliers_removed)

    # Comparison summary
    if len(all_aggregates) > 1:
        comparison = {}
        names = list(all_aggregates.keys())
        ref_name = names[0]
        ref = all_aggregates[ref_name]

        for name, agg in all_aggregates.items():
            comparison[name] = {
                "total_rps_mean": agg.total_rps_mean,
                "total_rps_stdev": agg.total_rps_stdev,
                "total_rps_cov": agg.total_rps_cov,
                "set_rps_mean": agg.set_rps_mean,
                "get_rps_mean": agg.get_rps_mean,
                "avg_cpu_percent": agg.avg_cpu_percent,
            }
            if name != ref_name and ref.total_rps_mean > 0:
                delta_pct = ((agg.total_rps_mean - ref.total_rps_mean)
                             / ref.total_rps_mean * 100)
                comparison[name]["delta_vs_" + ref_name] = round(delta_pct, 2)

        comp_path = output_root / "comparison.json"
        comp_path.write_text(json.dumps(comparison, indent=2) + "\n")

        logger.info("")
        logger.info("═══ COMPARISON ═══")
        for name, data in comparison.items():
            delta_key = f"delta_vs_{ref_name}"
            if delta_key in data:
                logger.info("  %s: %.0f RPS (%+.1f%% vs %s)",
                            name, data["total_rps_mean"],
                            data[delta_key], ref_name)
            else:
                logger.info("  %s: %.0f RPS (reference)", name, data["total_rps_mean"])

    logger.info("")
    logger.info("Experiment complete. Results in: %s", output_root)

    return all_aggregates


# ═══════════════════════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════════════════════

def parse_args():
    parser = argparse.ArgumentParser(
        description="Valkey Server Benchmark Orchestrator — run amz_valkey-benchmark "
                    "against multiple valkey-server binaries",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run experiment
  python orchestrator.py --config configs/examples/experiment-2servers.json

  # Dry run — show plan without executing
  python orchestrator.py --config experiment.json --dry-run

  # Verbose logging
  python orchestrator.py --config experiment.json -v
""",
    )
    parser.add_argument(
        "--config", "-c",
        required=True,
        help="Path to experiment configuration JSON file",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show experiment plan without running",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose (DEBUG) logging",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    config = load_config(args.config)

    # Create output directory with timestamp
    timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    output_root = Path(config["output_directory"]) / timestamp
    output_root.mkdir(parents=True, exist_ok=True)

    # Setup logging
    log_file = output_root / "orchestrator.log"
    setup_logging(log_file=log_file, verbose=args.verbose)

    clients = config["clients"]
    set_conns = clients["set_clients"]
    get_conns = clients["get_clients"]
    max_c = clients["max_connections_per_benchmark_process"]
    total_conns = set_conns + get_conns
    set_procs = max(1, math.ceil(set_conns / max_c)) if set_conns > 0 else 0
    get_procs = max(1, math.ceil(get_conns / max_c)) if get_conns > 0 else 0
    total_procs = set_procs + get_procs
    set_c_per = math.ceil(set_conns / set_procs) if set_procs > 0 else 0
    get_c_per = math.ceil(get_conns / get_procs) if get_procs > 0 else 0

    if args.dry_run:
        print("=" * 70)
        print("DRY RUN — Experiment Plan")
        print(f"  Description: {config.get('description', '')}")
        print(f"  Servers:")
        for srv in config["servers"]:
            if is_remote_server(srv):
                host = get_server_host(srv)
                port = get_server_port(srv, config["port"])
                print(f"    - {srv['name']}: remote → {host}:{port}")
            else:
                binary = get_server_binary(srv)
                args_list = get_server_args(srv)
                print(f"    - {srv['name']}: local → {binary}")
                if args_list:
                    print(f"      args: {' '.join(args_list)}")
        print(f"  Benchmark: {config['amz_valkey_benchmark_binary']}")
        print(f"  Iterations: {config['iterations']}")
        print(f"  Duration: {config['experiment_duration_seconds']}s per iteration")
        print(f"  Connections: {set_conns} SET + {get_conns} GET = {total_conns} total")
        print(f"  Processes: {set_procs} SET (×{set_c_per} conns) + "
              f"{get_procs} GET (×{get_c_per} conns) = {total_procs} total "
              f"(max {max_c} conns/proc)")
        print(f"  Keyspace: {config['keyspace']['key_count']} keys, "
              f"{config['keyspace']['data_size_bytes']}B values")
        numa = config.get("numa", {})
        if numa.get("enabled"):
            print(f"  NUMA: server→node {numa.get('server_node', 0)}, "
                  f"benchmarks→node {numa.get('benchmark_node', 1)}")
        else:
            print(f"  NUMA: disabled")
        print(f"  Warmup skip: {config.get('warmup_skip_seconds', 5)}s")
        print(f"  Output: {output_root}")
        print(f"  Total experiment time: ~{config['iterations'] * len(config['servers']) * config['experiment_duration_seconds'] / 60:.0f} min")
        print("=" * 70)
        return

    try:
        run_experiment(config, output_root)
    except Exception as e:
        logger.error("EXPERIMENT FAILED: %s", e, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
