# Benchmarks

Reproducible workloads for the runtime and its stores, how they are
measured. Results describe this library and the stated environment.
No other durable execution system is measured or compared here.

## Reproducing

```sh
# Memory and SQLite:
bench/run-matrix.sh
# With PostgreSQL (use an isolated database: each run creates and drops a schema):
AGENT_EFFECTS_POSTGRES_URL=postgres://postgres:pw@localhost:55432/effects bench/run-matrix.sh
# One configuration:
cargo run --release -p agent-effects-bench -- \
    --store sqlite --workload unique --effects 1000 --concurrency 16 --runs 3
```

`run-matrix.sh` builds in release mode, runs every workload at 100, 1,000 and
10,000 effects, writes one JSON line per measured run to
`target/bench-results/<time>.jsonl`, and prints a summary of medians. SQLite
files go to `target/bench-data` by default (`--data-dir`): keep them on a
real disk, since tmpfs skips `fsync`.

## Workloads

| Workload | What is timed |
|---|---|
| `unique` | `effects` calls, each a new effect, from `concurrency` callers: record, lease, attempt, commit |
| `duplicate` | `effects` calls over a tenth as many keys, interleaved so callers of a key race; each key's action must run once |
| `transitions` | Store operations without the runtime: insert, lease, start, succeed, release |
| `lease-contention` | `concurrency` workers racing for the leases of 8 effects (acquire, release on success) |
| `recovery` | One recovery pass over `effects` recorded intents with `concurrency` slots (setup untimed) |
| `retry` | Each effect fails transiently once and is retried after a 1 s backoff, waited out without the lease (300 ms lease) |
| `retry-holding-leases` | The same, holding the lease through the backoff (0.2.0 behavior) |
| `compensation` | Compensating `effects` committed effects (setup untimed) |
| `prune` | One retention pass deleting `effects` settled records (setup untimed) |

Every workload is deterministic: fixed keys and inputs, no retry jitter.
Actions do nothing unless `--work-ms` adds a simulated remote call.

## What is measured

- **Throughput**: effects (or operations) per second of the timed phase.
- **Latency** p50/p95/p99/max per call, where a workload has calls to time.
- **Store operations per effect**, counted by a wrapper around the store.
- **CPU**: this process's user + system time over the run (setup included),
  as a share of one core. The database server's CPU is not included.
- **Peak memory**: the process's peak resident set (`VmHWM`) at the end of
  the run; it includes the warm-up and earlier runs of the same invocation.
- **Duplicate side effects**: actions that ran more often than the workload
  allows, counted by the actions themselves. Must be 0.
- **Unresolved**: effects not settled at the end, where all should be. Must
  be 0.

Each configuration runs one untimed warm-up (at most 100 effects, on its
own store) and three measured runs, each on a fresh store (new SQLite file,
new PostgreSQL schema). The summary reports the median run by throughput.

## Environment

Record `rustc -Vv`, OS/kernel, CPU, memory, filesystem and database versions
with each result. Keep SQLite on a real disk and record its durability
settings. Document other workloads sharing the host. The benchmark uses
four Tokio threads; PostgreSQL pool size is concurrency + 4 (at least 10).

## Results

One JSON line per measured run contains the actual results, including
p50/p95/p99, CPU, peak memory, store operation counts, duplicate actions and
unresolved effects. Recovery and pruning time a whole pass, so their
per-effect latency percentiles are null. A failed invariant exits nonzero.

CI smoke runs verify every workload on memory and SQLite and recovery on
PostgreSQL. Smoke timings share CI resources and are not performance claims.

## Observations

Compare recovery with `--concurrency 1` and `16` under the same simulated
remote latency. Compare retry scheduling with `retry` and
`retry-holding-leases`: both have the same attempt budget and backoff. The
lease-free workload explicitly opts into releasing leases for every wait,
so even short smoke-test delays exercise the scheduler. The production
default remains the lease TTL. Count duplicate actions and unresolved
effects before interpreting throughput: both must be zero.

## Limitations

- Desktop and shared CI loads cause variance. Use several measured runs,
  separate the warm-up, and compare configurations on the same idle host.
- PostgreSQL ran in a container on the same host as the benchmark, so the
  network round trip is local; a networked database adds its latency to
  every store operation.
- Actions are trivial unless stated, so the workloads measure the runtime
  and the store, not a remote system.
- CPU and memory are the benchmark process's only.
