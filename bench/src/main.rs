//! Reproducible workload benchmarks for agent-effects. See
//! `guides/benchmarks.md` for the methodology.
//!
//! ```sh
//! cargo run --release -p agent-effects-bench -- \
//!     --store sqlite --workload unique --effects 1000 --concurrency 16 --runs 3
//! ```
//!
//! Each measured run gets a fresh store (a new SQLite file, a new PostgreSQL
//! schema), so runs never see each other's records. One JSON line per
//! measured run goes to standard output; a summary goes to standard error.

mod counted;
mod workloads;

use std::path::PathBuf;
use std::time::{Duration, Instant};

use agent_effects::store::EffectStore;
use agent_effects_memory::MemoryStore;
use agent_effects_postgres::PostgresOptions;
use agent_effects_sqlite::SqliteStore;
use clap::{Parser, ValueEnum};
use counted::Counted;
use serde_json::json;
use workloads::{Measured, Params};

#[derive(Debug, Parser)]
#[command(name = "agent-effects-bench")]
struct Args {
    /// The store to measure.
    #[arg(long, value_enum)]
    store: Store,
    /// The workload.
    #[arg(long, value_enum)]
    workload: Workload,
    /// Effects (or operations) per run.
    #[arg(long, default_value_t = 1000, value_parser = positive)]
    effects: usize,
    /// Concurrent callers, or recovery slots.
    #[arg(long, default_value_t = 16, value_parser = positive)]
    concurrency: usize,
    /// Measured runs.
    #[arg(long, default_value_t = 3, value_parser = positive)]
    runs: usize,
    /// Untimed warm-up runs before the measured ones, on their own store.
    #[arg(long, default_value_t = 1)]
    warmup: usize,
    /// Simulated remote latency inside each action, in milliseconds.
    #[arg(long, default_value_t = 0)]
    work_ms: u64,
    /// The retry workloads' backoff, in milliseconds (the lease is 300 ms).
    #[arg(long, default_value_t = 1000)]
    backoff_ms: u64,
    /// Where SQLite files go. Use a real disk: tmpfs skips fsync.
    #[arg(long, default_value = "target/bench-data")]
    data_dir: PathBuf,
    /// A label recorded with each result, e.g. the disk under `--data-dir`.
    #[arg(long, default_value = "")]
    label: String,
    /// PostgreSQL, for `--store postgres`.
    #[arg(long, env = "AGENT_EFFECTS_POSTGRES_URL", hide_env_values = true)]
    postgres_url: Option<String>,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Store {
    Memory,
    Sqlite,
    Postgres,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Workload {
    /// Unique keys: every call a new effect.
    Unique,
    /// A tenth as many keys as calls: callers of one key race.
    Duplicate,
    /// Store transitions without the runtime.
    Transitions,
    /// Workers racing for eight leases.
    LeaseContention,
    /// One recovery pass over a backlog of recorded intents.
    Recovery,
    /// One transient failure and a lease-free backoff per effect.
    Retry,
    /// As `retry`, holding the lease through the backoff (0.2.0 behavior).
    RetryHoldingLeases,
    /// Compensating committed effects.
    Compensation,
    /// Deleting settled effects with a retention policy.
    Prune,
}

fn positive(text: &str) -> Result<usize, String> {
    match text.parse::<usize>() {
        Ok(n) if n > 0 => Ok(n),
        _ => Err("must be a positive integer".into()),
    }
}

fn main() {
    let args = Args::parse();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("tokio runtime");
    runtime.block_on(run(args));
}

async fn run(args: Args) {
    let mut summaries = Vec::new();
    for round in 0..args.warmup + args.runs {
        let warmup = round < args.warmup;
        let params = Params {
            effects: if warmup {
                args.effects.min(100)
            } else {
                args.effects
            },
            concurrency: args.concurrency,
            work: Duration::from_millis(args.work_ms),
            prefix: format!("r{round}"),
            backoff: Duration::from_millis(args.backoff_ms),
        };
        let record = match args.store {
            Store::Memory => measure(Counted::new(MemoryStore::new()), &args, &params).await,
            Store::Sqlite => {
                std::fs::create_dir_all(&args.data_dir).expect("data dir");
                let path = args
                    .data_dir
                    .join(format!("bench-{}.db", agent_effects::EffectId::new()));
                let store = SqliteStore::open(&path).await.expect("sqlite");
                let record = measure(Counted::new(store), &args, &params).await;
                for suffix in ["", "-wal", "-shm"] {
                    let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
                }
                record
            }
            Store::Postgres => {
                let url = args
                    .postgres_url
                    .as_deref()
                    .expect("--postgres-url or AGENT_EFFECTS_POSTGRES_URL");
                let schema = format!(
                    "bench_{}",
                    agent_effects::EffectId::new().to_string().replace('-', "")
                );
                let pool_size = u32::try_from(args.concurrency + 4).unwrap_or(64).max(10);
                let store = PostgresOptions::new()
                    .schema(&schema)
                    .max_connections(pool_size)
                    .connect(url)
                    .await
                    .expect("postgres");
                let pool = store.pool().clone();
                let record = measure(Counted::new(store), &args, &params).await;
                let _ = sqlx_drop_schema(&pool, &schema).await;
                record
            }
        };
        if warmup {
            continue;
        }
        println!("{record}");
        assert_eq!(
            record["duplicate_side_effects"], 0,
            "duplicate external actions"
        );
        assert_eq!(record["unresolved"], 0, "workload left unresolved effects");
        summaries.push(record);
    }
    summarize(&args, &summaries);
}

async fn sqlx_drop_schema(pool: &sqlx::PgPool, schema: &str) -> Result<(), sqlx::Error> {
    sqlx::query(sqlx::AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
        .execute(pool)
        .await
        .map(|_| ())
}

async fn measure<S: EffectStore + Clone>(
    store: Counted<S>,
    args: &Args,
    params: &Params,
) -> serde_json::Value {
    let ops_before = store.counts.snapshot();
    let cpu_before = cpu_time();
    let started = Instant::now();
    let measured: Measured = match args.workload {
        Workload::Unique => workloads::unique(store.clone(), params).await,
        Workload::Duplicate => workloads::duplicate(store.clone(), params).await,
        Workload::Transitions => workloads::transitions(store.clone(), params).await,
        Workload::LeaseContention => workloads::lease_contention(store.clone(), params).await,
        Workload::Recovery => workloads::recovery(store.clone(), params).await,
        Workload::Retry => workloads::retry(store.clone(), params, false).await,
        Workload::RetryHoldingLeases => workloads::retry(store.clone(), params, true).await,
        Workload::Compensation => workloads::compensation(store.clone(), params).await,
        Workload::Prune => workloads::prune(store.clone(), params).await,
    };
    // CPU over the whole run (setup included), as a share of one core.
    let elapsed = started.elapsed();
    let cpu = cpu_time().zip(cpu_before).map(|(after, before)| {
        after.saturating_sub(before).as_secs_f64() / elapsed.as_secs_f64() * 100.0
    });
    let ops = store.counts.snapshot().since(ops_before);
    let mut latencies = measured.latencies;
    latencies.sort_unstable();
    let percentile = |q: f64| -> Option<f64> {
        if latencies.is_empty() {
            return None;
        }
        #[allow(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            clippy::cast_precision_loss
        )]
        let rank = ((q * latencies.len() as f64).ceil() as usize).clamp(1, latencies.len());
        Some(latencies[rank - 1].as_secs_f64() * 1000.0)
    };
    #[allow(clippy::cast_precision_loss)]
    let throughput = params.effects as f64 / measured.wall.as_secs_f64();
    #[allow(clippy::cast_precision_loss)]
    let ops_per_effect = ops.total as f64 / params.effects as f64;
    json!({
        "workload": format!("{:?}", args.workload),
        "label": args.label,
        "store": format!("{:?}", args.store).to_lowercase(),
        "effects": params.effects,
        "concurrency": params.concurrency,
        "work_ms": args.work_ms,
        "backoff_ms": args.backoff_ms,
        "wall_s": measured.wall.as_secs_f64(),
        "throughput_per_s": throughput,
        "latency_ms": {
            "p50": percentile(0.50),
            "p95": percentile(0.95),
            "p99": percentile(0.99),
            "max": percentile(1.0),
        },
        "cpu_percent_of_one_core": cpu,
        "peak_rss_mib": peak_rss_mib(),
        "store_ops": ops,
        "store_ops_per_effect": ops_per_effect,
        "duplicate_side_effects": measured.duplicate_side_effects,
        "unresolved": measured.unresolved,
        "notes": measured.notes,
    })
}

fn summarize(args: &Args, runs: &[serde_json::Value]) {
    if runs.is_empty() {
        return;
    }
    let mut throughput: Vec<f64> = runs
        .iter()
        .filter_map(|r| r["throughput_per_s"].as_f64())
        .collect();
    throughput.sort_by(f64::total_cmp);
    let median = throughput[throughput.len() / 2];
    let p99: Vec<String> = runs
        .iter()
        .map(|r| {
            r["latency_ms"]["p99"]
                .as_f64()
                .map_or("-".into(), |v| format!("{v:.2}"))
        })
        .collect();
    eprintln!(
        "{:?} on {:?}: {} effects x{} → median {median:.0}/s over {} runs (p99 ms per run: {}); duplicates {}, unresolved {}",
        args.workload,
        args.store,
        args.effects,
        args.concurrency,
        runs.len(),
        p99.join(", "),
        runs.iter()
            .map(|r| r["duplicate_side_effects"].as_u64().unwrap_or(0))
            .sum::<u64>(),
        runs.iter()
            .map(|r| r["unresolved"].as_u64().unwrap_or(0))
            .sum::<u64>(),
    );
}

/// User + system CPU time of this process, from `/proc/self/stat` (Linux).
fn cpu_time() -> Option<Duration> {
    let stat = std::fs::read_to_string("/proc/self/stat").ok()?;
    // Fields after the parenthesized command name; utime and stime are the
    // 14th and 15th fields overall, in clock ticks (100 per second on Linux).
    let rest = stat.rsplit_once(')')?.1;
    let fields: Vec<&str> = rest.split_whitespace().collect();
    let ticks: u64 = fields.get(11)?.parse::<u64>().ok()? + fields.get(12)?.parse::<u64>().ok()?;
    Some(Duration::from_millis(ticks * 10))
}

/// The process's peak resident memory so far, from `/proc/self/status`.
fn peak_rss_mib() -> Option<f64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let line = status.lines().find(|l| l.starts_with("VmHWM:"))?;
    let kib: f64 = line.split_whitespace().nth(1)?.parse().ok()?;
    Some(kib / 1024.0)
}
