//! The workloads. Each one sets up untimed state, then times one phase and
//! returns what it measured. Keys and inputs are deterministic, retry
//! jitter is off, and every effect counts its own executions, so duplicate
//! side effects are measured rather than assumed.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime};

use agent_effects::store::{
    EffectStore, ExecutionContract, ListQuery, NewEffect, Transition, TransitionRequest,
};
use agent_effects::{
    EffectContext, EffectFailure, EffectHandler, EffectKey, EffectKind, EffectName, EffectOutcome,
    Handler, LogicalKey, RecoveryConfig, RetentionPolicy, RetryPolicy, Runtime, WorkerId,
};

/// What one timed phase measured.
pub struct Measured {
    /// The timed phase's wall time.
    pub wall: Duration,
    /// Per-operation latencies, when the workload has operations to time.
    pub latencies: Vec<Duration>,
    /// Side effects that ran more often than the workload asked for.
    pub duplicate_side_effects: u64,
    /// Effects not settled at the end, where every effect should be.
    pub unresolved: u64,
    /// Workload-specific counts, e.g. lease conflicts.
    pub notes: serde_json::Value,
}

pub struct Params {
    pub effects: usize,
    pub concurrency: usize,
    /// Simulated remote latency inside each action.
    pub work: Duration,
    /// A per-run prefix, so runs against one database never share keys.
    pub prefix: String,
    /// The retry workloads' backoff.
    pub backoff: Duration,
}

const NO_JITTER: RetryPolicy = RetryPolicy {
    max_attempts: 3,
    initial_delay: Duration::from_millis(10),
    max_delay: Duration::from_millis(10),
    multiplier: 1.0,
    jitter: false,
};

fn key(name: &str, logical: &str) -> EffectKey {
    EffectKey::new(
        EffectName::new(name).unwrap(),
        LogicalKey::new(logical).unwrap(),
    )
}

async fn work(duration: Duration) {
    if !duration.is_zero() {
        tokio::time::sleep(duration).await;
    }
}

/// Runs `op(i)` for `i` in `0..n` on `concurrency` tasks, timing each.
async fn timed<F, Fut>(n: usize, concurrency: usize, op: F) -> (Duration, Vec<Duration>)
where
    F: Fn(usize) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send,
{
    let op = Arc::new(op);
    let next = Arc::new(AtomicUsize::new(0));
    let started = Instant::now();
    let tasks: Vec<_> = (0..concurrency.max(1))
        .map(|_| {
            let (op, next) = (Arc::clone(&op), Arc::clone(&next));
            tokio::spawn(async move {
                let mut latencies = Vec::new();
                loop {
                    let i = next.fetch_add(1, Ordering::Relaxed);
                    if i >= n {
                        return latencies;
                    }
                    let t = Instant::now();
                    op(i).await;
                    latencies.push(t.elapsed());
                }
            })
        })
        .collect();
    let mut latencies = Vec::with_capacity(n);
    for task in tasks {
        latencies.extend(task.await.expect("benchmark task panicked"));
    }
    (started.elapsed(), latencies)
}

async fn unresolved<S: EffectStore>(store: &S, prefix: &str) -> u64 {
    let mut count = 0;
    let mut after = None;
    loop {
        let mut query = ListQuery::statuses([]).limit(1000);
        query.after = after;
        let page = store.list(query).await.expect("list");
        let full = page.len() == 1000;
        after = page.last().map(|r| r.id);
        count += page
            .iter()
            .filter(|r| r.key.key.as_str().starts_with(prefix) && !r.status.is_settled())
            .count() as u64;
        if !full {
            return count;
        }
    }
}

/// Unique keys: every call is a new effect.
pub async fn unique<S: EffectStore + Clone>(store: S, p: &Params) -> Measured {
    let rt = Runtime::builder(store.clone())
        .retry_policy(NO_JITTER)
        .build();
    let executions = Arc::new(AtomicU32::new(0));
    let (wall, latencies) = timed(p.effects, p.concurrency, {
        let (rt, executions, prefix, delay) = (
            rt.clone(),
            Arc::clone(&executions),
            p.prefix.clone(),
            p.work,
        );
        move |i| {
            let (rt, executions) = (rt.clone(), Arc::clone(&executions));
            let key = format!("{prefix}-{i}");
            async move {
                let outcome = rt
                    .effect("bench.unique", key)
                    .run(move |_| {
                        let executions = Arc::clone(&executions);
                        async move {
                            executions.fetch_add(1, Ordering::Relaxed);
                            work(delay).await;
                            Ok::<_, EffectFailure>(i)
                        }
                    })
                    .await
                    .expect("run");
                assert!(matches!(outcome, EffectOutcome::Committed(_)));
            }
        }
    })
    .await;
    let ran = u64::from(executions.load(Ordering::Relaxed));
    Measured {
        wall,
        latencies,
        duplicate_side_effects: ran.saturating_sub(p.effects as u64),
        unresolved: unresolved(&store, &p.prefix).await,
        notes: serde_json::json!({ "executions": ran }),
    }
}

/// Duplicate keys: `effects` calls over a tenth as many keys, interleaved so
/// callers of one key race. Each key's action must run once.
pub async fn duplicate<S: EffectStore + Clone>(store: S, p: &Params) -> Measured {
    let keys = (p.effects / 10).max(1);
    let rt = Runtime::builder(store.clone())
        .retry_policy(NO_JITTER)
        .build();
    let runs: Arc<Vec<AtomicU32>> = Arc::new((0..keys).map(|_| AtomicU32::new(0)).collect());
    let in_progress = Arc::new(AtomicU32::new(0));
    let (wall, latencies) = timed(p.effects, p.concurrency, {
        let (rt, runs, in_progress, prefix, delay) = (
            rt.clone(),
            Arc::clone(&runs),
            Arc::clone(&in_progress),
            p.prefix.clone(),
            p.work,
        );
        move |i| {
            let (rt, runs, in_progress) = (rt.clone(), Arc::clone(&runs), Arc::clone(&in_progress));
            let k = i % keys;
            let key = format!("{prefix}-{k}");
            async move {
                let outcome = rt
                    .effect("bench.duplicate", key)
                    .run(move |_| {
                        let runs = Arc::clone(&runs);
                        async move {
                            runs[k].fetch_add(1, Ordering::Relaxed);
                            work(delay).await;
                            Ok::<_, EffectFailure>(k)
                        }
                    })
                    .await
                    .expect("run");
                match outcome {
                    EffectOutcome::Committed(_) => {}
                    EffectOutcome::InProgress { .. } => {
                        in_progress.fetch_add(1, Ordering::Relaxed);
                    }
                    other => panic!("unexpected {other:?}"),
                }
            }
        }
    })
    .await;
    let duplicates = runs
        .iter()
        .map(|r| u64::from(r.load(Ordering::Relaxed)).saturating_sub(1))
        .sum();
    Measured {
        wall,
        latencies,
        duplicate_side_effects: duplicates,
        unresolved: unresolved(&store, &p.prefix).await,
        notes: serde_json::json!({
            "keys": keys,
            "calls_answered_in_progress": in_progress.load(Ordering::Relaxed),
        }),
    }
}

/// Store state transitions without the runtime: record, lease, start,
/// succeed, release.
pub async fn transitions<S: EffectStore + Clone>(store: S, p: &Params) -> Measured {
    let (wall, latencies) = timed(p.effects, p.concurrency, {
        let (store, prefix) = (store.clone(), p.prefix.clone());
        move |i| {
            let store = store.clone();
            let key = key("bench.transitions", &format!("{prefix}-{i}"));
            async move {
                let now = SystemTime::now();
                let record = store
                    .insert_or_get(NewEffect::new(key, EffectKind::IdempotentWrite, now))
                    .await
                    .unwrap()
                    .record;
                let lease = store
                    .acquire_lease(
                        record.id,
                        &WorkerId::new("bench"),
                        now,
                        Duration::from_secs(30),
                    )
                    .await
                    .unwrap();
                let record = store
                    .transition(TransitionRequest::new(
                        &record,
                        Some(&lease),
                        Transition::StartAttempt,
                        now,
                    ))
                    .await
                    .unwrap();
                store
                    .transition(TransitionRequest::new(
                        &record,
                        Some(&lease),
                        Transition::Succeeded,
                        now,
                    ))
                    .await
                    .unwrap();
                store.release_lease(&lease).await.unwrap();
            }
        }
    })
    .await;
    Measured {
        wall,
        latencies,
        duplicate_side_effects: 0,
        unresolved: unresolved(&store, &p.prefix).await,
        notes: serde_json::json!({}),
    }
}

/// `concurrency` workers race for the leases of eight effects.
pub async fn lease_contention<S: EffectStore + Clone>(store: S, p: &Params) -> Measured {
    const RECORDS: usize = 8;
    let mut ids = Vec::new();
    for r in 0..RECORDS {
        let new = NewEffect::new(
            key("bench.lease", &format!("{}-{r}", p.prefix)),
            EffectKind::IdempotentWrite,
            SystemTime::now(),
        );
        ids.push(store.insert_or_get(new).await.unwrap().record.id);
    }
    let ids = Arc::new(ids);
    let (won, lost) = (Arc::new(AtomicU32::new(0)), Arc::new(AtomicU32::new(0)));
    let (wall, latencies) = timed(p.effects, p.concurrency, {
        let (store, ids, won, lost) = (
            store.clone(),
            Arc::clone(&ids),
            Arc::clone(&won),
            Arc::clone(&lost),
        );
        move |i| {
            let (store, ids, won, lost) = (
                store.clone(),
                Arc::clone(&ids),
                Arc::clone(&won),
                Arc::clone(&lost),
            );
            async move {
                let worker = WorkerId::new(format!("w{}", i % 64));
                match store
                    .acquire_lease(
                        ids[i % RECORDS],
                        &worker,
                        SystemTime::now(),
                        Duration::from_secs(30),
                    )
                    .await
                {
                    Ok(lease) => {
                        won.fetch_add(1, Ordering::Relaxed);
                        store.release_lease(&lease).await.unwrap();
                    }
                    Err(agent_effects::store::StoreError::LeaseHeld { .. }) => {
                        lost.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) => panic!("{e}"),
                }
            }
        }
    })
    .await;
    Measured {
        wall,
        latencies,
        duplicate_side_effects: 0,
        unresolved: 0,
        notes: serde_json::json!({
            "records": RECORDS,
            "acquired": won.load(Ordering::Relaxed),
            "conflicts": lost.load(Ordering::Relaxed),
        }),
    }
}

/// A durable job: counts its executions and waits `work`.
#[derive(Clone)]
struct Job {
    executions: Arc<AtomicU32>,
    work: Duration,
}

impl EffectHandler for Job {
    const NAME: &'static str = "bench.recover";
    type Input = u64;
    type Output = u64;
    type Error = EffectFailure;

    async fn execute(&self, _: &EffectContext, input: &u64) -> Result<u64, EffectFailure> {
        self.executions.fetch_add(1, Ordering::Relaxed);
        work(self.work).await;
        Ok(*input)
    }
}

/// A recovery backlog: `effects` recorded intents (untimed), then one
/// recovery pass with `concurrency` slots (timed).
pub async fn recovery<S: EffectStore + Clone>(store: S, p: &Params) -> Measured {
    let now = SystemTime::now();
    for i in 0..p.effects {
        let mut new = NewEffect::new(
            key(Job::NAME, &format!("{}-{i}", p.prefix)),
            EffectKind::IrreversibleWrite,
            now,
        )
        .with_contract(ExecutionContract::new(
            EffectKind::IrreversibleWrite,
            NO_JITTER.max_attempts,
        ));
        new.input = Some(serde_json::json!(i));
        store.insert_or_get(new).await.unwrap();
    }
    let executions = Arc::new(AtomicU32::new(0));
    let rt = Runtime::builder(store.clone())
        .retry_policy(NO_JITTER)
        .recovery(
            RecoveryConfig::SEQUENTIAL
                .max_concurrency(p.concurrency)
                .batch_size(100),
        )
        .register(Handler::new(Job {
            executions: Arc::clone(&executions),
            work: p.work,
        }))
        .build();
    let started = Instant::now();
    let report = rt.recover().await.expect("recover");
    let wall = started.elapsed();
    let ran = u64::from(executions.load(Ordering::Relaxed));
    Measured {
        wall,
        latencies: Vec::new(),
        duplicate_side_effects: ran.saturating_sub(p.effects as u64),
        unresolved: unresolved(&store, &p.prefix).await,
        notes: serde_json::json!({
            "resumed": report.resumed.len(),
            "resume_errors": report.resume_errors.len(),
            "executions": ran,
        }),
    }
}

/// Every effect fails transiently once, then succeeds after `backoff`, with
/// a 300 ms lease. Explicit thresholds exercise lease-free scheduling even
/// with the shorter backoffs used by smoke checks. With `hold_leases`, the
/// lease is kept (and renewed every 100 ms) through the wait instead.
pub async fn retry<S: EffectStore + Clone>(store: S, p: &Params, hold_leases: bool) -> Measured {
    let policy = RetryPolicy {
        max_attempts: 2,
        initial_delay: p.backoff,
        max_delay: p.backoff,
        multiplier: 1.0,
        jitter: false,
    };
    let builder = Runtime::builder(store.clone())
        .retry_policy(policy)
        .lease_ttl(Duration::from_millis(300))
        .lease_free_backoff(if hold_leases {
            Duration::MAX
        } else {
            Duration::ZERO
        });
    let rt = builder.build();
    let executions = Arc::new(AtomicU32::new(0));
    let (wall, latencies) = timed(p.effects, p.concurrency, {
        let (rt, executions, prefix) = (rt.clone(), Arc::clone(&executions), p.prefix.clone());
        move |i| {
            let (rt, executions) = (rt.clone(), Arc::clone(&executions));
            let key = format!("{prefix}-{i}");
            async move {
                let outcome = rt
                    .effect("bench.retry", key)
                    .kind(EffectKind::IdempotentWrite)
                    .run(move |ctx: EffectContext| {
                        let executions = Arc::clone(&executions);
                        async move {
                            executions.fetch_add(1, Ordering::Relaxed);
                            if ctx.attempt() == 1 {
                                Err(EffectFailure::transient("first attempt fails"))
                            } else {
                                Ok(i)
                            }
                        }
                    })
                    .await
                    .expect("run");
                assert!(
                    matches!(outcome, EffectOutcome::Committed(_)),
                    "{outcome:?}"
                );
            }
        }
    })
    .await;
    let ran = u64::from(executions.load(Ordering::Relaxed));
    Measured {
        wall,
        latencies,
        duplicate_side_effects: ran.saturating_sub(2 * p.effects as u64),
        unresolved: unresolved(&store, &p.prefix).await,
        notes: serde_json::json!({ "executions": ran, "hold_leases": hold_leases }),
    }
}

/// `effects` committed effects (untimed), then each compensated (timed).
pub async fn compensation<S: EffectStore + Clone>(store: S, p: &Params) -> Measured {
    let rt = Runtime::builder(store.clone())
        .retry_policy(NO_JITTER)
        .build();
    for i in 0..p.effects {
        rt.effect("bench.compensation", format!("{}-{i}", p.prefix))
            .run(move |_| async move { Ok::<_, EffectFailure>(i) })
            .await
            .unwrap();
    }
    let undone = Arc::new(AtomicU32::new(0));
    let (wall, latencies) = timed(p.effects, p.concurrency, {
        let (rt, undone, prefix, delay) =
            (rt.clone(), Arc::clone(&undone), p.prefix.clone(), p.work);
        move |i| {
            let (rt, undone) = (rt.clone(), Arc::clone(&undone));
            let key = format!("{prefix}-{i}");
            async move {
                let outcome = rt
                    .compensation("bench.compensation", key)
                    .run(move |_, _: Option<usize>| {
                        let undone = Arc::clone(&undone);
                        async move {
                            undone.fetch_add(1, Ordering::Relaxed);
                            work(delay).await;
                            Ok(())
                        }
                    })
                    .await
                    .unwrap();
                assert_eq!(outcome, agent_effects::CompensationOutcome::Compensated);
            }
        }
    })
    .await;
    let ran = u64::from(undone.load(Ordering::Relaxed));
    Measured {
        wall,
        latencies,
        duplicate_side_effects: ran.saturating_sub(p.effects as u64),
        unresolved: unresolved(&store, &p.prefix).await,
        notes: serde_json::json!({ "compensations": ran }),
    }
}

/// `effects` committed effects (untimed), then one retention pass deleting
/// them all (timed).
pub async fn prune<S: EffectStore + Clone>(store: S, p: &Params) -> Measured {
    let setup = Runtime::builder(store.clone())
        .retry_policy(NO_JITTER)
        .build();
    for i in 0..p.effects {
        setup
            .effect("bench.prune", format!("{}-{i}", p.prefix))
            .run(move |_| async move { Ok::<_, EffectFailure>(i) })
            .await
            .unwrap();
    }
    let rt = Runtime::builder(store.clone())
        .retention(RetentionPolicy::settled(Duration::ZERO))
        .build();
    let started = Instant::now();
    let report = rt.prune().await.unwrap();
    let wall = started.elapsed();
    assert_eq!(
        report.total(),
        p.effects as u64,
        "records were not all pruned"
    );
    Measured {
        wall,
        latencies: Vec::new(),
        duplicate_side_effects: 0,
        unresolved: 0,
        notes: serde_json::json!({ "pruned": report.total() }),
    }
}
