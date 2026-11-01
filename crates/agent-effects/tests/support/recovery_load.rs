//! Backend-independent scenarios for bounded-concurrency recovery: backlogs
//! of durable intents, slow handlers, competing runtimes, cancellation,
//! shutdown, store failures and lease expiry mid-pass. Every effect is a
//! non-idempotent write, so the only acceptable count per effect is one.
#![allow(dead_code)] // Each integration binary selects the scenarios it needs.

pub mod fault_store;

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use agent_effects::store::{ExecutionContract, ListQuery, NewEffect};
use agent_effects::{
    Clock, EffectContext, EffectFailure, EffectHandler, EffectKey, EffectKind, EffectName,
    EffectRecord, EffectStatus, EffectStore, Handler, LogicalKey, ManualClock, RecoveryConfig,
    RetryPolicy, Runtime, SystemClock, WorkerId,
};
use fault_store::{Edge, FaultStore, Operation};
use tokio::sync::watch;

pub const TTL: Duration = Duration::from_secs(30);
const RETRY: RetryPolicy = RetryPolicy {
    max_attempts: 3,
    initial_delay: Duration::ZERO,
    max_delay: Duration::ZERO,
    multiplier: 1.0,
    jitter: false,
};
/// Long enough for any scenario on a loaded CI runner.
const PATIENCE: Duration = Duration::from_secs(60);

/// What the jobs did: how often each one ran, and how many ran at once.
pub struct Probe {
    runs: Mutex<HashMap<u32, u32>>,
    by_runner: Mutex<HashMap<String, u32>>,
    in_flight: AtomicUsize,
    peak: AtomicUsize,
    work: Duration,
    /// Jobs that wait for `slow_gate`.
    slow: Vec<u32>,
    slow_gate: watch::Sender<bool>,
    /// Every job waits for this one.
    gate: watch::Sender<bool>,
}

impl Probe {
    pub fn new(work: Duration) -> Arc<Self> {
        Self::with_slow(work, Vec::new())
    }

    pub fn with_slow(work: Duration, slow: Vec<u32>) -> Arc<Self> {
        Arc::new(Self {
            runs: Mutex::default(),
            by_runner: Mutex::default(),
            in_flight: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            work,
            slow,
            slow_gate: watch::Sender::new(false),
            gate: watch::Sender::new(true),
        })
    }

    pub fn hold_all(&self) {
        self.gate.send_replace(false);
    }

    pub fn release_all(&self) {
        self.gate.send_replace(true);
    }

    pub fn release_slow(&self) {
        self.slow_gate.send_replace(true);
    }

    pub fn peak(&self) -> usize {
        self.peak.load(Ordering::SeqCst)
    }

    pub fn in_flight(&self) -> usize {
        self.in_flight.load(Ordering::SeqCst)
    }

    pub fn runs(&self, job: u32) -> u32 {
        self.runs.lock().unwrap().get(&job).copied().unwrap_or(0)
    }

    /// How many jobs each runtime ran, by worker id.
    pub fn by_runner(&self) -> HashMap<String, u32> {
        self.by_runner.lock().unwrap().clone()
    }

    pub fn total_runs(&self) -> u32 {
        self.runs.lock().unwrap().values().sum()
    }

    /// Every job in `jobs` ran exactly once, and nothing else ran.
    pub fn assert_each_ran_once(&self, jobs: std::ops::Range<u32>) {
        let runs = self.runs.lock().unwrap();
        let wrong: Vec<_> = jobs
            .clone()
            .filter(|job| runs.get(job).copied().unwrap_or(0) != 1)
            .map(|job| (job, runs.get(&job).copied().unwrap_or(0)))
            .collect();
        assert!(wrong.is_empty(), "jobs not run exactly once: {wrong:?}");
        assert_eq!(runs.len(), jobs.len(), "unexpected jobs ran");
    }
}

/// A non-idempotent, unverifiable write: re-running it would duplicate it.
#[derive(Clone)]
pub struct Job {
    pub probe: Arc<Probe>,
    pub runner: String,
}

impl EffectHandler for Job {
    const NAME: &'static str = "recovery.job";
    type Input = u32;
    type Output = u32;
    type Error = EffectFailure;

    async fn execute(&self, _: &EffectContext, job: &u32) -> Result<u32, EffectFailure> {
        let probe = &self.probe;
        let now = probe.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        probe.peak.fetch_max(now, Ordering::SeqCst);
        *probe.runs.lock().unwrap().entry(*job).or_default() += 1;
        *probe
            .by_runner
            .lock()
            .unwrap()
            .entry(self.runner.clone())
            .or_default() += 1;
        if probe.slow.contains(job) {
            let _ = probe.slow_gate.subscribe().wait_for(|open| *open).await;
        }
        let _ = probe.gate.subscribe().wait_for(|open| *open).await;
        if !probe.work.is_zero() {
            tokio::time::sleep(probe.work).await;
        }
        probe.in_flight.fetch_sub(1, Ordering::SeqCst);
        Ok(*job)
    }
}

pub fn key(job: u32) -> EffectKey {
    EffectKey::new(
        EffectName::new(Job::NAME).unwrap(),
        LogicalKey::new(job.to_string()).unwrap(),
    )
}

/// Durable intents for `jobs`, recorded and never started: what a crash
/// right after recording leaves, and what recovery must run exactly once.
pub async fn seed<S: EffectStore>(store: &S, jobs: std::ops::Range<u32>, now: SystemTime) {
    for job in jobs {
        let mut new = NewEffect::new(key(job), EffectKind::IrreversibleWrite, now).with_contract(
            ExecutionContract::new(EffectKind::IrreversibleWrite, RETRY.max_attempts),
        );
        new.input = Some(serde_json::json!(job));
        assert!(store.insert_or_get(new).await.unwrap().inserted);
    }
}

pub fn runtime<S: EffectStore>(
    store: S,
    clock: impl Clock,
    probe: &Arc<Probe>,
    worker: &str,
    config: RecoveryConfig,
) -> Runtime<S> {
    Runtime::builder(store)
        .clock(clock)
        .lease_ttl(TTL)
        .retry_policy(RETRY)
        .worker_id(WorkerId::new(worker))
        .recovery(config)
        .register(Handler::new(Job {
            probe: Arc::clone(probe),
            runner: worker.to_owned(),
        }))
        .build()
}

pub async fn all_records<S: EffectStore>(store: &S) -> Vec<EffectRecord> {
    let mut records = Vec::new();
    let mut after = None;
    loop {
        let mut query = ListQuery::statuses([]).limit(500);
        query.after = after;
        let page = store.list(query).await.unwrap();
        let full = page.len() == 500;
        after = page.last().map(|r| r.id);
        records.extend(page);
        if !full {
            return records;
        }
    }
}

pub async fn count<S: EffectStore>(store: &S, status: EffectStatus) -> usize {
    all_records(store)
        .await
        .iter()
        .filter(|r| r.status == status)
        .count()
}

async fn eventually(what: &str, mut done: impl AsyncFnMut() -> bool) {
    tokio::time::timeout(PATIENCE, async {
        while !done().await {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting until {what}"));
}

/// Recovers a backlog of `n` intents with `config`, and checks the bound.
pub async fn backlog<S: EffectStore + Clone>(store: S, n: u32, config: RecoveryConfig) {
    let probe = Probe::new(Duration::from_millis(2));
    seed(&store, 0..n, SystemTime::now()).await;
    let rt = runtime(store.clone(), SystemClock, &probe, "recovery", config);
    let report = rt.recover().await.unwrap();

    assert_eq!(
        report.resumed.len(),
        n as usize,
        "{:?}",
        report.resume_errors
    );
    assert!(report.resume_errors.is_empty() && report.unhandled.is_empty());
    assert!(
        report
            .resumed
            .iter()
            .all(|(_, status)| *status == EffectStatus::Committed)
    );
    probe.assert_each_ran_once(0..n);
    assert!(
        probe.peak() <= config.concurrency_limit(),
        "{} in flight, limit {}",
        probe.peak(),
        config.concurrency_limit()
    );
    if config.concurrency_limit() > 1 {
        assert!(probe.peak() > 1, "the pass never ran effects concurrently");
    }
    assert_eq!(count(&store, EffectStatus::Committed).await, n as usize);
    assert!(
        all_records(&store)
            .await
            .iter()
            .all(|r| r.attempt_count == 1)
    );
}

/// With room for several effects, a handler that never returns holds one
/// slot; the others all finish while it is stuck.
pub async fn slow_handler_does_not_hold_up_others<S: EffectStore + Clone>(store: S) {
    const FAST: u32 = 30;
    // Job 0 is recorded first, so a sequential pass would reach it first.
    let probe = Probe::with_slow(Duration::ZERO, vec![0]);
    seed(&store, 0..FAST + 1, SystemTime::now()).await;
    let config = RecoveryConfig::SEQUENTIAL.max_concurrency(4).batch_size(8);
    let rt = runtime(store.clone(), SystemClock, &probe, "recovery", config);
    let pass = tokio::spawn({
        let rt = rt.clone();
        async move { rt.recover().await }
    });

    eventually("every fast job commits", async || {
        count(&store, EffectStatus::Committed).await == FAST as usize
    })
    .await;
    assert!(!pass.is_finished(), "the pass waits for the slow job");
    assert_eq!(probe.in_flight(), 1, "only the slow job is still running");

    probe.release_slow();
    let report = pass.await.unwrap().unwrap();
    assert_eq!(report.resumed.len(), FAST as usize + 1);
    probe.assert_each_ran_once(0..FAST + 1);
    assert_eq!(
        count(&store, EffectStatus::Committed).await,
        FAST as usize + 1
    );
}

/// The same scenario with v0.2.0's sequential default: the slow job blocks
/// the rest, which is what `max_concurrency` exists to avoid.
pub async fn sequential_default_waits_behind_a_slow_handler<S: EffectStore + Clone>(store: S) {
    let probe = Probe::with_slow(Duration::ZERO, vec![0]);
    seed(&store, 0..5, SystemTime::now()).await;
    let rt = runtime(
        store.clone(),
        SystemClock,
        &probe,
        "recovery",
        RecoveryConfig::default(),
    );
    let pass = tokio::spawn({
        let rt = rt.clone();
        async move { rt.recover().await }
    });
    eventually("the slow job starts", async || probe.runs(0) == 1).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(probe.total_runs(), 1, "nothing else started");
    probe.release_slow();
    pass.await.unwrap().unwrap();
    probe.assert_each_ran_once(0..5);
}

/// Several runtimes (or processes) recovering one backlog at once run every
/// effect exactly once between them.
pub async fn competing_runtimes<S: EffectStore + Clone>(store: S, n: u32, runtimes: usize) {
    let probe = Probe::new(Duration::from_millis(1));
    seed(&store, 0..n, SystemTime::now()).await;
    let config = RecoveryConfig::SEQUENTIAL.max_concurrency(8).batch_size(32);
    let passes: Vec<_> = (0..runtimes)
        .map(|i| {
            let rt = runtime(store.clone(), SystemClock, &probe, &format!("w{i}"), config);
            tokio::spawn(async move { rt.recover().await })
        })
        .collect();
    for pass in passes {
        let report = pass.await.unwrap().unwrap();
        assert!(report.resume_errors.is_empty(), "{report:?}");
    }
    // A pass may skip a record another one locked or leased; one more pass
    // sweeps those up, and must find nothing left to run twice.
    let sweep = runtime(store.clone(), SystemClock, &probe, "sweep", config);
    sweep.recover().await.unwrap();

    probe.assert_each_ran_once(0..n);
    assert_eq!(count(&store, EffectStatus::Committed).await, n as usize);
    assert!(
        probe.peak() <= config.concurrency_limit() * (runtimes + 1),
        "{}",
        probe.peak()
    );
    let by_runner = probe.by_runner();
    assert!(
        by_runner.keys().filter(|r| r.starts_with('w')).count() > 1,
        "only one runtime did any work: {by_runner:?}"
    );
}

/// Dropping a pass mid-flight (a cancelled task, a worker shut down hard)
/// neither interrupts the effects it started nor loses the rest: a later
/// pass finishes them, and nothing runs twice.
pub async fn cancelled_pass_then_restart<S: EffectStore + Clone>(store: S) {
    const N: u32 = 40;
    let probe = Probe::new(Duration::ZERO);
    probe.hold_all();
    seed(&store, 0..N, SystemTime::now()).await;
    let config = RecoveryConfig::SEQUENTIAL.max_concurrency(4).batch_size(10);
    let rt = runtime(store.clone(), SystemClock, &probe, "first", config);
    let pass = tokio::spawn({
        let rt = rt.clone();
        async move { rt.recover().await }
    });
    eventually("four jobs are in flight", async || probe.in_flight() == 4).await;
    pass.abort();
    assert!(pass.await.unwrap_err().is_cancelled());

    probe.release_all();
    eventually("the started jobs commit", async || {
        count(&store, EffectStatus::Committed).await == 4
    })
    .await;
    assert_eq!(probe.total_runs(), 4, "nothing started after the cancel");

    let restarted = runtime(store.clone(), SystemClock, &probe, "second", config);
    let report = restarted.recover().await.unwrap();
    assert_eq!(report.resumed.len(), N as usize - 4);
    probe.assert_each_ran_once(0..N);
    assert_eq!(count(&store, EffectStatus::Committed).await, N as usize);
}

/// A shutdown signal mid-pass: nothing new starts, the started effects
/// finish, and `run_recovery_until` returns.
pub async fn graceful_shutdown<S: EffectStore + Clone>(store: S) {
    const N: u32 = 40;
    let probe = Probe::new(Duration::ZERO);
    probe.hold_all();
    seed(&store, 0..N, SystemTime::now()).await;
    let config = RecoveryConfig::SEQUENTIAL.max_concurrency(4).batch_size(10);
    let rt = runtime(store.clone(), SystemClock, &probe, "worker", config);
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let worker = tokio::spawn({
        let rt = rt.clone();
        async move {
            rt.run_recovery_until(Duration::from_millis(10), async {
                let _ = stopped.await;
            })
            .await;
        }
    });
    eventually("four jobs are in flight", async || probe.in_flight() == 4).await;
    stop.send(()).unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(!worker.is_finished(), "waits for the started effects");
    probe.release_all();
    tokio::time::timeout(PATIENCE, worker)
        .await
        .expect("the worker stops")
        .unwrap();

    assert_eq!(probe.total_runs(), 4, "nothing started after the signal");
    assert_eq!(count(&store, EffectStatus::Committed).await, 4);
    assert_eq!(count(&store, EffectStatus::Pending).await, N as usize - 4);

    let report = rt.recover().await.unwrap();
    assert!(!report.stopped);
    probe.assert_each_ran_once(0..N);
}

/// A shutdown signal that fires before a pass starts stops it at once.
pub async fn shutdown_before_a_pass<S: EffectStore + Clone>(store: S) {
    let probe = Probe::new(Duration::ZERO);
    seed(&store, 0..3, SystemTime::now()).await;
    let rt = runtime(
        store.clone(),
        SystemClock,
        &probe,
        "worker",
        RecoveryConfig::default(),
    );
    tokio::time::timeout(
        PATIENCE,
        rt.run_recovery_until(Duration::from_secs(3600), std::future::ready(())),
    )
    .await
    .expect("returns at once");
    assert_eq!(probe.total_runs(), 0);
}

/// The store fails while a pass is scanning for more work: the pass
/// reports the error only after the effects it started have finished, and
/// a later pass resumes the rest exactly once.
pub async fn store_failure_mid_pass<S: EffectStore>(inner: S) {
    const N: u32 = 25;
    let store = FaultStore::new(inner);
    let probe = Probe::new(Duration::ZERO);
    probe.hold_all();
    seed(&store, 0..N, SystemTime::now()).await;
    let config = RecoveryConfig::SEQUENTIAL.max_concurrency(4).batch_size(10);
    let rt = runtime(store.clone(), SystemClock, &probe, "worker", config);
    let pass = tokio::spawn({
        let rt = rt.clone();
        async move { rt.recover().await }
    });
    // The first page's effects are in flight; the next scan fails.
    eventually("four jobs are in flight", async || probe.in_flight() == 4).await;
    store.controls.fail(Operation::List, Edge::Before);
    probe.release_all();
    let error = pass.await.unwrap().unwrap_err();
    assert!(error.to_string().contains("injected"), "{error}");
    assert_eq!(
        count(&store, EffectStatus::Committed).await,
        10,
        "the first page finished before the error was reported"
    );
    assert_eq!(probe.total_runs(), 10);

    let report = rt.recover().await.unwrap();
    assert_eq!(report.resumed.len(), N as usize - 10);
    probe.assert_each_ran_once(0..N);
}

/// A database outage while effects are being resumed: the affected effects
/// are reported, never run twice, and recovered once the store is back.
pub async fn outage_mid_pass<S: EffectStore>(inner: S) {
    const N: u32 = 12;
    let store = FaultStore::new(inner);
    let clock = Arc::new(ManualClock::new(SystemTime::now()));
    let probe = Probe::new(Duration::ZERO);
    probe.hold_all();
    seed(&store, 0..N, clock.now()).await;
    let config = RecoveryConfig::SEQUENTIAL.max_concurrency(4).batch_size(4);
    let rt = runtime(store.clone(), Arc::clone(&clock), &probe, "worker", config);
    let pass = tokio::spawn({
        let rt = rt.clone();
        async move { rt.recover().await }
    });
    eventually("four jobs are in flight", async || probe.in_flight() == 4).await;
    store.controls.offline();
    probe.release_all();
    // The in-flight results cannot be recorded, and the next scan fails.
    assert!(pass.await.unwrap().is_err());
    store.controls.reconnect();
    assert_eq!(probe.total_runs(), 4);

    // The four interrupted attempts may have applied: once their leases
    // expire they are unknown and, being neither idempotent nor
    // verifiable, wait for an operator. The rest run once.
    clock.advance(TTL + Duration::from_secs(1));
    let report = rt.recover().await.unwrap();
    assert_eq!(report.marked_unknown.len(), 4, "{report:?}");
    probe.assert_each_ran_once(0..N);
    assert_eq!(count(&store, EffectStatus::NeedsIntervention).await, 4);
    assert_eq!(count(&store, EffectStatus::Committed).await, N as usize - 4);
}

/// A recovery worker stalls on one effect past its lease; another worker's
/// concurrent pass takes it over as unknown. The stalled worker's late
/// result is fenced off and kept only as evidence.
pub async fn lease_expiry_mid_pass<S: EffectStore + Clone>(store: S) {
    let clock = Arc::new(ManualClock::new(SystemTime::now()));
    let probe = Probe::with_slow(Duration::ZERO, vec![0]);
    seed(&store, 0..5, clock.now()).await;
    let config = RecoveryConfig::SEQUENTIAL.max_concurrency(4);
    let stalled = runtime(store.clone(), Arc::clone(&clock), &probe, "stalled", config);
    let pass = tokio::spawn({
        let rt = stalled.clone();
        async move { rt.recover().await }
    });
    eventually("the other jobs commit", async || {
        count(&store, EffectStatus::Committed).await == 4
    })
    .await;

    // The stalled worker cannot renew in time (its renewals are due every
    // TTL/3 of real time); by the store's clock its lease is gone.
    clock.advance(TTL + Duration::from_secs(1));
    let other = runtime(store.clone(), Arc::clone(&clock), &probe, "other", config);
    let report = other.recover().await.unwrap();
    let id = store.get_by_key(&key(0)).await.unwrap().unwrap().id;
    assert_eq!(report.marked_unknown, [id]);
    assert_eq!(report.resumed, [(id, EffectStatus::NeedsIntervention)]);

    probe.release_slow();
    let stalled_report = pass.await.unwrap().unwrap();
    assert_eq!(stalled_report.resumed.len(), 5);
    let record = store.get(id).await.unwrap().unwrap();
    assert_eq!(record.status, EffectStatus::NeedsIntervention);
    assert_eq!(record.attempt_count, 1);
    probe.assert_each_ran_once(0..5);
    let evidence = store.late_observations(id, None, 10).await.unwrap();
    assert_eq!(evidence.len(), 1, "the fenced result is kept as evidence");
}
