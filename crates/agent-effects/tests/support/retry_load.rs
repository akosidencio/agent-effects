//! Lease-free retry scheduling against real stores and real time: the
//! schedule is stored, the lease released and retaken, across workers and
//! crashes. Durations are short (a 900 ms lease, 1.2 s backoff) so the
//! suites stay quick; the database clock (PostgreSQL) still judges leases.
#![allow(dead_code)] // Each integration binary selects the scenarios it needs.

use std::future::Future;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Once};
use std::time::{Duration, SystemTime};

use agent_effects::fault::{FaultInjector, FaultPoint};
use agent_effects::store::Transition;
use agent_effects::{
    EffectContext, EffectFailure, EffectHandler, EffectKey, EffectName, EffectOutcome,
    EffectRecord, EffectStatus, EffectStore, Handler, LogicalKey, RetryPolicy, Runtime,
    RuntimeBuilder, WorkerId,
};

pub const TTL: Duration = Duration::from_millis(900);
pub const BACKOFF: Duration = Duration::from_millis(1200);
const POLICY: RetryPolicy = RetryPolicy {
    max_attempts: 3,
    initial_delay: BACKOFF,
    max_delay: BACKOFF,
    multiplier: 1.0,
    jitter: false,
};

/// Fails its first `fail_first` executions transiently (nothing applied),
/// then succeeds. Counts every execution.
#[derive(Clone)]
pub struct Flaky {
    pub fail_first: u32,
    pub executions: Arc<AtomicU32>,
}

impl EffectHandler for Flaky {
    const NAME: &'static str = "retry.flaky";
    type Input = String;
    type Output = String;
    type Error = EffectFailure;

    fn execute(
        &self,
        _: &EffectContext,
        input: &String,
    ) -> impl Future<Output = Result<String, EffectFailure>> + Send {
        let n = self.executions.fetch_add(1, Ordering::SeqCst);
        std::future::ready(if n < self.fail_first {
            Err(EffectFailure::transient("not yet"))
        } else {
            Ok(format!("{input}#{}", n + 1))
        })
    }
}

fn builder<S: EffectStore>(store: S, worker: &str) -> RuntimeBuilder<S> {
    Runtime::builder(store)
        .lease_ttl(TTL)
        .retry_policy(POLICY)
        .worker_id(WorkerId::new(worker))
}

fn flaky(fail_first: u32, executions: &Arc<AtomicU32>) -> Handler<Flaky> {
    Handler::new(Flaky {
        fail_first,
        executions: Arc::clone(executions),
    })
}

fn key(logical: &str) -> EffectKey {
    EffectKey::new(
        EffectName::new(Flaky::NAME).unwrap(),
        LogicalKey::new(logical).unwrap(),
    )
}

async fn record<S: EffectStore>(store: &S, logical: &str) -> EffectRecord {
    store.get_by_key(&key(logical)).await.unwrap().unwrap()
}

fn quiet_crashes() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let default = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            if !info.to_string().contains("fault injected") {
                default(info);
            }
        }));
    });
}

/// A caller's retry is stored, its lease released for the wait, and the
/// retry runs once it is due, on a fresh lease.
pub async fn lease_released_during_backoff<S: EffectStore + Clone>(store: S) {
    let executions = Arc::new(AtomicU32::new(0));
    let rt = builder(store.clone(), "w")
        .register(flaky(1, &executions))
        .build();
    let started = SystemTime::now();
    let call = tokio::spawn({
        let rt = rt.clone();
        async move { rt.submit::<Flaky>("k", "res".into()).await }
    });
    tokio::time::sleep(BACKOFF / 3).await;
    let waiting = record(&store, "k").await;
    assert_eq!(waiting.status, EffectStatus::Pending);
    assert_eq!(waiting.lease_owner, None, "released for the backoff");
    assert!(waiting.next_attempt_at.unwrap() >= started + BACKOFF);
    assert_eq!(executions.load(Ordering::SeqCst), 1);

    let outcome = call.await.unwrap().unwrap();
    assert_eq!(outcome, EffectOutcome::Committed("res#2".into()));
    assert!(started.elapsed().unwrap() >= BACKOFF);
    let events: Vec<_> = store
        .events(waiting.id)
        .await
        .unwrap()
        .into_iter()
        .map(|e| e.transition)
        .collect();
    assert_eq!(
        events,
        [
            Transition::StartAttempt,
            Transition::ScheduleRetry,
            Transition::StartAttempt,
            Transition::Succeeded
        ]
    );
}

/// The lifetime attempt budget holds across several lease-free waits.
pub async fn budget_across_backoffs<S: EffectStore + Clone>(store: S) {
    let executions = Arc::new(AtomicU32::new(0));
    let rt = builder(store.clone(), "w")
        .register(flaky(u32::MAX, &executions))
        .build();
    let outcome = rt.submit::<Flaky>("k", "res".into()).await.unwrap();
    assert!(matches!(outcome, EffectOutcome::Failed(_)), "{outcome:?}");
    assert_eq!(executions.load(Ordering::SeqCst), POLICY.max_attempts);
    assert_eq!(record(&store, "k").await.attempt_count, POLICY.max_attempts);
}

/// A worker dies at `point` after its first attempt failed; recovery runs
/// the retry once it is due, exactly once among several workers.
pub async fn crash_then_recover_when_due<S: EffectStore + Clone>(store: S, point: FaultPoint) {
    quiet_crashes();
    let executions = Arc::new(AtomicU32::new(0));
    let injector = Arc::new(FaultInjector::new().at(point).crash());
    let doomed = builder(store.clone(), "doomed")
        .register(flaky(1, &executions))
        .fault_injector(Arc::clone(&injector))
        .build();
    let started = SystemTime::now();
    assert!(doomed.submit::<Flaky>("k", "res".into()).await.is_err());
    assert!(injector.reached().contains(&point));
    let waiting = record(&store, "k").await;
    assert_eq!(
        (waiting.status, waiting.attempt_count),
        (EffectStatus::Pending, 1)
    );

    let workers: Vec<_> = (0..3)
        .map(|i| {
            builder(store.clone(), &format!("r{i}"))
                .register(flaky(1, &executions))
                .build()
        })
        .collect();
    // Not due yet: no worker may start it, whoever holds what.
    for worker in &workers {
        let report = worker.recover().await.unwrap();
        assert!(report.resumed.is_empty(), "{report:?}");
    }
    assert_eq!(executions.load(Ordering::SeqCst), 1);

    let due = waiting.next_attempt_at.unwrap();
    let wait = due
        .duration_since(SystemTime::now())
        .unwrap_or_default()
        .max(TTL);
    tokio::time::sleep(wait + Duration::from_millis(100)).await;
    let passes: Vec<_> = workers
        .into_iter()
        .map(|rt| tokio::spawn(async move { rt.recover().await.unwrap() }))
        .collect();
    for pass in passes {
        pass.await.unwrap();
    }
    assert!(started.elapsed().unwrap() >= BACKOFF);
    let done = record(&store, "k").await;
    assert_eq!(done.status, EffectStatus::Committed);
    assert_eq!(done.attempt_count, 2);
    assert_eq!(
        executions.load(Ordering::SeqCst),
        2,
        "the failed attempt and one retry"
    );
}
