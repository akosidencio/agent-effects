//! The operational instruments: attempts and retries, verification,
//! approvals, operator resolutions, leases, recovery and pruning, and the
//! guarantee that no attribute is unbounded or identifies an effect.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use agent_effects::handler::{EffectHandler, Handler, VerifiableEffect};
use agent_effects::store::EffectStore;
use agent_effects::testkit::{Behavior, FakeRemote};
use agent_effects::{
    EffectContext, EffectFailure, EffectKey, EffectKind, EffectName, EffectOutcome, FailureClass,
    LogicalKey, RecoveryConfig, Resolution, RetentionPolicy, RetryPolicy, Runtime, SystemClock,
    TokioClock, Verification, WorkerId,
};
use agent_effects_memory::MemoryStore;
use agent_effects_otel::OtelObserver;
use opentelemetry::metrics::MeterProvider;
use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, PeriodicReader, SdkMeterProvider};

type Attributes = BTreeMap<String, String>;

/// Every data point of every metric after one flush: counter and gauge
/// values, histogram counts.
struct Points(BTreeMap<String, Vec<(Attributes, f64)>>);

impl Points {
    fn collect(provider: &SdkMeterProvider, exporter: &InMemoryMetricExporter) -> Self {
        provider.force_flush().unwrap();
        let batches = exporter.get_finished_metrics().unwrap();
        let mut points: BTreeMap<String, Vec<(Attributes, f64)>> = BTreeMap::new();
        let last = batches.last().expect("one export");
        for scope in last.scope_metrics() {
            for metric in scope.metrics() {
                let entry = points.entry(metric.name().to_owned()).or_default();
                let attrs = |kvs: &mut dyn Iterator<Item = &opentelemetry::KeyValue>| {
                    kvs.map(|kv| (kv.key.to_string(), kv.value.to_string()))
                        .collect::<Attributes>()
                };
                #[allow(clippy::cast_precision_loss)]
                match metric.data() {
                    AggregatedMetrics::U64(MetricData::Sum(sum)) => {
                        for p in sum.data_points() {
                            entry.push((attrs(&mut p.attributes()), p.value() as f64));
                        }
                    }
                    AggregatedMetrics::I64(MetricData::Sum(sum)) => {
                        for p in sum.data_points() {
                            entry.push((attrs(&mut p.attributes()), p.value() as f64));
                        }
                    }
                    AggregatedMetrics::U64(MetricData::Gauge(gauge)) => {
                        for p in gauge.data_points() {
                            entry.push((attrs(&mut p.attributes()), p.value() as f64));
                        }
                    }
                    AggregatedMetrics::F64(MetricData::Histogram(histogram)) => {
                        for p in histogram.data_points() {
                            entry.push((attrs(&mut p.attributes()), p.count() as f64));
                        }
                    }
                    other => panic!("unexpected aggregation for {}: {other:?}", metric.name()),
                }
            }
        }
        Self(points)
    }

    /// The total over points whose attributes include `with`.
    fn value(&self, name: &str, with: &[(&str, &str)]) -> f64 {
        self.0
            .get(name)
            .into_iter()
            .flatten()
            .filter(|(attrs, _)| {
                with.iter()
                    .all(|(k, v)| attrs.get(*k).map(String::as_str) == Some(*v))
            })
            .map(|(_, v)| v)
            .sum()
    }
}

fn setup() -> (SdkMeterProvider, InMemoryMetricExporter, OtelObserver) {
    let exporter = InMemoryMetricExporter::default();
    let provider = SdkMeterProvider::builder()
        .with_reader(PeriodicReader::builder(exporter.clone()).build())
        .build();
    let observer = OtelObserver::new(&provider.meter("test"));
    (provider, exporter, observer)
}

const QUICK: RetryPolicy = RetryPolicy {
    max_attempts: 3,
    initial_delay: Duration::from_secs(2),
    max_delay: Duration::from_secs(2),
    multiplier: 1.0,
    jitter: false,
};

/// A verifiable create on a `FakeRemote`.
struct Create(FakeRemote);

impl EffectHandler for Create {
    const NAME: &'static str = "resource.create";
    type Input = String;
    type Output = String;
    type Error = EffectFailure;
    async fn execute(&self, _: &EffectContext, name: &String) -> Result<String, EffectFailure> {
        self.0.create(name, None).await
    }
}

impl VerifiableEffect for Create {
    async fn verify(
        &self,
        _: &EffectContext,
        name: &String,
    ) -> Result<Verification<String>, EffectFailure> {
        Ok(match self.0.find(name).await? {
            Some(id) => Verification::Confirmed(id),
            None => Verification::NotApplied,
        })
    }
}

#[tokio::test(start_paused = true)]
async fn attempts_retries_verification_approvals_and_operators_are_measured() {
    let (provider, exporter, observer) = setup();
    let clock = TokioClock::new();
    // Lost once (verified not applied, re-run), then dropped after applying
    // (verified applied).
    let remote = FakeRemote::new(clock).script([Behavior::LoseRequest, Behavior::CommitThenDrop]);
    let rt = Runtime::builder(MemoryStore::new())
        .clock(clock)
        .retry_policy(QUICK)
        .observer(observer)
        .register(Handler::new(Create(remote.clone())).verifiable())
        .build();

    assert!(matches!(
        rt.submit::<Create>("k1", "a".into()).await.unwrap(),
        EffectOutcome::Committed(_)
    ));
    assert!(matches!(
        rt.submit::<Create>("k2", "b".into()).await.unwrap(),
        EffectOutcome::Committed(_)
    ));

    approve_one_and_deny_one(&rt).await;

    // Escalated, then resolved by an operator.
    let EffectOutcome::NeedsIntervention { id: lost } = rt
        .effect("email.send", "e1")
        .run(|_| async { Err::<(), _>(EffectFailure::ambiguous("timeout")) })
        .await
        .unwrap()
    else {
        panic!()
    };
    let points = Points::collect(&provider, &exporter);
    assert_eq!(
        points.value("agent_effects.awaiting_intervention", &[]),
        1.0
    );
    rt.resolve(lost, Resolution::NotApplied, "operator:a", "checked")
        .await
        .unwrap();

    let points = Points::collect(&provider, &exporter);
    let create = ("effect.name", "resource.create");
    assert_eq!(
        points.value("agent_effects.attempts", &[create, ("attempt", "first")]),
        2.0
    );
    assert_eq!(
        points.value("agent_effects.attempts", &[create, ("attempt", "retry")]),
        1.0
    );
    assert_eq!(points.value("agent_effects.retry.delay", &[create]), 1.0);
    // The lost request, its re-run's result, and the dropped answer.
    assert_eq!(
        points.value("agent_effects.verification.started", &[create]),
        3.0
    );
    assert_eq!(
        points.value(
            "agent_effects.verification.outcomes",
            &[("outcome", "not_applied")]
        ),
        1.0
    );
    assert_eq!(
        points.value(
            "agent_effects.verification.outcomes",
            &[("outcome", "confirmed")]
        ),
        2.0
    );
    assert_eq!(
        points.value(
            "agent_effects.approval.decisions",
            &[("decision", "approved")]
        ),
        1.0
    );
    assert_eq!(
        points.value(
            "agent_effects.approval.decisions",
            &[("decision", "denied")]
        ),
        1.0
    );
    assert_eq!(
        points.value("agent_effects.approval.duration", &[("decision", "denied")]),
        1.0
    );
    assert_eq!(
        points.value(
            "agent_effects.operator.resolutions",
            &[("resolution", "not_applied")]
        ),
        1.0
    );
    assert_eq!(
        points.value("agent_effects.awaiting_intervention", &[]),
        0.0,
        "resolved, so no longer waiting"
    );
    assert_eq!(remote.applications("a") + remote.applications("b"), 2);
}

/// Two effects awaiting approval: one approved after a minute, the other
/// denied after two.
async fn approve_one_and_deny_one<S: EffectStore>(rt: &Runtime<S>) {
    let ask = |key: &'static str| {
        rt.effect("refund.issue", key)
            .require_approval()
            .run(|_| async { Ok::<_, EffectFailure>(()) })
    };
    let EffectOutcome::AwaitingApproval { id: yes } = ask("r1").await.unwrap() else {
        panic!()
    };
    let EffectOutcome::AwaitingApproval { id: no } = ask("r2").await.unwrap() else {
        panic!()
    };
    tokio::time::sleep(Duration::from_secs(60)).await;
    rt.approve(yes, "operator:a", "ok").await.unwrap();
    tokio::time::sleep(Duration::from_secs(60)).await;
    rt.deny(no, "operator:a", "no").await.unwrap();
}

/// A durable job that never finishes on its own.
struct Stuck;

impl EffectHandler for Stuck {
    const NAME: &'static str = "job.stuck";
    type Input = u32;
    type Output = u32;
    type Error = EffectFailure;
    async fn execute(&self, _: &EffectContext, n: &u32) -> Result<u32, EffectFailure> {
        std::future::pending::<()>().await;
        Ok(*n)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn leases_recovery_and_pruning_are_measured_without_identifying_effects() {
    let (provider, exporter, observer) = setup();
    let store = MemoryStore::new();
    let worker = |name: &str| {
        Runtime::builder(store.clone())
            .clock(SystemClock)
            .worker_id(WorkerId::new(name))
            .lease_ttl(Duration::from_secs(60))
            .recovery(RecoveryConfig::SEQUENTIAL.max_concurrency(4))
            .retention(RetentionPolicy::settled(Duration::ZERO))
            .observer(observer.clone())
            .register(Handler::new(Stuck))
            .build()
    };
    let (rt, other) = (worker("worker-1"), worker("worker-2"));

    // Three jobs that never finish, holding their leases.
    let keys: Vec<String> = (0..3)
        .map(|n| format!("customer-{n}@example.com"))
        .collect();
    let stuck: Vec<_> = keys
        .iter()
        .zip(0..)
        .map(|(key, n)| {
            let (rt, key) = (rt.clone(), key.clone());
            tokio::spawn(async move { rt.submit::<Stuck>(key, n).await })
        })
        .collect();
    tokio::time::timeout(Duration::from_secs(30), async {
        while leased(&store, &keys).await < keys.len() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the stuck jobs start");
    let mut ids = Vec::new();
    for key in &keys {
        ids.push(record(&store, Stuck::NAME, key).await.id);
    }

    let scheduled = escalate_one_and_schedule_one(&rt, &store).await;

    // Contention: another worker asking for a held job is told it is in
    // progress, and its recovery pass finds the rest waiting.
    assert!(matches!(
        other.submit::<Stuck>(keys[0].clone(), 0).await.unwrap(),
        EffectOutcome::InProgress { .. }
    ));
    let report = other.recover().await.unwrap();
    assert_eq!(report.waiting_for_operator.len(), 1, "{report:?}");
    assert_eq!(report.not_due.len(), 1, "{report:?}");
    for task in stuck {
        task.abort();
    }
    scheduled.abort();

    // A committed effect, pruned at once.
    rt.effect("note.write", "customer-7@example.com")
        .run(|_| async { Ok::<_, EffectFailure>(()) })
        .await
        .unwrap();
    assert_eq!(rt.prune().await.unwrap().committed, 1);

    let points = Points::collect(&provider, &exporter);
    assert_eq!(
        points.value(
            "agent_effects.lease.conflicts",
            &[("effect.name", Stuck::NAME)]
        ),
        1.0
    );
    assert_eq!(
        points.value("agent_effects.recovery.passes", &[("result", "completed")]),
        1.0
    );
    assert_eq!(points.value("agent_effects.recovery.duration", &[]), 1.0);
    assert_eq!(
        points.value(
            "agent_effects.recovery.backlog",
            &[("state", "awaiting_operator")]
        ),
        1.0
    );
    assert_eq!(
        points.value("agent_effects.recovery.backlog", &[("state", "not_due")]),
        1.0
    );
    assert_eq!(
        points.value("agent_effects.pruned", &[("status", "committed")]),
        1.0
    );

    assert_bounded(&points, &ids);
}

/// One escalated effect waiting for a person, and one retry an hour out
/// (its caller's task is returned, waiting).
async fn escalate_one_and_schedule_one(
    rt: &Runtime<MemoryStore>,
    store: &MemoryStore,
) -> tokio::task::JoinHandle<Result<EffectOutcome<()>, agent_effects::RuntimeError>> {
    rt.effect("email.send", "customer-9@example.com")
        .run(|_| async { Err::<(), _>(EffectFailure::ambiguous("timeout")) })
        .await
        .unwrap();
    let scheduled = tokio::spawn(
        rt.effect("sms.send", "customer-8@example.com")
            .kind(EffectKind::IdempotentWrite)
            .retry(RetryPolicy {
                initial_delay: Duration::from_secs(3600),
                max_delay: Duration::from_secs(3600),
                ..QUICK
            })
            .run(|ctx: EffectContext| async move {
                if ctx.attempt() == 1 {
                    Err(EffectFailure::new(FailureClass::Transient, "busy"))
                } else {
                    Ok(())
                }
            }),
    );
    tokio::time::timeout(Duration::from_secs(30), async {
        while scheduled_at(store).await.is_none() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the retry is scheduled");
    scheduled
}

/// Every attribute is from a small fixed set: no ids, no logical keys, no
/// actors or workers.
fn assert_bounded(points: &Points, ids: &[agent_effects::EffectId]) {
    let allowed: BTreeSet<&str> = [
        "effect.name",
        "effect.kind",
        "outcome",
        "attempt",
        "decision",
        "resolution",
        "result",
        "state",
        "status",
    ]
    .into();
    assert!(!points.0.is_empty());
    for (name, series) in &points.0 {
        for (attrs, _) in series {
            for (key, value) in attrs {
                assert!(allowed.contains(key.as_str()), "{name}: attribute {key}");
                assert!(
                    !value.contains('@'),
                    "{name}: {key}={value} identifies someone"
                );
                assert!(
                    ids.iter().all(|id| !value.contains(&id.to_string())),
                    "{name}: {key}={value} is an effect id"
                );
                assert!(!value.contains("worker-"), "{name}: {key}={value}");
            }
        }
    }
}

async fn record(store: &MemoryStore, name: &str, key: &str) -> agent_effects::EffectRecord {
    let key = EffectKey::new(
        EffectName::new(name).unwrap(),
        LogicalKey::new(key).unwrap(),
    );
    store.get_by_key(&key).await.unwrap().unwrap()
}

async fn scheduled_at(store: &MemoryStore) -> Option<std::time::SystemTime> {
    let key = EffectKey::new(
        EffectName::new("sms.send").unwrap(),
        LogicalKey::new("customer-8@example.com").unwrap(),
    );
    store.get_by_key(&key).await.unwrap()?.next_attempt_at
}

async fn leased(store: &MemoryStore, keys: &[String]) -> usize {
    let mut leased = 0;
    for key in keys {
        let key = EffectKey::new(
            EffectName::new(Stuck::NAME).unwrap(),
            LogicalKey::new(key.as_str()).unwrap(),
        );
        if let Some(record) = store.get_by_key(&key).await.unwrap()
            && record.attempt_count == 1
            && record
                .live_lease_owner(std::time::SystemTime::now())
                .is_some()
        {
            leased += 1;
        }
    }
    leased
}
