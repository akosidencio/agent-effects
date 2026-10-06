//! Approval: durable human decisions before an effect runs.

use std::collections::VecDeque;
use std::io::{Cursor, Write};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex, Once};
use std::time::Duration;

use agent_effects::fault::{FaultInjector, FaultPoint};
use agent_effects::handler::{EffectHandler, Handler};
use agent_effects::store::{EffectStore, ErrorRecord};
use agent_effects::testkit::{Behavior, FakeRemote};
use agent_effects::{
    ApprovalDecision, ApprovalProvider, ApprovalRequest, CliApproval, EffectContext, EffectFailure,
    EffectKey, EffectName, EffectOutcome, EffectStatus, FailureClass, LogicalKey, Precondition,
    Runtime, TokioClock, Transition, WorkerId,
};
use agent_effects_memory::MemoryStore;
use serde_json::json;

const TTL: Duration = Duration::from_secs(30);

/// Answers from a script; records every request.
#[derive(Clone, Default)]
struct Scripted {
    decisions: Arc<Mutex<VecDeque<ApprovalDecision>>>,
    asked: Arc<Mutex<Vec<ApprovalRequest>>>,
}

impl Scripted {
    fn new(decisions: impl IntoIterator<Item = ApprovalDecision>) -> Self {
        Self {
            decisions: Arc::new(Mutex::new(decisions.into_iter().collect())),
            asked: Arc::default(),
        }
    }

    fn asked(&self) -> Vec<ApprovalRequest> {
        self.asked.lock().unwrap().clone()
    }
}

impl ApprovalProvider for Scripted {
    fn request(
        &self,
        request: ApprovalRequest,
    ) -> impl std::future::Future<Output = ApprovalDecision> + Send {
        self.asked.lock().unwrap().push(request);
        std::future::ready(
            self.decisions
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or(ApprovalDecision::Deferred),
        )
    }
}

fn approved(by: &str) -> ApprovalDecision {
    ApprovalDecision::Approved { by: by.into() }
}

fn runtime(
    store: &MemoryStore,
    clock: TokioClock,
    provider: Option<Scripted>,
) -> Runtime<MemoryStore> {
    let mut builder = Runtime::builder(store.clone())
        .clock(clock)
        .worker_id(WorkerId::new("w"))
        .lease_ttl(TTL);
    if let Some(provider) = provider {
        builder = builder.approval_provider(provider);
    }
    builder.build()
}

fn key(name: &str, logical: &str) -> EffectKey {
    EffectKey::new(
        EffectName::new(name).unwrap(),
        LogicalKey::new(logical).unwrap(),
    )
}

async fn trail(store: &MemoryStore, key: &EffectKey) -> Vec<(Transition, Option<String>)> {
    let record = store.get_by_key(key).await.unwrap().unwrap();
    store
        .events(record.id)
        .await
        .unwrap()
        .into_iter()
        .map(|e| (e.transition, e.actor))
        .collect()
}

/// Deletes a cluster (counting runs); requires approval.
async fn delete_cluster(rt: &Runtime<MemoryStore>, runs: &Arc<AtomicU32>) -> EffectOutcome<String> {
    let runs = Arc::clone(runs);
    rt.effect("cluster.delete", "prod-eu")
        .input(&json!({ "cluster": "prod-eu" }))
        .actor("agent:ops")
        .require_approval()
        .run(move |_| {
            runs.fetch_add(1, Ordering::SeqCst);
            async { Ok::<_, EffectFailure>("deleted".to_string()) }
        })
        .await
        .unwrap()
}

#[tokio::test(start_paused = true)]
async fn an_approved_effect_runs_and_records_the_approver() {
    let store = MemoryStore::new();
    let provider = Scripted::new([approved("alice")]);
    let rt = runtime(&store, TokioClock::new(), Some(provider.clone()));
    let runs = Arc::new(AtomicU32::new(0));

    assert_eq!(
        delete_cluster(&rt, &runs).await,
        EffectOutcome::Committed("deleted".into())
    );
    assert_eq!(runs.load(Ordering::SeqCst), 1);
    let asked = provider.asked();
    assert_eq!(asked.len(), 1);
    assert_eq!(asked[0].input, Some(json!({ "cluster": "prod-eu" })));
    assert_eq!(asked[0].requested_by.as_deref(), Some("agent:ops"));
    let trail = trail(&store, &key("cluster.delete", "prod-eu")).await;
    assert_eq!(trail[0].0, Transition::RequestApproval);
    assert_eq!(trail[1], (Transition::Approve, Some("alice".into())));
    assert_eq!(trail[2].0, Transition::StartAttempt);
}

#[tokio::test(start_paused = true)]
async fn a_denied_effect_never_runs() {
    let store = MemoryStore::new();
    let provider = Scripted::new([ApprovalDecision::Denied {
        by: "bob".into(),
        reason: "not during business hours".into(),
    }]);
    let rt = runtime(&store, TokioClock::new(), Some(provider));
    let runs = Arc::new(AtomicU32::new(0));

    assert_eq!(
        delete_cluster(&rt, &runs).await,
        EffectOutcome::Rejected(ErrorRecord {
            class: None,
            message: "not during business hours".into(),
        })
    );
    assert_eq!(runs.load(Ordering::SeqCst), 0);
    assert_eq!(
        trail(&store, &key("cluster.delete", "prod-eu")).await,
        [
            (Transition::RequestApproval, Some("agent:ops".into())),
            (Transition::Deny, Some("bob".into())),
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn without_a_provider_an_operator_decides() {
    let store = MemoryStore::new();
    let rt = runtime(&store, TokioClock::new(), None);
    let runs = Arc::new(AtomicU32::new(0));

    let waiting = delete_cluster(&rt, &runs).await;
    let EffectOutcome::AwaitingApproval { id } = waiting else {
        panic!("{waiting:?}");
    };
    assert_eq!(delete_cluster(&rt, &runs).await, waiting, "still waiting");
    assert_eq!(runs.load(Ordering::SeqCst), 0);
    let pending = rt.pending(None, 10).await.unwrap();
    assert_eq!(pending.len(), 1, "operators see it in pending()");

    let record = rt
        .approve(id, "operator:dennis", "change ticket CHG-12")
        .await
        .unwrap();
    assert_eq!(record.status, EffectStatus::Pending);
    assert!(record.approved);
    assert_eq!(
        delete_cluster(&rt, &runs).await,
        EffectOutcome::Committed("deleted".into())
    );
    assert_eq!(runs.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn an_operator_can_deny() {
    let store = MemoryStore::new();
    let rt = runtime(&store, TokioClock::new(), None);
    let runs = Arc::new(AtomicU32::new(0));
    let EffectOutcome::AwaitingApproval { id } = delete_cluster(&rt, &runs).await else {
        panic!("expected to wait for approval");
    };
    rt.deny(id, "operator:dennis", "wrong cluster")
        .await
        .unwrap();
    assert_eq!(
        delete_cluster(&rt, &runs).await,
        EffectOutcome::Rejected(ErrorRecord {
            class: None,
            message: "wrong cluster".into(),
        })
    );
    assert_eq!(runs.load(Ordering::SeqCst), 0);
}

#[tokio::test(start_paused = true)]
async fn a_deferred_decision_is_asked_again_on_the_next_call() {
    let store = MemoryStore::new();
    let provider = Scripted::new([ApprovalDecision::Deferred, approved("carol")]);
    let rt = runtime(&store, TokioClock::new(), Some(provider.clone()));
    let runs = Arc::new(AtomicU32::new(0));

    assert!(matches!(
        delete_cluster(&rt, &runs).await,
        EffectOutcome::AwaitingApproval { .. }
    ));
    assert_eq!(
        delete_cluster(&rt, &runs).await,
        EffectOutcome::Committed("deleted".into())
    );
    let asked = provider.asked();
    assert_eq!(asked.len(), 2);
    assert_eq!(
        asked[0].effect_id, asked[1].effect_id,
        "same effect, so providers can deduplicate"
    );
}

#[tokio::test(start_paused = true)]
async fn approval_is_asked_once_even_when_the_effect_retries() {
    let store = MemoryStore::new();
    let clock = TokioClock::new();
    let remote = FakeRemote::new(clock).script([Behavior::Fail(FailureClass::Transient)]);
    let provider = Scripted::new([approved("alice")]);
    let rt = runtime(&store, clock, Some(provider.clone()));
    let outcome = rt
        .effect("cluster.scale", "prod-eu")
        .require_approval()
        .run(move |_| {
            let remote = remote.clone();
            async move { remote.create("scale", None).await }
        })
        .await
        .unwrap();
    assert_eq!(outcome, EffectOutcome::Committed("scale#1".into()));
    assert_eq!(provider.asked().len(), 1);
}

#[tokio::test(start_paused = true)]
async fn the_precondition_is_checked_again_after_approval() {
    let store = MemoryStore::new();
    let rt = runtime(&store, TokioClock::new(), None);
    let still_valid = Arc::new(AtomicBool::new(true));
    let runs = Arc::new(AtomicU32::new(0));
    let refund = || {
        let (still_valid, runs) = (Arc::clone(&still_valid), Arc::clone(&runs));
        rt.effect("payment.refund", "order-9")
            .require_approval()
            .precondition(move |_| {
                let valid = still_valid.load(Ordering::SeqCst);
                async move {
                    if valid {
                        Precondition::Satisfied
                    } else {
                        Precondition::reject("already refunded by a human")
                    }
                }
            })
            .run(move |_| {
                runs.fetch_add(1, Ordering::SeqCst);
                async { Ok::<_, EffectFailure>(()) }
            })
    };

    let EffectOutcome::AwaitingApproval { id } = refund().await.unwrap() else {
        panic!("expected to wait for approval");
    };
    // While the approval sat in a queue, someone refunded by hand.
    still_valid.store(false, Ordering::SeqCst);
    rt.approve(id, "operator:dennis", "ok").await.unwrap();
    let outcome = refund().await.unwrap();
    assert!(
        matches!(&outcome, EffectOutcome::Rejected(e) if e.message == "already refunded by a human"),
        "{outcome:?}"
    );
    assert_eq!(
        runs.load(Ordering::SeqCst),
        0,
        "a stale approval must not run"
    );
}

struct DeleteCluster {
    runs: Arc<AtomicU32>,
}

impl EffectHandler for DeleteCluster {
    const NAME: &'static str = "cluster.delete";
    type Input = String;
    type Output = String;
    type Error = EffectFailure;

    fn requires_approval(&self) -> bool {
        true
    }

    fn execute(
        &self,
        _: &EffectContext,
        cluster: &String,
    ) -> impl std::future::Future<Output = Result<String, EffectFailure>> + Send {
        self.runs.fetch_add(1, Ordering::SeqCst);
        std::future::ready(Ok(format!("deleted {cluster}")))
    }
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

#[tokio::test(start_paused = true)]
async fn a_pending_approval_survives_a_crash_and_recovery_runs_it_once_approved() {
    quiet_crashes();
    let store = MemoryStore::new();
    let clock = TokioClock::new();
    let runs = Arc::new(AtomicU32::new(0));
    let handler = || {
        Handler::new(DeleteCluster {
            runs: Arc::clone(&runs),
        })
    };

    let doomed = Runtime::builder(store.clone())
        .clock(clock)
        .lease_ttl(TTL)
        .register(handler())
        .fault_injector(Arc::new(
            FaultInjector::new()
                .at(FaultPoint::AfterApprovalRequested)
                .crash(),
        ))
        .build();
    assert!(
        doomed
            .submit::<DeleteCluster>("prod-eu", "prod-eu".into())
            .await
            .is_err()
    );
    tokio::time::sleep(TTL).await;

    let restarted = Runtime::builder(store.clone())
        .clock(clock)
        .register(handler())
        .build();
    let report = restarted.recover().await.unwrap();
    assert!(
        report.resumed.is_empty(),
        "recovery never decides an approval: {report:?}"
    );
    let pending = restarted.pending(None, 10).await.unwrap();
    assert_eq!(pending[0].status, EffectStatus::AwaitingApproval);

    restarted
        .approve(
            pending[0].id,
            "operator:dennis",
            "approved in the change board",
        )
        .await
        .unwrap();
    let report = restarted.recover().await.unwrap();
    assert_eq!(report.resumed, [(pending[0].id, EffectStatus::Committed)]);
    assert_eq!(runs.load(Ordering::SeqCst), 1);
}

/// A writer whose contents the test can read back.
#[derive(Clone, Default)]
struct Captured(Arc<Mutex<Vec<u8>>>);

impl Write for Captured {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn the_cli_provider_reads_a_decision() {
    let request = ApprovalRequest {
        effect_id: agent_effects::EffectId::new(),
        key: key("cluster.delete", "prod-eu"),
        kind: agent_effects::EffectKind::IrreversibleWrite,
        risk: agent_effects::RiskLevel::Critical,
        input: Some(json!({ "cluster": "prod-eu" })),
        requested_by: Some("agent:ops".into()),
    };
    for (answer, expected) in [
        ("y\n", approved("cli:dennis")),
        ("YES\n", approved("cli:dennis")),
        (
            "n\n",
            ApprovalDecision::Denied {
                by: "cli:dennis".into(),
                reason: "denied at the command line".into(),
            },
        ),
        ("", ApprovalDecision::Deferred),
    ] {
        let prompt = Captured::default();
        let cli = CliApproval::with_io(Cursor::new(answer.to_string()), prompt.clone())
            .approver("cli:dennis");
        assert_eq!(
            cli.request(request.clone()).await,
            expected,
            "answer {answer:?}"
        );
        let shown = String::from_utf8(prompt.0.lock().unwrap().clone()).unwrap();
        assert!(shown.contains("cluster.delete:prod-eu"), "{shown}");
        assert!(shown.contains("critical risk"), "{shown}");
        assert!(shown.contains(r#"{"cluster":"prod-eu"}"#), "{shown}");
        assert!(shown.ends_with("Approve? [y/N]: "), "{shown}");
    }
}
