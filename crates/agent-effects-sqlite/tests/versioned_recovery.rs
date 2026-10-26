//! Different handler implementations on either side of real process death.
use std::future::{Future, ready};
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use agent_effects::fault::{FaultInjector, FaultPoint};
use agent_effects::store::{ExecutionContract, NewEffect};
use agent_effects::{
    Clock, CompensableEffect, CompensationContext, EffectContext, EffectFailure, EffectHandler,
    EffectKey, EffectKind, EffectName, EffectOutcome, EffectRecord, EffectStatus, EffectStore,
    Handler, LogicalKey, PayloadError, PayloadSchema, RetryPolicy, Runtime, RuntimeError, WorkerId,
};
use agent_effects_sqlite::SqliteStore;
use serde::{Deserialize, Serialize};
use serde_json::json;

const CHILD: &str = "AGENT_EFFECTS_VERSIONED_CHILD";
const CHILD_DIR: &str = "AGENT_EFFECTS_VERSIONED_DIR";
const TTL: Duration = Duration::from_secs(60);
const RETRY: RetryPolicy = RetryPolicy {
    max_attempts: 5,
    initial_delay: Duration::ZERO,
    max_delay: Duration::ZERO,
    multiplier: 1.0,
    jitter: false,
};

#[derive(Clone)]
struct Remote(PathBuf);
impl Remote {
    fn apply(&self, kind: &str, key: &impl ToString) {
        let dir = self.0.join(kind);
        std::fs::create_dir_all(&dir).unwrap();
        // Creating the marker is the mock external operation itself. Its
        // exclusive creation is atomic and deduplicates across processes.
        match std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(dir.join(key.to_string()))
        {
            Ok(file) => file.sync_all().unwrap(),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => panic!("mock provider: {error}"),
        }
    }
    fn count(&self, kind: &str) -> usize {
        match std::fs::read_dir(self.0.join(kind)) {
            Ok(files) => files.count(),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
            Err(error) => panic!("mock provider: {error}"),
        }
    }
}

struct Old {
    remote: Remote,
    abort_undo: bool,
}
impl EffectHandler for Old {
    const NAME: &'static str = "versioned.action";
    type Input = u64;
    type Output = u64;
    type Error = EffectFailure;
    fn remote_idempotency(&self) -> bool {
        true
    }
    fn execute(
        &self,
        ctx: &EffectContext,
        input: &u64,
    ) -> impl Future<Output = Result<u64, EffectFailure>> + Send {
        self.remote.apply("apply", &ctx.idempotency_key());
        ready(Ok(*input))
    }
}
impl CompensableEffect for Old {
    fn compensate(
        &self,
        ctx: &CompensationContext,
        _: &u64,
        _: Option<&u64>,
    ) -> impl Future<Output = Result<(), EffectFailure>> + Send {
        self.remote.apply("undo", &ctx.idempotency_key());
        if self.abort_undo {
            std::process::abort();
        }
        ready(Ok(()))
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Current {
    value: u64,
}
struct New {
    remote: Remote,
    contract: &'static str,
    migrate_output: bool,
}

fn schema() -> PayloadSchema {
    PayloadSchema::new(2).migrate(|version, value| {
        if version != 1 {
            return Err(PayloadError::UnsupportedVersion {
                stored: version,
                current: 2,
            });
        }
        let value = value
            .as_u64()
            .ok_or_else(|| PayloadError::Migration("expected legacy integer".into()))?;
        Ok(json!({ "value": value }))
    })
}
impl EffectHandler for New {
    const NAME: &'static str = Old::NAME;
    type Input = Current;
    type Output = Current;
    type Error = EffectFailure;
    fn remote_idempotency(&self) -> bool {
        true
    }
    fn contract_version(&self) -> &str {
        self.contract
    }
    fn input_schema(&self) -> PayloadSchema {
        schema()
    }
    fn output_schema(&self) -> PayloadSchema {
        if self.migrate_output {
            schema()
        } else {
            PayloadSchema::new(2)
        }
    }
    fn execute(
        &self,
        ctx: &EffectContext,
        input: &Current,
    ) -> impl Future<Output = Result<Current, EffectFailure>> + Send {
        self.remote.apply("apply", &ctx.idempotency_key());
        ready(Ok(Current { value: input.value }))
    }
}
impl CompensableEffect for New {
    fn compensate(
        &self,
        ctx: &CompensationContext,
        input: &Current,
        output: Option<&Current>,
    ) -> impl Future<Output = Result<(), EffectFailure>> + Send {
        assert_eq!(output.unwrap(), input);
        self.remote.apply("undo", &ctx.idempotency_key());
        ready(Ok(()))
    }
}

fn key() -> EffectKey {
    EffectKey::new(
        EffectName::new(Old::NAME).unwrap(),
        LogicalKey::new("one").unwrap(),
    )
}

#[test]
fn versioned_child() {
    let Ok(mode) = std::env::var(CHILD) else {
        return;
    };
    let dir = PathBuf::from(std::env::var_os(CHILD_DIR).unwrap());
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let store = SqliteStore::open(dir.join("effects.db")).await.unwrap();
            if mode == "legacy" || mode == "future-contract" {
                let mut new =
                    NewEffect::new(key(), EffectKind::IrreversibleWrite, SystemTime::now());
                new.input = Some(json!(7));
                if mode == "future-contract" {
                    let mut contract =
                        ExecutionContract::new(EffectKind::IrreversibleWrite, RETRY.max_attempts);
                    contract.format_version = 2;
                    contract.remote_idempotency = true;
                    new.execution_contract = Some(contract);
                }
                store.insert_or_get(new).await.unwrap();
                std::process::abort();
            }
            let fault = match mode.as_str() {
                "insert" => Some(FaultPoint::AfterInsert),
                "attempt" => Some(FaultPoint::AfterAttemptPersisted),
                "applied" => Some(FaultPoint::AfterActionReturned),
                "undo-intent" => Some(FaultPoint::AfterCompensationStarted),
                "committed" | "undo-applied" => None,
                _ => panic!("unknown child mode"),
            };
            let mut builder = Runtime::builder(store)
                .lease_ttl(TTL)
                .retry_policy(RETRY)
                .register(
                    Handler::new(Old {
                        remote: Remote(dir.clone()),
                        abort_undo: mode == "undo-applied",
                    })
                    .compensable(),
                );
            if let Some(point) = fault {
                builder = builder.fault_injector(Arc::new(FaultInjector::new().at(point).abort()));
            }
            let rt = builder.build();
            assert_eq!(
                rt.submit::<Old>("one", 7).await.unwrap(),
                EffectOutcome::Committed(7)
            );
            if mode.starts_with("undo-") {
                rt.compensate::<Old>("one").await.unwrap();
            }
            // Committed output must survive termination without Drop/cleanup.
            std::process::abort();
        });
}

fn assert_aborted(status: ExitStatus) {
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        assert_eq!(
            status.signal(),
            Some(6),
            "expected SIGABRT, not a setup panic: {status}"
        );
    }
    #[cfg(not(unix))]
    assert!(!status.success());
}

fn produce(dir: &Path, mode: &str) {
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "versioned_child",
            "--exact",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(CHILD, mode)
        .env(CHILD_DIR, dir)
        .current_dir(dir)
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            assert_aborted(status);
            return;
        }
        if Instant::now() >= deadline {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!("versioned child timed out");
        }
        std::thread::sleep(Duration::from_millis(5));
    }
}

struct Expired;
impl Clock for Expired {
    fn now(&self) -> SystemTime {
        SystemTime::now() + TTL + Duration::from_secs(1)
    }
}

async fn restarted(
    dir: &Path,
    contract: &'static str,
    migrate_output: bool,
) -> Runtime<SqliteStore> {
    Runtime::builder(SqliteStore::open(dir.join("effects.db")).await.unwrap())
        .clock(Expired)
        .lease_ttl(TTL)
        .retry_policy(RETRY)
        .worker_id(WorkerId::new("new-release"))
        .register(
            Handler::new(New {
                remote: Remote(dir.into()),
                contract,
                migrate_output,
            })
            .compensable(),
        )
        .build()
}
async fn record(rt: &Runtime<SqliteStore>) -> EffectRecord {
    rt.store().get_by_key(&key()).await.unwrap().unwrap()
}

#[tokio::test]
async fn process_restart_migrates_old_inputs_and_deduplicates_old_inflight_actions() {
    for mode in ["insert", "attempt", "applied"] {
        let dir = tempfile::tempdir().unwrap();
        produce(dir.path(), mode);
        let rt = restarted(dir.path(), "1", true).await;
        let before = record(&rt).await;
        let report = rt.recover().await.unwrap();
        assert!(report.resume_errors.is_empty(), "{mode}: {report:?}");
        let current = record(&rt).await;
        assert_eq!(current.status, EffectStatus::Committed);
        assert_eq!(current.input, Some(json!(7)));
        assert_eq!(current.input_fingerprint, before.input_fingerprint);
        assert_eq!(current.input_schema_version, 1);
        assert_eq!(current.output_schema_version, 2);
        assert_eq!(current.output, Some(json!({ "value": 7 })));
        assert_eq!(Remote(dir.path().into()).count("apply"), 1);
    }
}

#[tokio::test]
async fn incompatible_deployment_blocks_then_a_compatible_handler_can_recover() {
    let dir = tempfile::tempdir().unwrap();
    produce(dir.path(), "applied");
    let incompatible = restarted(dir.path(), "2", true).await;
    let report = incompatible.recover().await.unwrap();
    assert_eq!(report.resume_errors.len(), 1);
    assert!(report.resume_errors[0].1.contains("execution contract"));
    assert_eq!(record(&incompatible).await.status, EffectStatus::Unknown);
    assert_eq!(Remote(dir.path().into()).count("apply"), 1);
    let compatible = restarted(dir.path(), "1", true).await;
    assert_eq!(
        compatible.recover().await.unwrap().resume_errors,
        [] as [(agent_effects::EffectId, String); 0]
    );
    assert_eq!(record(&compatible).await.status, EffectStatus::Committed);
    assert_eq!(Remote(dir.path().into()).count("apply"), 1);
}

#[tokio::test]
async fn legacy_and_unknown_contract_formats_fail_closed_after_process_death() {
    for mode in ["legacy", "future-contract"] {
        let dir = tempfile::tempdir().unwrap();
        produce(dir.path(), mode);
        let rt = restarted(dir.path(), "1", true).await;
        let before = record(&rt).await;
        let report = rt.recover().await.unwrap();
        assert_eq!(report.resume_errors.len(), 1);
        assert!(report.resume_errors[0].1.contains(if mode == "legacy" {
            "no execution contract"
        } else {
            "execution contract"
        }));
        assert_eq!(record(&rt).await, before);
        assert_eq!(Remote(dir.path().into()).count("apply"), 0);
    }
}

#[tokio::test]
async fn old_committed_output_requires_a_supported_migration_and_never_reexecutes() {
    let dir = tempfile::tempdir().unwrap();
    produce(dir.path(), "committed");
    let rt = restarted(dir.path(), "1", false).await;
    let before = record(&rt).await;
    assert_eq!(
        rt.recover().await.unwrap().resumed,
        [] as [(agent_effects::EffectId, EffectStatus); 0]
    );
    assert!(matches!(
        rt.wait_with_output_schema::<Current>(before.id, Duration::ZERO, PayloadSchema::new(2))
            .await,
        Err(RuntimeError::OutputPayload { .. })
    ));
    assert_eq!(
        rt.wait_with_output_schema::<Current>(before.id, Duration::ZERO, schema())
            .await
            .unwrap(),
        EffectOutcome::Committed(Current { value: 7 })
    );
    assert_eq!(record(&rt).await, before);
    assert_eq!(Remote(dir.path().into()).count("apply"), 1);
}

#[tokio::test]
async fn interrupted_old_compensation_migrates_both_payloads_and_deduplicates_undo() {
    for mode in ["undo-intent", "undo-applied"] {
        let dir = tempfile::tempdir().unwrap();
        produce(dir.path(), mode);
        let rt = restarted(dir.path(), "1", true).await;
        let before = record(&rt).await;
        assert_eq!(before.status, EffectStatus::Compensating);
        let report = rt.recover().await.unwrap();
        assert!(report.resume_errors.is_empty(), "{mode}: {report:?}");
        let current = record(&rt).await;
        assert_eq!(current.status, EffectStatus::Compensated);
        assert_eq!(current.input_fingerprint, before.input_fingerprint);
        assert_eq!(current.output, before.output);
        assert_eq!(Remote(dir.path().into()).count("apply"), 1);
        assert_eq!(Remote(dir.path().into()).count("undo"), 1);
    }
}

#[tokio::test]
async fn unreadable_compensation_output_never_invokes_the_new_compensator() {
    let dir = tempfile::tempdir().unwrap();
    produce(dir.path(), "undo-intent");
    let rt = restarted(dir.path(), "1", false).await;
    let report = rt.recover().await.unwrap();
    assert_eq!(
        report.resume_errors,
        [] as [(agent_effects::EffectId, String); 0]
    );
    let current = record(&rt).await;
    assert_eq!(current.status, EffectStatus::CompensationFailed);
    assert!(current.committed_at.is_some());
    assert_eq!(Remote(dir.path().into()).count("apply"), 1);
    assert_eq!(Remote(dir.path().into()).count("undo"), 0);
}
