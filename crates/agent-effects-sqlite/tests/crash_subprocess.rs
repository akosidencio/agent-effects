//! Crash suite with real process death: one test per crash point
//! (design §11), on SQLite.
//!
//! For each point and protection level, a child process (this test binary,
//! re-invoked) runs the effect and calls `abort()` at the point: no
//! destructors, no lease release, nothing flushed but what SQLite already
//! committed. The parent then waits out the child's lease, runs recovery and
//! re-runs the effect, as the restarted service would.
//!
//! The remote system is a file, so its state survives the child: each line
//! is one application of the side effect.
//!
//! The same is done for durable handlers, where the parent never calls
//! again: only `recover()` finishes the effect. Two more cases kill the
//! child right after it asks for approval (an operator approves, recovery
//! runs it) and in the middle of a compensation (recovery finishes undoing
//! it).

use std::fs::OpenOptions;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use std::sync::Arc;

use agent_effects::fault::{FaultInjector, FaultPoint};
use agent_effects::handler::{CompensableEffect, EffectHandler, Handler, VerifiableEffect};
use agent_effects::{
    CompensationContext, EffectContext, EffectFailure, EffectKey, EffectName, EffectOutcome,
    EffectRecord, EffectStatus, EffectStore, IdempotencyKey, LogicalKey, RetryPolicy, Runtime,
    RuntimeError, Verification, WorkerId,
};
use agent_effects_sqlite::SqliteStore;

const CHILD_ENV: &str = "AGENT_EFFECTS_SQLITE_CRASH";
const CHILD_TTL: Duration = Duration::from_millis(300);

const NO_WAIT: RetryPolicy = RetryPolicy {
    max_attempts: 5,
    initial_delay: Duration::ZERO,
    max_delay: Duration::ZERO,
    multiplier: 1.0,
    jitter: false,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Protection {
    Verified,
    Idempotent,
    Unprotected,
}

impl Protection {
    const ALL: [Self; 3] = [Self::Verified, Self::Idempotent, Self::Unprotected];

    fn parse(s: &str) -> Self {
        Self::ALL
            .into_iter()
            .find(|p| format!("{p:?}") == s)
            .unwrap()
    }
}

const POINTS: [FaultPoint; 6] = [
    FaultPoint::BeforeInsert,
    FaultPoint::AfterInsert,
    FaultPoint::AfterAttemptPersisted,
    FaultPoint::AfterActionStarted,
    FaultPoint::AfterActionReturned,
    FaultPoint::AfterVerificationStarted,
];

/// What the child runs before it dies.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    /// A closure effect; the restarted caller runs it again.
    Closure,
    /// A registered handler; only recovery finishes it.
    Handler,
    /// A handler that needs approval; an operator approves, recovery runs it.
    Approval,
    /// A committed handler effect being compensated; recovery finishes it.
    Compensation,
}

impl Mode {
    const ALL: [Self; 4] = [
        Self::Closure,
        Self::Handler,
        Self::Approval,
        Self::Compensation,
    ];

    fn parse(s: &str) -> Self {
        Self::ALL
            .into_iter()
            .find(|m| format!("{m:?}") == s)
            .unwrap()
    }
}

/// A remote system whose state is files: one line per application, and one
/// per distinct cancellation.
#[derive(Clone)]
struct FileRemote {
    path: PathBuf,
}

impl FileRemote {
    fn lines(&self) -> Vec<String> {
        std::fs::read_to_string(&self.path)
            .unwrap_or_default()
            .lines()
            .map(str::to_owned)
            .collect()
    }

    fn create(&self, key: Option<IdempotencyKey>) -> String {
        let lines = self.lines();
        if let Some(key) = key
            && let Some(position) = lines.iter().position(|line| *line == key.to_string())
        {
            return format!("res#{}", position + 1);
        }
        let line = key.map_or_else(|| "-".to_owned(), |k| k.to_string());
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .unwrap();
        file.write_all(format!("{line}\n").as_bytes()).unwrap();
        file.sync_all().unwrap();
        format!("res#{}", lines.len() + 1)
    }

    fn find(&self) -> Option<String> {
        (!self.lines().is_empty()).then(|| "res#1".to_owned())
    }

    fn applications(&self) -> usize {
        self.lines().len()
    }

    fn cancellations_path(&self) -> PathBuf {
        self.path.with_extension("cancelled")
    }

    /// Cancels idempotently: a key already cancelled is not written again.
    fn cancel(&self, key: &IdempotencyKey) {
        let path = self.cancellations_path();
        let done = std::fs::read_to_string(&path).unwrap_or_default();
        if done.lines().any(|line| line == key.to_string()) {
            return;
        }
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .unwrap();
        file.write_all(format!("{key}\n").as_bytes()).unwrap();
        file.sync_all().unwrap();
    }

    fn cancellations(&self) -> usize {
        std::fs::read_to_string(self.cancellations_path())
            .unwrap_or_default()
            .lines()
            .count()
    }
}

/// The effect as a durable handler.
struct FileEffect {
    remote: FileRemote,
    protection: Protection,
    approval: bool,
}

impl EffectHandler for FileEffect {
    const NAME: &'static str = "op";
    type Input = u32;
    type Output = String;
    type Error = EffectFailure;

    fn remote_idempotency(&self) -> bool {
        self.protection == Protection::Idempotent
    }

    fn requires_approval(&self) -> bool {
        self.approval
    }

    async fn execute(&self, ctx: &EffectContext, _: &u32) -> Result<String, EffectFailure> {
        let key = self.remote_idempotency().then(|| ctx.idempotency_key());
        Ok(self.remote.create(key))
    }
}

impl VerifiableEffect for FileEffect {
    async fn verify(
        &self,
        _: &EffectContext,
        _: &u32,
    ) -> Result<Verification<String>, EffectFailure> {
        Ok(match self.remote.find() {
            Some(id) => Verification::Confirmed(id),
            None => Verification::NotApplied,
        })
    }
}

impl CompensableEffect for FileEffect {
    async fn compensate(
        &self,
        ctx: &CompensationContext,
        _: &u32,
        _: Option<&String>,
    ) -> Result<(), EffectFailure> {
        self.remote.cancel(&ctx.idempotency_key());
        Ok(())
    }
}

/// A runtime over the database at `db`, with the handler registered for
/// every mode but `Closure`.
async fn runtime(
    db: &Path,
    remote: &FileRemote,
    mode: Mode,
    protection: Protection,
    worker: &str,
    faults: Option<Arc<FaultInjector>>,
) -> Runtime<SqliteStore> {
    let mut builder = Runtime::builder(SqliteStore::open(db).await.unwrap())
        .worker_id(WorkerId::new(worker))
        .lease_ttl(CHILD_TTL)
        .retry_policy(NO_WAIT);
    if let Some(faults) = faults {
        builder = builder.fault_injector(faults);
    }
    if mode != Mode::Closure {
        let mut handler = Handler::new(FileEffect {
            remote: remote.clone(),
            protection,
            approval: mode == Mode::Approval,
        })
        .compensable();
        if protection == Protection::Verified {
            handler = handler.verifiable();
        }
        builder = builder.register(handler);
    }
    builder.build()
}

async fn record(db: &Path) -> Option<EffectRecord> {
    let key = EffectKey::new(
        EffectName::new("op").unwrap(),
        LogicalKey::new("crash").unwrap(),
    );
    SqliteStore::open(db)
        .await
        .unwrap()
        .get_by_key(&key)
        .await
        .unwrap()
}

/// Runs the child: this test binary, re-invoked to run `crash_child`.
fn spawn_child(mode: Mode, dir: &Path, point: FaultPoint, protection: Protection) -> bool {
    let status = Command::new(std::env::current_exe().unwrap())
        .args(["crash_child", "--exact", "--nocapture", "--test-threads=1"])
        .env(
            CHILD_ENV,
            format!("{mode:?}|{}|{point:?}|{protection:?}", dir.display()),
        )
        .output()
        .unwrap()
        .status;
    !status.success()
}

async fn run(
    rt: &Runtime<SqliteStore>,
    remote: &FileRemote,
    protection: Protection,
) -> Result<EffectOutcome<String>, RuntimeError> {
    let send_key = protection == Protection::Idempotent;
    let action = {
        let remote = remote.clone();
        move |ctx: EffectContext| {
            let remote = remote.clone();
            let key = send_key.then(|| ctx.idempotency_key());
            async move { Ok::<_, EffectFailure>(remote.create(key)) }
        }
    };
    let builder = rt.effect("op", "crash").remote_idempotency(send_key);
    if protection == Protection::Verified {
        let lookup = remote.clone();
        builder
            .verify(move |_| {
                let lookup = lookup.clone();
                async move {
                    Ok::<_, EffectFailure>(match lookup.find() {
                        Some(id) => Verification::Confirmed(id),
                        None => Verification::NotApplied,
                    })
                }
            })
            .run(action)
            .await
    } else {
        builder.run(action).await
    }
}

fn paths(dir: &Path) -> (PathBuf, FileRemote) {
    (
        dir.join("effects.db"),
        FileRemote {
            path: dir.join("remote.log"),
        },
    )
}

/// The child: runs the effect once, aborting at the point if it is reached.
#[test]
fn crash_child() {
    let Ok(spec) = std::env::var(CHILD_ENV) else {
        return;
    };
    let mut parts = spec.split('|');
    let (mode, dir, point, protection) = (
        Mode::parse(parts.next().unwrap()),
        PathBuf::from(parts.next().unwrap()),
        parts.next().unwrap().to_owned(),
        Protection::parse(parts.next().unwrap()),
    );
    let point = POINTS
        .into_iter()
        .chain([
            FaultPoint::AfterApprovalRequested,
            FaultPoint::AfterCompensationStarted,
        ])
        .find(|p| format!("{p:?}") == point)
        .unwrap();
    let (db, remote) = paths(&dir);
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let faults = Arc::new(FaultInjector::new().at(point).abort());
            let rt = runtime(&db, &remote, mode, protection, "child", Some(faults)).await;
            // Reaching the point aborts the process; getting here means it
            // was not on this effect's path.
            match mode {
                Mode::Closure => {
                    run(&rt, &remote, protection).await.unwrap();
                }
                Mode::Handler | Mode::Approval => {
                    rt.submit::<FileEffect>("crash", 1).await.unwrap();
                }
                Mode::Compensation => {
                    let outcome = rt.submit::<FileEffect>("crash", 1).await.unwrap();
                    assert_eq!(outcome, EffectOutcome::Committed("res#1".into()));
                    rt.compensate::<FileEffect>("crash").await.unwrap();
                }
            }
        });
}

/// Kills a child at `point` for every protection level. With
/// `Mode::Closure` the restarted caller runs the effect again; with
/// `Mode::Handler` nobody does, and recovery alone must finish it.
async fn crash_point(mode: Mode, point: FaultPoint) {
    for protection in Protection::ALL {
        let case = format!("{mode:?} / {point:?} / {protection:?}");
        let dir = tempfile::tempdir().unwrap();
        let (db, remote) = paths(dir.path());
        SqliteStore::open(&db).await.unwrap();

        let died = spawn_child(mode, dir.path(), point, protection);
        let reachable =
            point != FaultPoint::AfterVerificationStarted || protection == Protection::Verified;
        assert_eq!(died, reachable, "{case}: child died at the point");

        // The child is dead; its lease runs out.
        tokio::time::sleep(CHILD_TTL + Duration::from_millis(100)).await;
        let rt = runtime(&db, &remote, mode, protection, "restarted", None).await;
        let report = rt.recover().await.unwrap();
        assert_eq!(report.resume_errors, Vec::new(), "{case}");

        if mode == Mode::Handler && point == FaultPoint::BeforeInsert {
            // Nothing was recorded, so there is nothing to finish.
            assert!(record(&db).await.is_none(), "{case}");
            assert_eq!(remote.applications(), 0, "{case}");
            continue;
        }
        let in_doubt = reachable
            && matches!(
                point,
                FaultPoint::AfterAttemptPersisted
                    | FaultPoint::AfterActionStarted
                    | FaultPoint::AfterActionReturned
            );
        let status = if mode == Mode::Closure {
            match run(&rt, &remote, protection).await.unwrap() {
                EffectOutcome::Committed(id) => {
                    assert_eq!(id, "res#1", "{case}");
                    EffectStatus::Committed
                }
                EffectOutcome::NeedsIntervention { .. } => EffectStatus::NeedsIntervention,
                other => panic!("{case}: {other:?}"),
            }
        } else {
            // No caller: whatever recovery left is the result.
            record(&db).await.unwrap().status
        };

        let created = remote.applications();
        if protection == Protection::Unprotected && in_doubt {
            assert_eq!(status, EffectStatus::NeedsIntervention, "{case}");
            assert!(created <= 1, "{case}: created {created} times");
            if point == FaultPoint::AfterAttemptPersisted {
                assert_eq!(created, 0, "{case}: the action never ran");
            }
            if point == FaultPoint::AfterActionReturned {
                assert_eq!(created, 1, "{case}: the action ran before the crash");
            }
        } else {
            assert_eq!(status, EffectStatus::Committed, "{case}");
            assert_eq!(created, 1, "{case}: created exactly once");
        }
        assert_eq!(record(&db).await.unwrap().status, status, "{case}: stored");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn killed_before_insert() {
    crash_point(Mode::Closure, FaultPoint::BeforeInsert).await;
    crash_point(Mode::Handler, FaultPoint::BeforeInsert).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn killed_after_insert() {
    crash_point(Mode::Closure, FaultPoint::AfterInsert).await;
    crash_point(Mode::Handler, FaultPoint::AfterInsert).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn killed_after_the_attempt_is_persisted() {
    crash_point(Mode::Closure, FaultPoint::AfterAttemptPersisted).await;
    crash_point(Mode::Handler, FaultPoint::AfterAttemptPersisted).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn killed_while_the_request_is_in_flight() {
    // The request may or may not have left before the kill; either way it
    // is created at most once, and exactly once if protected.
    crash_point(Mode::Closure, FaultPoint::AfterActionStarted).await;
    crash_point(Mode::Handler, FaultPoint::AfterActionStarted).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn killed_after_the_response_before_persisting_it() {
    crash_point(Mode::Closure, FaultPoint::AfterActionReturned).await;
    crash_point(Mode::Handler, FaultPoint::AfterActionReturned).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn killed_during_verification() {
    crash_point(Mode::Closure, FaultPoint::AfterVerificationStarted).await;
    crash_point(Mode::Handler, FaultPoint::AfterVerificationStarted).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn killed_after_asking_for_approval() {
    for protection in Protection::ALL {
        let case = format!("{protection:?}");
        let dir = tempfile::tempdir().unwrap();
        let (db, remote) = paths(dir.path());
        SqliteStore::open(&db).await.unwrap();
        let point = FaultPoint::AfterApprovalRequested;
        assert!(
            spawn_child(Mode::Approval, dir.path(), point, protection),
            "{case}"
        );

        tokio::time::sleep(CHILD_TTL + Duration::from_millis(100)).await;
        let rt = runtime(&db, &remote, Mode::Approval, protection, "restarted", None).await;
        rt.recover().await.unwrap();
        let waiting = record(&db).await.unwrap();
        assert_eq!(
            waiting.status,
            EffectStatus::AwaitingApproval,
            "{case}: the request survived, and recovery never decides"
        );
        assert_eq!(remote.applications(), 0, "{case}");

        rt.approve(waiting.id, "operator:test", "ok").await.unwrap();
        rt.recover().await.unwrap();
        assert_eq!(
            record(&db).await.unwrap().status,
            EffectStatus::Committed,
            "{case}"
        );
        assert_eq!(remote.applications(), 1, "{case}: created exactly once");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn killed_while_compensating() {
    for protection in Protection::ALL {
        let case = format!("{protection:?}");
        let dir = tempfile::tempdir().unwrap();
        let (db, remote) = paths(dir.path());
        SqliteStore::open(&db).await.unwrap();
        let point = FaultPoint::AfterCompensationStarted;
        assert!(
            spawn_child(Mode::Compensation, dir.path(), point, protection),
            "{case}"
        );
        assert_eq!(
            record(&db).await.unwrap().status,
            EffectStatus::Compensating,
            "{case}"
        );

        tokio::time::sleep(CHILD_TTL + Duration::from_millis(100)).await;
        let rt = runtime(
            &db,
            &remote,
            Mode::Compensation,
            protection,
            "restarted",
            None,
        )
        .await;
        rt.recover().await.unwrap();
        assert_eq!(
            record(&db).await.unwrap().status,
            EffectStatus::Compensated,
            "{case}: recovery finished the undo"
        );
        assert_eq!(remote.applications(), 1, "{case}");
        assert_eq!(remote.cancellations(), 1, "{case}: undone once");
    }
}
