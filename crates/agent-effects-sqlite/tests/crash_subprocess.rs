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

use std::fs::OpenOptions;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use agent_effects::fault::{FaultInjector, FaultPoint};
use agent_effects::{
    EffectContext, EffectFailure, EffectKey, EffectName, EffectOutcome, EffectStatus, EffectStore,
    IdempotencyKey, LogicalKey, RetryPolicy, Runtime, RuntimeError, Verification, WorkerId,
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

/// A remote system whose state is a file: one line per application.
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
    let (dir, point, protection) = (
        PathBuf::from(parts.next().unwrap()),
        parts.next().unwrap().to_owned(),
        Protection::parse(parts.next().unwrap()),
    );
    let point = POINTS
        .into_iter()
        .find(|p| format!("{p:?}") == point)
        .unwrap();
    let (db, remote) = paths(&dir);
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let rt = Runtime::builder(SqliteStore::open(&db).await.unwrap())
                .worker_id(WorkerId::new("child"))
                .lease_ttl(CHILD_TTL)
                .retry_policy(NO_WAIT)
                .fault_injector(std::sync::Arc::new(FaultInjector::new().at(point).abort()))
                .build();
            // Reaching the point aborts the process; getting here means it
            // was not on this effect's path.
            run(&rt, &remote, protection).await.unwrap();
        });
}

async fn crash_point(point: FaultPoint) {
    for protection in Protection::ALL {
        let case = format!("{point:?} / {protection:?}");
        let dir = tempfile::tempdir().unwrap();
        let (db, remote) = paths(dir.path());
        SqliteStore::open(&db).await.unwrap();

        let status = Command::new(std::env::current_exe().unwrap())
            .args(["crash_child", "--exact", "--nocapture", "--test-threads=1"])
            .env(
                CHILD_ENV,
                format!("{}|{point:?}|{protection:?}", dir.path().display()),
            )
            .output()
            .unwrap()
            .status;
        let reachable =
            point != FaultPoint::AfterVerificationStarted || protection == Protection::Verified;
        assert_eq!(
            !status.success(),
            reachable,
            "{case}: child died at the point ({status})"
        );

        // The child is dead; its lease runs out.
        tokio::time::sleep(CHILD_TTL + Duration::from_millis(100)).await;
        let rt = Runtime::builder(SqliteStore::open(&db).await.unwrap())
            .worker_id(WorkerId::new("restarted"))
            .retry_policy(NO_WAIT)
            .build();
        rt.recover().await.unwrap();
        let outcome = run(&rt, &remote, protection).await.unwrap();

        let in_doubt = reachable
            && matches!(
                point,
                FaultPoint::AfterAttemptPersisted
                    | FaultPoint::AfterActionStarted
                    | FaultPoint::AfterActionReturned
            );
        let created = remote.applications();
        if protection == Protection::Unprotected && in_doubt {
            assert!(
                matches!(outcome, EffectOutcome::NeedsIntervention { .. }),
                "{case}: {outcome:?}"
            );
            assert!(created <= 1, "{case}: created {created} times");
            if point == FaultPoint::AfterAttemptPersisted {
                assert_eq!(created, 0, "{case}: the action never ran");
            }
            if point == FaultPoint::AfterActionReturned {
                assert_eq!(created, 1, "{case}: the action ran before the crash");
            }
        } else {
            assert_eq!(outcome, EffectOutcome::Committed("res#1".into()), "{case}");
            assert_eq!(created, 1, "{case}: created exactly once");
        }

        let store = SqliteStore::open(&db).await.unwrap();
        let key = EffectKey::new(
            EffectName::new("op").unwrap(),
            LogicalKey::new("crash").unwrap(),
        );
        let record = store.get_by_key(&key).await.unwrap().unwrap();
        assert!(
            matches!(
                record.status,
                EffectStatus::Committed | EffectStatus::NeedsIntervention
            ),
            "{case}: settled"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn killed_before_insert() {
    crash_point(FaultPoint::BeforeInsert).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn killed_after_insert() {
    crash_point(FaultPoint::AfterInsert).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn killed_after_the_attempt_is_persisted() {
    crash_point(FaultPoint::AfterAttemptPersisted).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn killed_while_the_request_is_in_flight() {
    // The request may or may not have left before the kill; either way it
    // is created at most once, and exactly once if protected.
    crash_point(FaultPoint::AfterActionStarted).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn killed_after_the_response_before_persisting_it() {
    crash_point(FaultPoint::AfterActionReturned).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn killed_during_verification() {
    crash_point(FaultPoint::AfterVerificationStarted).await;
}
