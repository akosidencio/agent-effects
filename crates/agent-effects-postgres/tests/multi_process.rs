//! Several processes sharing one PostgreSQL database must still run each
//! effect once. Skipped unless `AGENT_EFFECTS_POSTGRES_URL` is set.
//!
//! The test re-invokes its own binary as worker processes; `worker` is a
//! no-op unless `AGENT_EFFECTS_POSTGRES_WORKER` is set.

use std::fs::OpenOptions;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use agent_effects::store::ListQuery;
use agent_effects::store::{ExecutionContract, NewEffect};
use agent_effects::{
    EffectContext, EffectFailure, EffectHandler, EffectKey, EffectKind, EffectName, EffectOutcome,
    EffectStatus, EffectStore, Handler, LogicalKey, Runtime, WorkerId,
};
use agent_effects_postgres::PostgresStore;
use sqlx::Executor;
use sqlx::postgres::PgConnectOptions;

const WORKER_ENV: &str = "AGENT_EFFECTS_POSTGRES_WORKER";
const RECOVERY_ENV: &str = "AGENT_EFFECTS_RECOVERY_ONLY";
const EFFECTS: u32 = 150;
const WORKERS: usize = 3;

async fn connect(url: &str, schema: &str) -> PostgresStore {
    let options: PgConnectOptions = url.parse().unwrap();
    PostgresStore::connect_with(options.options([("search_path", schema)]))
        .await
        .unwrap()
}

#[derive(Default)]
struct RecoveryChildren(Vec<std::process::Child>);

impl RecoveryChildren {
    async fn finish(&mut self) {
        tokio::time::timeout(Duration::from_secs(60), async {
            for child in &mut self.0 {
                loop {
                    if let Some(status) = child.try_wait().unwrap() {
                        assert!(status.success(), "recovery process failed: {status}");
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            }
        })
        .await
        .expect("recovery processes timed out");
    }
}
impl Drop for RecoveryChildren {
    fn drop(&mut self) {
        for child in &mut self.0 {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

struct RecoveryJob {
    log: PathBuf,
    runner: String,
}

impl EffectHandler for RecoveryJob {
    const NAME: &'static str = "job.recover";
    type Input = u32;
    type Output = u32;
    type Error = EffectFailure;
    async fn execute(&self, _: &EffectContext, input: &u32) -> Result<u32, EffectFailure> {
        let dir = self.log.parent().unwrap();
        std::fs::write(dir.join(format!("claimed-{}", self.runner)), b"").unwrap();
        tokio::time::timeout(Duration::from_secs(30), async {
            while std::fs::read_dir(dir)
                .unwrap()
                .filter(|entry| {
                    entry
                        .as_ref()
                        .unwrap()
                        .file_name()
                        .to_string_lossy()
                        .starts_with("claimed-")
                })
                .count()
                < WORKERS
            {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("all recovery processes must acquire an effect before proceeding");
        tokio::time::sleep(Duration::from_millis(2)).await;
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.log)
            .unwrap();
        file.write_all(format!("{input} {}\n", self.runner).as_bytes())
            .unwrap();
        Ok(*input)
    }
}

/// One worker process: runs every effect in the same order as the others;
/// each action appends a line to a shared log.
#[test]
fn worker() {
    let Ok(spec) = std::env::var(WORKER_ENV) else {
        return;
    };
    let mut parts = spec.split('|');
    let (url, schema, dir, name) = (
        parts.next().unwrap().to_owned(),
        parts.next().unwrap().to_owned(),
        parts.next().unwrap().to_owned(),
        parts.next().unwrap().to_owned(),
    );
    let log = Path::new(&dir).join("side-effects.log");
    let go = Path::new(&dir).join("go");
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
        .block_on(async move {
            let rt = Runtime::builder(connect(&url, &schema).await)
                .worker_id(WorkerId::new(name.clone()))
                .register(Handler::new(RecoveryJob {
                    log: log.clone(),
                    runner: name.clone(),
                }))
                .build();
            std::fs::write(Path::new(&dir).join(format!("ready-{name}")), b"").unwrap();
            while !go.exists() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            if std::env::var_os(RECOVERY_ENV).is_some() {
                tokio::time::timeout(Duration::from_secs(60), async {
                    loop {
                        let report = rt.recover().await.unwrap();
                        assert!(report.resume_errors.is_empty(), "{report:?}");
                        if Path::new(&dir).join(format!("claimed-{name}")).exists() {
                            break;
                        }
                        // Another scan can temporarily lock an entire page.
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                })
                .await
                .expect("recovery worker did not acquire an effect");
                return;
            }
            for n in 0..EFFECTS {
                let (log, runner) = (log.clone(), name.clone());
                let outcome = rt
                    .effect("job.run", n)
                    .run(move |_| {
                        let (log, name) = (log.clone(), runner.clone());
                        async move {
                            tokio::time::sleep(Duration::from_millis(2)).await;
                            let mut file = OpenOptions::new()
                                .create(true)
                                .append(true)
                                .open(&log)
                                .unwrap();
                            file.write_all(format!("{n} {name}\n").as_bytes()).unwrap();
                            Ok::<_, EffectFailure>(n)
                        }
                    })
                    .await
                    .unwrap();
                assert!(
                    matches!(
                        outcome,
                        EffectOutcome::Committed(_) | EffectOutcome::InProgress { .. }
                    ),
                    "{name}: effect {n}: {outcome:?}"
                );
            }
        });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn processes_sharing_a_database_run_each_effect_once() {
    let Ok(url) = std::env::var("AGENT_EFFECTS_POSTGRES_URL") else {
        assert!(
            std::env::var_os("AGENT_EFFECTS_REQUIRE_POSTGRES").is_none(),
            "AGENT_EFFECTS_REQUIRE_POSTGRES is set but AGENT_EFFECTS_POSTGRES_URL is not"
        );
        eprintln!("skipped: set AGENT_EFFECTS_POSTGRES_URL to run against PostgreSQL");
        return;
    };
    let schema = format!(
        "mp_{}",
        agent_effects::EffectId::new().to_string().replace('-', "")
    );
    sqlx::PgPool::connect(&url)
        .await
        .unwrap()
        .execute(sqlx::AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .await
        .unwrap();
    // Migrate up front, so the workers only contend on effects.
    let store = connect(&url, &schema).await;

    let dir = std::env::temp_dir().join(&schema);
    std::fs::create_dir_all(&dir).unwrap();
    let exe = std::env::current_exe().unwrap();
    let mut children: Vec<_> = (0..WORKERS)
        .map(|i| {
            Command::new(&exe)
                .args(["worker", "--exact", "--nocapture", "--test-threads=1"])
                .env(
                    WORKER_ENV,
                    format!("{url}|{schema}|{}|process-{i}", dir.display()),
                )
                .spawn()
                .unwrap()
        })
        .collect();
    std::fs::write(dir.join("go"), b"").unwrap();
    for child in &mut children {
        assert!(child.wait().unwrap().success(), "a worker process failed");
    }

    let log = std::fs::read_to_string(dir.join("side-effects.log")).unwrap();
    let mut runs = vec![0u32; EFFECTS as usize];
    let mut runners = std::collections::BTreeSet::new();
    for line in log.lines() {
        let (n, name) = line.split_once(' ').unwrap();
        runs[n.parse::<usize>().unwrap()] += 1;
        runners.insert(name.to_owned());
    }
    let _ = std::fs::remove_dir_all(&dir);
    let duplicated: Vec<_> = (0..EFFECTS).filter(|&n| runs[n as usize] > 1).collect();
    let missing: Vec<_> = (0..EFFECTS).filter(|&n| runs[n as usize] == 0).collect();
    assert!(duplicated.is_empty(), "ran more than once: {duplicated:?}");
    assert!(missing.is_empty(), "never ran: {missing:?}");
    assert!(
        runners.len() > 1,
        "only {runners:?} did any work: no real contention"
    );

    let records = store
        .list(ListQuery::statuses([]).limit(1000))
        .await
        .unwrap();
    assert_eq!(records.len(), EFFECTS as usize);
    assert!(records.iter().all(|r| r.status == EffectStatus::Committed));
    assert!(records.iter().all(|r| r.attempt_count == 1));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recovery_processes_share_durable_pending_intents_without_duplicate_actions() {
    let Ok(url) = std::env::var("AGENT_EFFECTS_POSTGRES_URL") else {
        assert!(
            std::env::var_os("AGENT_EFFECTS_REQUIRE_POSTGRES").is_none(),
            "PostgreSQL is required"
        );
        eprintln!("skipped: set AGENT_EFFECTS_POSTGRES_URL");
        return;
    };
    let schema = format!(
        "recover_{}",
        agent_effects::EffectId::new().to_string().replace('-', "")
    );
    sqlx::PgPool::connect(&url)
        .await
        .unwrap()
        .execute(sqlx::AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .await
        .unwrap();
    let store = connect(&url, &schema).await;
    for n in 0..EFFECTS {
        let key = EffectKey::new(
            EffectName::new(RecoveryJob::NAME).unwrap(),
            LogicalKey::new(n.to_string()).unwrap(),
        );
        let mut new = NewEffect::new(
            key,
            EffectKind::IrreversibleWrite,
            std::time::SystemTime::now(),
        )
        .with_contract(ExecutionContract::new(EffectKind::IrreversibleWrite, 5));
        new.input = Some(serde_json::json!(n));
        store.insert_or_get(new).await.unwrap();
    }
    let dir = std::env::temp_dir().join(&schema);
    std::fs::create_dir_all(&dir).unwrap();
    let mut children = RecoveryChildren::default();
    for index in 0..WORKERS {
        children.0.push(
            Command::new(std::env::current_exe().unwrap())
                .args(["worker", "--exact", "--nocapture", "--test-threads=1"])
                .env(
                    WORKER_ENV,
                    format!("{url}|{schema}|{}|recover-{index}", dir.display()),
                )
                .env(RECOVERY_ENV, "1")
                .spawn()
                .unwrap(),
        );
    }
    tokio::time::timeout(Duration::from_secs(30), async {
        while (0..WORKERS).any(|i| !dir.join(format!("ready-recover-{i}")).exists()) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("recovery workers did not initialize");
    std::fs::write(dir.join("go"), b"").unwrap();
    children.finish().await;
    let log = std::fs::read_to_string(dir.join("side-effects.log")).unwrap();
    let mut counts = vec![0; EFFECTS as usize];
    let mut runners = std::collections::BTreeSet::new();
    for line in log.lines() {
        let (n, runner) = line.split_once(' ').unwrap();
        counts[n.parse::<usize>().unwrap()] += 1;
        runners.insert(runner);
    }
    assert!(counts.iter().all(|count| *count == 1));
    assert_eq!(
        runners.len(),
        WORKERS,
        "every process must actually execute a handler"
    );
    let records = store
        .list(ListQuery::statuses([]).limit(1000))
        .await
        .unwrap();
    assert_eq!(records.len(), EFFECTS as usize);
    assert!(
        records
            .iter()
            .all(|record| record.status == EffectStatus::Committed && record.attempt_count == 1)
    );
    std::fs::remove_dir_all(dir).unwrap();
}
