//! Several processes sharing one database file must still run each effect
//! once.
//!
//! The test re-invokes its own binary as worker processes: `worker` is a
//! no-op unless `AGENT_EFFECTS_SQLITE_WORKER` is set, which only the parent
//! does for its children.

use std::fs::OpenOptions;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use agent_effects::{EffectFailure, EffectOutcome, EffectStatus, EffectStore, Runtime, WorkerId};
use agent_effects_sqlite::SqliteStore;

const WORKER_ENV: &str = "AGENT_EFFECTS_SQLITE_WORKER";
const EFFECTS: u32 = 150;
const WORKERS: usize = 3;

struct Paths {
    db: PathBuf,
    log: PathBuf,
    go: PathBuf,
}

impl Paths {
    fn new(dir: &Path) -> Self {
        Self {
            db: dir.join("effects.db"),
            log: dir.join("side-effects.log"),
            go: dir.join("go"),
        }
    }
}

/// One worker process: runs every effect, in the same order as the others
/// to maximise contention. The action is the "side effect": appending a line
/// to a shared log. A short sleep keeps attempts overlapping.
#[test]
fn worker() {
    let Ok(spec) = std::env::var(WORKER_ENV) else {
        return;
    };
    let (dir, name) = spec.split_once('|').expect("dir|name");
    let paths = Paths::new(Path::new(dir));
    let name = name.to_owned();

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async move {
            let store = SqliteStore::open(&paths.db).await.unwrap();
            let rt = Runtime::builder(store)
                .worker_id(WorkerId::new(name.clone()))
                .build();
            while !paths.go.exists() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            for n in 0..EFFECTS {
                let (log, runner) = (paths.log.clone(), name.clone());
                let outcome = rt
                    .effect("job.run", n)
                    .run(move |_| {
                        let (log, name) = (log.clone(), runner.clone());
                        async move {
                            tokio::time::sleep(Duration::from_millis(2)).await;
                            // One small O_APPEND write per line: atomic.
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
    let dir = tempfile::tempdir().unwrap();
    let paths = Paths::new(dir.path());
    // Create and migrate up front, so the workers only contend on effects.
    SqliteStore::open(&paths.db).await.unwrap();

    let exe = std::env::current_exe().unwrap();
    let mut children: Vec<_> = (0..WORKERS)
        .map(|i| {
            Command::new(&exe)
                .args(["worker", "--exact", "--nocapture", "--test-threads=1"])
                .env(WORKER_ENV, format!("{}|process-{i}", dir.path().display()))
                .spawn()
                .unwrap()
        })
        .collect();
    std::fs::write(&paths.go, b"").unwrap();
    for child in &mut children {
        assert!(child.wait().unwrap().success(), "a worker process failed");
    }

    let log = std::fs::read_to_string(&paths.log).unwrap();
    let mut runs = vec![0u32; EFFECTS as usize];
    let mut runners = std::collections::BTreeSet::new();
    for line in log.lines() {
        let (n, name) = line.split_once(' ').unwrap();
        runs[n.parse::<usize>().unwrap()] += 1;
        runners.insert(name.to_owned());
    }
    let duplicated: Vec<_> = (0..EFFECTS).filter(|&n| runs[n as usize] > 1).collect();
    let missing: Vec<_> = (0..EFFECTS).filter(|&n| runs[n as usize] == 0).collect();
    assert!(duplicated.is_empty(), "ran more than once: {duplicated:?}");
    assert!(missing.is_empty(), "never ran: {missing:?}");
    assert!(
        runners.len() > 1,
        "only {runners:?} did any work: no real contention"
    );

    let store = SqliteStore::open(&paths.db).await.unwrap();
    let records = store
        .list(agent_effects::store::ListQuery::statuses([]).limit(1000))
        .await
        .unwrap();
    assert_eq!(records.len(), EFFECTS as usize);
    assert!(records.iter().all(|r| r.status == EffectStatus::Committed));
    assert!(records.iter().all(|r| r.attempt_count == 1));
}
