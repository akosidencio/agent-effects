//! Retention: settled records are pruned once old enough; nothing else is.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use agent_effects::store::EffectStore;
use agent_effects::{
    EffectFailure, EffectKey, EffectName, EffectOutcome, EffectStatus, LogicalKey, Precondition,
    RetentionPolicy, RetryPolicy, Runtime, TokioClock,
};
use agent_effects_memory::MemoryStore;

const MINUTE: Duration = Duration::from_secs(60);
const HOUR: Duration = Duration::from_secs(3600);

fn runtime(store: &MemoryStore, retention: RetentionPolicy) -> Runtime<MemoryStore> {
    Runtime::builder(store.clone())
        .clock(TokioClock::new())
        .retry_policy(RetryPolicy::NONE)
        .retention(retention)
        .build()
}

async fn status(store: &MemoryStore, logical: &str) -> Option<EffectStatus> {
    let key = EffectKey::new(
        EffectName::new("op").unwrap(),
        LogicalKey::new(logical).unwrap(),
    );
    store.get_by_key(&key).await.unwrap().map(|r| r.status)
}

async fn commit(rt: &Runtime<MemoryStore>, logical: &str) {
    let outcome = rt
        .effect("op", logical)
        .run(|_| async { Ok::<_, EffectFailure>(()) })
        .await
        .unwrap();
    assert_eq!(outcome, EffectOutcome::Committed(()));
}

#[tokio::test(start_paused = true)]
async fn settled_effects_are_pruned_once_old_enough_and_nothing_else_ever() {
    let store = MemoryStore::new();
    let rt = runtime(&store, RetentionPolicy::settled(HOUR).failed(10 * MINUTE));
    commit(&rt, "committed").await;
    rt.effect("op", "failed")
        .run(|_| async { Err::<(), _>(EffectFailure::permanent("declined")) })
        .await
        .unwrap();
    rt.effect("op", "rejected")
        .precondition(|_| async { Precondition::reject("no longer pending") })
        .run(|_| async { Ok::<_, EffectFailure>(()) })
        .await
        .unwrap();
    commit(&rt, "compensated").await;
    rt.compensation("op", "compensated")
        .run(|_, _: Option<()>| async { Ok(()) })
        .await
        .unwrap();
    // Unsettled: an operator must decide, and an approver.
    rt.effect("op", "in-doubt")
        .run(|_| async { Err::<(), _>(EffectFailure::ambiguous("timed out")) })
        .await
        .unwrap();
    rt.effect("op", "awaiting")
        .require_approval()
        .run(|_| async { Ok::<_, EffectFailure>(()) })
        .await
        .unwrap();
    assert_eq!(
        status(&store, "in-doubt").await,
        Some(EffectStatus::NeedsIntervention)
    );
    assert_eq!(
        status(&store, "awaiting").await,
        Some(EffectStatus::AwaitingApproval)
    );

    assert_eq!(rt.prune().await.unwrap().total(), 0, "nothing is old yet");

    tokio::time::sleep(11 * MINUTE).await;
    let report = rt.prune().await.unwrap();
    assert_eq!((report.failed, report.total()), (1, 1));
    assert_eq!(status(&store, "failed").await, None);

    tokio::time::sleep(HOUR).await;
    let report = rt.prune().await.unwrap();
    assert_eq!(
        (report.committed, report.rejected, report.compensated),
        (1, 1, 1)
    );

    tokio::time::sleep(10_000 * HOUR).await;
    assert_eq!(rt.prune().await.unwrap().total(), 0);
    assert_eq!(store.len(), 2, "unsettled effects are kept however old");
    assert!(status(&store, "in-doubt").await.is_some());
    assert!(status(&store, "awaiting").await.is_some());
}

#[tokio::test(start_paused = true)]
async fn records_are_kept_forever_by_default() {
    let store = MemoryStore::new();
    let rt = Runtime::builder(store.clone())
        .clock(TokioClock::new())
        .build();
    commit(&rt, "k").await;
    tokio::time::sleep(100_000 * HOUR).await;
    assert_eq!(rt.prune().await.unwrap().total(), 0);
    assert_eq!(store.len(), 1);
}

#[tokio::test(start_paused = true)]
async fn a_pruned_key_runs_again() {
    let store = MemoryStore::new();
    let rt = runtime(&store, RetentionPolicy::KEEP_ALL.committed(HOUR));
    let runs = Arc::new(AtomicU32::new(0));
    let run = || {
        let runs = Arc::clone(&runs);
        rt.effect("op", "k").run(move |_| {
            runs.fetch_add(1, Ordering::SeqCst);
            async { Ok::<_, EffectFailure>(()) }
        })
    };
    run().await.unwrap();
    tokio::time::sleep(MINUTE).await;
    run().await.unwrap();
    assert_eq!(runs.load(Ordering::SeqCst), 1, "replayed while kept");

    tokio::time::sleep(HOUR).await;
    assert_eq!(rt.prune().await.unwrap().committed, 1);
    run().await.unwrap();
    assert_eq!(
        runs.load(Ordering::SeqCst),
        2,
        "pruning forgets: the same key is a new effect"
    );
}

#[tokio::test(start_paused = true)]
async fn one_pass_prunes_every_batch() {
    let store = MemoryStore::new();
    let rt = runtime(&store, RetentionPolicy::settled(MINUTE));
    for n in 0..1_201 {
        commit(&rt, &n.to_string()).await;
    }
    tokio::time::sleep(2 * MINUTE).await;
    assert_eq!(rt.prune().await.unwrap().committed, 1_201);
    assert!(store.is_empty());
}

#[tokio::test(start_paused = true)]
async fn the_recovery_loop_prunes() {
    let store = MemoryStore::new();
    let rt = runtime(&store, RetentionPolicy::settled(HOUR));
    commit(&rt, "k").await;
    let worker = tokio::spawn({
        let rt = rt.clone();
        async move { rt.run_recovery(MINUTE).await }
    });
    tokio::time::sleep(30 * MINUTE).await;
    assert_eq!(store.len(), 1);
    tokio::time::sleep(HOUR).await;
    assert!(store.is_empty());
    worker.abort();
}
