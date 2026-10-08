//! The store contract, the database clock, lock-skipping scans and use from
//! the runtime, against a real PostgreSQL.
//!
//! Set `AGENT_EFFECTS_POSTGRES_URL` (e.g.
//! `postgres://postgres:pw@localhost:55432/effects`) to run them; without it
//! they are skipped. Each test works in its own schema.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, SystemTime};

use agent_effects::store::{
    EffectStore, ListQuery, NewEffect, PruneQuery, StoreError, TransitionRequest,
};
use agent_effects::{
    EffectFailure, EffectKey, EffectKind, EffectName, EffectOutcome, EffectStatus, LogicalKey,
    ManualClock, Runtime, Transition, WorkerId,
};
use agent_effects_postgres::{ClockSource, PostgresStore};
use sqlx::Executor;
use sqlx::postgres::PgConnectOptions;

fn url() -> Option<String> {
    let url = std::env::var("AGENT_EFFECTS_POSTGRES_URL").ok();
    if url.is_none() {
        // CI sets this so a missing database fails instead of skipping.
        assert!(
            std::env::var_os("AGENT_EFFECTS_REQUIRE_POSTGRES").is_none(),
            "AGENT_EFFECTS_REQUIRE_POSTGRES is set but AGENT_EFFECTS_POSTGRES_URL is not"
        );
        eprintln!("skipped: set AGENT_EFFECTS_POSTGRES_URL to run against PostgreSQL");
    }
    url
}

/// A store in a fresh, uniquely named schema.
async fn fresh(url: &str, clock: ClockSource) -> PostgresStore {
    let schema = format!("t_{}", uuid_like());
    let admin = sqlx::PgPool::connect(url).await.unwrap();
    admin
        .execute(sqlx::AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .await
        .unwrap();
    let options: PgConnectOptions = url.parse().unwrap();
    PostgresStore::connect_with(options.options([("search_path", schema.as_str())]))
        .await
        .unwrap()
        .with_clock_source(clock)
}

fn uuid_like() -> String {
    agent_effects::EffectId::new().to_string().replace('-', "")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn passes_the_conformance_suite() {
    let Some(url) = url() else { return };
    // The suite drives time itself, so the store takes the caller's clock.
    agent_effects_store::testkit::conformance(|| fresh(&url, ClockSource::Caller)).await;
}

fn key(logical: &str) -> EffectKey {
    EffectKey::new(
        EffectName::new("op").unwrap(),
        LogicalKey::new(logical).unwrap(),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn leases_follow_the_database_clock_not_the_workers() {
    let Some(url) = url() else { return };
    let store = fresh(&url, ClockSource::Database).await;
    let record = store
        .insert_or_get(NewEffect::new(
            key("k"),
            EffectKind::IrreversibleWrite,
            SystemTime::UNIX_EPOCH,
        ))
        .await
        .unwrap()
        .record;
    let before = SystemTime::now() - Duration::from_secs(5);
    assert!(
        record.created_at > before,
        "created_at is the database's time, not the caller's"
    );

    // Worker a's clock is an hour behind; worker b's is an hour ahead and
    // believes a's 30 s lease long expired.
    let hour = Duration::from_secs(3600);
    let a = store
        .acquire_lease(
            record.id,
            &WorkerId::new("a"),
            SystemTime::now() - hour,
            Duration::from_secs(30),
        )
        .await
        .unwrap();
    assert!(
        a.expires_at > SystemTime::now(),
        "expiry counts from the database's now"
    );
    let b = store
        .acquire_lease(
            record.id,
            &WorkerId::new("b"),
            SystemTime::now() + hour,
            Duration::from_secs(30),
        )
        .await;
    assert!(matches!(b, Err(StoreError::LeaseHeld { .. })), "{b:?}");
    // And a recovery scan by a fast clock does not see it as abandoned.
    let mut started = TransitionRequest::new(
        &record,
        Some(&a),
        Transition::StartAttempt,
        SystemTime::UNIX_EPOCH,
    );
    started.actor = Some("a".into());
    store.transition(started).await.unwrap();
    let scan = store
        .list(ListQuery::expired_leases(SystemTime::now() + hour))
        .await
        .unwrap();
    assert_eq!(scan, Vec::new());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_lease_that_expires_while_waiting_for_the_row_lock_is_not_renewed() {
    let Some(url) = url() else { return };
    let store = fresh(&url, ClockSource::Database).await;
    let record = store
        .insert_or_get(NewEffect::new(
            key("k"),
            EffectKind::IrreversibleWrite,
            SystemTime::now(),
        ))
        .await
        .unwrap()
        .record;
    let lease = store
        .acquire_lease(
            record.id,
            &WorkerId::new("a"),
            SystemTime::now(),
            Duration::from_secs(1),
        )
        .await
        .unwrap();

    // Someone holds the row lock, without changing the row, past the
    // lease's expiry.
    let mut locker = store.pool().begin().await.unwrap();
    sqlx::query("SELECT id FROM effects WHERE id = $1 FOR UPDATE")
        .bind(*record.id.as_uuid())
        .execute(&mut *locker)
        .await
        .unwrap();
    let renewing = tokio::spawn({
        let store = store.clone();
        let lease = lease.clone();
        async move {
            store
                .renew_lease(&lease, SystemTime::now(), Duration::from_secs(30))
                .await
        }
    });
    tokio::time::sleep(Duration::from_millis(1500)).await;
    locker.commit().await.unwrap();

    let renewed = renewing.await.unwrap();
    assert!(
        matches!(renewed, Err(StoreError::LeaseLost)),
        "the lease expired before the renewal got the row: {renewed:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_worker_behind_the_database_clock_takes_over_an_expired_lease() {
    let Some(url) = url() else { return };
    let store = fresh(&url, ClockSource::Database).await;
    let record = store
        .insert_or_get(NewEffect::new(
            key("k"),
            EffectKind::IrreversibleWrite,
            SystemTime::UNIX_EPOCH,
        ))
        .await
        .unwrap()
        .record;
    // A worker died holding the lease, which the database lets expire.
    store
        .acquire_lease(
            record.id,
            &WorkerId::new("dead"),
            SystemTime::UNIX_EPOCH,
            Duration::from_millis(50),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    // This worker's clock is an hour behind the database's.
    let behind = ManualClock::new(SystemTime::now() - Duration::from_secs(3600));
    let outcome = Runtime::builder(store)
        .clock(behind)
        .build()
        .effect("op", "k")
        .run(|_| async { Ok::<_, EffectFailure>(7_u32) })
        .await
        .unwrap();
    assert_eq!(outcome, EffectOutcome::Committed(7));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recovery_scans_skip_rows_being_changed() {
    let Some(url) = url() else { return };
    let store = fresh(&url, ClockSource::Caller).await;
    let now = SystemTime::now();
    let mut ids = Vec::new();
    for n in 0..2 {
        let record = store
            .insert_or_get(NewEffect::new(
                key(&format!("k{n}")),
                EffectKind::IrreversibleWrite,
                now,
            ))
            .await
            .unwrap()
            .record;
        let lease = store
            .acquire_lease(
                record.id,
                &WorkerId::new("dead"),
                now,
                Duration::from_secs(1),
            )
            .await
            .unwrap();
        store
            .transition(TransitionRequest::new(
                &record,
                Some(&lease),
                Transition::StartAttempt,
                now,
            ))
            .await
            .unwrap();
        ids.push(record.id);
    }
    // Another worker holds a row lock on the first effect, mid-change.
    let mut locker = store.pool().begin().await.unwrap();
    sqlx::query("SELECT id FROM effects WHERE id = $1 FOR UPDATE")
        .bind(*ids[0].as_uuid())
        .execute(&mut *locker)
        .await
        .unwrap();

    let later = now + Duration::from_secs(60);
    let scan = tokio::time::timeout(
        Duration::from_secs(5),
        store.list(ListQuery::expired_leases(later)),
    )
    .await
    .expect("the scan must not wait on the locked row")
    .unwrap();
    assert_eq!(scan.iter().map(|r| r.id).collect::<Vec<_>>(), [ids[1]]);
    locker.rollback().await.unwrap();
    let scan = store.list(ListQuery::expired_leases(later)).await.unwrap();
    assert_eq!(scan.len(), 2, "both once the lock is gone");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pruning_ages_by_the_database_clock_and_skips_rows_being_changed() {
    let Some(url) = url() else { return };
    let store = fresh(&url, ClockSource::Database).await;
    let rt = Runtime::new(store.clone());
    let mut ids = Vec::new();
    for n in 0..2 {
        rt.effect("op", format!("k{n}"))
            .run(|_| async { Ok::<_, EffectFailure>(()) })
            .await
            .unwrap();
        ids.push(
            store
                .get_by_key(&key(&format!("k{n}")))
                .await
                .unwrap()
                .unwrap()
                .id,
        );
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    let committed =
        |older_than, caller_now| PruneQuery::new(EffectStatus::Committed, older_than, caller_now);

    // A caller whose clock is a day ahead still sees them as just settled.
    let day_ahead = SystemTime::now() + Duration::from_hours(24);
    assert_eq!(
        store
            .prune(committed(Duration::from_secs(3600), day_ahead))
            .await
            .unwrap(),
        0,
        "settled milliseconds ago by the database's clock"
    );

    // Another worker holds a row lock on the first effect, mid-change.
    let mut locker = store.pool().begin().await.unwrap();
    sqlx::query("SELECT id FROM effects WHERE id = $1 FOR UPDATE")
        .bind(*ids[0].as_uuid())
        .execute(&mut *locker)
        .await
        .unwrap();
    let pruned = tokio::time::timeout(
        Duration::from_secs(5),
        store.prune(committed(Duration::from_millis(10), SystemTime::UNIX_EPOCH)),
    )
    .await
    .expect("pruning must not wait on the locked row")
    .unwrap();
    assert_eq!(pruned, 1);
    assert!(
        store.get(ids[0]).await.unwrap().is_some(),
        "skipped while locked"
    );
    assert!(store.get(ids[1]).await.unwrap().is_none());
    locker.rollback().await.unwrap();
    assert_eq!(
        store
            .prune(committed(Duration::from_millis(10), SystemTime::now()))
            .await
            .unwrap(),
        1
    );
    assert!(store.get(ids[0]).await.unwrap().is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_runtime_runs_on_postgres_and_replays_across_connections() {
    let Some(url) = url() else { return };
    let store = fresh(&url, ClockSource::Database).await;
    let calls = Arc::new(AtomicU32::new(0));
    let charge = |store: PostgresStore| {
        let calls = Arc::clone(&calls);
        async move {
            Runtime::new(store)
                .effect("payment.charge", "order-1")
                .input(&42)
                .run(move |_| {
                    calls.fetch_add(1, Ordering::SeqCst);
                    async { Ok::<_, EffectFailure>("pi_1".to_string()) }
                })
                .await
                .unwrap()
        }
    };
    let first = charge(store.clone()).await;
    // A second pool on the same schema, as a second process would have.
    let second_pool = PostgresStore::from_pool(store.pool().clone())
        .await
        .unwrap();
    let second = charge(second_pool).await;
    assert_eq!(first, EffectOutcome::Committed("pi_1".into()));
    assert_eq!(second, first);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}
