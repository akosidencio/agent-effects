//! The store through PgBouncer in transaction mode with
//! `max_prepared_statements = 0`, as an application sharing its database
//! through one uses it: migrated over a direct connection, attached to the
//! pooled one.
//!
//! CI's PgBouncer ignores no startup parameters, so the pooled pool turns
//! off sqlx's `extra_float_digits`, as an application's must.
//!
//! Set `AGENT_EFFECTS_PGBOUNCER_URL` to a PgBouncer in front of the database
//! at `AGENT_EFFECTS_POSTGRES_URL` to run them; without both they are
//! skipped.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use agent_effects::store::ListQuery;
use agent_effects::{EffectFailure, EffectOutcome, EffectStatus, EffectStore, Runtime, WorkerId};
use agent_effects_postgres::{ClockSource, PostgresOptions, PostgresStore};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};

fn urls() -> Option<(String, String)> {
    let direct = std::env::var("AGENT_EFFECTS_POSTGRES_URL").ok();
    let pooled = std::env::var("AGENT_EFFECTS_PGBOUNCER_URL").ok();
    if direct.is_none() || pooled.is_none() {
        // CI sets this so a missing PgBouncer fails instead of skipping.
        assert!(
            std::env::var_os("AGENT_EFFECTS_REQUIRE_PGBOUNCER").is_none(),
            "AGENT_EFFECTS_REQUIRE_PGBOUNCER is set but AGENT_EFFECTS_POSTGRES_URL or \
             AGENT_EFFECTS_PGBOUNCER_URL is not"
        );
        eprintln!(
            "skipped: set AGENT_EFFECTS_POSTGRES_URL and AGENT_EFFECTS_PGBOUNCER_URL to run \
             through PgBouncer"
        );
    }
    Some((direct?, pooled?))
}

/// A store in a fresh schema, migrated directly and attached through
/// PgBouncer with `connections` client connections.
async fn attached(direct: &str, pooled: &str, connections: u32) -> PostgresStore {
    let options = PostgresOptions::new()
        .schema(format!("pgb_{}", uuid_like()))
        .transaction_pooling(true);
    let direct = PgPoolOptions::new()
        .max_connections(1)
        .connect(direct)
        .await
        .unwrap();
    options.migrate(&direct).await.unwrap();
    direct.close().await;
    let pooled: PgConnectOptions = pooled.parse().unwrap();
    let pool = PgPoolOptions::new()
        .max_connections(connections)
        .connect_with(pooled.extra_float_digits(None))
        .await
        .unwrap();
    options.attach(pool).await.unwrap()
}

fn uuid_like() -> String {
    agent_effects::EffectId::new().to_string().replace('-', "")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn passes_the_conformance_suite_through_pgbouncer() {
    let Some((direct, pooled)) = urls() else {
        return;
    };
    agent_effects_store::testkit::conformance(|| async {
        attached(&direct, &pooled, 4)
            .await
            .with_clock_source(ClockSource::Caller)
    })
    .await;
}

/// More client connections than PgBouncer has server connections, so
/// statements are handed between servers constantly. Without the pinning,
/// a bind regularly reaches a server that never parsed its statement.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn many_workers_through_pgbouncer_run_each_effect_once() {
    const WORKERS: usize = 24;
    const EFFECTS: u32 = 40;
    let Some((direct, pooled)) = urls() else {
        return;
    };
    let store = attached(&direct, &pooled, 32).await;
    let runs: Arc<Mutex<HashMap<u32, u32>>> = Arc::default();
    let mut workers = Vec::new();
    for w in 0..WORKERS {
        let (store, runs) = (store.clone(), Arc::clone(&runs));
        workers.push(tokio::spawn(async move {
            let rt = Runtime::builder(store)
                .worker_id(WorkerId::new(format!("w{w}")))
                .build();
            for n in 0..EFFECTS {
                let runs = Arc::clone(&runs);
                let outcome = rt
                    .effect("cart.add", n)
                    .input(&n)
                    .run(move |_| {
                        *runs.lock().unwrap().entry(n).or_default() += 1;
                        async move { Ok::<_, EffectFailure>(n) }
                    })
                    .await
                    .unwrap();
                assert!(
                    matches!(
                        outcome,
                        EffectOutcome::Committed(_) | EffectOutcome::InProgress { .. }
                    ),
                    "{outcome:?}"
                );
            }
        }));
    }
    for worker in workers {
        worker.await.unwrap();
    }

    let runs = runs.lock().unwrap().clone();
    assert_eq!(runs.len(), EFFECTS as usize);
    assert!(runs.values().all(|&n| n == 1), "each ran once: {runs:?}");
    let records = store.list(ListQuery::statuses([])).await.unwrap();
    assert_eq!(records.len(), EFFECTS as usize);
    for record in records {
        assert_eq!(record.status, EffectStatus::Committed);
        let events = store.events(record.id).await.unwrap();
        assert_eq!(
            events.last().map(|e| e.to),
            Some(EffectStatus::Committed),
            "the audit trail ends at the commit"
        );
    }
}
