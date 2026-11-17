//! Lease-free retry scheduling against real SQLite transactions.
#[path = "../../agent-effects/tests/support/retry_load.rs"]
mod retry_load;

use agent_effects::fault::FaultPoint;
use agent_effects_sqlite::SqliteStore;

async fn fresh(dir: &tempfile::TempDir) -> SqliteStore {
    SqliteStore::open(dir.path().join("retry.db"))
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_lease_released_during_backoff() {
    let dir = tempfile::tempdir().unwrap();
    retry_load::lease_released_during_backoff(fresh(&dir).await).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_budget_across_backoffs() {
    let dir = tempfile::tempdir().unwrap();
    retry_load::budget_across_backoffs(fresh(&dir).await).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_crash_after_scheduling() {
    let dir = tempfile::tempdir().unwrap();
    retry_load::crash_then_recover_when_due(fresh(&dir).await, FaultPoint::AfterRetryScheduled)
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_crash_during_lease_free_backoff() {
    let dir = tempfile::tempdir().unwrap();
    retry_load::crash_then_recover_when_due(
        fresh(&dir).await,
        FaultPoint::AfterBackoffLeaseReleased,
    )
    .await;
}
