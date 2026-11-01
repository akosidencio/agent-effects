//! Bounded-concurrency recovery against real SQLite transactions.
#[path = "../../agent-effects/tests/support/recovery_load.rs"]
mod recovery_load;

use agent_effects::RecoveryConfig;
use agent_effects_sqlite::SqliteStore;

async fn fresh(dir: &tempfile::TempDir, name: &str) -> SqliteStore {
    SqliteStore::open(dir.path().join(format!("{name}.db")))
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlite_recovers_100_intents_concurrently() {
    let dir = tempfile::tempdir().unwrap();
    let config = RecoveryConfig::SEQUENTIAL.max_concurrency(8);
    recovery_load::backlog(fresh(&dir, "backlog").await, 100, config).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlite_recovers_1000_intents_with_bounded_concurrency() {
    let dir = tempfile::tempdir().unwrap();
    let config = RecoveryConfig::SEQUENTIAL.max_concurrency(8).batch_size(64);
    recovery_load::backlog(fresh(&dir, "backlog").await, 1000, config).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlite_slow_handler_does_not_hold_up_the_others() {
    let dir = tempfile::tempdir().unwrap();
    recovery_load::slow_handler_does_not_hold_up_others(fresh(&dir, "slow").await).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlite_competing_runtimes_run_each_intent_once() {
    let dir = tempfile::tempdir().unwrap();
    recovery_load::competing_runtimes(fresh(&dir, "competing").await, 200, 3).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlite_cancelled_pass_then_restart() {
    let dir = tempfile::tempdir().unwrap();
    recovery_load::cancelled_pass_then_restart(fresh(&dir, "cancel").await).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlite_graceful_shutdown() {
    let dir = tempfile::tempdir().unwrap();
    recovery_load::graceful_shutdown(fresh(&dir, "shutdown").await).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlite_store_failure_mid_pass() {
    let dir = tempfile::tempdir().unwrap();
    recovery_load::store_failure_mid_pass(fresh(&dir, "failure").await).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlite_outage_mid_pass() {
    let dir = tempfile::tempdir().unwrap();
    recovery_load::outage_mid_pass(fresh(&dir, "outage").await).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlite_lease_expiry_mid_pass() {
    let dir = tempfile::tempdir().unwrap();
    recovery_load::lease_expiry_mid_pass(fresh(&dir, "expiry").await).await;
}
