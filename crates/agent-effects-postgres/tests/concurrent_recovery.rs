//! Bounded-concurrency recovery against PostgreSQL: row locks, lock-skipping
//! scans and the database clock. Scenarios that move time forward use the
//! caller's clock; the rest use the database's.
#[path = "../../agent-effects/tests/support/recovery_load.rs"]
mod recovery_load;

use agent_effects::RecoveryConfig;
use agent_effects_postgres::{ClockSource, PostgresOptions, PostgresStore};

fn url() -> Option<String> {
    let url = std::env::var("AGENT_EFFECTS_POSTGRES_URL").ok();
    assert!(
        url.is_some() || std::env::var_os("AGENT_EFFECTS_REQUIRE_POSTGRES").is_none(),
        "AGENT_EFFECTS_REQUIRE_POSTGRES is set but AGENT_EFFECTS_POSTGRES_URL is not"
    );
    if url.is_none() {
        eprintln!("skipped: set AGENT_EFFECTS_POSTGRES_URL");
    }
    url
}

async fn fresh(url: &str) -> PostgresStore {
    PostgresOptions::new()
        .schema(format!(
            "recovery_{}",
            agent_effects::EffectId::new().to_string().replace('-', "")
        ))
        .max_connections(24)
        .connect(url)
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_recovers_100_intents_concurrently() {
    let Some(url) = url() else { return };
    let config = RecoveryConfig::SEQUENTIAL.max_concurrency(8);
    recovery_load::backlog(fresh(&url).await, 100, config).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_recovers_1000_intents_with_bounded_concurrency() {
    let Some(url) = url() else { return };
    let config = RecoveryConfig::SEQUENTIAL
        .max_concurrency(16)
        .batch_size(64);
    recovery_load::backlog(fresh(&url).await, 1000, config).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_slow_handler_does_not_hold_up_the_others() {
    let Some(url) = url() else { return };
    recovery_load::slow_handler_does_not_hold_up_others(fresh(&url).await).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_competing_runtimes_run_each_intent_once() {
    let Some(url) = url() else { return };
    recovery_load::competing_runtimes(fresh(&url).await, 300, 3).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_cancelled_pass_then_restart() {
    let Some(url) = url() else { return };
    recovery_load::cancelled_pass_then_restart(fresh(&url).await).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_graceful_shutdown() {
    let Some(url) = url() else { return };
    recovery_load::graceful_shutdown(fresh(&url).await).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_store_failure_mid_pass() {
    let Some(url) = url() else { return };
    recovery_load::store_failure_mid_pass(fresh(&url).await).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_outage_mid_pass() {
    let Some(url) = url() else { return };
    let store = fresh(&url).await.with_clock_source(ClockSource::Caller);
    recovery_load::outage_mid_pass(store).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_lease_expiry_mid_pass() {
    let Some(url) = url() else { return };
    let store = fresh(&url).await.with_clock_source(ClockSource::Caller);
    recovery_load::lease_expiry_mid_pass(store).await;
}
