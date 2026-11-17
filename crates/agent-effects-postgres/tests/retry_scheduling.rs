//! Lease-free retry scheduling against PostgreSQL, with leases judged by the
//! database's clock and retry times computed by the workers.
#[path = "../../agent-effects/tests/support/retry_load.rs"]
mod retry_load;

use agent_effects::fault::FaultPoint;
use agent_effects_postgres::{PostgresOptions, PostgresStore};

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
            "retry_{}",
            agent_effects::EffectId::new().to_string().replace('-', "")
        ))
        .connect(url)
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_lease_released_during_backoff() {
    let Some(url) = url() else { return };
    retry_load::lease_released_during_backoff(fresh(&url).await).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_budget_across_backoffs() {
    let Some(url) = url() else { return };
    retry_load::budget_across_backoffs(fresh(&url).await).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_crash_after_scheduling() {
    let Some(url) = url() else { return };
    retry_load::crash_then_recover_when_due(fresh(&url).await, FaultPoint::AfterRetryScheduled)
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_crash_during_lease_free_backoff() {
    let Some(url) = url() else { return };
    retry_load::crash_then_recover_when_due(
        fresh(&url).await,
        FaultPoint::AfterBackoffLeaseReleased,
    )
    .await;
}
