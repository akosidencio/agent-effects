//! Bounded-concurrency recovery on the in-memory store.
#[path = "support/recovery_load.rs"]
mod recovery_load;

use agent_effects::RecoveryConfig;
use agent_effects_memory::MemoryStore;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recovers_100_intents_concurrently() {
    let config = RecoveryConfig::SEQUENTIAL.max_concurrency(8);
    recovery_load::backlog(MemoryStore::new(), 100, config).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recovers_1000_intents_with_bounded_concurrency_across_pages() {
    let config = RecoveryConfig::SEQUENTIAL
        .max_concurrency(16)
        .batch_size(64);
    recovery_load::backlog(MemoryStore::new(), 1000, config).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_default_stays_sequential() {
    recovery_load::backlog(MemoryStore::new(), 100, RecoveryConfig::default()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_slow_handler_does_not_hold_up_the_others() {
    recovery_load::slow_handler_does_not_hold_up_others(MemoryStore::new()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_sequential_pass_waits_behind_a_slow_handler() {
    recovery_load::sequential_default_waits_behind_a_slow_handler(MemoryStore::new()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn competing_runtimes_run_each_intent_once() {
    recovery_load::competing_runtimes(MemoryStore::new(), 300, 3).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cancelled_pass_neither_interrupts_nor_loses_effects() {
    recovery_load::cancelled_pass_then_restart(MemoryStore::new()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_stops_starting_effects_and_waits_for_started_ones() {
    recovery_load::graceful_shutdown(MemoryStore::new()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_before_a_pass_returns_at_once() {
    recovery_load::shutdown_before_a_pass(MemoryStore::new()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_store_failure_mid_pass_is_reported_after_started_effects_finish() {
    recovery_load::store_failure_mid_pass(MemoryStore::new()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_outage_mid_pass_never_duplicates_an_effect() {
    recovery_load::outage_mid_pass(MemoryStore::new()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lease_expiring_mid_pass_is_taken_over_and_fenced() {
    recovery_load::lease_expiry_mid_pass(MemoryStore::new()).await;
}

#[test]
fn zero_limits_are_raised_to_one() {
    let config = RecoveryConfig::SEQUENTIAL.max_concurrency(0).batch_size(0);
    assert_eq!((config.concurrency_limit(), config.page_size()), (1, 1));
    assert_eq!(RecoveryConfig::default(), RecoveryConfig::SEQUENTIAL);
    assert_eq!(
        (
            RecoveryConfig::SEQUENTIAL.concurrency_limit(),
            RecoveryConfig::SEQUENTIAL.page_size()
        ),
        (1, 100)
    );
}
