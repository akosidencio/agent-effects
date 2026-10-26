//! Deterministic connectivity failures against real backend operations.
#[path = "support/reliability.rs"]
mod reliability;

use agent_effects_memory::MemoryStore;

#[tokio::test]
async fn storage_failures_preserve_effect_outcomes() {
    reliability::storage_matrix(|| async { MemoryStore::new() }).await;
}

#[tokio::test]
async fn compensation_failures_preserve_explicit_intent_and_deduplication() {
    reliability::compensation_storage_matrix(|| async { MemoryStore::new() }).await;
}

#[tokio::test]
async fn disconnected_workers_recover_without_duplicate_applications() {
    reliability::reconnect_with_simultaneous_recovery(|| async { MemoryStore::new() }).await;
}
