//! Unchanged v0.2.0 consumer: compile/run it against both source trees.
#[path = "legacy_memory.rs"]
mod legacy_memory;
use std::time::SystemTime;
use agent_effects::{EffectContext, EffectFailure, EffectHandler, EffectKey, EffectKind,
    EffectName, EffectObserver, EffectOutcome, EffectStatus, Handler, LogicalKey, Runtime};
use agent_effects::store::{EffectStore, ExecutionContract, NewEffect};
use agent_effects_sqlite::SqliteStore;
struct Observer;
impl EffectObserver for Observer {}
struct Job;
impl EffectHandler for Job {
    const NAME: &'static str = "compat.pending";
    type Input = u32;
    type Output = u32;
    type Error = EffectFailure;
    async fn execute(&self, _: &EffectContext, input: &u32) -> Result<u32, EffectFailure> { Ok(*input) }
}
fn key(name: &str) -> EffectKey {
    EffectKey::new(EffectName::new(name).unwrap(), LogicalKey::new("stable").unwrap())
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().collect();
    let writer = args[1] == "write";
    // An unmodified custom EffectStore implementation from the release tag.
    let memory = legacy_memory::MemoryStore::new();
    let _ = Runtime::builder(memory).observer(Observer).build();
    let store = SqliteStore::open(&args[2]).await?;
    let rt = Runtime::builder(store.clone()).register(Handler::new(Job)).build();
    let outcome = rt.effect("compat.replay", "stable").input(&42_u32).run(move |_| async move {
        assert!(writer, "committed v0.2.0 action was repeated");
        Ok::<_, EffectFailure>(7_u32)
    }).await?;
    assert_eq!(outcome, EffectOutcome::Committed(7));
    let replay = store.get_by_key(&key("compat.replay")).await?.unwrap();
    let identity = serde_json::json!({"id": replay.id, "fingerprint": replay.input_fingerprint,
        "idempotency_key": key("compat.replay").idempotency_key(), "contract": replay.execution_contract});
    let identity_path = format!("{}.identity", args[2]);
    if writer {
        std::fs::write(identity_path, serde_json::to_vec(&identity)?)?;
        let mut intent = NewEffect::new(key(Job::NAME), EffectKind::IrreversibleWrite, SystemTime::now())
            .with_contract(ExecutionContract::new(EffectKind::IrreversibleWrite, 5));
        intent.input = Some(serde_json::json!(42_u32));
        assert!(store.insert_or_get(intent).await?.inserted);
    } else {
        let old: serde_json::Value = serde_json::from_slice(&std::fs::read(identity_path)?)?;
        assert_eq!(identity, old, "identity, fingerprint or contract changed");
        let report = rt.recover().await?;
        assert!(report.resume_errors.is_empty(), "{report:?}");
        let recovered = store.get_by_key(&key(Job::NAME)).await?.unwrap();
        assert_eq!(recovered.status, EffectStatus::Committed);
        assert_eq!(recovered.attempt_count, 1);
        assert_eq!(recovered.output, Some(serde_json::json!(42)));
    }
    println!("compatibility {mode}: ok", mode=if writer {"write"} else {"read"});
    Ok(())
}
