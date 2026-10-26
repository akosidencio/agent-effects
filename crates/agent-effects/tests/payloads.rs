//! Schema upgrades and replay errors cannot authorize another external action.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, SystemTime};

use agent_effects::store::{ExecutionContract, NewEffect, TransitionRequest};
use agent_effects::{
    CompensableEffect, CompensationContext, CompensationOutcome, EffectContext, EffectFailure,
    EffectHandler, EffectKey, EffectKind, EffectName, EffectOutcome, EffectStatus, EffectStore,
    Handler, LogicalKey, PayloadError, PayloadSchema, Resolution, Runtime, RuntimeError,
    Transition, WorkerId,
};
use agent_effects_memory::MemoryStore;
use serde::{Deserialize, Serialize, Serializer};
use serde_json::{Value, json};

fn key(logical: &str) -> EffectKey {
    EffectKey::new(
        EffectName::new("payload.op").unwrap(),
        LogicalKey::new(logical).unwrap(),
    )
}

async fn forbidden<T>(_: EffectContext) -> Result<T, EffectFailure> {
    panic!("replay invoked an external action")
}

fn output_schema() -> PayloadSchema {
    PayloadSchema::new(2).migrate(|version, value| {
        if version != 1 {
            return Err(PayloadError::UnsupportedVersion {
                stored: version,
                current: 2,
            });
        }
        let value: u64 =
            serde_json::from_value(value).map_err(|e| PayloadError::Migration(e.to_string()))?;
        Ok(json!({ "value": value }))
    })
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Current {
    value: u64,
}

#[tokio::test]
async fn committed_outputs_migrate_on_replay_without_reexecuting_or_rewriting_history() {
    let store = MemoryStore::new();
    assert_eq!(
        Runtime::new(store.clone())
            .effect("payload.op", "out")
            .run(|_| async { Ok::<_, EffectFailure>(7_u64) })
            .await
            .unwrap(),
        EffectOutcome::Committed(7)
    );
    let before = store.get_by_key(&key("out")).await.unwrap().unwrap();
    let replay = Runtime::new(store.clone())
        .effect("payload.op", "out")
        .output_schema(output_schema())
        .run(forbidden::<Current>)
        .await
        .unwrap();
    assert_eq!(replay, EffectOutcome::Committed(Current { value: 7 }));
    assert_eq!(store.get(before.id).await.unwrap().unwrap(), before);
    let waited: EffectOutcome<Current> = Runtime::new(store)
        .wait_with_output_schema(before.id, Duration::ZERO, output_schema())
        .await
        .unwrap();
    assert_eq!(waited, replay);
}

#[tokio::test]
async fn unsupported_output_schemas_leave_effects_committed() {
    let store = MemoryStore::new();
    Runtime::new(store.clone())
        .effect("payload.op", "unsupported")
        .output_schema(PayloadSchema::new(9))
        .run(|_| async { Ok::<_, EffectFailure>(7_u64) })
        .await
        .unwrap();
    let rt = Runtime::new(store.clone());
    let result = rt
        .effect("payload.op", "unsupported")
        .run(forbidden::<u64>)
        .await;
    assert!(matches!(
        result,
        Err(RuntimeError::OutputPayload {
            source: PayloadError::UnsupportedVersion {
                stored: 9,
                current: 1
            },
            ..
        })
    ));
    assert_eq!(
        store
            .get_by_key(&key("unsupported"))
            .await
            .unwrap()
            .unwrap()
            .status,
        EffectStatus::Committed
    );
}

#[tokio::test]
async fn malformed_committed_output_is_reported_without_reexecution() {
    let store = MemoryStore::new();
    let rt = Runtime::new(store.clone());
    let outcome = rt
        .effect("payload.op", "malformed")
        .run(|_| async { Err::<u64, _>(EffectFailure::ambiguous("response lost")) })
        .await
        .unwrap();
    let EffectOutcome::NeedsIntervention { id } = outcome else {
        panic!("{outcome:?}")
    };
    rt.resolve(
        id,
        Resolution::Applied {
            output: Some(json!("not a number")),
        },
        "operator",
        "confirmed remotely",
    )
    .await
    .unwrap();
    assert!(matches!(
        rt.effect("payload.op", "malformed")
            .run(forbidden::<u64>)
            .await,
        Err(RuntimeError::Output { .. })
    ));
    assert_eq!(
        store.get(id).await.unwrap().unwrap().status,
        EffectStatus::Committed
    );
}

#[derive(Debug, PartialEq, Deserialize)]
struct NonSerializable;

impl Serialize for NonSerializable {
    fn serialize<S: Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
        Err(serde::ser::Error::custom("output cannot be serialized"))
    }
}

#[tokio::test]
async fn serialization_failure_preserves_commit_and_reports_unavailable_replay() {
    let store = MemoryStore::new();
    let rt = Runtime::new(store.clone());
    assert_eq!(
        rt.effect("payload.op", "unserializable")
            .run(|_| async { Ok::<_, EffectFailure>(NonSerializable) })
            .await
            .unwrap(),
        EffectOutcome::Committed(NonSerializable)
    );
    let record = store
        .get_by_key(&key("unserializable"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(record.status, EffectStatus::Committed);
    assert!(record.output.is_none());
    let events = store.events(record.id).await.unwrap();
    assert!(
        events
            .last()
            .unwrap()
            .payload
            .as_ref()
            .unwrap()
            .get("output_not_stored")
            .is_some()
    );
    assert!(
        matches!(rt.effect("payload.op", "unserializable").run(forbidden::<NonSerializable>).await,
        Err(RuntimeError::OutputUnavailable { id }) if id == record.id)
    );
    // Missing output must not silently replay as unit/null either.
    assert!(matches!(
        rt.wait::<()>(record.id, Duration::ZERO).await,
        Err(RuntimeError::OutputUnavailable { .. })
    ));
    assert_eq!(store.get(record.id).await.unwrap().unwrap(), record);
}

struct Upgraded {
    executions: Arc<AtomicU32>,
    compensations: Arc<AtomicU32>,
}

impl EffectHandler for Upgraded {
    const NAME: &'static str = "payload.op";
    type Input = Current;
    type Output = Current;
    type Error = EffectFailure;

    fn input_schema(&self) -> PayloadSchema {
        PayloadSchema::new(2).migrate(|version, value| {
            if version != 1 {
                return Err(PayloadError::UnsupportedVersion {
                    stored: version,
                    current: 2,
                });
            }
            let old = value
                .get("old")
                .and_then(Value::as_u64)
                .ok_or_else(|| PayloadError::Migration("missing old input field".into()))?;
            Ok(json!({ "value": old }))
        })
    }
    fn output_schema(&self) -> PayloadSchema {
        output_schema()
    }
    fn execute(
        &self,
        _: &EffectContext,
        input: &Current,
    ) -> impl std::future::Future<Output = Result<Current, EffectFailure>> + Send {
        self.executions.fetch_add(1, Ordering::SeqCst);
        std::future::ready(Ok(Current { value: input.value }))
    }
}

impl CompensableEffect for Upgraded {
    fn compensate(
        &self,
        _: &CompensationContext,
        input: &Current,
        output: Option<&Current>,
    ) -> impl std::future::Future<Output = Result<(), EffectFailure>> + Send {
        assert_eq!(output.unwrap(), input);
        self.compensations.fetch_add(1, Ordering::SeqCst);
        std::future::ready(Ok(()))
    }
}

async fn seed(store: &MemoryStore, logical: &str, version: u32) -> agent_effects::EffectRecord {
    let mut new = NewEffect::new(
        key(logical),
        EffectKind::IrreversibleWrite,
        SystemTime::now(),
    )
    .with_contract(ExecutionContract::new(EffectKind::IrreversibleWrite, 5));
    new.input = Some(json!({ "old": 7 }));
    new.input_schema_version = version;
    store.insert_or_get(new).await.unwrap().record
}

fn runtime(store: &MemoryStore) -> (Runtime<MemoryStore>, Arc<AtomicU32>, Arc<AtomicU32>) {
    let executions = Arc::<AtomicU32>::default();
    let compensations = Arc::<AtomicU32>::default();
    let rt = Runtime::builder(store.clone())
        .register(
            Handler::new(Upgraded {
                executions: executions.clone(),
                compensations: compensations.clone(),
            })
            .compensable(),
        )
        .build();
    (rt, executions, compensations)
}

#[tokio::test]
async fn recovery_migrates_old_inputs_and_writes_versioned_new_outputs() {
    let store = MemoryStore::new();
    let old = seed(&store, "input", 1).await;
    let (rt, executions, _) = runtime(&store);
    let report = rt.recover().await.unwrap();
    assert!(report.resume_errors.is_empty(), "{report:?}");
    assert_eq!(report.resumed, [(old.id, EffectStatus::Committed)]);
    assert_eq!(executions.load(Ordering::SeqCst), 1);
    let record = store.get(old.id).await.unwrap().unwrap();
    assert_eq!(record.input, old.input);
    assert_eq!(record.input_schema_version, 1);
    assert_eq!(record.output_schema_version, 2);
    assert_eq!(record.output, Some(json!({ "value": 7 })));
    assert!(matches!(
        rt.submit::<Upgraded>("input", Current { value: 7 }).await,
        Err(RuntimeError::InputSchemaMismatch {
            stored: 1,
            requested: 2,
            ..
        })
    ));
}

#[tokio::test]
async fn unsupported_input_migration_is_reported_without_running_the_handler() {
    let store = MemoryStore::new();
    let old = seed(&store, "unsupported-input", 9).await;
    let (rt, executions, _) = runtime(&store);
    let report = rt.recover().await.unwrap();
    assert_eq!(report.resume_errors.len(), 1);
    assert!(
        report.resume_errors[0]
            .1
            .contains("unsupported payload schema 9")
    );
    assert_eq!(executions.load(Ordering::SeqCst), 0);
    assert_eq!(store.get(old.id).await.unwrap().unwrap(), old);
}

#[tokio::test]
async fn durable_compensation_migrates_both_old_input_and_old_output() {
    let store = MemoryStore::new();
    let old = seed(&store, "compensate", 1).await;
    let now = SystemTime::now();
    let lease = store
        .acquire_lease(
            old.id,
            &WorkerId::new("original"),
            now,
            Duration::from_secs(30),
        )
        .await
        .unwrap();
    let executing = store
        .transition(TransitionRequest::new(
            &old,
            Some(&lease),
            Transition::StartAttempt,
            now,
        ))
        .await
        .unwrap();
    let mut request = TransitionRequest::new(&executing, Some(&lease), Transition::Succeeded, now);
    request.output = Some(json!(7));
    store.transition(request).await.unwrap();
    store.release_lease(&lease).await.unwrap();
    let (rt, executions, compensations) = runtime(&store);
    assert_eq!(
        rt.compensate::<Upgraded>("compensate").await.unwrap(),
        CompensationOutcome::Compensated
    );
    assert_eq!(executions.load(Ordering::SeqCst), 0);
    assert_eq!(compensations.load(Ordering::SeqCst), 1);
}
