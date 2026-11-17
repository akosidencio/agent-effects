//! `EffectObserver::on_event`: what the runtime reports besides stored
//! transitions.
#[path = "support/fault_store.rs"]
mod fault_store;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use agent_effects::{
    EffectFailure, EffectObserver, RetentionPolicy, Runtime, RuntimeEvent, TokioClock, WorkerId,
};
use agent_effects_memory::MemoryStore;
use fault_store::{Edge, FaultStore, Operation};

/// Records each event as a short description.
#[derive(Clone, Default)]
struct Events(Arc<Mutex<Vec<String>>>);

impl Events {
    fn seen(&self) -> Vec<String> {
        self.0.lock().unwrap().clone()
    }
}

impl EffectObserver for Events {
    fn on_event(&self, event: &RuntimeEvent<'_>) {
        let text = match event {
            RuntimeEvent::LeaseConflict { name } => format!("conflict {name}"),
            RuntimeEvent::LeaseRenewalFailed => "renewal failed".into(),
            RuntimeEvent::RecoveryPass {
                report: Some(r), ..
            } => {
                format!("pass resumed={}", r.resumed.len())
            }
            RuntimeEvent::RecoveryPass { report: None, .. } => "pass failed".into(),
            RuntimeEvent::Pruned { status, count } => format!("pruned {status} {count}"),
            _ => "other".into(),
        };
        self.0.lock().unwrap().push(text);
    }
}

#[tokio::test(start_paused = true)]
async fn renewal_failures_and_failed_passes_are_reported() {
    let store = FaultStore::new(MemoryStore::new());
    let events = Events::default();
    let rt = Runtime::builder(store.clone())
        .clock(TokioClock::new())
        .worker_id(WorkerId::new("w"))
        .lease_ttl(Duration::from_secs(30))
        .retention(RetentionPolicy::settled(Duration::ZERO))
        .observer(events.clone())
        .build();

    // The first renewal (after 10 s) fails; the next one succeeds.
    store.controls.fail(Operation::Renew, Edge::Before);
    rt.effect("slow.job", "k")
        .run(|_| async {
            tokio::time::sleep(Duration::from_secs(25)).await;
            Ok::<_, EffectFailure>(())
        })
        .await
        .unwrap();
    assert_eq!(events.seen(), ["renewal failed"]);

    store.controls.fail(Operation::List, Edge::Before);
    assert!(rt.recover().await.is_err());
    assert_eq!(rt.recover().await.unwrap().resumed.len(), 0);
    assert_eq!(rt.prune().await.unwrap().committed, 1);
    assert_eq!(
        events.seen(),
        [
            "renewal failed",
            "pass failed",
            "pass resumed=0",
            "pruned committed 1"
        ]
    );
}
