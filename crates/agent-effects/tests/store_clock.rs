//! A store that judges leases by its own clock, as `PostgresStore` does by
//! default. Whether a lease is still live is the store's call; a worker
//! whose clock disagrees must not second-guess it.

use std::time::{Duration, SystemTime};

use agent_effects::store::{
    EffectEvent, EffectRecord, EffectStore, InsertOutcome, Lease, ListQuery, NewEffect, PruneQuery,
    StoreError, TransitionRequest,
};
use agent_effects::{
    CompensationOutcome, EffectFailure, EffectId, EffectKey, EffectKind, EffectName, EffectOutcome,
    LogicalKey, Runtime, WorkerId,
};
use agent_effects_memory::MemoryStore;

/// A [`MemoryStore`] that ignores the times workers pass in and uses its
/// own clock, which runs `ahead` of theirs.
#[derive(Clone)]
struct OwnClock {
    inner: MemoryStore,
    ahead: Duration,
}

impl OwnClock {
    fn now(&self) -> SystemTime {
        SystemTime::now() + self.ahead
    }
}

impl EffectStore for OwnClock {
    async fn insert_or_get(&self, mut new: NewEffect) -> Result<InsertOutcome, StoreError> {
        new.now = self.now();
        self.inner.insert_or_get(new).await
    }

    async fn get(&self, id: EffectId) -> Result<Option<EffectRecord>, StoreError> {
        self.inner.get(id).await
    }

    async fn get_by_key(&self, key: &EffectKey) -> Result<Option<EffectRecord>, StoreError> {
        self.inner.get_by_key(key).await
    }

    async fn acquire_lease(
        &self,
        id: EffectId,
        owner: &WorkerId,
        _now: SystemTime,
        ttl: Duration,
    ) -> Result<Lease, StoreError> {
        self.inner.acquire_lease(id, owner, self.now(), ttl).await
    }

    async fn renew_lease(
        &self,
        lease: &Lease,
        _now: SystemTime,
        ttl: Duration,
    ) -> Result<Lease, StoreError> {
        self.inner.renew_lease(lease, self.now(), ttl).await
    }

    async fn release_lease(&self, lease: &Lease) -> Result<(), StoreError> {
        self.inner.release_lease(lease).await
    }

    async fn transition(&self, mut request: TransitionRequest) -> Result<EffectRecord, StoreError> {
        request.now = self.now();
        self.inner.transition(request).await
    }

    async fn list(&self, mut query: ListQuery) -> Result<Vec<EffectRecord>, StoreError> {
        if query.lease_expired_at.is_some() {
            query.lease_expired_at = Some(self.now());
        }
        self.inner.list(query).await
    }

    async fn events(&self, id: EffectId) -> Result<Vec<EffectEvent>, StoreError> {
        self.inner.events(id).await
    }

    async fn prune(&self, mut query: PruneQuery) -> Result<u64, StoreError> {
        query.now = self.now();
        self.inner.prune(query).await
    }
}

/// A store whose clock is an hour ahead of this worker's.
fn ahead() -> OwnClock {
    OwnClock {
        inner: MemoryStore::new(),
        ahead: Duration::from_secs(3600),
    }
}

fn key(logical: &str) -> EffectKey {
    EffectKey::new(
        EffectName::new("op").unwrap(),
        LogicalKey::new(logical).unwrap(),
    )
}

/// Leaves `key` held by a worker that died: its lease expires, by the
/// store's clock, right away.
async fn abandoned(store: &OwnClock, key: EffectKey) -> EffectRecord {
    let record = store
        .insert_or_get(NewEffect::new(
            key,
            EffectKind::IrreversibleWrite,
            SystemTime::now(),
        ))
        .await
        .unwrap()
        .record;
    store
        .acquire_lease(
            record.id,
            &WorkerId::new("dead"),
            SystemTime::now(),
            Duration::from_millis(20),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(60)).await;
    record
}

#[tokio::test]
async fn a_worker_behind_the_stores_clock_takes_over_an_expired_lease() {
    let store = ahead();
    abandoned(&store, key("run")).await;
    let outcome = Runtime::new(store)
        .effect("op", "run")
        .run(|_| async { Ok::<_, EffectFailure>(7_u32) })
        .await
        .unwrap();
    assert_eq!(outcome, EffectOutcome::Committed(7));
}

#[tokio::test]
async fn a_worker_behind_the_stores_clock_takes_over_an_expired_compensation() {
    let store = ahead();
    let rt = Runtime::new(store.clone());
    rt.effect("op", "undo")
        .run(|_| async { Ok::<_, EffectFailure>(7_u32) })
        .await
        .unwrap();
    let record = store.get_by_key(&key("undo")).await.unwrap().unwrap();
    store
        .acquire_lease(
            record.id,
            &WorkerId::new("dead"),
            SystemTime::now(),
            Duration::from_millis(20),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(60)).await;
    let outcome = rt
        .compensation("op", "undo")
        .run(|_, _: Option<u32>| async { Ok::<_, EffectFailure>(()) })
        .await
        .unwrap();
    assert_eq!(outcome, CompensationOutcome::Compensated);
}
