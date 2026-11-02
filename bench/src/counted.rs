//! A store wrapper that counts operations, so a benchmark can report store
//! round trips per effect next to its timings.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime};

use agent_effects::store::{
    EffectEvent, EffectRecord, EffectStore, InsertOutcome, LateObservation, Lease, ListQuery,
    NewEffect, PruneQuery, StoreError, TransitionRequest,
};
use agent_effects::{EffectId, EffectKey, WorkerId};
use serde::Serialize;

/// Operations by kind.
#[derive(Debug, Default)]
pub struct Counts {
    insert: AtomicU64,
    get: AtomicU64,
    acquire: AtomicU64,
    acquire_conflict: AtomicU64,
    renew: AtomicU64,
    release: AtomicU64,
    transition: AtomicU64,
    list: AtomicU64,
    events: AtomicU64,
    prune: AtomicU64,
    evidence: AtomicU64,
}

/// A snapshot of [`Counts`].
#[derive(Clone, Copy, Debug, Default, Serialize)]
pub struct Snapshot {
    pub total: u64,
    pub insert: u64,
    pub get: u64,
    pub acquire: u64,
    pub acquire_conflict: u64,
    pub renew: u64,
    pub release: u64,
    pub transition: u64,
    pub list: u64,
    pub events: u64,
    pub prune: u64,
    pub evidence: u64,
}

impl Counts {
    pub fn snapshot(&self) -> Snapshot {
        let get = |c: &AtomicU64| c.load(Ordering::Relaxed);
        let mut s = Snapshot {
            total: 0,
            insert: get(&self.insert),
            get: get(&self.get),
            acquire: get(&self.acquire),
            acquire_conflict: get(&self.acquire_conflict),
            renew: get(&self.renew),
            release: get(&self.release),
            transition: get(&self.transition),
            list: get(&self.list),
            events: get(&self.events),
            prune: get(&self.prune),
            evidence: get(&self.evidence),
        };
        s.total = s.insert
            + s.get
            + s.acquire
            + s.renew
            + s.release
            + s.transition
            + s.list
            + s.events
            + s.prune
            + s.evidence;
        s
    }
}

impl Snapshot {
    pub fn since(self, before: Self) -> Self {
        Self {
            total: self.total - before.total,
            insert: self.insert - before.insert,
            get: self.get - before.get,
            acquire: self.acquire - before.acquire,
            acquire_conflict: self.acquire_conflict - before.acquire_conflict,
            renew: self.renew - before.renew,
            release: self.release - before.release,
            transition: self.transition - before.transition,
            list: self.list - before.list,
            events: self.events - before.events,
            prune: self.prune - before.prune,
            evidence: self.evidence - before.evidence,
        }
    }
}

/// `S`, counting every call.
pub struct Counted<S> {
    pub inner: Arc<S>,
    pub counts: Arc<Counts>,
}

impl<S> Clone for Counted<S> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            counts: Arc::clone(&self.counts),
        }
    }
}

impl<S> Counted<S> {
    pub fn new(inner: S) -> Self {
        Self {
            inner: Arc::new(inner),
            counts: Arc::default(),
        }
    }
}

fn bump(counter: &AtomicU64) {
    counter.fetch_add(1, Ordering::Relaxed);
}

impl<S: EffectStore> EffectStore for Counted<S> {
    async fn append_late_observation(
        &self,
        observation: LateObservation,
    ) -> Result<(), StoreError> {
        bump(&self.counts.evidence);
        self.inner.append_late_observation(observation).await
    }

    async fn late_observations(
        &self,
        id: EffectId,
        after: Option<EffectId>,
        limit: usize,
    ) -> Result<Vec<LateObservation>, StoreError> {
        bump(&self.counts.evidence);
        self.inner.late_observations(id, after, limit).await
    }

    async fn insert_or_get(&self, new: NewEffect) -> Result<InsertOutcome, StoreError> {
        bump(&self.counts.insert);
        self.inner.insert_or_get(new).await
    }

    async fn get(&self, id: EffectId) -> Result<Option<EffectRecord>, StoreError> {
        bump(&self.counts.get);
        self.inner.get(id).await
    }

    async fn get_by_key(&self, key: &EffectKey) -> Result<Option<EffectRecord>, StoreError> {
        bump(&self.counts.get);
        self.inner.get_by_key(key).await
    }

    async fn acquire_lease(
        &self,
        id: EffectId,
        owner: &WorkerId,
        now: SystemTime,
        ttl: Duration,
    ) -> Result<Lease, StoreError> {
        bump(&self.counts.acquire);
        let result = self.inner.acquire_lease(id, owner, now, ttl).await;
        if matches!(result, Err(StoreError::LeaseHeld { .. })) {
            bump(&self.counts.acquire_conflict);
        }
        result
    }

    async fn renew_lease(
        &self,
        lease: &Lease,
        now: SystemTime,
        ttl: Duration,
    ) -> Result<Lease, StoreError> {
        bump(&self.counts.renew);
        self.inner.renew_lease(lease, now, ttl).await
    }

    async fn release_lease(&self, lease: &Lease) -> Result<(), StoreError> {
        bump(&self.counts.release);
        self.inner.release_lease(lease).await
    }

    async fn transition(&self, request: TransitionRequest) -> Result<EffectRecord, StoreError> {
        bump(&self.counts.transition);
        self.inner.transition(request).await
    }

    async fn list(&self, query: ListQuery) -> Result<Vec<EffectRecord>, StoreError> {
        bump(&self.counts.list);
        self.inner.list(query).await
    }

    async fn events(&self, id: EffectId) -> Result<Vec<EffectEvent>, StoreError> {
        bump(&self.counts.events);
        self.inner.events(id).await
    }

    async fn prune(&self, query: PruneQuery) -> Result<u64, StoreError> {
        bump(&self.counts.prune);
        self.inner.prune(query).await
    }
}
