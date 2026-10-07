//! In-memory store for [`agent-effects`](https://crates.io/crates/agent-effects).
//!
//! For tests, examples and development. Nothing survives the process.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, SystemTime};

use agent_effects_store::{
    EffectEvent, EffectId, EffectKey, EffectRecord, EffectStore, InsertOutcome, Lease, ListQuery,
    NewEffect, PruneQuery, StoreError, TransitionRequest, WorkerId,
};

/// An in-process store for tests, examples and development.
///
/// Nothing survives the process. Clones share the same data, so a test can
/// drop a runtime and build a new one over the same store to simulate a
/// restart.
#[derive(Clone, Debug, Default)]
pub struct MemoryStore {
    inner: Arc<Mutex<Inner>>,
}

#[derive(Debug, Default)]
struct Inner {
    records: BTreeMap<EffectId, EffectRecord>,
    by_key: HashMap<EffectKey, EffectId>,
    events: HashMap<EffectId, Vec<EffectEvent>>,
}

impl MemoryStore {
    /// An empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// The number of records.
    pub fn len(&self) -> usize {
        self.lock().records.len()
    }

    /// Whether the store has no records.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        // Every mutation completes before the guard drops, so a panic while
        // holding it cannot leave a half-applied change behind.
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn with_record<T>(
        &self,
        id: EffectId,
        f: impl FnOnce(&mut EffectRecord) -> Result<T, StoreError>,
    ) -> Result<T, StoreError> {
        let mut inner = self.lock();
        let record = inner.records.get_mut(&id).ok_or(StoreError::NotFound(id))?;
        // Work on a copy so a failed check cannot leave a partial change.
        let mut updated = record.clone();
        let result = f(&mut updated)?;
        *record = updated;
        Ok(result)
    }
}

// Every operation completes under one lock without awaiting; the methods are
// `async` only to satisfy the trait.
#[allow(unknown_lints, clippy::unused_async_trait_impl)]
impl EffectStore for MemoryStore {
    async fn insert_or_get(&self, new: NewEffect) -> Result<InsertOutcome, StoreError> {
        let mut inner = self.lock();
        if let Some(id) = inner.by_key.get(&new.key) {
            return Ok(InsertOutcome {
                record: inner.records[id].clone(),
                inserted: false,
            });
        }
        let record = EffectRecord::new(new);
        inner.by_key.insert(record.key.clone(), record.id);
        inner.records.insert(record.id, record.clone());
        Ok(InsertOutcome {
            record,
            inserted: true,
        })
    }

    async fn get(&self, id: EffectId) -> Result<Option<EffectRecord>, StoreError> {
        Ok(self.lock().records.get(&id).cloned())
    }

    async fn get_by_key(&self, key: &EffectKey) -> Result<Option<EffectRecord>, StoreError> {
        let inner = self.lock();
        Ok(inner.by_key.get(key).map(|id| inner.records[id].clone()))
    }

    async fn acquire_lease(
        &self,
        id: EffectId,
        owner: &WorkerId,
        now: SystemTime,
        ttl: Duration,
    ) -> Result<Lease, StoreError> {
        self.with_record(id, |record| record.acquire_lease(owner, now, ttl))
    }

    async fn renew_lease(
        &self,
        lease: &Lease,
        now: SystemTime,
        ttl: Duration,
    ) -> Result<Lease, StoreError> {
        self.with_record(lease.effect_id, |record| {
            record.renew_lease(lease, now, ttl)
        })
    }

    async fn release_lease(&self, lease: &Lease) -> Result<(), StoreError> {
        self.with_record(lease.effect_id, |record| {
            record.release_lease(lease);
            Ok(())
        })
    }

    async fn transition(&self, request: TransitionRequest) -> Result<EffectRecord, StoreError> {
        let mut inner = self.lock();
        let id = request.id;
        let record = inner.records.get_mut(&id).ok_or(StoreError::NotFound(id))?;
        let mut updated = record.clone();
        let event = updated.apply(request)?;
        *record = updated.clone();
        inner.events.entry(id).or_default().push(event);
        Ok(updated)
    }

    async fn list(&self, query: ListQuery) -> Result<Vec<EffectRecord>, StoreError> {
        Ok(self
            .lock()
            .records
            .values()
            .filter(|record| query.matches(record))
            .take(query.limit)
            .cloned()
            .collect())
    }

    async fn events(&self, id: EffectId) -> Result<Vec<EffectEvent>, StoreError> {
        let inner = self.lock();
        if !inner.records.contains_key(&id) {
            return Err(StoreError::NotFound(id));
        }
        Ok(inner.events.get(&id).cloned().unwrap_or_default())
    }

    async fn prune(&self, query: PruneQuery) -> Result<u64, StoreError> {
        let mut inner = self.lock();
        let doomed: Vec<EffectId> = inner
            .records
            .values()
            .filter(|record| query.matches(record))
            .take(query.limit)
            .map(|record| record.id)
            .collect();
        for id in &doomed {
            if let Some(record) = inner.records.remove(id) {
                inner.by_key.remove(&record.key);
            }
            inner.events.remove(id);
        }
        Ok(doomed.len() as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::MemoryStore;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn passes_the_conformance_suite() {
        agent_effects_store::testkit::conformance(|| async { MemoryStore::new() }).await;
    }
}
