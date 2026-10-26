//! Store fault boundaries for integration tests. Every successful mutation
//! still goes through the real backend; failures can occur before or after it.
#![allow(dead_code)] // Shared by test binaries exercising different boundaries.

use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use agent_effects::store::{
    EffectEvent, EffectRecord, EffectStore, InsertOutcome, LateObservation, Lease, ListQuery,
    NewEffect, PruneQuery, StoreError, TransitionRequest,
};
use agent_effects::{EffectId, EffectKey, Transition, WorkerId};
use tokio::sync::Notify;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Operation {
    Insert,
    Acquire,
    Renew,
    Release,
    Get,
    List,
    Evidence,
    Transition(Transition),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Edge {
    Before,
    After,
}

#[derive(Clone, Default)]
pub struct Gate(Arc<Signals>);

#[derive(Default)]
struct Signals {
    entered: Notify,
    release: Notify,
}

impl Gate {
    pub async fn entered(&self) {
        self.0.entered.notified().await;
    }
    pub fn release(&self) {
        self.0.release.notify_one();
    }
    pub async fn wait(&self) {
        self.0.entered.notify_one();
        self.0.release.notified().await;
    }
}

enum Action {
    Fail,
    Pause(Gate),
}
struct Rule {
    operation: Operation,
    edge: Edge,
    action: Action,
}

pub struct Controls {
    online: AtomicBool,
    next: Mutex<Option<Rule>>,
    pub renewals: AtomicU32,
}

impl Default for Controls {
    fn default() -> Self {
        Self {
            online: AtomicBool::new(true),
            next: Mutex::new(None),
            renewals: AtomicU32::new(0),
        }
    }
}

impl Controls {
    pub fn offline(&self) {
        self.online.store(false, Ordering::SeqCst);
    }
    pub fn reconnect(&self) {
        self.online.store(true, Ordering::SeqCst);
    }
    pub fn fail(&self, operation: Operation, edge: Edge) {
        *self.next.lock().unwrap() = Some(Rule {
            operation,
            edge,
            action: Action::Fail,
        });
    }
    pub fn pause(&self, operation: Operation, edge: Edge) -> Gate {
        let gate = Gate::default();
        *self.next.lock().unwrap() = Some(Rule {
            operation,
            edge,
            action: Action::Pause(gate.clone()),
        });
        gate
    }
    async fn checkpoint(&self, operation: Operation, edge: Edge) -> Result<(), StoreError> {
        if !self.online.load(Ordering::SeqCst) {
            return Err(StoreError::backend("injected database outage"));
        }
        let action = {
            let mut next = self.next.lock().unwrap();
            if next
                .as_ref()
                .is_some_and(|r| r.operation == operation && r.edge == edge)
            {
                next.take().map(|r| r.action)
            } else {
                None
            }
        };
        match action {
            Some(Action::Fail) => {
                return Err(StoreError::backend(
                    "injected database acknowledgement failure",
                ));
            }
            Some(Action::Pause(gate)) => gate.wait().await,
            None => {}
        }
        if !self.online.load(Ordering::SeqCst) {
            return Err(StoreError::backend("injected database outage"));
        }
        Ok(())
    }
}

pub struct FaultStore<S> {
    pub inner: Arc<S>,
    pub controls: Arc<Controls>,
}

impl<S> Clone for FaultStore<S> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            controls: self.controls.clone(),
        }
    }
}

impl<S> FaultStore<S> {
    pub fn new(inner: S) -> Self {
        Self {
            inner: Arc::new(inner),
            controls: Arc::default(),
        }
    }
}

impl<S: EffectStore> EffectStore for FaultStore<S> {
    async fn insert_or_get(&self, new: NewEffect) -> Result<InsertOutcome, StoreError> {
        self.controls
            .checkpoint(Operation::Insert, Edge::Before)
            .await?;
        let result = self.inner.insert_or_get(new).await?;
        self.controls
            .checkpoint(Operation::Insert, Edge::After)
            .await?;
        Ok(result)
    }
    async fn get(&self, id: EffectId) -> Result<Option<EffectRecord>, StoreError> {
        self.controls
            .checkpoint(Operation::Get, Edge::Before)
            .await?;
        let result = self.inner.get(id).await?;
        self.controls
            .checkpoint(Operation::Get, Edge::After)
            .await?;
        Ok(result)
    }
    async fn get_by_key(&self, key: &EffectKey) -> Result<Option<EffectRecord>, StoreError> {
        self.controls
            .checkpoint(Operation::Get, Edge::Before)
            .await?;
        let result = self.inner.get_by_key(key).await?;
        self.controls
            .checkpoint(Operation::Get, Edge::After)
            .await?;
        Ok(result)
    }
    async fn acquire_lease(
        &self,
        id: EffectId,
        owner: &WorkerId,
        now: SystemTime,
        ttl: Duration,
    ) -> Result<Lease, StoreError> {
        self.controls
            .checkpoint(Operation::Acquire, Edge::Before)
            .await?;
        let result = self.inner.acquire_lease(id, owner, now, ttl).await?;
        self.controls
            .checkpoint(Operation::Acquire, Edge::After)
            .await?;
        Ok(result)
    }
    async fn renew_lease(
        &self,
        lease: &Lease,
        now: SystemTime,
        ttl: Duration,
    ) -> Result<Lease, StoreError> {
        self.controls.renewals.fetch_add(1, Ordering::SeqCst);
        self.controls
            .checkpoint(Operation::Renew, Edge::Before)
            .await?;
        let result = self.inner.renew_lease(lease, now, ttl).await?;
        self.controls
            .checkpoint(Operation::Renew, Edge::After)
            .await?;
        Ok(result)
    }
    async fn release_lease(&self, lease: &Lease) -> Result<(), StoreError> {
        self.controls
            .checkpoint(Operation::Release, Edge::Before)
            .await?;
        self.inner.release_lease(lease).await?;
        self.controls
            .checkpoint(Operation::Release, Edge::After)
            .await
    }
    async fn transition(&self, request: TransitionRequest) -> Result<EffectRecord, StoreError> {
        let operation = Operation::Transition(request.transition);
        self.controls.checkpoint(operation, Edge::Before).await?;
        let result = self.inner.transition(request).await?;
        self.controls.checkpoint(operation, Edge::After).await?;
        Ok(result)
    }
    async fn list(&self, query: ListQuery) -> Result<Vec<EffectRecord>, StoreError> {
        self.controls
            .checkpoint(Operation::List, Edge::Before)
            .await?;
        let result = self.inner.list(query).await?;
        self.controls
            .checkpoint(Operation::List, Edge::After)
            .await?;
        Ok(result)
    }
    async fn events(&self, id: EffectId) -> Result<Vec<EffectEvent>, StoreError> {
        self.controls
            .checkpoint(Operation::Get, Edge::Before)
            .await?;
        self.inner.events(id).await
    }
    async fn prune(&self, query: PruneQuery) -> Result<u64, StoreError> {
        self.controls
            .checkpoint(Operation::List, Edge::Before)
            .await?;
        self.inner.prune(query).await
    }
    async fn append_late_observation(
        &self,
        observation: LateObservation,
    ) -> Result<(), StoreError> {
        self.controls
            .checkpoint(Operation::Evidence, Edge::Before)
            .await?;
        self.inner.append_late_observation(observation).await
    }
    async fn late_observations(
        &self,
        id: EffectId,
        after: Option<EffectId>,
        limit: usize,
    ) -> Result<Vec<LateObservation>, StoreError> {
        self.controls
            .checkpoint(Operation::Evidence, Edge::Before)
            .await?;
        self.inner.late_observations(id, after, limit).await
    }
}
