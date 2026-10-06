//! Test doubles for code built on `agent-effects`. Enabled by the `testkit`
//! feature.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, SystemTime};

use crate::clock::Clock;
use crate::effect::EffectFailure;
use crate::failure::FailureClass;
use crate::id::IdempotencyKey;

/// What the [`FakeRemote`] does with one request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Behavior {
    /// Apply the request and answer.
    Succeed,
    /// Refuse the request without applying it, with this failure class.
    Fail(FailureClass),
    /// Refuse the connection: nothing was sent, nothing applied.
    Unreachable,
    /// Apply the request, then drop the connection before answering. The
    /// caller sees an ambiguous failure for an effect that happened.
    CommitThenDrop,
    /// Lose the request: nothing applied, and the caller sees an ambiguous
    /// failure. Indistinguishable from `CommitThenDrop` for the caller.
    LoseRequest,
    /// Never answer.
    Hang,
}

/// A scripted remote system for exercising failure handling.
///
/// It creates resources by name and counts how often each was really
/// created: [`FakeRemote::applications`] above 1 is a duplicated side effect.
/// Each request consumes the next scripted [`Behavior`]; once the script is
/// empty, requests succeed.
///
/// A request with an idempotency key the remote has already applied returns
/// the earlier result without applying again, like a provider that honours
/// `Idempotency-Key`. That includes a request scripted to
/// [`Behavior::Fail`]: the remote answers a replay before it would evaluate
/// the request. Network-level behaviors (`Unreachable`, `LoseRequest`,
/// `CommitThenDrop`'s dropped answer, `Hang`) still happen. [`FakeRemote::find`] sees a resource only `lag` after
/// it was created, like an eventually consistent search API.
#[derive(Clone)]
pub struct FakeRemote {
    clock: Arc<dyn Clock>,
    state: Arc<Mutex<State>>,
}

#[derive(Default)]
struct State {
    script: VecDeque<Behavior>,
    lag: Duration,
    requests: u32,
    created: HashMap<String, (String, SystemTime)>,
    applications: HashMap<String, u32>,
    by_idempotency_key: HashMap<IdempotencyKey, String>,
    cancellations: HashMap<String, u32>,
    cancelled_keys: HashMap<IdempotencyKey, String>,
}

impl FakeRemote {
    /// A remote that reads time from `clock` (for the lookup lag).
    pub fn new(clock: impl Clock) -> Self {
        Self {
            clock: Arc::new(clock),
            state: Arc::default(),
        }
    }

    /// Queues behaviors for the next requests, in order.
    #[must_use]
    pub fn script(self, behaviors: impl IntoIterator<Item = Behavior>) -> Self {
        self.lock().script.extend(behaviors);
        self
    }

    /// Makes [`Self::find`] lag `lag` behind creation.
    #[must_use]
    pub fn lag(self, lag: Duration) -> Self {
        self.lock().lag = lag;
        self
    }

    /// Creates `resource`, deduplicating on `idempotency_key` if given.
    /// Returns the resource's id.
    ///
    /// # Errors
    ///
    /// Whatever the script says.
    pub async fn create(
        &self,
        resource: &str,
        idempotency_key: Option<IdempotencyKey>,
    ) -> Result<String, EffectFailure> {
        let (behavior, replay) = {
            let mut state = self.lock();
            state.requests += 1;
            let replay = idempotency_key.and_then(|k| state.by_idempotency_key.get(&k).cloned());
            (
                state.script.pop_front().unwrap_or(Behavior::Succeed),
                replay,
            )
        };
        match behavior {
            Behavior::Succeed => Ok(self.apply(resource, idempotency_key)),
            // A remote that deduplicates answers a replayed key with the
            // original result before it would evaluate the request again.
            Behavior::Fail(class) => match replay {
                Some(id) => Ok(id),
                None => Err(EffectFailure::new(class, "remote refused the request")),
            },
            Behavior::Unreachable => {
                Err(EffectFailure::ambiguous("connection refused").request_sent(false))
            }
            Behavior::CommitThenDrop => {
                self.apply(resource, idempotency_key);
                Err(EffectFailure::ambiguous(
                    "connection reset before the response",
                ))
            }
            Behavior::LoseRequest => {
                Err(EffectFailure::ambiguous("timed out waiting for a response"))
            }
            Behavior::Hang => std::future::pending().await,
        }
    }

    /// Looks `resource` up, seeing it only once the lookup lag has passed.
    ///
    /// # Errors
    ///
    /// Never; the signature matches a real lookup.
    // `async` to match a real lookup's signature.
    #[allow(unknown_lints, clippy::unused_async, clippy::unused_async_trait_impl)]
    pub async fn find(&self, resource: &str) -> Result<Option<String>, EffectFailure> {
        let now = self.clock.now();
        let state = self.lock();
        Ok(state
            .created
            .get(resource)
            .filter(|(_, at)| *at + state.lag <= now)
            .map(|(id, _)| id.clone()))
    }

    /// How often `resource` was really created. More than 1 is a duplicate.
    pub fn applications(&self, resource: &str) -> u32 {
        self.lock().applications.get(resource).copied().unwrap_or(0)
    }

    /// Undoes `resource`, deduplicating on `idempotency_key` if given. Each
    /// request consumes the next scripted [`Behavior`], like `create`:
    /// `CommitThenDrop` cancels and then reports an ambiguous failure.
    /// Cancelling a resource that does not exist succeeds and changes
    /// nothing, as idempotent deletes do.
    ///
    /// # Errors
    ///
    /// Whatever the script says.
    pub async fn cancel(
        &self,
        resource: &str,
        idempotency_key: Option<IdempotencyKey>,
    ) -> Result<(), EffectFailure> {
        let behavior = {
            let mut state = self.lock();
            state.requests += 1;
            state.script.pop_front().unwrap_or(Behavior::Succeed)
        };
        match behavior {
            Behavior::Succeed => {
                self.apply_cancel(resource, idempotency_key);
                Ok(())
            }
            Behavior::Fail(class) => Err(EffectFailure::new(class, "remote refused the cancel")),
            Behavior::Unreachable => {
                Err(EffectFailure::ambiguous("connection refused").request_sent(false))
            }
            Behavior::CommitThenDrop => {
                self.apply_cancel(resource, idempotency_key);
                Err(EffectFailure::ambiguous(
                    "connection reset before the response",
                ))
            }
            Behavior::LoseRequest => {
                Err(EffectFailure::ambiguous("timed out waiting for a response"))
            }
            Behavior::Hang => std::future::pending().await,
        }
    }

    /// How often `resource` was really cancelled. More than 1 means an undo
    /// ran twice.
    pub fn cancellations(&self, resource: &str) -> u32 {
        self.lock()
            .cancellations
            .get(resource)
            .copied()
            .unwrap_or(0)
    }

    /// Whether `resource` exists: created and not cancelled.
    pub fn exists(&self, resource: &str) -> bool {
        self.lock().created.contains_key(resource)
    }

    fn apply_cancel(&self, resource: &str, idempotency_key: Option<IdempotencyKey>) {
        let mut state = self.lock();
        if idempotency_key.is_some_and(|k| state.cancelled_keys.contains_key(&k)) {
            return;
        }
        if state.created.remove(resource).is_some() {
            *state.cancellations.entry(resource.to_owned()).or_default() += 1;
        }
        if let Some(key) = idempotency_key {
            state.cancelled_keys.insert(key, resource.to_owned());
        }
    }

    /// How many create requests arrived.
    pub fn requests(&self) -> u32 {
        self.lock().requests
    }

    fn apply(&self, resource: &str, idempotency_key: Option<IdempotencyKey>) -> String {
        let now = self.clock.now();
        let mut state = self.lock();
        if let Some(id) = idempotency_key.and_then(|k| state.by_idempotency_key.get(&k)) {
            return id.clone();
        }
        let count = state.applications.entry(resource.to_owned()).or_default();
        *count += 1;
        let id = format!("{resource}#{count}");
        state.created.insert(resource.to_owned(), (id.clone(), now));
        if let Some(key) = idempotency_key {
            state.by_idempotency_key.insert(key, id.clone());
        }
        id
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}
