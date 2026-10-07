//! Retention: pruning settled records.
//!
//! Records are kept forever unless the runtime has a [`RetentionPolicy`].
//! With one, [`Runtime::prune`] (and every round of
//! [`Runtime::run_recovery`]) deletes records that settled long enough ago,
//! with their audit trail:
//!
//! ```
//! use std::time::Duration;
//! use agent_effects::{RetentionPolicy, Runtime};
//! use agent_effects_memory::MemoryStore;
//!
//! const DAY: Duration = Duration::from_secs(86_400);
//! let runtime = Runtime::builder(MemoryStore::new())
//!     .retention(RetentionPolicy::settled(30 * DAY).failed(7 * DAY))
//!     .build();
//! ```
//!
//! Only settled records are pruned: `Committed`, `Failed`, `Rejected` and
//! `Compensated`. An effect that is unknown, waits for an operator or
//! approval, or is still running is kept however old it is, and so is one
//! whose lease is live (a compensation about to start).
//!
//! **Pruning forgets.** A pruned key is free: the next call with it starts a
//! new effect and runs it again, and a pruned committed effect can no longer
//! be compensated. Keep committed records at least as long as any caller
//! might retry with the same key, which is the same rule as for a remote
//! system's idempotency keys.

use std::time::Duration;

use tracing::debug;

use crate::error::RuntimeError;
use crate::runtime::Runtime;
use crate::state::EffectStatus;
use crate::store::{EffectStore, PruneQuery};

/// How long settled records are kept, per status. `None` keeps them
/// forever, which is the default.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RetentionPolicy {
    committed: Option<Duration>,
    failed: Option<Duration>,
    rejected: Option<Duration>,
    compensated: Option<Duration>,
}

impl RetentionPolicy {
    /// Keeps every record forever.
    pub const KEEP_ALL: Self = Self {
        committed: None,
        failed: None,
        rejected: None,
        compensated: None,
    };

    /// Prunes every settled record `older_than` after it settled.
    pub const fn settled(older_than: Duration) -> Self {
        Self {
            committed: Some(older_than),
            failed: Some(older_than),
            rejected: Some(older_than),
            compensated: Some(older_than),
        }
    }

    /// Prunes committed effects `older_than` after they committed. A
    /// later call with a pruned key runs the effect again.
    #[must_use]
    pub const fn committed(mut self, older_than: Duration) -> Self {
        self.committed = Some(older_than);
        self
    }

    /// Prunes failed effects `older_than` after they failed.
    #[must_use]
    pub const fn failed(mut self, older_than: Duration) -> Self {
        self.failed = Some(older_than);
        self
    }

    /// Prunes rejected effects (precondition or approver said no)
    /// `older_than` after they were rejected.
    #[must_use]
    pub const fn rejected(mut self, older_than: Duration) -> Self {
        self.rejected = Some(older_than);
        self
    }

    /// Prunes compensated effects `older_than` after they were undone.
    #[must_use]
    pub const fn compensated(mut self, older_than: Duration) -> Self {
        self.compensated = Some(older_than);
        self
    }

    /// Whether this policy never prunes anything.
    pub const fn keeps_all(&self) -> bool {
        self.committed.is_none()
            && self.failed.is_none()
            && self.rejected.is_none()
            && self.compensated.is_none()
    }

    /// Each status this policy prunes, with its retention.
    pub fn rules(&self) -> impl Iterator<Item = (EffectStatus, Duration)> {
        [
            (EffectStatus::Committed, self.committed),
            (EffectStatus::Failed, self.failed),
            (EffectStatus::Rejected, self.rejected),
            (EffectStatus::Compensated, self.compensated),
        ]
        .into_iter()
        .filter_map(|(status, age)| Some((status, age?)))
    }
}

/// What one [`Runtime::prune`] pass deleted, per status.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct PruneReport {
    /// Committed effects deleted.
    pub committed: u64,
    /// Failed effects deleted.
    pub failed: u64,
    /// Rejected effects deleted.
    pub rejected: u64,
    /// Compensated effects deleted.
    pub compensated: u64,
}

impl PruneReport {
    /// All records deleted.
    pub const fn total(&self) -> u64 {
        self.committed + self.failed + self.rejected + self.compensated
    }

    fn add(&mut self, status: EffectStatus, deleted: u64) {
        match status {
            EffectStatus::Committed => self.committed += deleted,
            EffectStatus::Failed => self.failed += deleted,
            EffectStatus::Rejected => self.rejected += deleted,
            EffectStatus::Compensated => self.compensated += deleted,
            _ => {}
        }
    }
}

impl<S: EffectStore> Runtime<S> {
    /// Deletes the settled records the [retention policy](RetentionPolicy)
    /// says are old enough, in batches, with their audit trails. Does
    /// nothing without a policy. [`Self::run_recovery`] calls it every
    /// round; call it yourself to prune on another schedule.
    ///
    /// # Errors
    ///
    /// [`RuntimeError::Store`] if the store fails. Batches deleted before
    /// the failure stay deleted.
    pub async fn prune(&self) -> Result<PruneReport, RuntimeError> {
        let mut report = PruneReport::default();
        let now = self.now();
        for (status, older_than) in self.retention().rules() {
            let query = PruneQuery::new(status, older_than, now);
            loop {
                let deleted = self.store().prune(query.clone()).await?;
                report.add(status, deleted);
                if deleted < query.limit as u64 {
                    break;
                }
            }
        }
        if report.total() > 0 {
            debug!(?report, "pruned settled effects");
        }
        Ok(report)
    }
}
