use std::error::Error;
use std::time::SystemTime;

use crate::id::{EffectId, WorkerId};
use crate::state::InvalidTransition;

/// Why a store operation failed.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum StoreError {
    /// No record has this id.
    #[error("effect {0} not found")]
    NotFound(EffectId),
    /// The transition is not legal from the record's status.
    #[error(transparent)]
    InvalidTransition(#[from] InvalidTransition),
    /// The record changed since the caller read it.
    #[error("effect changed concurrently: expected version {expected}, found {actual}")]
    VersionConflict {
        /// The version the caller expected.
        expected: u64,
        /// The record's current version.
        actual: u64,
    },
    /// Another worker, or another task of this one, holds a live lease.
    #[error("lease is held by {owner} until {expires_at:?}")]
    LeaseHeld {
        /// The current holder.
        owner: WorkerId,
        /// When its lease lapses unless renewed.
        expires_at: SystemTime,
    },
    /// The caller's lease expired or was taken over. The caller must stop
    /// acting on the effect.
    #[error("lease lost: it expired or another worker took over")]
    LeaseLost,
    /// The storage backend failed.
    #[error("store backend error: {0}")]
    Backend(#[source] Box<dyn Error + Send + Sync>),
}

impl StoreError {
    /// Wraps a backend error.
    pub fn backend(error: impl Into<Box<dyn Error + Send + Sync>>) -> Self {
        Self::Backend(error.into())
    }
}
