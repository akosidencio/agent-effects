//! Infrastructure errors.
//!
//! These are failures of the runtime itself: storage, configuration,
//! serialization. What happened to an effect, including "unknown", is an
//! [`EffectOutcome`](crate::EffectOutcome), never a `RuntimeError`.

use crate::id::{EffectId, IdentityError};
use crate::kind::EffectKind;
use crate::store::StoreError;

/// Why the runtime could not process an effect.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum RuntimeError {
    /// The effect name or logical key was invalid.
    #[error("invalid effect identity: {0}")]
    Identity(#[from] IdentityError),

    /// The key was used before with a different input. Returning the earlier
    /// effect's result would answer a question nobody asked.
    #[error("effect {id} was recorded with a different input")]
    InputMismatch {
        /// The existing effect.
        id: EffectId,
    },

    /// The key was used before with a different [`EffectKind`].
    #[error("effect {id} was recorded as {stored:?}, not {requested:?}")]
    KindMismatch {
        /// The existing effect.
        id: EffectId,
        /// The kind on record.
        stored: EffectKind,
        /// The kind of this call.
        requested: EffectKind,
    },

    /// The input could not be serialized.
    #[error("could not serialize the effect input: {0}")]
    Input(#[source] serde_json::Error),

    /// A committed effect's stored output does not deserialize into the
    /// requested type, e.g. because the type changed since it was recorded.
    #[error(
        "effect {id} committed, but its stored output does not match the requested type: {source}"
    )]
    Output {
        /// The committed effect.
        id: EffectId,
        /// The deserialization error.
        #[source]
        source: serde_json::Error,
    },

    /// No handler of the submitted type is registered under its name.
    #[error("no handler registered for effect `{name}`")]
    NotRegistered {
        /// The effect name.
        name: &'static str,
    },

    /// A stored input no longer deserializes into the handler's input type,
    /// e.g. because the type changed since the effect was recorded.
    #[error("effect {id}'s stored input does not match its handler's input type: {source}")]
    StoredInput {
        /// The effect.
        id: EffectId,
        /// The deserialization error.
        #[source]
        source: serde_json::Error,
    },

    /// The store failed. If this happens after the action ran, the effect
    /// stays in doubt and recovery will treat its outcome as unknown.
    #[error(transparent)]
    Store(#[from] StoreError),

    /// The runtime's own task failed (a bug in `agent-effects`).
    #[error("effect task failed: {0}")]
    Internal(String),
}
