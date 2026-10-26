//! Persisted assumptions that determine whether execution is safe.

use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::EffectKind;

/// The verification guarantee recorded with an effect.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "mode")]
pub enum VerificationContract {
    /// No remote verification is available.
    None,
    /// A lookup reads its own writes.
    Authoritative,
    /// Absence is trusted only after the recorded settle interval.
    EventuallyConsistent {
        /// Minimum time since the attempt ended.
        settle: Duration,
    },
}

/// Immutable execution assumptions recorded before the first attempt.
///
/// Contracts are compared before replay, lease acquisition, or execution.
/// Retry delays and timeouts are incidental configuration; the lifetime
/// attempt ceiling is safety relevant and must remain unchanged on reattach.
/// Changing code while retaining `handler_version` asserts that its external
/// semantics are compatible with this contract.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionContract {
    /// Contract representation version. Currently `1`.
    pub format_version: u32,
    /// What repeating the external operation does.
    pub kind: EffectKind,
    /// The application attests remote deduplication of the stable key.
    pub remote_idempotency: bool,
    /// How the runtime may interpret a remote lookup.
    pub verification: VerificationContract,
    /// Application-defined semantic version, not a crate release version.
    pub handler_version: String,
    /// Optional trusted provider or adapter identity; never a credential.
    pub adapter: Option<String>,
    /// A human decision is required before the first attempt.
    pub require_approval: bool,
    /// Risk policy permits automatic retries.
    pub automatic_retry: bool,
    /// Lifetime attempt ceiling, including the initial attempt.
    pub max_attempts: u32,
}

impl ExecutionContract {
    /// Conservative assumptions for an unverified irreversible or reversible
    /// operation, with the specified lifetime retry budget.
    pub fn new(kind: EffectKind, max_attempts: u32) -> Self {
        Self {
            format_version: 1,
            kind,
            remote_idempotency: false,
            verification: VerificationContract::None,
            handler_version: "1".into(),
            adapter: None,
            require_approval: false,
            automatic_retry: true,
            max_attempts,
        }
    }

    /// Whether `requested` preserves the recorded execution assumptions.
    /// All fields must match; unknown representation versions are refused.
    pub fn permits(&self, requested: &Self) -> bool {
        if self.format_version != 1 || requested.format_version != 1 {
            return false;
        }
        self == requested
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn compatibility_never_grants_changed_execution_assumptions(
            remote in any::<bool>(), approval in any::<bool>(),
            automatic in any::<bool>(), attempts in any::<u32>(),
            format in 2_u32..u32::MAX,
        ) {
            let mut stored = ExecutionContract::new(EffectKind::IrreversibleWrite, attempts);
            stored.remote_idempotency = remote;
            stored.require_approval = approval;
            stored.automatic_retry = automatic;
            prop_assert!(stored.permits(&stored));
            let mut changed = stored.clone();
            changed.remote_idempotency = !remote;
            prop_assert!(!stored.permits(&changed));
            changed = stored.clone();
            changed.require_approval = !approval;
            prop_assert!(!stored.permits(&changed));
            changed = stored.clone();
            changed.automatic_retry = !automatic;
            prop_assert!(!stored.permits(&changed));
            changed = stored.clone();
            changed.format_version = format;
            prop_assert!(!stored.permits(&changed));
            prop_assert!(!changed.permits(&changed));
        }
    }
}
