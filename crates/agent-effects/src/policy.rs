//! What the runtime may do on its own when an outcome is unknown.
//!
//! This is the rule that keeps the runtime from turning "I don't know" into a
//! duplicate side effect. It is pure, so it can be tested exhaustively.

use serde::{Deserialize, Serialize};

use crate::kind::EffectKind;
use crate::verification::VerificationMode;

/// The properties of an effect that decide how its unknown outcomes are
/// resolved.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Capabilities {
    /// The effect's kind.
    pub kind: EffectKind,
    /// Whether the remote system deduplicates on the effect's
    /// [`IdempotencyKey`](crate::id::IdempotencyKey), so a repeated request
    /// cannot apply twice.
    pub remote_idempotency: bool,
    /// Whether, and how reliably, the effect can be verified.
    pub verification: VerificationMode,
}

/// What to do with an effect whose last attempt has an unknown outcome.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UnknownPlan {
    /// Ask the remote system what happened.
    Verify,
    /// Run the action again; repeating it cannot duplicate the effect.
    Reexecute,
    /// Nothing the runtime can do safely; an operator must resolve it.
    Escalate,
}

impl Capabilities {
    /// What to do when an attempt's outcome is unknown.
    ///
    /// Verification is preferred even when re-executing would be safe: it
    /// usually costs less than the action and never repeats it.
    pub const fn unknown_plan(&self) -> UnknownPlan {
        if !matches!(self.verification, VerificationMode::None) {
            UnknownPlan::Verify
        } else if self.kind.is_naturally_idempotent() || self.remote_idempotency {
            UnknownPlan::Reexecute
        } else {
            UnknownPlan::Escalate
        }
    }

    /// Whether every unknown outcome of this effect will need an operator.
    ///
    /// True for writes that are neither idempotent nor verifiable. Such
    /// effects are allowed, but the runtime warns when one is built.
    pub const fn unknown_always_escalates(&self) -> bool {
        matches!(self.unknown_plan(), UnknownPlan::Escalate)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    const KINDS: [EffectKind; 4] = [
        EffectKind::Read,
        EffectKind::IdempotentWrite,
        EffectKind::ReversibleWrite,
        EffectKind::IrreversibleWrite,
    ];

    const MODES: [VerificationMode; 3] = [
        VerificationMode::None,
        VerificationMode::Authoritative,
        VerificationMode::EventuallyConsistent {
            settle: Duration::from_secs(5),
        },
    ];

    fn all_capabilities() -> impl Iterator<Item = Capabilities> {
        KINDS.into_iter().flat_map(|kind| {
            [false, true]
                .into_iter()
                .flat_map(move |remote_idempotency| {
                    MODES.into_iter().map(move |verification| Capabilities {
                        kind,
                        remote_idempotency,
                        verification,
                    })
                })
        })
    }

    #[test]
    fn non_idempotent_writes_never_blindly_reexecute() {
        for caps in all_capabilities() {
            if caps.unknown_plan() == UnknownPlan::Reexecute {
                assert!(
                    caps.kind.is_naturally_idempotent() || caps.remote_idempotency,
                    "{caps:?} re-executes blindly"
                );
            }
        }
    }

    #[test]
    fn escalation_happens_only_without_any_safe_option() {
        for caps in all_capabilities() {
            let no_safe_option = caps.verification == VerificationMode::None
                && !caps.kind.is_naturally_idempotent()
                && !caps.remote_idempotency;
            assert_eq!(caps.unknown_always_escalates(), no_safe_option, "{caps:?}");
        }
    }
}
