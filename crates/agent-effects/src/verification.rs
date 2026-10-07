//! Verification: asking the remote system whether an effect applied.
//!
//! A verification runs after every successful attempt (a postcondition) and
//! to reconcile an attempt whose outcome is unknown.

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::effect::{EffectContext, EffectFailure};

/// What a verification found in the remote system.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Verification<T> {
    /// The effect applied; carries the remote state, which becomes the
    /// effect's output.
    Confirmed(T),
    /// No trace of the effect. How far this is trusted depends on the
    /// [`VerificationMode`].
    NotApplied,
    /// The remote system cannot tell right now. The runtime checks again
    /// later and never treats this as "not applied".
    Inconclusive,
    /// The remote state contradicts the effect, e.g. a payment exists for
    /// the order but with a different amount. Needs an operator.
    Conflict {
        /// What contradicts what.
        details: String,
    },
}

/// A boxed verification future.
pub type VerificationFuture<T> =
    Pin<Box<dyn Future<Output = Result<Verification<T>, EffectFailure>> + Send>>;

/// The verification attached to an effect, if any.
///
/// Implemented by [`NoVerification`] and [`VerifyWith`]; set through
/// `EffectBuilder::verify`. A failing check (`Err`) counts as
/// [`Verification::Inconclusive`].
pub trait Verifier<T>: Send + Sync + 'static {
    /// Starts a check, or `None` if the effect has no verification.
    fn check(&self, ctx: EffectContext) -> Option<VerificationFuture<T>>;
}

/// The effect has no verification.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoVerification;

impl<T> Verifier<T> for NoVerification {
    fn check(&self, _ctx: EffectContext) -> Option<VerificationFuture<T>> {
        None
    }
}

/// Verification by a closure.
#[derive(Clone, Copy, Debug)]
pub struct VerifyWith<F>(pub(crate) F);

impl<T, F, Fut> Verifier<T> for VerifyWith<F>
where
    F: Fn(EffectContext) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Verification<T>, EffectFailure>> + Send + 'static,
{
    fn check(&self, ctx: EffectContext) -> Option<VerificationFuture<T>> {
        Some(Box::pin((self.0)(ctx)))
    }
}

/// How far a verification result can be trusted.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "mode")]
pub enum VerificationMode {
    /// The effect has no verification.
    #[default]
    None,
    /// The remote system reads its own writes: "not found" means the effect
    /// did not apply.
    Authoritative,
    /// The remote system's lookup lags behind its writes, as many search and
    /// list APIs do. "Not found" is only trusted once `settle` has passed
    /// since the attempt ended (its action returned or was found
    /// interrupted); earlier, it counts as inconclusive.
    EventuallyConsistent {
        /// How long a write may take to become visible to the lookup.
        settle: Duration,
    },
}

/// How to read a verification that found no trace of the effect.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NotFoundReading {
    /// The effect did not apply; re-executing it is safe.
    NotApplied,
    /// Too early to tell; verify again after `wait`.
    TooEarly {
        /// Remaining time until the lookup can be trusted.
        wait: Duration,
    },
}

impl VerificationMode {
    /// How to read "not found" when `elapsed` has passed since the attempt
    /// in question ended.
    ///
    /// [`VerificationMode::None`] has no verification to read; it is
    /// reported as [`NotFoundReading::TooEarly`] with no wait so callers that
    /// get here by mistake never re-execute.
    pub fn read_not_found(self, elapsed: Duration) -> NotFoundReading {
        match self {
            Self::Authoritative => NotFoundReading::NotApplied,
            Self::EventuallyConsistent { settle } if elapsed >= settle => {
                NotFoundReading::NotApplied
            }
            Self::EventuallyConsistent { settle } => NotFoundReading::TooEarly {
                wait: settle.saturating_sub(elapsed),
            },
            Self::None => NotFoundReading::TooEarly {
                wait: Duration::ZERO,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn eventually_consistent_lookups_wait_out_the_settle_delay() {
        let mode = VerificationMode::EventuallyConsistent {
            settle: Duration::from_secs(5),
        };
        assert_eq!(
            mode.read_not_found(Duration::from_secs(2)),
            NotFoundReading::TooEarly {
                wait: Duration::from_secs(3)
            }
        );
        assert_eq!(
            mode.read_not_found(Duration::from_secs(5)),
            NotFoundReading::NotApplied
        );
        assert_eq!(
            VerificationMode::None.read_not_found(Duration::MAX),
            NotFoundReading::TooEarly {
                wait: Duration::ZERO
            }
        );
    }
}
