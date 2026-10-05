//! Verification: asking the remote system whether an effect applied.

use std::time::Duration;

use serde::{Deserialize, Serialize};

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
    /// since the attempt started; earlier, it counts as inconclusive.
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
    /// in question started.
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
