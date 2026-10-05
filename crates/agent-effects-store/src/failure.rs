//! Classification of execution failures.

use std::time::Duration;

use serde::{Deserialize, Serialize};

/// What kind of failure an action reported.
///
/// The classification, not the mere presence of an error, decides whether the
/// runtime retries, fails or treats the outcome as unknown.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "class")]
pub enum FailureClass {
    /// The request did not take effect and may succeed if repeated, e.g. a
    /// connection refused before anything was sent.
    Transient,
    /// The request did not take effect and will not succeed if repeated.
    Permanent,
    /// The request may or may not have taken effect, e.g. a timeout after
    /// the request was sent.
    Ambiguous,
    /// The remote system asked the caller to slow down.
    RateLimited {
        /// How long the remote system asked the caller to wait, if it said.
        retry_after: Option<Duration>,
    },
    /// The credentials were missing or invalid.
    Authentication,
    /// The credentials were valid but not allowed to perform the action.
    Authorization,
    /// The remote system rejected the request as malformed.
    Validation,
}

/// What the runtime does with a failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Disposition {
    /// The effect definitely did not apply; retry if the policy allows.
    Retry,
    /// The effect definitely did not apply; stop.
    Fail,
    /// The effect may have applied; the outcome is unknown.
    Unknown,
}

impl FailureClass {
    /// How the runtime treats this failure.
    pub const fn disposition(self) -> Disposition {
        match self {
            Self::Transient | Self::RateLimited { .. } => Disposition::Retry,
            Self::Ambiguous => Disposition::Unknown,
            Self::Permanent | Self::Authentication | Self::Authorization | Self::Validation => {
                Disposition::Fail
            }
        }
    }

    /// The minimum delay the remote system asked for, if any.
    pub const fn retry_after(self) -> Option<Duration> {
        match self {
            Self::RateLimited { retry_after } => retry_after,
            _ => None,
        }
    }

    /// Refines a classification with whether the request reached the remote
    /// system.
    ///
    /// Knowing the request was never sent (`Some(false)`, e.g. a refused
    /// connection or a DNS failure) turns [`Self::Ambiguous`] into
    /// [`Self::Transient`], because nothing can have been applied. Nothing
    /// else changes: a sent request that got a definitive answer, such as a
    /// 503, is still whatever its class says.
    #[must_use]
    pub const fn with_request_sent(self, request_sent: Option<bool>) -> Self {
        match (self, request_sent) {
            (Self::Ambiguous, Some(false)) => Self::Transient,
            _ => self,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unsent_requests_are_never_ambiguous() {
        assert_eq!(
            FailureClass::Ambiguous.with_request_sent(Some(false)),
            FailureClass::Transient
        );
        assert_eq!(
            FailureClass::Ambiguous.with_request_sent(Some(true)),
            FailureClass::Ambiguous
        );
        assert_eq!(
            FailureClass::Ambiguous.with_request_sent(None),
            FailureClass::Ambiguous
        );
        assert_eq!(
            FailureClass::Transient.with_request_sent(Some(true)),
            FailureClass::Transient
        );
    }

    #[test]
    fn dispositions() {
        assert_eq!(FailureClass::Ambiguous.disposition(), Disposition::Unknown);
        assert_eq!(
            FailureClass::RateLimited { retry_after: None }.disposition(),
            Disposition::Retry
        );
        assert_eq!(
            FailureClass::Authentication.disposition(),
            Disposition::Fail
        );
    }
}
