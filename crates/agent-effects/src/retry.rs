//! Retry policy: how many attempts, and how long to wait between them.
//!
//! Whether a failure may be retried at all is decided elsewhere, by its
//! [`FailureClass`] and [`crate::policy`]. This module only does the counting
//! and the arithmetic.
//!
//! # Scheduling
//!
//! A retry is decided and stored before anyone waits for it: under the
//! effect's lease, the runtime records `ScheduleRetry` with the time the next
//! attempt may start (`next_attempt_at`), which is the policy's backoff or a
//! provider's `retry_after`, whichever is later. Only then does it wait.
//!
//! - **Short waits** (under
//!   [`RuntimeBuilder::lease_free_backoff`](crate::RuntimeBuilder::lease_free_backoff),
//!   the lease TTL by default) keep the lease: they cost at most a few
//!   renewals, fewer store round trips than releasing and retaking it.
//! - **Longer waits** release the lease. The caller sleeps without it, then
//!   takes a fresh lease, re-reads the record and continues from whatever
//!   it finds: the scheduled retry, or another worker's result. A call that
//!   finds another worker holding the lease at that point waits for it and
//!   reports its outcome.
//! - **Recovery** stores the retry and moves on; a later pass resumes it
//!   once it is due ([`RecoveryReport::not_due`](crate::RecoveryReport::not_due)
//!   until then).
//!
//! Whoever starts the retry checks the stored time again under its own
//! lease and waits if it is not due, so a released lease never lets a retry
//! start early. A worker that dies during the wait leaves a `Pending` record
//! with its schedule: recovery, or the next call with the same key, resumes
//! it when it is due, without first waiting for a lease to expire.
//!
//! Releasing the lease never grants a retry. The decision was made, and
//! stored, under the lease before the wait, from the failure's class, the
//! effect's execution contract and its lifetime attempt budget. In
//! particular a provider's `Retry-After` only sets *when* an attempt may
//! start: an ambiguous outcome is re-run only if the effect is idempotent,
//! remotely idempotent, or verified not to have applied.
//! Compensation retries are scheduled the same way.

use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::failure::FailureClass;

/// Exponential backoff with optional jitter.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct RetryPolicy {
    /// Total attempts allowed, including the first. `1` disables retries.
    pub max_attempts: u32,
    /// Delay before the first retry.
    pub initial_delay: Duration,
    /// Upper bound on the computed backoff. A rate limit's `retry_after`
    /// may exceed it.
    pub max_delay: Duration,
    /// Growth factor per retry. Values below `1.0` are treated as `1.0`.
    pub multiplier: f64,
    /// Whether to randomize each delay into `[delay / 2, delay]`.
    pub jitter: bool,
}

impl RetryPolicy {
    /// A policy that never retries.
    pub const NONE: Self = Self {
        max_attempts: 1,
        initial_delay: Duration::ZERO,
        max_delay: Duration::ZERO,
        multiplier: 1.0,
        jitter: false,
    };

    /// Whether another attempt is allowed after `attempts_made` attempts.
    pub const fn allows_another(&self, attempts_made: u32) -> bool {
        attempts_made < self.max_attempts
    }

    /// The backoff before retry number `retry` (0 for the first retry),
    /// before jitter.
    pub fn backoff(&self, retry: u32) -> Duration {
        // `max` maps NaN to 1.0 as well.
        let multiplier = self.multiplier.max(1.0);
        let exponent = i32::try_from(retry).unwrap_or(i32::MAX);
        let secs = self.initial_delay.as_secs_f64() * multiplier.powi(exponent);
        if secs.is_nan() {
            // Only `0 * inf`: a zero initial delay stays zero.
            return Duration::ZERO;
        }
        if secs >= self.max_delay.as_secs_f64() {
            // Also catches the infinity a large exponent produces.
            return self.max_delay;
        }
        Duration::from_secs_f64(secs).min(self.max_delay)
    }

    /// The delay before retry number `retry`, given the failure that caused
    /// it and a uniform random `sample` in `[0, 1)` used for jitter.
    ///
    /// Taking the sample as an argument keeps this function deterministic.
    pub fn delay(&self, retry: u32, failure: FailureClass, sample: f64) -> Duration {
        let backoff = self.backoff(retry);
        let jittered = if self.jitter {
            let sample = if sample.is_finite() {
                sample.clamp(0.0, 1.0)
            } else {
                0.0
            };
            // Duration::MAX rounds up to 2^64 seconds as f64. Even a
            // multiplier of 1 can then overflow mul_f64; retain the cap
            // rather than panicking on a valid (very large) policy.
            Duration::try_from_secs_f64(backoff.as_secs_f64() * (0.5 + 0.5 * sample))
                .unwrap_or(backoff)
                .min(backoff)
        } else {
            backoff
        };
        match failure.retry_after() {
            Some(asked) => jittered.max(asked),
            None => jittered,
        }
    }
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            initial_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(30),
            multiplier: 2.0,
            jitter: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    #[test]
    fn default_backoff_doubles_up_to_the_cap() {
        let policy = RetryPolicy::default();
        let secs: Vec<u64> = (0..7).map(|r| policy.backoff(r).as_secs()).collect();
        assert_eq!(secs, [1, 2, 4, 8, 16, 30, 30]);
    }

    #[test]
    fn rate_limits_may_exceed_the_cap() {
        let policy = RetryPolicy::default();
        let failure = FailureClass::RateLimited {
            retry_after: Some(Duration::from_secs(120)),
        };
        assert_eq!(policy.delay(0, failure, 0.0), Duration::from_secs(120));
    }

    #[test]
    fn attempt_counting_includes_the_first_attempt() {
        assert!(!RetryPolicy::NONE.allows_another(1));
        let policy = RetryPolicy::default();
        assert!(policy.allows_another(4));
        assert!(!policy.allows_another(5));
    }

    #[test]
    fn jitter_at_the_duration_limit_does_not_overflow() {
        let policy = RetryPolicy {
            initial_delay: Duration::MAX,
            max_delay: Duration::MAX,
            ..RetryPolicy::default()
        };
        assert_eq!(policy.delay(0, FailureClass::Transient, 1.0), Duration::MAX);
        for sample in [0.0, 0.5, 0.99, 1.0, f64::NAN, f64::INFINITY] {
            let delay = policy.delay(u32::MAX, FailureClass::Transient, sample);
            assert!(delay <= policy.max_delay);
            assert!(delay >= policy.max_delay / 2);
        }
    }

    proptest! {
        #[test]
        fn delays_are_bounded_and_monotone(
            initial_ms in 0u64..10_000,
            max_ms in 0u64..600_000,
            multiplier in prop_oneof![Just(f64::NAN), Just(f64::INFINITY), -10.0f64..10.0],
            retry in 0u32..u32::MAX,
            sample in prop_oneof![Just(f64::NAN), -1.0f64..2.0],
        ) {
            let policy = RetryPolicy {
                max_attempts: 5,
                initial_delay: Duration::from_millis(initial_ms),
                max_delay: Duration::from_millis(max_ms),
                multiplier,
                jitter: true,
            };
            let backoff = policy.backoff(retry);
            prop_assert!(backoff <= policy.max_delay);
            prop_assert!(policy.backoff(retry.saturating_add(1)) >= backoff);
            let delay = policy.delay(retry, FailureClass::Transient, sample);
            prop_assert!(delay <= backoff);
            // Float rounding may land a nanosecond under the exact half.
            prop_assert!(delay + Duration::from_nanos(1) >= backoff / 2);
        }
    }
}
