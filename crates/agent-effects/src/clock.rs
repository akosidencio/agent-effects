//! Time source for leases, retry scheduling and settle delays.
//!
//! Injected rather than read from [`SystemTime::now`] directly, so tests can
//! step time deterministically and stores can substitute their own clock.

use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

/// A source of wall-clock time.
pub trait Clock: Send + Sync + 'static {
    /// The current time.
    fn now(&self) -> SystemTime;
}

/// Lets a test keep a handle to a clock it gave to a runtime.
impl<C: Clock> Clock for Arc<C> {
    fn now(&self) -> SystemTime {
        C::now(self)
    }
}

/// The operating system clock.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> SystemTime {
        SystemTime::now()
    }
}

/// A clock that only moves when told to. For tests.
#[derive(Debug)]
pub struct ManualClock {
    now: Mutex<SystemTime>,
}

impl ManualClock {
    /// A clock frozen at `start`.
    pub const fn new(start: SystemTime) -> Self {
        Self {
            now: Mutex::new(start),
        }
    }

    /// Moves the clock forward by `by`.
    pub fn advance(&self, by: Duration) {
        let mut now = self
            .now
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *now += by;
    }

    /// Sets the clock to `to`, which may be earlier, to simulate skew.
    pub fn set(&self, to: SystemTime) {
        *self
            .now
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = to;
    }
}

impl Default for ManualClock {
    fn default() -> Self {
        Self::new(SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000))
    }
}

impl Clock for ManualClock {
    fn now(&self) -> SystemTime {
        *self
            .now
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manual_clock_only_moves_when_told() {
        let clock = ManualClock::default();
        let start = clock.now();
        assert_eq!(clock.now(), start);
        clock.advance(Duration::from_secs(30));
        assert_eq!(clock.now(), start + Duration::from_secs(30));
    }
}
