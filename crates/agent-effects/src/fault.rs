//! Crash simulation for testing recovery. The injector is enabled by the
//! `fault-injection` feature.
//!
//! A `FaultInjector` armed at a [`FaultPoint`] stops the runtime there:
//!
//! - `Fault::Crash` panics inside the runtime's task. Nothing after the
//!   point runs: no further writes, no lease release, no heartbeat. The
//!   caller gets a `RuntimeError::Internal`, and a new runtime over the same
//!   store plays the restarted process.
//! - `Fault::Abort` calls [`std::process::abort`], killing the process at
//!   that exact point, for tests that run the runtime in a subprocess.
//!
//! An action already spawned when the crash hits keeps running, like a
//! request already on the wire.
//!
//! ```ignore
//! let injector = Arc::new(FaultInjector::new().at(FaultPoint::AfterActionReturned).crash());
//! let runtime = Runtime::builder(store).fault_injector(Arc::clone(&injector)).build();
//! ```

/// Where in an effect's execution a crash can be injected, in the order
/// they are reached.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum FaultPoint {
    /// The call started; nothing is recorded yet.
    BeforeInsert,
    /// The record exists (`Pending`); no lease is held yet.
    AfterInsert,
    /// `StartAttempt` is persisted (`Executing`); the action was not called.
    AfterAttemptPersisted,
    /// The action is running: its request may or may not reach the remote
    /// system.
    AfterActionStarted,
    /// The action returned; its result is not persisted.
    AfterActionReturned,
    /// `StartVerification` is persisted (`Verifying`); no check has run.
    AfterVerificationStarted,
    /// A compensation attempt is recorded (`Compensating`); the compensation
    /// was not called.
    AfterCompensationStarted,
}

#[cfg(feature = "fault-injection")]
pub use injector::{Arming, Fault, FaultInjector};

#[cfg(feature = "fault-injection")]
mod injector {
    use std::collections::HashMap;
    use std::sync::{Mutex, PoisonError};

    use super::FaultPoint;

    /// What happens at an armed point.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum Fault {
        /// Panic in the runtime's task: an in-process crash.
        Crash,
        /// Abort the process.
        Abort,
    }

    /// Stops the runtime at chosen points. Each armed fault fires once.
    #[derive(Debug, Default)]
    pub struct FaultInjector {
        armed: Mutex<HashMap<FaultPoint, Fault>>,
        reached: Mutex<Vec<FaultPoint>>,
    }

    /// A point being armed; finish with [`Arming::crash`] or
    /// [`Arming::abort`].
    #[must_use]
    pub struct Arming {
        injector: FaultInjector,
        point: FaultPoint,
    }

    impl Arming {
        /// Crash in-process at the point.
        pub fn crash(self) -> FaultInjector {
            self.arm(Fault::Crash)
        }

        /// Abort the process at the point.
        pub fn abort(self) -> FaultInjector {
            self.arm(Fault::Abort)
        }

        fn arm(self, fault: Fault) -> FaultInjector {
            self.injector
                .armed
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(self.point, fault);
            self.injector
        }
    }

    impl FaultInjector {
        /// An injector with nothing armed.
        pub fn new() -> Self {
            Self::default()
        }

        /// Arms `point`.
        pub fn at(self, point: FaultPoint) -> Arming {
            Arming {
                injector: self,
                point,
            }
        }

        /// The points reached so far, in order, including any that crashed.
        pub fn reached(&self) -> Vec<FaultPoint> {
            self.reached
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .clone()
        }

        pub(crate) fn reach(&self, point: FaultPoint) {
            self.reached
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(point);
            let fault = self
                .armed
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .remove(&point);
            match fault {
                Some(Fault::Crash) => panic!("fault injected at {point:?}"),
                Some(Fault::Abort) => std::process::abort(),
                None => {}
            }
        }
    }
}
