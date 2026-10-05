//! Reliable side-effect execution for AI agents and autonomous applications.
//!
//! `agent-effects` puts a durable execution boundary around operations that
//! change the outside world: charging a card, sending an email, provisioning a
//! server. It records intent before acting, distinguishes "failed" from "don't
//! know", and resolves unknown outcomes by verification or idempotent retry
//! instead of guessing.
//!
//! It does not provide exactly-once side effects. Nothing can, without
//! cooperation from the target system. It provides controlled execution and
//! deduplication, and refuses to retry when a retry could duplicate an effect.
//!
//! ```
//! use agent_effects::{EffectFailure, EffectKind, EffectOutcome, Runtime};
//! use agent_effects_memory::MemoryStore;
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<(), agent_effects::RuntimeError> {
//! let runtime = Runtime::new(MemoryStore::new());
//!
//! let outcome = runtime
//!     .effect("ticket.create", "incident-42")
//!     .kind(EffectKind::IrreversibleWrite)
//!     .input(&"disk full on db-1")
//!     .run(|ctx| async move {
//!         // Call the remote system here, sending ctx.idempotency_key().
//!         Ok::<_, EffectFailure>(format!("TICKET-1 (attempt {})", ctx.attempt()))
//!     })
//!     .await?;
//!
//! match outcome {
//!     EffectOutcome::Committed(ticket) => println!("created {ticket}"),
//!     EffectOutcome::Unknown { id } | EffectOutcome::NeedsIntervention { id } => {
//!         println!("effect {id} may have happened; not retrying blindly")
//!     }
//!     other => println!("{other:?}"),
//! }
//! # Ok(())
//! # }
//! ```
//!
//! **Status:** pre-release. The runtime runs effects with retries,
//! timeouts, preconditions and verification, records their outcome, and
//! re-attaches later calls to it; [`recovery`] adds the operator tools. The
//! SQLite store comes next; see `docs/roadmap.md`. The persisted vocabulary
//! (identity, kinds, the state machine) and the [`EffectStore`] contract come
//! from [`agent-effects-store`](store) and are re-exported here.

pub mod clock;
pub mod effect;
pub mod error;
pub mod policy;
pub mod recovery;
pub mod retry;
pub mod runtime;
#[cfg(feature = "testkit")]
pub mod testkit;
pub mod verification;

mod fingerprint;

pub use agent_effects_store as store;
pub use agent_effects_store::{failure, id, kind, state};

pub use agent_effects_store::{
    Disposition, EffectId, EffectKey, EffectKind, EffectName, EffectRecord, EffectStatus,
    EffectStore, ErrorRecord, FailureClass, IdempotencyKey, IdentityError, InvalidTransition,
    Lease, LogicalKey, StoreError, Transition, WorkerId,
};
pub use clock::{Clock, ManualClock, SystemClock, TokioClock};
pub use effect::{EffectBuilder, EffectContext, EffectFailure, EffectOutcome, Precondition};
pub use error::RuntimeError;
pub use policy::{Capabilities, UnknownPlan};
pub use recovery::{RecoveryReport, Resolution};
pub use retry::RetryPolicy;
pub use runtime::{Runtime, RuntimeBuilder};
pub use verification::{NotFoundReading, Verification, VerificationMode};
