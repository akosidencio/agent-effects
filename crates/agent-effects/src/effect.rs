//! Describing an effect: the builder, what its action sees, and how it ends.

use std::error::Error;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::error::RuntimeError;
use crate::failure::FailureClass;
use crate::id::{EffectId, EffectKey, IdempotencyKey};
use crate::kind::EffectKind;
use crate::policy::{Capabilities, RiskLevel};
use crate::retry::RetryPolicy;
use crate::runtime::Runtime;
use crate::store::{EffectStore, ErrorRecord};
use crate::verification::{NoVerification, Verification, VerificationMode, Verifier, VerifyWith};

/// What an effect's action knows about the attempt it is running.
#[derive(Clone, Debug)]
pub struct EffectContext {
    pub(crate) id: EffectId,
    pub(crate) key: EffectKey,
    pub(crate) attempt: u32,
}

impl EffectContext {
    /// The effect's record id.
    pub fn effect_id(&self) -> EffectId {
        self.id
    }

    /// The effect's logical identity.
    pub fn key(&self) -> &EffectKey {
        &self.key
    }

    /// The key to forward to the remote system, e.g. as an HTTP
    /// `Idempotency-Key` header. The same for every attempt of the effect.
    pub fn idempotency_key(&self) -> IdempotencyKey {
        self.key.idempotency_key()
    }

    /// The attempt number, starting at 1.
    pub fn attempt(&self) -> u32 {
        self.attempt
    }
}

/// A failed action, classified so the runtime knows whether it applied.
///
/// ```
/// # use agent_effects::EffectFailure;
/// // The connection was refused: nothing reached the remote system.
/// let failure = EffectFailure::ambiguous("connection refused").request_sent(false);
/// ```
#[derive(Debug)]
pub struct EffectFailure {
    class: FailureClass,
    request_sent: Option<bool>,
    source: Box<dyn Error + Send + Sync>,
}

impl EffectFailure {
    /// A failure of class `class`.
    pub fn new(class: FailureClass, source: impl Into<Box<dyn Error + Send + Sync>>) -> Self {
        Self {
            class,
            request_sent: None,
            source: source.into(),
        }
    }

    /// The request did not apply and may succeed if repeated.
    pub fn transient(source: impl Into<Box<dyn Error + Send + Sync>>) -> Self {
        Self::new(FailureClass::Transient, source)
    }

    /// The request did not apply and will not succeed if repeated.
    pub fn permanent(source: impl Into<Box<dyn Error + Send + Sync>>) -> Self {
        Self::new(FailureClass::Permanent, source)
    }

    /// The request may or may not have applied, e.g. a timeout after sending.
    pub fn ambiguous(source: impl Into<Box<dyn Error + Send + Sync>>) -> Self {
        Self::new(FailureClass::Ambiguous, source)
    }

    /// The remote system asked the caller to slow down.
    pub fn rate_limited(
        retry_after: Option<Duration>,
        source: impl Into<Box<dyn Error + Send + Sync>>,
    ) -> Self {
        Self::new(FailureClass::RateLimited { retry_after }, source)
    }

    /// The credentials were missing or invalid.
    pub fn authentication(source: impl Into<Box<dyn Error + Send + Sync>>) -> Self {
        Self::new(FailureClass::Authentication, source)
    }

    /// The credentials were not allowed to perform the action.
    pub fn authorization(source: impl Into<Box<dyn Error + Send + Sync>>) -> Self {
        Self::new(FailureClass::Authorization, source)
    }

    /// The remote system rejected the request as malformed.
    pub fn validation(source: impl Into<Box<dyn Error + Send + Sync>>) -> Self {
        Self::new(FailureClass::Validation, source)
    }

    /// Records whether the request reached the remote system. `false` turns
    /// an ambiguous failure into a transient one: nothing can have applied.
    #[must_use]
    pub fn request_sent(mut self, sent: bool) -> Self {
        self.request_sent = Some(sent);
        self
    }

    /// The classification after taking [`Self::request_sent`] into account.
    pub fn class(&self) -> FailureClass {
        self.class.with_request_sent(self.request_sent)
    }

    pub(crate) fn to_record(&self) -> ErrorRecord {
        ErrorRecord {
            class: Some(self.class()),
            message: self.source.to_string(),
        }
    }
}

impl fmt::Display for EffectFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?} failure: {}", self.class(), self.source)
    }
}

impl Error for EffectFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.source.as_ref())
    }
}

/// How an effect ended, or where it stands.
///
/// `Unknown` and `NeedsIntervention` are results, not errors: the effect may
/// have changed the outside world, and the caller must not treat it as
/// failed.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum EffectOutcome<T> {
    /// The effect applied. Carries the action's output, from this call or
    /// from the record of an earlier one.
    Committed(T),
    /// The effect definitely did not apply and will not be retried.
    Failed(ErrorRecord),
    /// The effect was refused before it ran.
    Rejected(ErrorRecord),
    /// The effect may or may not have applied. Calling again with the same
    /// key resolves it if that is safe.
    Unknown {
        /// The effect.
        id: EffectId,
    },
    /// The runtime cannot resolve the outcome safely; an operator must.
    NeedsIntervention {
        /// The effect.
        id: EffectId,
    },
    /// Another caller or worker is running the effect right now.
    InProgress {
        /// The effect.
        id: EffectId,
    },
    /// The effect waits for a human decision. Call again later, or have an
    /// operator use [`Runtime::approve`](crate::Runtime::approve) /
    /// [`Runtime::deny`](crate::Runtime::deny).
    AwaitingApproval {
        /// The effect.
        id: EffectId,
    },
    /// The effect applied and was later undone by compensation.
    Compensated {
        /// The effect.
        id: EffectId,
    },
}

/// Whether an effect may still run, checked before its first attempt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Precondition {
    /// Go ahead.
    Satisfied,
    /// Never run this effect, e.g. "order is no longer pending". The effect
    /// ends [`EffectOutcome::Rejected`].
    Rejected {
        /// Why, recorded on the effect.
        reason: String,
    },
    /// Not yet; check again after `after`, e.g. while a dependency settles.
    /// After as many checks as the retry policy allows attempts, the effect
    /// is rejected.
    RetryLater {
        /// How long to wait before the next check.
        after: Duration,
        /// Why, recorded if the effect is eventually rejected.
        reason: String,
    },
}

impl Precondition {
    /// A rejection with `reason`.
    pub fn reject(reason: impl Into<String>) -> Self {
        Self::Rejected {
            reason: reason.into(),
        }
    }

    /// A deferral of `after`, with `reason`.
    pub fn retry_later(after: Duration, reason: impl Into<String>) -> Self {
        Self::RetryLater {
            after,
            reason: reason.into(),
        }
    }
}

pub(crate) type PreconditionFn =
    Arc<dyn Fn(EffectContext) -> Pin<Box<dyn Future<Output = Precondition> + Send>> + Send + Sync>;

/// Builds and runs one effect. Created by [`Runtime::effect`].
///
/// Invalid names, keys or inputs are reported when the effect is run, so the
/// whole chain needs a single `?`.
#[must_use = "an effect does nothing until `run` is awaited"]
pub struct EffectBuilder<S, V = NoVerification> {
    runtime: Runtime<S>,
    name: String,
    key: String,
    kind: EffectKind,
    remote_idempotency: bool,
    input: Option<Result<Value, serde_json::Error>>,
    actor: Option<String>,
    retry: RetryPolicy,
    attempt_timeout: Option<Duration>,
    precondition: Option<PreconditionFn>,
    require_approval: bool,
    risk: RiskLevel,
    verification: VerificationMode,
    verifier: V,
}

impl<S: EffectStore> EffectBuilder<S> {
    pub(crate) fn new(runtime: Runtime<S>, name: String, key: String) -> Self {
        let retry = runtime.default_retry();
        Self {
            runtime,
            name,
            key,
            kind: EffectKind::IrreversibleWrite,
            remote_idempotency: false,
            input: None,
            actor: None,
            retry,
            attempt_timeout: None,
            precondition: None,
            require_approval: false,
            risk: RiskLevel::Low,
            verification: VerificationMode::None,
            verifier: NoVerification,
        }
    }

    /// Verifies the effect against a remote system that reads its own
    /// writes: [`Verification::NotApplied`] is trusted at once.
    ///
    /// The check runs after every successful attempt (a postcondition) and
    /// to reconcile an attempt whose outcome is unknown. It makes re-running
    /// safe even for irreversible effects, because the runtime only re-runs
    /// an effect the remote system says did not apply.
    pub fn verify<T, F, Fut>(self, check: F) -> EffectBuilder<S, VerifyWith<F>>
    where
        F: Fn(EffectContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Verification<T>, EffectFailure>> + Send + 'static,
    {
        self.with_verifier(VerificationMode::Authoritative, VerifyWith(check))
    }

    /// Like [`Self::verify`], for a remote lookup that lags behind its writes
    /// by up to `settle`. [`Verification::NotApplied`] is trusted only once
    /// `settle` has passed since the attempt ended; until then the runtime
    /// waits and checks again.
    pub fn verify_eventually<T, F, Fut>(
        self,
        settle: Duration,
        check: F,
    ) -> EffectBuilder<S, VerifyWith<F>>
    where
        F: Fn(EffectContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Verification<T>, EffectFailure>> + Send + 'static,
    {
        self.with_verifier(
            VerificationMode::EventuallyConsistent { settle },
            VerifyWith(check),
        )
    }

    fn with_verifier<V>(self, mode: VerificationMode, verifier: V) -> EffectBuilder<S, V> {
        EffectBuilder {
            runtime: self.runtime,
            name: self.name,
            key: self.key,
            kind: self.kind,
            remote_idempotency: self.remote_idempotency,
            input: self.input,
            actor: self.actor,
            retry: self.retry,
            attempt_timeout: self.attempt_timeout,
            precondition: self.precondition,
            require_approval: self.require_approval,
            risk: self.risk,
            verification: mode,
            verifier,
        }
    }
}

impl<S: EffectStore, V> EffectBuilder<S, V> {
    /// The effect's kind. Defaults to [`EffectKind::IrreversibleWrite`], the
    /// most cautious choice.
    pub fn kind(mut self, kind: EffectKind) -> Self {
        self.kind = kind;
        self
    }

    /// Declares that the remote system deduplicates on
    /// [`EffectContext::idempotency_key`], which the action must send. This
    /// makes re-running after an unknown outcome safe.
    pub fn remote_idempotency(mut self, supported: bool) -> Self {
        self.remote_idempotency = supported;
        self
    }

    /// The input the action acts on. It is stored for audit and fingerprinted:
    /// reusing the key with a different input fails with
    /// [`RuntimeError::InputMismatch`].
    ///
    /// The input is persisted as-is. Keep credentials in the action's
    /// captured state, not in the input.
    pub fn input<I: Serialize + ?Sized>(mut self, input: &I) -> Self {
        self.input = Some(serde_json::to_value(input));
        self
    }

    /// Who is asking for the effect, e.g. `agent:refund-agent`. Recorded on
    /// the effect and its audit events.
    pub fn actor(mut self, actor: impl Into<String>) -> Self {
        self.actor = Some(actor.into());
        self
    }

    /// The retry policy. Defaults to the runtime's
    /// ([`RuntimeBuilder::retry_policy`](crate::RuntimeBuilder::retry_policy)).
    ///
    /// `max_attempts` bounds the attempts over the effect's whole life,
    /// across calls and restarts. It also bounds verification and
    /// precondition checks per call.
    pub fn retry(mut self, policy: RetryPolicy) -> Self {
        self.retry = policy;
        self
    }

    /// Gives up waiting for an attempt after `timeout`. The request may have
    /// been sent, so a timeout is an ambiguous failure.
    pub fn attempt_timeout(mut self, timeout: Duration) -> Self {
        self.attempt_timeout = Some(timeout);
        self
    }

    /// A check that must pass before the effect's first attempt, so a stale
    /// decision is not carried out, e.g. "refund only if the order is still
    /// unrefunded".
    ///
    /// It runs only before the first attempt. Once an attempt may have
    /// applied, the effect's own success could falsify the check: a refund
    /// that went through makes "not yet refunded" false.
    pub fn precondition<F, Fut>(mut self, check: F) -> Self
    where
        F: Fn(EffectContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Precondition> + Send + 'static,
    {
        self.precondition = Some(Arc::new(move |ctx| Box::pin(check(ctx))));
        self
    }

    /// How much damage the effect could do; the runtime's
    /// [`RiskPolicy`](crate::RiskPolicy) adds requirements by risk. Defaults
    /// to [`RiskLevel::Low`].
    pub fn risk(mut self, risk: RiskLevel) -> Self {
        self.risk = risk;
        self
    }

    /// Requires a human decision before the first attempt; see
    /// [`approval`](crate::approval). The effect waits in
    /// `AwaitingApproval`, durably, until the runtime's approval provider or
    /// an operator decides.
    pub fn require_approval(mut self) -> Self {
        self.require_approval = true;
        self
    }

    /// Runs the effect, or attaches to an earlier run with the same key.
    ///
    /// The action may be called more than once over the effect's life (for
    /// example to re-run an idempotent effect whose outcome was unknown), so
    /// it is an `Fn`. It runs on a spawned task: dropping the returned future
    /// does not abort an attempt that has started, and its result is still
    /// recorded.
    ///
    /// # Errors
    ///
    /// Infrastructure failures only; see [`RuntimeError`]. Every effect
    /// result, including an unknown one, is an [`EffectOutcome`].
    pub async fn run<T, F, Fut>(self, action: F) -> Result<EffectOutcome<T>, RuntimeError>
    where
        T: Serialize + DeserializeOwned + Send + 'static,
        F: Fn(EffectContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<T, EffectFailure>> + Send + 'static,
        V: Verifier<T>,
    {
        let key = EffectKey::new(
            crate::id::EffectName::new(self.name)?,
            crate::id::LogicalKey::new(self.key)?,
        );
        let input = self.input.transpose().map_err(RuntimeError::Input)?;
        let spec = EffectSpec {
            // Computed by the runtime, after redaction.
            fingerprint: None,
            input,
            key,
            capabilities: Capabilities {
                kind: self.kind,
                remote_idempotency: self.remote_idempotency,
                verification: self.verification,
            },
            actor: self.actor,
            retry: self.retry,
            attempt_timeout: self.attempt_timeout,
            precondition: self.precondition,
            require_approval: self.require_approval,
            risk: self.risk,
            automatic_retry: true,
            input_stored: false,
        };
        self.runtime.execute(spec, action, self.verifier).await
    }
}

/// Everything about an effect except its action and verifier.
pub(crate) struct EffectSpec {
    pub(crate) key: EffectKey,
    pub(crate) capabilities: Capabilities,
    pub(crate) input: Option<Value>,
    pub(crate) fingerprint: Option<String>,
    pub(crate) actor: Option<String>,
    pub(crate) retry: RetryPolicy,
    pub(crate) attempt_timeout: Option<Duration>,
    pub(crate) precondition: Option<PreconditionFn>,
    pub(crate) require_approval: bool,
    pub(crate) risk: RiskLevel,
    /// Whether the runtime may retry or re-run on its own; a risk policy
    /// can turn it off.
    pub(crate) automatic_retry: bool,
    /// The input comes from the store (a resumed effect): it is already
    /// redacted, and `fingerprint` is the stored one, used as is.
    pub(crate) input_stored: bool,
}
