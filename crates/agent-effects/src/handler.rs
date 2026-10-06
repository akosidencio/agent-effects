//! Durable effect handlers: effects the runtime can finish without a caller.
//!
//! A closure effect can only be finished by a caller holding the closure. A
//! handler is registered with the runtime under its effect name, and its
//! input is stored with the record. So after a crash,
//! [`Runtime::recover`](crate::Runtime::recover) can rebuild the call from
//! the store and finish the effect with nobody calling: verify it, re-run it
//! if that is safe, or escalate it.
//!
//! ```
//! use agent_effects::handler::{EffectHandler, Handler, VerifiableEffect};
//! use agent_effects::{EffectContext, EffectFailure, EffectKind, EffectOutcome, Runtime, Verification};
//! use agent_effects_memory::MemoryStore;
//!
//! struct SendInvoice;
//!
//! impl EffectHandler for SendInvoice {
//!     const NAME: &'static str = "invoice.send";
//!     type Input = String; // the customer's email
//!     type Output = String; // the provider's message id
//!     type Error = EffectFailure;
//!
//!     fn kind(&self) -> EffectKind {
//!         EffectKind::IrreversibleWrite
//!     }
//!
//!     async fn execute(&self, ctx: &EffectContext, to: &String) -> Result<String, EffectFailure> {
//!         // Send through the provider, forwarding ctx.idempotency_key().
//!         Ok(format!("msg-for-{to}"))
//!     }
//! }
//!
//! impl VerifiableEffect for SendInvoice {
//!     async fn verify(&self, _: &EffectContext, to: &String) -> Result<Verification<String>, EffectFailure> {
//!         // Look the message up in the provider's outbox.
//!         Ok(Verification::Confirmed(format!("msg-for-{to}")))
//!     }
//! }
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<(), agent_effects::RuntimeError> {
//! let runtime = Runtime::builder(MemoryStore::new())
//!     .register(Handler::new(SendInvoice).verifiable())
//!     .build();
//!
//! let outcome = runtime
//!     .submit::<SendInvoice>("invoice-1001", "ada@example.com".to_string())
//!     .actor("agent:billing")
//!     .await?;
//! assert_eq!(outcome, EffectOutcome::Committed("msg-for-ada@example.com".to_string()));
//! # Ok(())
//! # }
//! ```
//!
//! Optional capabilities are separate traits ([`VerifiableEffect`]; the
//! compensation trait follows), so a handler only implements what it can
//! actually do. The [`Handler`] builder only offers `.verifiable()` for
//! handlers that implement it.

use std::any::Any;
use std::collections::HashMap;
use std::fmt::Display;
use std::future::{Future, IntoFuture};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::compensation::{
    CompensationContext, CompensationOutcome, CompensationSpec, Compensator,
};
use crate::effect::{EffectContext, EffectFailure, EffectOutcome, EffectSpec, Precondition};
use crate::error::RuntimeError;
use crate::fingerprint::fingerprint;
use crate::id::{EffectKey, EffectName, LogicalKey};
use crate::kind::EffectKind;
use crate::policy::Capabilities;
use crate::retry::RetryPolicy;
use crate::runtime::Runtime;
use crate::state::EffectStatus;
use crate::store::{EffectRecord, EffectStore, StoreError};
use crate::verification::{NoVerification, Verification, VerificationMode, VerifyWith};

/// An effect the runtime can execute, and re-execute after a crash, from
/// its stored input.
///
/// Implement `execute`; override the other methods to describe the effect.
/// The defaults are the most cautious choices.
pub trait EffectHandler: Send + Sync + 'static {
    /// The effect's name, e.g. `payment.charge`. One handler per name.
    const NAME: &'static str;

    /// What the effect acts on. Stored with the record so recovery can
    /// rebuild the call: keep credentials in the handler, not here.
    type Input: Serialize + DeserializeOwned + Send + Sync + 'static;

    /// What the effect produces. Stored and replayed to later callers.
    type Output: Serialize + DeserializeOwned + Send + 'static;

    /// The handler's error, classified for the runtime.
    type Error: Into<EffectFailure> + Send + 'static;

    /// What re-executing does to the outside world. Defaults to
    /// [`EffectKind::IrreversibleWrite`].
    fn kind(&self) -> EffectKind {
        EffectKind::IrreversibleWrite
    }

    /// Whether the remote system deduplicates on
    /// [`EffectContext::idempotency_key`], which `execute` must send.
    fn remote_idempotency(&self) -> bool {
        false
    }

    /// The retry policy; `None` uses the runtime's.
    fn retry_policy(&self) -> Option<RetryPolicy> {
        None
    }

    /// How long to wait for one attempt; `None` waits indefinitely.
    fn attempt_timeout(&self) -> Option<Duration> {
        None
    }

    /// Checked before the first attempt; see
    /// [`EffectBuilder::precondition`](crate::EffectBuilder::precondition).
    fn precondition(
        &self,
        ctx: &EffectContext,
        input: &Self::Input,
    ) -> impl Future<Output = Precondition> + Send {
        let _ = (ctx, input);
        async { Precondition::Satisfied }
    }

    /// Performs the effect once.
    fn execute(
        &self,
        ctx: &EffectContext,
        input: &Self::Input,
    ) -> impl Future<Output = Result<Self::Output, Self::Error>> + Send;
}

/// A handler whose effect can be looked up in the remote system.
///
/// Register it with [`Handler::verifiable`] to verify after every success and
/// to resolve unknown outcomes; see
/// [`EffectBuilder::verify`](crate::EffectBuilder::verify).
pub trait VerifiableEffect: EffectHandler {
    /// How far a lookup can be trusted. Defaults to
    /// [`VerificationMode::Authoritative`]; use
    /// [`VerificationMode::EventuallyConsistent`] for lookups that lag.
    fn verification_mode(&self) -> VerificationMode {
        VerificationMode::Authoritative
    }

    /// Asks the remote system whether the effect applied.
    fn verify(
        &self,
        ctx: &EffectContext,
        input: &Self::Input,
    ) -> impl Future<Output = Result<Verification<Self::Output>, Self::Error>> + Send;
}

/// A handler whose effect can be undone.
///
/// Register it with [`Handler::compensable`]; undo an effect with
/// [`Runtime::compensate`](crate::Runtime::compensate). The compensation must
/// be idempotent: a crashed or ambiguous attempt is run again. Send
/// [`CompensationContext::idempotency_key`] to the remote system.
pub trait CompensableEffect: EffectHandler {
    /// Undoes the effect. `output` is what `execute` returned, if it was
    /// stored.
    fn compensate(
        &self,
        ctx: &CompensationContext,
        input: &Self::Input,
        output: Option<&Self::Output>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;

type VerifyFn<H> = Arc<
    dyn Fn(
            Arc<H>,
            EffectContext,
            Arc<<H as EffectHandler>::Input>,
        ) -> BoxFuture<Result<Verification<<H as EffectHandler>::Output>, EffectFailure>>
        + Send
        + Sync,
>;

/// A handler and the capabilities it is registered with. Pass it to
/// [`RuntimeBuilder::register`](crate::RuntimeBuilder::register).
pub struct Handler<H: EffectHandler> {
    effect: Arc<H>,
    verify: Option<(VerificationMode, VerifyFn<H>)>,
    compensate: Option<Compensator>,
}

impl<H: EffectHandler> Clone for Handler<H> {
    fn clone(&self) -> Self {
        Self {
            effect: Arc::clone(&self.effect),
            verify: self.verify.clone(),
            compensate: self.compensate.clone(),
        }
    }
}

impl<H: EffectHandler> Handler<H> {
    /// Registers `handler` with no optional capabilities.
    pub fn new(handler: H) -> Self {
        Self {
            effect: Arc::new(handler),
            verify: None,
            compensate: None,
        }
    }
}

impl<H: CompensableEffect> Handler<H> {
    /// Lets the effect be undone with
    /// [`Runtime::compensate`](crate::Runtime::compensate), using
    /// [`CompensableEffect::compensate`], and lets recovery finish an
    /// interrupted compensation.
    #[must_use]
    pub fn compensable(mut self) -> Self {
        let handler = Arc::clone(&self.effect);
        self.compensate = Some(Arc::new(move |ctx, input, output| {
            let handler = Arc::clone(&handler);
            Box::pin(async move {
                let input: H::Input = serde_json::from_value(input.unwrap_or(Value::Null))
                    .map_err(|e| {
                        EffectFailure::permanent(format!("stored input does not match: {e}"))
                    })?;
                let output: Option<H::Output> = output
                    .map(serde_json::from_value)
                    .transpose()
                    .map_err(|e| {
                        EffectFailure::permanent(format!("stored output does not match: {e}"))
                    })?;
                handler
                    .compensate(&ctx, &input, output.as_ref())
                    .await
                    .map_err(Into::into)
            })
        }));
        self
    }
}

impl<H: VerifiableEffect> Handler<H> {
    /// Verifies the effect after every success and to resolve unknown
    /// outcomes, using [`VerifiableEffect::verify`].
    #[must_use]
    pub fn verifiable(mut self) -> Self {
        let mode = self.effect.verification_mode();
        let verify: VerifyFn<H> = Arc::new(|handler, ctx, input| {
            Box::pin(async move { handler.verify(&ctx, &input).await.map_err(Into::into) })
        });
        self.verify = Some((mode, verify));
        self
    }
}

/// A registered handler, erased so the runtime can hold handlers of many
/// types and resume their effects by name.
pub(crate) struct Registered<S> {
    /// The `Handler<H>`, for typed submission.
    typed: Arc<dyn Any + Send + Sync>,
    /// Finishes an effect of this handler from its stored record.
    pub(crate) resume: Resume<S>,
}

pub(crate) type Resume<S> = Arc<
    dyn Fn(Runtime<S>, EffectRecord) -> BoxFuture<Result<EffectStatus, RuntimeError>> + Send + Sync,
>;

pub(crate) type Registry<S> = HashMap<&'static str, Registered<S>>;

impl<S: EffectStore> Registered<S> {
    pub(crate) fn new<H: EffectHandler>(handler: Handler<H>) -> Self {
        let typed: Arc<dyn Any + Send + Sync> = Arc::new(handler.clone());
        let resume: Resume<S> = Arc::new(move |runtime: Runtime<S>, record: EffectRecord| {
            let handler = handler.clone();
            Box::pin(async move { resume(&runtime, &handler, record).await })
        });
        Self { typed, resume }
    }

    pub(crate) fn typed<H: EffectHandler>(&self) -> Option<Handler<H>> {
        self.typed.downcast_ref::<Handler<H>>().cloned()
    }
}

/// A submission of input to a registered handler. Await it to run the
/// effect. Created by [`Runtime::submit`].
#[must_use = "a submission does nothing until it is awaited"]
pub struct Submission<'a, S, H: EffectHandler> {
    runtime: &'a Runtime<S>,
    key: String,
    input: H::Input,
    actor: Option<String>,
}

impl<'a, S, H: EffectHandler> Submission<'a, S, H> {
    pub(crate) fn new(runtime: &'a Runtime<S>, key: impl Display, input: H::Input) -> Self {
        Self {
            runtime,
            key: key.to_string(),
            input,
            actor: None,
        }
    }

    /// Who is asking for the effect, e.g. `agent:billing`.
    pub fn actor(mut self, actor: impl Into<String>) -> Self {
        self.actor = Some(actor.into());
        self
    }
}

impl<'a, S: EffectStore, H: EffectHandler> IntoFuture for Submission<'a, S, H> {
    type Output = Result<EffectOutcome<H::Output>, RuntimeError>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let handler = self
                .runtime
                .handler::<H>()
                .ok_or(RuntimeError::NotRegistered { name: H::NAME })?;
            let key = EffectKey::new(EffectName::new(H::NAME)?, LogicalKey::new(self.key)?);
            let json = serde_json::to_value(&self.input).map_err(RuntimeError::Input)?;
            let stored = Stored {
                fingerprint: Some(fingerprint(&json)),
                json,
                actor: self.actor,
            };
            run(self.runtime, &handler, key, Arc::new(self.input), stored).await
        })
    }
}

/// Finishes an effect of `handler` from its record, with no caller: its
/// compensation if one is under way, else the effect itself.
async fn resume<S: EffectStore, H: EffectHandler>(
    runtime: &Runtime<S>,
    handler: &Handler<H>,
    record: EffectRecord,
) -> Result<EffectStatus, RuntimeError> {
    let id = record.id;
    if record.status == EffectStatus::Compensating {
        let compensate = handler
            .compensate
            .clone()
            .ok_or(RuntimeError::NotCompensable { name: H::NAME })?;
        let spec = CompensationSpec {
            key: record.key.clone(),
            reason: None,
            actor: Some(format!("recovery:{}", runtime.worker_id())),
            retry: handler.effect.retry_policy(),
            attempt_timeout: handler.effect.attempt_timeout(),
        };
        runtime.compensate_effect(spec, compensate).await?;
        let settled = runtime
            .store()
            .get(id)
            .await?
            .ok_or(StoreError::NotFound(id))?;
        return Ok(settled.status);
    }
    let json = record.input.clone().unwrap_or(Value::Null);
    let input: H::Input = serde_json::from_value(json.clone())
        .map_err(|source| RuntimeError::StoredInput { id, source })?;
    // Rebuilt from the record itself, so it is the same effect by
    // definition: reuse its fingerprint rather than recompute it, which
    // would refuse every older record if canonicalization ever changed.
    let stored = Stored {
        json,
        fingerprint: record.input_fingerprint.clone(),
        actor: record.created_by.clone(),
    };
    run(
        runtime,
        handler,
        record.key.clone(),
        Arc::new(input),
        stored,
    )
    .await?;
    let settled = runtime
        .store()
        .get(id)
        .await?
        .ok_or(StoreError::NotFound(id))?;
    Ok(settled.status)
}

/// What is recorded about an effect besides its key.
struct Stored {
    json: Value,
    fingerprint: Option<String>,
    actor: Option<String>,
}

/// Runs `handler`'s effect through the same machinery as a closure effect.
async fn run<S: EffectStore, H: EffectHandler>(
    runtime: &Runtime<S>,
    handler: &Handler<H>,
    key: EffectKey,
    input: Arc<H::Input>,
    stored: Stored,
) -> Result<EffectOutcome<H::Output>, RuntimeError> {
    let effect = Arc::clone(&handler.effect);
    let precondition = {
        let (effect, input) = (Arc::clone(&effect), Arc::clone(&input));
        Arc::new(move |ctx: EffectContext| -> BoxFuture<Precondition> {
            let (effect, input) = (Arc::clone(&effect), Arc::clone(&input));
            Box::pin(async move { effect.precondition(&ctx, &input).await })
        })
    };
    let spec = EffectSpec {
        fingerprint: stored.fingerprint,
        input: Some(stored.json),
        key,
        capabilities: Capabilities {
            kind: effect.kind(),
            remote_idempotency: effect.remote_idempotency(),
            verification: handler
                .verify
                .as_ref()
                .map_or(VerificationMode::None, |(mode, _)| *mode),
        },
        actor: stored.actor,
        retry: effect
            .retry_policy()
            .unwrap_or_else(|| runtime.default_retry()),
        attempt_timeout: effect.attempt_timeout(),
        precondition: Some(precondition),
    };
    let action = {
        let (effect, input) = (Arc::clone(&effect), Arc::clone(&input));
        move |ctx: EffectContext| {
            let (effect, input) = (Arc::clone(&effect), Arc::clone(&input));
            async move { effect.execute(&ctx, &input).await.map_err(Into::into) }
        }
    };
    match &handler.verify {
        Some((_, verify)) => {
            let verify = Arc::clone(verify);
            let checker =
                VerifyWith(move |ctx| verify(Arc::clone(&effect), ctx, Arc::clone(&input)));
            runtime.execute(spec, action, checker).await
        }
        None => runtime.execute(spec, action, NoVerification).await,
    }
}

/// A request to undo a registered handler's effect. Await it. Created by
/// [`Runtime::compensate`](crate::Runtime::compensate).
#[must_use = "a compensation does nothing until it is awaited"]
pub struct CompensationSubmission<'a, S, H: EffectHandler> {
    runtime: &'a Runtime<S>,
    key: String,
    reason: Option<String>,
    actor: Option<String>,
    _handler: std::marker::PhantomData<fn() -> H>,
}

impl<'a, S, H: EffectHandler> CompensationSubmission<'a, S, H> {
    pub(crate) fn new(runtime: &'a Runtime<S>, key: impl Display) -> Self {
        Self {
            runtime,
            key: key.to_string(),
            reason: None,
            actor: None,
            _handler: std::marker::PhantomData,
        }
    }

    /// Why the effect is being undone; recorded in the audit trail.
    pub fn reason(mut self, reason: impl Into<String>) -> Self {
        self.reason = Some(reason.into());
        self
    }

    /// Who is undoing it.
    pub fn actor(mut self, actor: impl Into<String>) -> Self {
        self.actor = Some(actor.into());
        self
    }
}

impl<'a, S: EffectStore, H: EffectHandler> IntoFuture for CompensationSubmission<'a, S, H> {
    type Output = Result<CompensationOutcome, RuntimeError>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let handler = self
                .runtime
                .handler::<H>()
                .ok_or(RuntimeError::NotRegistered { name: H::NAME })?;
            let compensate = handler
                .compensate
                .clone()
                .ok_or(RuntimeError::NotCompensable { name: H::NAME })?;
            let spec = CompensationSpec {
                key: EffectKey::new(EffectName::new(H::NAME)?, LogicalKey::new(self.key)?),
                reason: self.reason,
                actor: self.actor,
                retry: handler.effect.retry_policy(),
                attempt_timeout: handler.effect.attempt_timeout(),
            };
            self.runtime.compensate_effect(spec, compensate).await
        })
    }
}
