//! Human approval before an effect runs.
//!
//! An effect that requires approval
//! ([`EffectBuilder::require_approval`](crate::EffectBuilder::require_approval),
//! [`EffectHandler::requires_approval`](crate::EffectHandler::requires_approval))
//! goes `Pending → AwaitingApproval` before its first attempt. The record is
//! durable, so a pending approval survives restarts. Decisions come from two
//! places:
//!
//! - the runtime's [`ApprovalProvider`], asked when the effect needs a
//!   decision and again on every later call while none has been made. It
//!   may answer at once (a CLI prompt), or return
//!   [`ApprovalDecision::Deferred`] and let the decision arrive later
//!   (Slack, a dashboard);
//! - an operator, through [`Runtime::approve`](crate::Runtime::approve) or
//!   [`Runtime::deny`](crate::Runtime::deny).
//!
//! Approval is asked once: an approved effect is never asked again, even if
//! it retries. The precondition is checked again after approval, since the
//! decision may have taken a while.

use std::fmt;
use std::future::Future;
use std::io::{BufRead, Write};
use std::pin::Pin;
use std::sync::{Arc, Mutex, PoisonError};

use serde_json::Value;

use crate::id::{EffectId, EffectKey};
use crate::kind::EffectKind;

/// What an approver is asked to decide.
#[derive(Clone, Debug, PartialEq)]
pub struct ApprovalRequest {
    /// The effect's record id. Stable across repeated requests for the same
    /// effect, so a provider can deduplicate them.
    pub effect_id: EffectId,
    /// What the effect is.
    pub key: EffectKey,
    /// How reversible it is.
    pub kind: EffectKind,
    /// The stored input.
    pub input: Option<Value>,
    /// Who asked for the effect, e.g. `agent:refund-agent`.
    pub requested_by: Option<String>,
}

impl fmt::Display for ApprovalRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "  effect: {} ({:?})", self.key, self.kind)?;
        writeln!(f, "  id:     {}", self.effect_id)?;
        if let Some(by) = &self.requested_by {
            writeln!(f, "  by:     {by}")?;
        }
        if let Some(input) = &self.input {
            writeln!(f, "  input:  {input}")?;
        }
        Ok(())
    }
}

/// An approver's answer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ApprovalDecision {
    /// Go ahead.
    Approved {
        /// Who approved, recorded as the audit event's actor.
        by: String,
    },
    /// Never run it. The effect ends `Rejected` with `reason`.
    Denied {
        /// Who denied.
        by: String,
        /// Why; recorded as the effect's error.
        reason: String,
    },
    /// No decision yet. The effect stays `AwaitingApproval`.
    Deferred,
}

/// Decides whether effects that require approval may run.
pub trait ApprovalProvider: Send + Sync + 'static {
    /// Asks for a decision. May be called again for the same effect while
    /// no decision has been made.
    fn request(&self, request: ApprovalRequest) -> impl Future<Output = ApprovalDecision> + Send;
}

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// [`ApprovalProvider`], erased so the runtime can hold any provider.
pub(crate) trait ErasedApproval: Send + Sync + 'static {
    fn request_boxed(&self, request: ApprovalRequest) -> BoxFuture<'_, ApprovalDecision>;
}

impl<P: ApprovalProvider> ErasedApproval for P {
    fn request_boxed(&self, request: ApprovalRequest) -> BoxFuture<'_, ApprovalDecision> {
        Box::pin(self.request(request))
    }
}

/// Asks at the terminal: prints the request and reads `y` or `n`.
///
/// Anything but `y`/`yes` denies. End of input (no human attached) defers,
/// leaving the effect for an operator.
pub struct CliApproval {
    io: Arc<Mutex<CliIo>>,
    approver: String,
}

struct CliIo {
    input: Box<dyn BufRead + Send>,
    output: Box<dyn Write + Send>,
}

impl CliApproval {
    /// Reads standard input and writes to standard error. The approver is
    /// recorded as `cli:$USER`.
    pub fn new() -> Self {
        let user = std::env::var("USER").unwrap_or_else(|_| "unknown".into());
        Self::with_io(std::io::BufReader::new(std::io::stdin()), std::io::stderr())
            .approver(format!("cli:{user}"))
    }

    /// Reads answers from `input` and writes prompts to `output`.
    pub fn with_io(
        input: impl BufRead + Send + 'static,
        output: impl Write + Send + 'static,
    ) -> Self {
        Self {
            io: Arc::new(Mutex::new(CliIo {
                input: Box::new(input),
                output: Box::new(output),
            })),
            approver: "cli".into(),
        }
    }

    /// The name recorded as approver or denier.
    #[must_use]
    pub fn approver(mut self, name: impl Into<String>) -> Self {
        self.approver = name.into();
        self
    }
}

impl Default for CliApproval {
    fn default() -> Self {
        Self::new()
    }
}

impl ApprovalProvider for CliApproval {
    async fn request(&self, request: ApprovalRequest) -> ApprovalDecision {
        let (io, by) = (Arc::clone(&self.io), self.approver.clone());
        // Reading a terminal blocks: keep it off the async workers.
        tokio::task::spawn_blocking(move || {
            let mut io = io.lock().unwrap_or_else(PoisonError::into_inner);
            let asked = write!(
                io.output,
                "agent-effects: approval needed\n{request}Approve? [y/N]: "
            )
            .and_then(|()| io.output.flush());
            if asked.is_err() {
                return ApprovalDecision::Deferred;
            }
            let mut answer = String::new();
            match io.input.read_line(&mut answer) {
                Ok(0) | Err(_) => ApprovalDecision::Deferred,
                Ok(_) => match answer.trim().to_ascii_lowercase().as_str() {
                    "y" | "yes" => ApprovalDecision::Approved { by },
                    _ => ApprovalDecision::Denied {
                        by,
                        reason: "denied at the command line".into(),
                    },
                },
            }
        })
        .await
        .unwrap_or(ApprovalDecision::Deferred)
    }
}
