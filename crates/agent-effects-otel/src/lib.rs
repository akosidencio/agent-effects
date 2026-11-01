//! OpenTelemetry metrics for [`agent-effects`](https://crates.io/crates/agent-effects).
//!
//! ```no_run
//! use agent_effects::Runtime;
//! use agent_effects_memory::MemoryStore;
//! use agent_effects_otel::OtelObserver;
//!
//! // Uses the global meter provider; set it up with your exporter (OTLP,
//! // Prometheus, …) as usual.
//! let runtime = Runtime::builder(MemoryStore::new())
//!     .observer(OtelObserver::global())
//!     .build();
//! ```
//!
//! Instruments, each with attributes `effect.name` and `effect.kind` unless
//! the table says otherwise:
//!
//! | Instrument | Kind | Counts / measures |
//! |---|---|---|
//! | `agent_effects.started` | counter | effects recorded |
//! | `agent_effects.completed` | counter | effects committed |
//! | `agent_effects.failed` | counter | effects that definitely did not apply |
//! | `agent_effects.rejected` | counter | effects refused by a precondition or approver |
//! | `agent_effects.unknown` | counter | outcomes that became unknown: **watch this one**. A spike means remote systems are answering ambiguously |
//! | `agent_effects.needs_intervention` | counter | effects escalated to an operator |
//! | `agent_effects.awaiting_intervention` | up/down counter | effects waiting for an operator: escalated or failed compensations (deltas since start) |
//! | `agent_effects.attempts` | counter, `attempt` = `first` \| `retry` | attempts started |
//! | `agent_effects.retry.count` | counter | retries scheduled |
//! | `agent_effects.retry.delay` | histogram, s | time from scheduling a retry until it may start |
//! | `agent_effects.verification.started` | counter | verifications started |
//! | `agent_effects.verification.outcomes` | counter, `outcome` = `confirmed` \| `not_applied` \| `conflict` \| `inconclusive` | how verifications ended |
//! | `agent_effects.compensation.started` / `.completed` / `.failed` | counter | compensations |
//! | `agent_effects.pending_approval` | up/down counter | effects awaiting approval (deltas since start) |
//! | `agent_effects.approval.decisions` | counter, `decision` = `approved` \| `denied` | approval decisions |
//! | `agent_effects.approval.duration` | histogram, s, `decision` | approval requested → decided |
//! | `agent_effects.operator.resolutions` | counter, `resolution` = `applied` \| `not_applied` \| `retry` \| `compensated` | operator decisions on unresolved effects |
//! | `agent_effects.lease.expired` | counter | attempts whose worker lost its lease mid-flight |
//! | `agent_effects.lease.conflicts` | counter, `effect.name` only | a worker found another holding an effect |
//! | `agent_effects.lease.renewal_failures` | counter, no attributes | lease renewals that failed with a store error |
//! | `agent_effects.duration` | histogram, s | recorded → committed, failed or rejected |
//! | `agent_effects.attempt.duration` | histogram, s | one attempt, also tagged `outcome` (the status it ended in) |
//! | `agent_effects.unknown.duration` | histogram, s | time spent unknown before being resolved |
//! | `agent_effects.recovery.passes` | counter, `result` = `completed` \| `stopped` \| `failed` | recovery passes |
//! | `agent_effects.recovery.duration` | histogram, s, `result` | one recovery pass |
//! | `agent_effects.recovery.effects` | counter, `outcome` = `resumed` \| `marked_unknown` \| `skipped` \| `unhandled` \| `resume_error` | what passes did, per effect |
//! | `agent_effects.recovery.backlog` | gauge, `state` = `due` \| `not_due` \| `awaiting_operator` | what this worker's latest pass found waiting |
//! | `agent_effects.pruned` | counter, `status` | settled records deleted by retention |
//!
//! Recovery and pruning instruments carry no effect attributes.
//!
//! No attribute carries an effect id, a logical key, an input, an output, an
//! actor or an error message: they are unbounded and may identify customers.
//! Every attribute value comes from a small fixed set, apart from
//! `effect.name`, which the application chooses. Traces need no extra crate: the runtime's `tracing` spans
//! (`agent_effect.execute`, `agent_effect.compensate`) export through
//! `tracing-opentelemetry` like any other.

use agent_effects::{
    EffectObserver, EffectRecord, EffectStatus, Observation, RuntimeEvent, Transition,
};
use opentelemetry::KeyValue;
use opentelemetry::metrics::{Counter, Gauge, Histogram, Meter, UpDownCounter};

/// An [`EffectObserver`] that records OpenTelemetry metrics.
#[derive(Clone, Debug)]
pub struct OtelObserver {
    started: Counter<u64>,
    completed: Counter<u64>,
    failed: Counter<u64>,
    rejected: Counter<u64>,
    unknown: Counter<u64>,
    needs_intervention: Counter<u64>,
    retries: Counter<u64>,
    compensation_started: Counter<u64>,
    compensation_completed: Counter<u64>,
    compensation_failed: Counter<u64>,
    pending_approval: UpDownCounter<i64>,
    duration: Histogram<f64>,
    attempt_duration: Histogram<f64>,
    unknown_duration: Histogram<f64>,
    awaiting_intervention: UpDownCounter<i64>,
    attempts: Counter<u64>,
    retry_delay: Histogram<f64>,
    verification_started: Counter<u64>,
    verification_outcomes: Counter<u64>,
    approval_decisions: Counter<u64>,
    approval_duration: Histogram<f64>,
    operator_resolutions: Counter<u64>,
    lease_expired: Counter<u64>,
    lease_conflicts: Counter<u64>,
    lease_renewal_failures: Counter<u64>,
    recovery_passes: Counter<u64>,
    recovery_duration: Histogram<f64>,
    recovery_effects: Counter<u64>,
    recovery_backlog: Gauge<u64>,
    pruned: Counter<u64>,
}

impl OtelObserver {
    /// Instruments from the global meter provider, under the meter name
    /// `agent-effects`.
    pub fn global() -> Self {
        Self::new(&opentelemetry::global::meter("agent-effects"))
    }

    /// Instruments from `meter`.
    // One instrument per field: long, but flat.
    #[allow(clippy::too_many_lines)]
    pub fn new(meter: &Meter) -> Self {
        let counter = |name: &'static str, description: &'static str| {
            meter
                .u64_counter(name)
                .with_unit("{effect}")
                .with_description(description)
                .build()
        };
        let seconds = |name: &'static str, description: &'static str| {
            meter
                .f64_histogram(name)
                .with_unit("s")
                .with_description(description)
                .build()
        };
        Self {
            started: counter("agent_effects.started", "Effects recorded"),
            completed: counter("agent_effects.completed", "Effects committed"),
            failed: counter(
                "agent_effects.failed",
                "Effects that definitely did not apply",
            ),
            rejected: counter(
                "agent_effects.rejected",
                "Effects refused by a precondition or an approver",
            ),
            unknown: counter(
                "agent_effects.unknown",
                "Attempts whose outcome became unknown",
            ),
            needs_intervention: counter(
                "agent_effects.needs_intervention",
                "Effects escalated to an operator",
            ),
            retries: meter
                .u64_counter("agent_effects.retry.count")
                .with_unit("{retry}")
                .with_description("Retries scheduled")
                .build(),
            compensation_started: counter(
                "agent_effects.compensation.started",
                "Compensations started",
            ),
            compensation_completed: counter(
                "agent_effects.compensation.completed",
                "Effects undone",
            ),
            compensation_failed: counter(
                "agent_effects.compensation.failed",
                "Compensations that failed for good",
            ),
            pending_approval: meter
                .i64_up_down_counter("agent_effects.pending_approval")
                .with_unit("{effect}")
                .with_description("Effects awaiting approval (changes since this process started)")
                .build(),
            duration: seconds(
                "agent_effects.duration",
                "Time from recording an effect to its committing, failing or being rejected",
            ),
            attempt_duration: seconds("agent_effects.attempt.duration", "Duration of one attempt"),
            unknown_duration: seconds(
                "agent_effects.unknown.duration",
                "Time an effect spent with an unknown outcome",
            ),
            awaiting_intervention: meter
                .i64_up_down_counter("agent_effects.awaiting_intervention")
                .with_unit("{effect}")
                .with_description(
                    "Effects waiting for an operator (changes since this process started)",
                )
                .build(),
            attempts: meter
                .u64_counter("agent_effects.attempts")
                .with_unit("{attempt}")
                .with_description("Attempts started, first or retry")
                .build(),
            retry_delay: seconds(
                "agent_effects.retry.delay",
                "Time from scheduling a retry until it may start",
            ),
            verification_started: meter
                .u64_counter("agent_effects.verification.started")
                .with_unit("{verification}")
                .with_description("Verifications started")
                .build(),
            verification_outcomes: meter
                .u64_counter("agent_effects.verification.outcomes")
                .with_unit("{verification}")
                .with_description("How verifications ended")
                .build(),
            approval_decisions: meter
                .u64_counter("agent_effects.approval.decisions")
                .with_unit("{decision}")
                .with_description("Approval decisions, by a provider or an operator")
                .build(),
            approval_duration: seconds(
                "agent_effects.approval.duration",
                "Time from requesting approval to the decision",
            ),
            operator_resolutions: meter
                .u64_counter("agent_effects.operator.resolutions")
                .with_unit("{decision}")
                .with_description("Operator decisions on unresolved effects")
                .build(),
            lease_expired: counter(
                "agent_effects.lease.expired",
                "Attempts whose worker lost its lease mid-flight",
            ),
            lease_conflicts: meter
                .u64_counter("agent_effects.lease.conflicts")
                .with_unit("{conflict}")
                .with_description("A worker found another holding an effect's lease")
                .build(),
            lease_renewal_failures: meter
                .u64_counter("agent_effects.lease.renewal_failures")
                .with_unit("{renewal}")
                .with_description("Lease renewals that failed with a store error")
                .build(),
            recovery_passes: meter
                .u64_counter("agent_effects.recovery.passes")
                .with_unit("{pass}")
                .with_description("Recovery passes")
                .build(),
            recovery_duration: seconds(
                "agent_effects.recovery.duration",
                "Duration of one recovery pass",
            ),
            recovery_effects: counter(
                "agent_effects.recovery.effects",
                "What recovery passes did, per effect",
            ),
            recovery_backlog: meter
                .u64_gauge("agent_effects.recovery.backlog")
                .with_unit("{effect}")
                .with_description("What this worker's latest recovery pass found waiting")
                .build(),
            pruned: meter
                .u64_counter("agent_effects.pruned")
                .with_unit("{effect}")
                .with_description("Settled records deleted by retention")
                .build(),
        }
    }
}

fn tagged(attrs: &[KeyValue; 2], key: &'static str, value: &'static str) -> [KeyValue; 3] {
    let [name, kind] = attrs.clone();
    [name, kind, KeyValue::new(key, value)]
}

fn count(n: usize) -> u64 {
    u64::try_from(n).unwrap_or(u64::MAX)
}

fn attributes(record: &EffectRecord) -> [KeyValue; 2] {
    [
        KeyValue::new("effect.name", record.key.name.to_string()),
        KeyValue::new("effect.kind", record.kind.as_str()),
    ]
}

impl OtelObserver {
    /// How a verification or an approval ended.
    fn on_decision(&self, o: &Observation<'_>, attrs: &[KeyValue; 2]) {
        if o.from == EffectStatus::Verifying {
            let outcome = match o.transition {
                Transition::VerificationConfirmed => Some("confirmed"),
                Transition::VerificationNotApplied | Transition::ScheduleRetry => {
                    Some("not_applied")
                }
                Transition::VerificationConflict => Some("conflict"),
                Transition::OutcomeUnknown => Some("inconclusive"),
                _ => None,
            };
            if let Some(outcome) = outcome {
                self.verification_outcomes
                    .add(1, &tagged(attrs, "outcome", outcome));
            }
        }
        if o.from == EffectStatus::AwaitingApproval {
            let decision = match o.transition {
                Transition::Approve => Some("approved"),
                Transition::Deny => Some("denied"),
                _ => None,
            };
            if let Some(decision) = decision {
                let decided = tagged(attrs, "decision", decision);
                self.approval_decisions.add(1, &decided);
                self.approval_duration
                    .record(o.in_previous_status.as_secs_f64(), &decided);
            }
        }
    }
}

impl EffectObserver for OtelObserver {
    fn on_created(&self, record: &EffectRecord) {
        self.started.add(1, &attributes(record));
    }

    fn on_transition(&self, o: &Observation<'_>) {
        let attrs = attributes(o.record);
        let settled = |counter: &Counter<u64>| {
            counter.add(1, &attrs);
            self.duration.record(o.since_created.as_secs_f64(), &attrs);
        };
        match o.to {
            EffectStatus::Committed => settled(&self.completed),
            EffectStatus::Failed => settled(&self.failed),
            EffectStatus::Rejected => settled(&self.rejected),
            EffectStatus::Unknown => self.unknown.add(1, &attrs),
            EffectStatus::NeedsIntervention => self.needs_intervention.add(1, &attrs),
            EffectStatus::Compensated => self.compensation_completed.add(1, &attrs),
            EffectStatus::CompensationFailed => self.compensation_failed.add(1, &attrs),
            _ => {}
        }
        let waits_for_operator = |s: EffectStatus| {
            matches!(
                s,
                EffectStatus::NeedsIntervention | EffectStatus::CompensationFailed
            )
        };
        if waits_for_operator(o.to) && !waits_for_operator(o.from) {
            self.awaiting_intervention.add(1, &attrs);
        } else if waits_for_operator(o.from) && !waits_for_operator(o.to) {
            self.awaiting_intervention.add(-1, &attrs);
        }
        match o.transition {
            Transition::ScheduleRetry => {
                self.retries.add(1, &attrs);
                if let Some(delay) = o
                    .record
                    .next_attempt_at
                    .and_then(|at| at.duration_since(o.record.updated_at).ok())
                {
                    self.retry_delay.record(delay.as_secs_f64(), &attrs);
                }
            }
            Transition::StartAttempt => {
                let attempt = if o.record.attempt_count > 1 {
                    "retry"
                } else {
                    "first"
                };
                self.attempts.add(1, &tagged(&attrs, "attempt", attempt));
            }
            Transition::StartVerification => self.verification_started.add(1, &attrs),
            Transition::LeaseExpired => self.lease_expired.add(1, &attrs),
            Transition::StartCompensation => self.compensation_started.add(1, &attrs),
            Transition::RequestApproval => self.pending_approval.add(1, &attrs),
            Transition::ResolvedApplied => self
                .operator_resolutions
                .add(1, &tagged(&attrs, "resolution", "applied")),
            Transition::ResolvedNotApplied => self
                .operator_resolutions
                .add(1, &tagged(&attrs, "resolution", "not_applied")),
            Transition::ResolvedRetry => self
                .operator_resolutions
                .add(1, &tagged(&attrs, "resolution", "retry")),
            Transition::ResolvedCompensated => self
                .operator_resolutions
                .add(1, &tagged(&attrs, "resolution", "compensated")),
            _ => {}
        }
        self.on_decision(o, &attrs);
        match o.from {
            EffectStatus::AwaitingApproval => self.pending_approval.add(-1, &attrs),
            EffectStatus::Executing => {
                let [name, kind] = attrs.clone();
                let outcome = KeyValue::new("outcome", o.to.as_str());
                self.attempt_duration
                    .record(o.in_previous_status.as_secs_f64(), &[name, kind, outcome]);
            }
            EffectStatus::Unknown => self
                .unknown_duration
                .record(o.in_previous_status.as_secs_f64(), &attrs),
            _ => {}
        }
    }

    fn on_event(&self, event: &RuntimeEvent<'_>) {
        match event {
            RuntimeEvent::LeaseConflict { name } => self
                .lease_conflicts
                .add(1, &[KeyValue::new("effect.name", name.to_string())]),
            RuntimeEvent::LeaseRenewalFailed => self.lease_renewal_failures.add(1, &[]),
            RuntimeEvent::RecoveryPass { report, duration } => {
                let result = match report {
                    None => "failed",
                    Some(report) if report.stopped => "stopped",
                    Some(_) => "completed",
                };
                let result = [KeyValue::new("result", result)];
                self.recovery_passes.add(1, &result);
                self.recovery_duration
                    .record(duration.as_secs_f64(), &result);
                let Some(report) = report else { return };
                for (outcome, n) in [
                    ("resumed", report.resumed.len()),
                    ("marked_unknown", report.marked_unknown.len()),
                    ("skipped", report.skipped.len()),
                    ("unhandled", report.unhandled.len()),
                    ("resume_error", report.resume_errors.len()),
                ] {
                    if n > 0 {
                        self.recovery_effects
                            .add(count(n), &[KeyValue::new("outcome", outcome)]);
                    }
                }
                let due =
                    report.resumed.len() + report.unhandled.len() + report.resume_errors.len();
                for (state, n) in [
                    ("due", due),
                    ("not_due", report.not_due.len()),
                    ("awaiting_operator", report.waiting_for_operator.len()),
                ] {
                    self.recovery_backlog
                        .record(count(n), &[KeyValue::new("state", state)]);
                }
            }
            RuntimeEvent::Pruned { status, count } => self
                .pruned
                .add(*count, &[KeyValue::new("status", status.as_str())]),
            _ => {}
        }
    }
}
