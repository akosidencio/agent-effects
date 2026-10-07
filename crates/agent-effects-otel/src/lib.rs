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
//! Instruments, each with attributes `effect.name` and `effect.kind`:
//!
//! | Instrument | Kind | Counts / measures |
//! |---|---|---|
//! | `agent_effects.started` | counter | effects recorded |
//! | `agent_effects.completed` | counter | effects committed |
//! | `agent_effects.failed` | counter | effects that definitely did not apply |
//! | `agent_effects.rejected` | counter | effects refused by a precondition or approver |
//! | `agent_effects.unknown` | counter | outcomes that became unknown: **watch this one**. A spike means remote systems are answering ambiguously |
//! | `agent_effects.needs_intervention` | counter | effects escalated to an operator |
//! | `agent_effects.retry.count` | counter | retries scheduled |
//! | `agent_effects.compensation.started` / `.completed` / `.failed` | counter | compensations |
//! | `agent_effects.pending_approval` | up/down counter | effects awaiting approval (deltas since start) |
//! | `agent_effects.duration` | histogram, s | recorded → committed, failed or rejected |
//! | `agent_effects.attempt.duration` | histogram, s | one attempt, also tagged `outcome` (the status it ended in) |
//! | `agent_effects.unknown.duration` | histogram, s | time spent unknown before being resolved |
//!
//! The logical key is never an attribute: it is unbounded and may identify
//! customers. Traces need no extra crate: the runtime's `tracing` spans
//! (`agent_effect.execute`, `agent_effect.compensate`) export through
//! `tracing-opentelemetry` like any other.

use agent_effects::{EffectObserver, EffectRecord, EffectStatus, Observation, Transition};
use opentelemetry::KeyValue;
use opentelemetry::metrics::{Counter, Histogram, Meter, UpDownCounter};

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
}

impl OtelObserver {
    /// Instruments from the global meter provider, under the meter name
    /// `agent-effects`.
    pub fn global() -> Self {
        Self::new(&opentelemetry::global::meter("agent-effects"))
    }

    /// Instruments from `meter`.
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
        }
    }
}

fn attributes(record: &EffectRecord) -> [KeyValue; 2] {
    [
        KeyValue::new("effect.name", record.key.name.to_string()),
        KeyValue::new("effect.kind", record.kind.as_str()),
    ]
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
        match o.transition {
            Transition::ScheduleRetry => self.retries.add(1, &attrs),
            Transition::StartCompensation => self.compensation_started.add(1, &attrs),
            Transition::RequestApproval => self.pending_approval.add(1, &attrs),
            _ => {}
        }
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
}
