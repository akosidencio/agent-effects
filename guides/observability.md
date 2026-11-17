# Observability

What to measure, what the numbers mean, and when to page someone.

## Sources

- **Metrics** come from an `EffectObserver`. `agent-effects-otel` provides
  `OtelObserver`, which records OpenTelemetry instruments through the global
  meter provider or one you pass in; export them with OTLP, Prometheus or any
  other exporter. The core crate depends on no metrics library.
- **Traces** are `tracing` spans: `agent_effect.execute` and
  `agent_effect.compensate`, with `effect.name`, `effect.kind`,
  `effect.risk_level`, `effect.id`, `effect.status` and `effect.attempt`.
  Export them with `tracing-opentelemetry` like any other span. Spans carry
  the logical key (`effect.logical_key`) for debugging a single effect; keep
  it out of metric labels.
- **State** is in the store: `Runtime::pending`, the `RecoveryReport` of each
  pass, and the `agent-effects` CLI (`stats`, `list`, `inspect`, `history`).
  Metrics show rates and trends; the store answers "which effects, exactly".

```rust
let runtime = Runtime::builder(store)
    .observer(agent_effects_otel::OtelObserver::global())
    .build();
```

An observer runs synchronously after each stored change. A panicking
observer is caught and logged; it never affects an effect.

## Instruments

Every instrument carries `effect.name` and `effect.kind` unless noted. No
instrument carries an effect id, a logical key, an input, an output, an
actor or an error message: those are unbounded or sensitive. Effect names
are chosen by the application and should stay a small, fixed set (`payment.charge`, not
`payment.charge.order-17`).

| Instrument | Type | Meaning |
|---|---|---|
| `agent_effects.started` | counter | Effects recorded |
| `agent_effects.completed` | counter | Effects committed |
| `agent_effects.failed` | counter | Effects that definitely did not apply |
| `agent_effects.rejected` | counter | Refused by a precondition or an approver |
| `agent_effects.unknown` | counter | Outcomes that became unknown |
| `agent_effects.needs_intervention` | counter | Effects escalated to an operator |
| `agent_effects.awaiting_intervention` | up/down counter | Change in effects waiting for an operator (escalated or failed compensation), since each process started |
| `agent_effects.duration` | histogram, s | Recorded → committed, failed or rejected |
| `agent_effects.attempts` | counter, `attempt` = `first` \| `retry` | Attempts started |
| `agent_effects.attempt.duration` | histogram, s, `outcome` | One attempt, by the status it ended in |
| `agent_effects.retry.count` | counter | Retries scheduled |
| `agent_effects.retry.delay` | histogram, s | Time between scheduling a retry and when it may start |
| `agent_effects.unknown.duration` | histogram, s | Time spent unknown before being resolved |
| `agent_effects.verification.started` | counter | Verifications started |
| `agent_effects.verification.outcomes` | counter, `outcome` = `confirmed` \| `not_applied` \| `conflict` \| `inconclusive` | How verifications ended |
| `agent_effects.pending_approval` | up/down counter | Change in effects awaiting approval, since each process started |
| `agent_effects.approval.decisions` | counter, `decision` = `approved` \| `denied` | Approval decisions, by a provider or an operator |
| `agent_effects.approval.duration` | histogram, s, `decision` | Time from requesting approval to the decision |
| `agent_effects.operator.resolutions` | counter, `resolution` = `applied` \| `not_applied` \| `retry` \| `compensated` | Operator decisions on unresolved effects |
| `agent_effects.compensation.started` / `.completed` / `.failed` | counter | Compensations |
| `agent_effects.lease.expired` | counter | Attempts whose worker lost its lease mid-flight (found by recovery or a later call) |
| `agent_effects.lease.conflicts` | counter | A worker found another holding an effect and left it alone |
| `agent_effects.lease.renewal_failures` | counter, no attributes | Lease renewals that failed with a store error (retried) |
| `agent_effects.recovery.passes` | counter, `result` = `completed` \| `stopped` \| `failed`; no effect attributes | Recovery passes |
| `agent_effects.recovery.duration` | histogram, s; no effect attributes | Duration of one pass |
| `agent_effects.recovery.effects` | counter, `outcome` = `resumed` \| `marked_unknown` \| `skipped` \| `unhandled` \| `resume_error`; no effect attributes | What passes did, per effect |
| `agent_effects.recovery.backlog` | gauge, `state` = `due` \| `not_due` \| `awaiting_operator`; no effect attributes | What the latest pass of this worker found waiting |
| `agent_effects.pruned` | counter, `status`; no effect attributes | Settled records deleted by retention |

Up/down counters record changes seen by each process since it started, so
their sum across processes drifts after restarts. The recovery backlog gauge
is a per-worker snapshot of what its latest pass encountered, including work
it subsequently finished; it is not an atomic global queue length. Use
`agent-effects stats` to inspect stored levels, allowing for concurrent writes.

## Reading the numbers

**`unknown` is the one to watch.** Each unknown outcome is an effect that may
or may not have happened: the remote system answered ambiguously, timed out,
or the worker died mid-attempt. Idempotent and verifiable effects resolve
themselves; the rest escalate. A rise usually means a remote system is
degraded, not that agent-effects is.

**`needs_intervention` and `awaiting_intervention`** are work for people.
Every escalated effect needs an operator decision (`agent-effects resolve`).
A steady trickle is normal for unprotected effects; a burst follows an outage.

**Recovery.** `recovery.effects{outcome="resumed"}` per second is recovery
throughput; `recovery.duration` should stay well under the recovery
interval. A growing `recovery.backlog{state="due"}` means recovery cannot
keep up: raise `RecoveryConfig::max_concurrency` (within the connection
pool), add workers, or look for slow handlers. `not_due` is retries waiting
for their time, which is normal. `marked_unknown` means workers are dying
mid-attempt.

**Leases.** `lease.conflicts` is normal when several workers or callers share
work; it measures contention, not errors. `lease.expired` should be rare: each
one is a worker that stopped renewing while working (crash, long pause,
network partition). `lease.renewal_failures` means the store was unreachable
for a renewal; persistent failures lead to lost leases.

**Retries.** `attempts{attempt="retry"}` against `attempts{attempt="first"}`
is the retry rate. `retry.delay` shows how long effects wait; long delays
are waited out without holding a lease.

**Approvals.** `approval.duration` is how long agents wait for people.
`pending_approval` rising without decisions means approvals are not being
looked at.

## Example alerts (Prometheus)

Names below follow the OpenTelemetry Prometheus exporter's conventions
(dots become underscores, counters get `_total`, units are appended); adjust
to your exporter.

```yaml
groups:
  - name: agent-effects
    rules:
      - alert: AgentEffectsUnknownOutcomes
        # More than 1% of attempts end unknown for 10 minutes.
        expr: |
          sum by (effect_name) (rate(agent_effects_unknown_total[10m]))
            / sum by (effect_name) (rate(agent_effects_attempts_total[10m])) > 0.01
        for: 10m
        labels: { severity: warning }
        annotations:
          summary: "{{ $labels.effect_name }}: ambiguous outcomes from the remote system"

      - alert: AgentEffectsNeedIntervention
        expr: sum by (effect_name) (increase(agent_effects_needs_intervention_total[15m])) > 0
        labels: { severity: ticket }
        annotations:
          summary: "{{ $labels.effect_name }}: effects wait for an operator (agent-effects list --status needs_intervention)"

      - alert: AgentEffectsRecoveryBehind
        expr: max(agent_effects_recovery_backlog{state="due"}) > 1000
        for: 15m
        labels: { severity: warning }

      - alert: AgentEffectsRecoveryFailing
        expr: sum(increase(agent_effects_recovery_passes_total{result="failed"}[15m])) > 3
        labels: { severity: page }
        annotations:
          summary: "Recovery passes are failing: check store connectivity"

      - alert: AgentEffectsWorkersDying
        expr: sum(increase(agent_effects_lease_expired_total[15m])) > 10
        labels: { severity: warning }

      - alert: AgentEffectsCompensationFailed
        expr: sum by (effect_name) (increase(agent_effects_compensation_failed_total[1h])) > 0
        labels: { severity: ticket }
```

## Sensitive data

Metrics carry no payloads, ids, keys, actors or messages. Spans carry the
logical key; if keys identify people, drop or hash `effect.logical_key` in
your tracing pipeline. Stored inputs, outputs, audit payloads and error
messages pass through the runtime's `Redactor` before they are written; the
CLI hides inputs and outputs unless asked.
