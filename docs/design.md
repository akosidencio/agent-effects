# Agent Effects — Design

This is the authoritative design. It refines the original draft spec. Where
they disagree, this document wins, and the reason is in the
[decisions log](#decisions-log). Sections marked *(planned)* describe
milestones not yet built; see [roadmap.md](roadmap.md).

## 1. Scope and guarantees

`agent-effects` executes operations that change the outside world (payments,
emails, cloud resources, tickets) so that:

- intent is recorded durably **before** the external call is made;
- the same logical effect is never started twice by this runtime;
- a failure is only called a failure when the effect definitely did not
  apply. Otherwise the outcome is **Unknown**;
- unknown outcomes are resolved by verification or provably safe retry, and
  otherwise handed to an operator, never guessed.

It does **not** provide exactly-once side effects. That is impossible without
cooperation from the target system. The strongest guarantee comes from
targets that honour an idempotency key.

Non-goals: workflow engine, job queue, scheduler, LLM/agent framework, MCP
implementation, authorization. Agent output must pass the application's own
authorization before it reaches an effect. Wrapping a call in an effect makes
it reliable, not permitted.

## 2. Identity

| Concept | Type | Notes |
|---|---|---|
| Record id | `EffectId` | UUIDv7, time-ordered |
| Logical identity | `EffectKey { name, key }` | e.g. `payment.charge` + `order_5824`; unique per store |
| Remote idempotency key | `IdempotencyKey` | UUIDv5 of the `EffectKey`; deterministic, so it survives record loss. The derivation is frozen (pinned by a test) |
| Lease holder | `WorkerId` | one per runtime instance |

Names are ≤ 200 bytes, keys ≤ 512 bytes, neither empty nor containing control
characters.

## 3. State machine

Implemented in `agent-effects-store` (`state.rs`) as a pure function `EffectStatus::apply(Transition)`.
Stores never change a status except through it.

| From | Transition | To |
|---|---|---|
| Pending | `RequestApproval` | AwaitingApproval |
| AwaitingApproval | `Approve` | Pending |
| AwaitingApproval | `Deny` | **Rejected** |
| Pending | `PreconditionRejected` | **Rejected** |
| Pending | `StartAttempt` | Executing |
| Executing | `Succeeded` | **Committed** |
| Executing | `FailedDefinitively` | **Failed** |
| Executing / Verifying / Unknown | `ScheduleRetry` | Pending |
| Executing / Verifying | `OutcomeUnknown`, `LeaseExpired` | Unknown |
| Executing / Unknown | `StartVerification` | Verifying |
| Verifying | `VerificationConfirmed` | **Committed** |
| Verifying | `VerificationNotApplied` | **Failed** |
| Verifying | `VerificationConflict` | NeedsIntervention |
| Unknown | `Escalate` | NeedsIntervention |
| Unknown / NeedsIntervention | `ResolvedApplied` | **Committed** |
| Unknown / NeedsIntervention | `ResolvedNotApplied` | **Failed** |
| Unknown / NeedsIntervention | `ResolvedRetry` | Pending |
| Committed | `StartCompensation` | Compensating |
| Compensating | `ScheduleCompensationRetry` | Compensating |
| Compensating | `StartCompensationRetry` | Compensating |
| Compensating | `CompensationSucceeded` | **Compensated** |
| Compensating | `CompensationFailed` | CompensationFailed |
| CompensationFailed | `ResolvedCompensated` | **Compensated** |
| CompensationFailed | `ResolvedRetry` | Compensating |

**Bold** = terminal (`Committed` is not: compensation is its one exit). Every transition is also an audit event
(`Transition::as_str`, e.g. `effect.attempt_started`). Retries between attempts
sit in `Pending` with a `next_attempt_at`, and the attempt counter lives on the
record, not in the status.

Invariants, all checked by tests:

- terminal states have no exits;
- every state is reachable from `Pending`, and every state can reach a
  terminal state;
- only `Pending` can start an attempt;
- an in-doubt state (`Executing`, `Unknown`, `Verifying`,
  `NeedsIntervention`) returns to `Pending` only through an explicit retry
  decision (`ScheduleRetry`, `ResolvedRetry`);
- `Committed` requires evidence: a success, a confirmed verification, or an
  operator;
- a lost lease leads to `Unknown`, never to `Failed`;
- `Failed` is refused while an earlier attempt may have applied the effect
  (`may_have_applied`, D28);
- compensation starts only from `Committed` (or an operator's retry of a
  failed compensation), and `Compensated` requires a successful compensation
  or an operator.

Compensation starts from **Committed**, because a `Failed` effect changed
nothing; see [Compensation](#compensation).

## 4. Failure classification

An action reports a `FailureClass`. Its `disposition()` drives the runtime:

| Class | Disposition |
|---|---|
| Transient | retry (definitely not applied) |
| RateLimited { retry_after } | retry, waiting at least `retry_after` |
| Ambiguous | **Unknown** |
| Permanent, Authentication, Authorization, Validation | fail |

`with_request_sent(Some(false))` turns Ambiguous into Transient. A request
that was never sent cannot have applied. The reverse is never inferred: a
sent request with a definitive answer (e.g. 503) keeps its class. Adapters are
responsible for classifying a timeout after sending as Ambiguous.

## 5. Unknown outcomes

`Capabilities { kind, remote_idempotency, verification }` decides what the
runtime may do on its own (`policy.rs`):

1. **Verify** if any verification is configured. This is preferred even when
   re-executing is safe, because it never repeats the action.
2. **Re-execute** if the kind is `Read`/`IdempotentWrite`, or the target
   honours the idempotency key.
3. Otherwise **Escalate** to `NeedsIntervention`. The runtime warns when an
   effect is built whose every unknown outcome would escalate.

Verification modes:

- `Authoritative`: the target reads its own writes. "Not found" means not
  applied.
- `EventuallyConsistent { settle }`: "not found" is trusted only once
  `settle` has passed since the attempt ended. Before that it counts as
  inconclusive and the runtime verifies again later. "Ended" is
  `attempt_ended_at`, the first transition out of `Executing`: when the
  action returned, timed out or panicked, or when its lease was found
  expired. Counting from the start would trust "not found" at once after a
  request slower than `settle` that wrote just before it returned.
  Both times come from the store's clock (the elapsed time since the check
  began is measured locally), so worker clock skew does not enter.

Known limit: a request that a stalled worker sent long ago, or one a timeout
abandoned while the server kept processing it, can still land after
verification said "not applied". Settle delays shrink this window and
remote idempotency closes it. Nothing else can. This is documented, not hidden.

## 6. Retry policy

`RetryPolicy { max_attempts, initial_delay, max_delay, multiplier, jitter }`.
`max_attempts` counts the first attempt. The backoff is exponential, capped at
`max_delay`. With jitter, each delay is drawn from `[d/2, d]` ("equal
jitter", so there is always some wait). A rate limit's `retry_after` is a
floor and may exceed the cap. The arithmetic is pure and takes the random
sample as an argument.

## 7. API: closures and handlers

Built in M3 and M4.

```rust
let runtime = Runtime::builder(SqliteStore::open("effects.db").await?)
    .worker_id(WorkerId::new("refund-agent-1"))
    .lease_ttl(Duration::from_secs(30))
    .build();

let outcome = runtime
    .effect("payment.charge", &order.id)           // name + logical key (any Display)
    .kind(EffectKind::IrreversibleWrite)           // default: IrreversibleWrite
    .input(&charge)                                // fingerprinted + stored
    .remote_idempotency(true)                      // target honours ctx.idempotency_key()
    .actor("agent:refund-agent")                   // recorded on the effect and its events
    .retry(RetryPolicy::default())                 // default: the runtime's policy
    .attempt_timeout(Duration::from_secs(20))      // a timeout is an ambiguous failure
    .precondition(|ctx| async move { /* -> Precondition */ })
    .verify_eventually(Duration::from_secs(10),    // or .verify(...) if the lookup
        |ctx| async move { /* -> Result<Verification<Payment>, EffectFailure> */ })
    .run(move |ctx| {
        let (stripe, charge) = (stripe.clone(), charge.clone());
        async move {
            stripe.charge(&charge, ctx.idempotency_key()).await.map_err(classify)
        }
    })
    .await?;                                       // the only `?`: infrastructure errors

match outcome {
    EffectOutcome::Committed(payment) => {}
    EffectOutcome::Failed(error) => {}             // ErrorRecord { class, message }
    EffectOutcome::Rejected(error) => {}
    EffectOutcome::Unknown { id } => {}
    EffectOutcome::NeedsIntervention { id } => {}
    EffectOutcome::InProgress { id } => {}
    _ => {}                                        // #[non_exhaustive]
}
```

The action returns `Result<T, EffectFailure>`. `EffectFailure` has a
constructor per `FailureClass` (`transient`, `permanent`, `ambiguous`,
`rate_limited`, …). `.request_sent(false)` marks a request that never left
the process, which turns an ambiguous failure into a transient one.

Rules:

- **Outcome, not error.** `Unknown` is a result the caller must handle, not
  an `Err` to bubble up with `?`. `Err(RuntimeError)` is reserved for invalid
  identity, input/kind mismatches, (de)serialization, store failures and
  internal task failures.
- **One `?` per chain.** Invalid names, keys and unserializable inputs are
  collected by the builder and reported by `run`.
- **Same key, same effect.** A record stores the kind and a fingerprint of
  the input (SHA-256 of canonical JSON with keys sorted at every level). A
  later call with a different kind or input fails with `KindMismatch` /
  `InputMismatch` instead of returning someone else's result.
- **Cancellation safety.** The whole call, from insert to the final record
  write, runs on a spawned task. Dropping the caller's future does not abort
  an attempt; its result is still recorded, and a later call replays it.
- **Panics.** The action runs on its own task. A panic is recorded as an
  ambiguous failure, since the request may have been sent before it.
- **Heartbeat.** While the action runs, the lease is renewed every third of
  its TTL. If renewal reports the lease lost, the attempt keeps running (it
  is already in flight), but none of its writes will be accepted.
- **Concurrent callers.** A second caller whose key is held by a live lease
  gets `InProgress { id }` immediately. Whether the lease is live is the
  store's call: the caller tries `acquire_lease` and reports `InProgress` on
  `LeaseHeld`, never comparing the lease's expiry with its own clock, which
  may disagree with a store that uses its own (`PostgresStore`). `runtime.wait::<T>(id, timeout)` polls
  the store until nobody holds the effect (10 ms doubling to 250 ms), then
  reports it.
- **Closures must be `Fn`**, not `FnOnce`, because an effect may be attempted
  more than once. They are `Send + Sync + 'static` because they run on a
  spawned task.
- **Retry budget.** `max_attempts` counts attempts over the effect's whole
  life, across calls and restarts (`attempt_count` on the record). A
  retryable failure (`Transient`, `RateLimited`) or a safely repeatable
  unknown outcome schedules a retry while budget remains. With no budget
  left, the first becomes `Failed` and the second escalates to
  `NeedsIntervention`.
- **Waiting between attempts.** `ScheduleRetry` is persisted first: the
  record is `Pending` with `next_attempt_at`. Then the call waits inline,
  holding and renewing the lease. A crash during the wait leaves a record
  that is safe to resume, because nothing is in flight, and the next caller
  waits out the same schedule. A rate limit's `retry_after` is honoured
  even when it exceeds `max_delay`.
- **Attempt timeout.** The attempt's task is aborted and the failure
  classified `Ambiguous`: the request may have been sent.
- **Preconditions run only before the first attempt.** A check is
  `Satisfied`, `Rejected { reason }` (the effect ends `Rejected`, nothing
  ran) or `RetryLater { after, reason }` (checked again, up to `max_attempts`
  checks, then rejected). After an attempt that may have applied, the
  effect's own success can falsify the check ("not yet refunded"), so
  re-running it could reject an effect that happened. A panicking check
  rejects.
- **Verification** runs after every successful attempt (a postcondition)
  and to reconcile an unknown outcome:
  - `Confirmed(t)` commits, with `t` as the output.
  - `NotApplied` is read through the [verification mode](#5-unknown-outcomes).
    Within the settle delay it waits the rest of the delay and checks again.
    Once trusted, the effect definitely did not apply, so it is re-run if
    budget remains, else `Failed`.
  - `Conflict { details }` goes to `NeedsIntervention`.
  - `Inconclusive`, a failing check or a panicking check are retried with
    backoff, up to `max_attempts` checks per call, a success's
    postcondition checks included. Then the effect stays `Unknown`, not
    escalated, and a later call or recovery checks again.

### Re-attaching

Closures are not durable, so after a crash only the caller can run them again.
Each call to the same `(name, key)` therefore re-attaches to the existing
record and continues from its status:

| Record status | Behaviour of a new call |
|---|---|
| Committed | return the cached output (`Committed(T)`) |
| Failed / Rejected | return the recorded result |
| Pending | acquire the lease and continue attempts |
| Executing, lease live | `InProgress` |
| Executing / Verifying, lease expired | `LeaseExpired` → Unknown, then as below |
| Unknown | apply the [unknown plan](#5-unknown-outcomes) with this call's closures: verify, re-run (budget permitting), or escalate |
| Pending with `next_attempt_at` | wait until then, then attempt |
| NeedsIntervention | `NeedsIntervention { id }` |
| any non-terminal status, lease live | `InProgress` |

Every row is covered by a test in `crates/agent-effects/tests/runtime.rs`,
including a worker that stalls past its lease. The new holder escalates, and
the stalled worker's late success is refused by fencing and does not
overwrite that decision.

### Recovery and operators

Implemented in `recovery.rs`:

- **`runtime.recover()`** scans for `Executing`/`Verifying` effects whose
  lease expired (`ListQuery::expired_leases`, paged 100 at a time). It takes
  each one under its own lease, re-checks the status, applies `LeaseExpired`
  (→ `Unknown`, actor `recovery:<worker>`) and releases. Effects that another
  worker grabs first are reported as `skipped`. Then it **resumes every
  unsettled effect nobody holds whose name has a registered handler**
  (see [Durable handlers](#durable-handlers)), from its stored input:
  - It skips `NeedsIntervention`, and retries scheduled for later.
  - It reports `resumed` with the status each effect reached.
  - It lists closure effects as `unhandled`: only a caller can re-run those.
  - Inputs that no longer deserialize go into `resume_errors`.

  It is safe to run on several workers at once; every change happens under
  a lease.
- **`runtime.run_recovery(interval)`** runs `recover` on a timer, forever.
  Spawn it. A failed pass is logged and retried at the next tick.
- **`runtime.pending(after, limit)`** lists unsettled effects that nobody
  holds: `Pending`, expired `Executing`/`Verifying`, `Unknown` and
  `NeedsIntervention`. That is the startup list of what to re-run or hand to
  an operator.
- **`runtime.resolve(id, resolution, actor, note)`** records an operator's
  decision on an `Unknown` or `NeedsIntervention` effect, as a lease-less
  transition (refused while anyone holds the effect):
  - `Resolution::Applied { output }` → `Committed`; later calls replay
    `output`.
  - `NotApplied` → `Failed`, with the note as the error.
  - `Retry` → `Pending`; the next call runs it, even with the retry budget
    spent, because the operator decided.

  The note is stored in the audit event's payload.

The M5 exit test, a stalled-worker takeover, is in `tests/recovery.rs`, in
three variants. In each, worker *a* stalls past its lease, recovery marks the
effect unknown, and worker *b* takes over:

| *a*'s request | *b*'s effect has | Result |
|---|---|---|
| applied before the stall | verification | *b* confirms, never re-runs; created once |
| still in flight, lands after *b* re-runs | remote idempotency | both send, the remote applies once |
| unknown | neither | *b* escalates; an operator resolves; *b*'s action never runs |

In every case *a*'s late write is fenced off and its call reports the
outcome *b* recorded. Mutation checks confirm the first two depend on
verification and on the stable idempotency key respectively.

### Compensation

Implemented in `compensation.rs`. Undoing a committed effect is a durable
lifecycle of its own:

```text
Committed ─StartCompensation→ Compensating ─CompensationSucceeded→ Compensated
                                  │  ↺ ScheduleCompensationRetry, StartCompensationRetry
                                  └─CompensationFailed→ CompensationFailed ─ResolvedCompensated→ Compensated
                                                                     └─ResolvedRetry→ Compensating
```

```rust
// Closure effects: the compensation receives the effect's stored output.
runtime.compensation("inventory.reserve", &order.id)
    .reason("order cancelled")
    .run(|ctx, reservation: Option<Reservation>| async move {
        inventory.release(&reservation, ctx.idempotency_key()).await.map_err(classify)
    })
    .await?;                                   // -> CompensationOutcome

// Handlers: implement CompensableEffect, register with .compensable().
runtime.compensate::<ReserveInventory>(&order.id).reason("order cancelled").await?;
```

- **Compensations must be idempotent.** An attempt that crashed or failed
  ambiguously is simply run again, so compensation has no `Unknown` state.
  `CompensationContext::idempotency_key()` is stable across attempts, and
  distinct from the effect's own key, so a remote never mistakes the undo
  for a replay.
- **Recorded before it runs.** `StartCompensation` is persisted before the
  first attempt. A retry is a `ScheduleCompensationRetry` (sets
  `next_attempt_at`, records the error in `last_error`), then a
  `StartCompensationRetry` when it begins (counts it in
  `compensation_attempts`, clears `next_attempt_at`). The reason goes in
  the event payload. So a resumed compensation can tell a retry that was
  scheduled but never started (`next_attempt_at` set, due or not: it waits,
  then starts it) from an attempt that a crash cut short (`next_attempt_at`
  clear: it starts another, marked `resumed`), and counts each attempt
  once.
- **Retries.** Transient and ambiguous failures retry with backoff within
  the retry budget. A permanent failure, or a spent budget, ends
  `CompensationFailed`. An operator then calls
  `resolve(id, Resolution::Retry | Resolution::Compensated, …)`.
- **Refusals.** An effect that never committed returns
  `CompensationOutcome::NotCommitted { status }`. A `Failed` effect has
  nothing to undo; an `Unknown` one must be resolved first. Calling again
  after `Compensated` sends nothing.
- **What callers see.** Running the effect again after compensation returns
  `EffectOutcome::Compensated { id }`. During a failed compensation it
  returns `NeedsIntervention { id }`.
- **Crashes.** A crash mid-compensation leaves `Compensating`. Recovery
  resumes it for a compensable handler; a closure compensation is reported
  `unhandled` until its caller runs it again.

Tests: `tests/compensation.rs` covers success, refusals, retries with an
ambiguous cancel deduplicated by the key, permanent failure with operator
retry, a manual undo, handler compensation, and an interrupted compensation
finished by recovery or by a caller. The model test also mixes compensation
and crashes mid-compensation into its random histories. Its invariants: a
cancelled resource implies compensation started, and `Compensated` implies
the resource is gone.

### Approval

Implemented in `approval.rs`. An effect that requires approval
(`.require_approval()`, or `EffectHandler::requires_approval`) goes
`Pending → AwaitingApproval` before its first attempt, after its
precondition passes:

- **Providers.** The runtime's `ApprovalProvider` (`RuntimeBuilder::approval_provider`)
  is asked on its own task, under the lease. It answers `Approved { by }`,
  `Denied { by, reason }`, or `Deferred`. A deferred effect stays
  `AwaitingApproval`: callers get `EffectOutcome::AwaitingApproval`, and
  each later call asks again. The request carries the effect id, so a
  provider can deduplicate. `CliApproval` prompts on a terminal; end of
  input defers.
- **Operators.** `runtime.approve(id, actor, note)` and
  `runtime.deny(id, actor, reason)` decide without a provider, and are
  refused while someone holds the effect. Approval makes the effect
  `Pending`: a caller's next call runs it, and so does recovery for a
  registered handler. Recovery itself never decides an approval.
- **Asked once.** `Approve` sets the record's `approved`, so retries and
  restarts never ask again.
- **Re-checked after approval.** The precondition runs again after
  approval: a decision that sat in a queue may have gone stale.
- **Durable.** The record is durable, so a pending approval survives
  restarts (crash point `AfterApprovalRequested`). The approver and the
  denier are the audit events' actors.

Tests: `tests/approval.rs` covers approval, denial, operator decisions with
no provider, deferral, asking only once across retries, a stale decision
caught by the re-checked precondition, a crash while awaiting approval, and
the CLI provider. The model test gives random cases an approval requirement
and a scripted provider, and adds operator approve/deny steps. It checks
that every attempt follows an approval and that a denied effect never ran;
skipping approval fails it.

### Risk policy

Implemented in `policy.rs`. Every effect has a `RiskLevel` (`Low` by
default, `.risk(..)` or `EffectHandler::risk`). The runtime's `RiskPolicy`
(`RuntimeBuilder::risk_policy`), built with spec §25's `PolicyBuilder`
chain, adds requirements by risk level, effect kind, or both:

```rust
PolicyBuilder::new()
    .for_risk(RiskLevel::Low).auto_execute()
    .for_risk(RiskLevel::Medium).require_verification()
    .for_risk(RiskLevel::High).require_approval()
    .for_risk(RiskLevel::Critical).require_approval().disable_automatic_retry()
    .for_kind(EffectKind::IrreversibleWrite).require_verification()
    .build()
```

**Precedence: requirements only accumulate.** An effect gets its own
settings plus the requirements of *every* rule that matches it, so the
strictest one always wins. No rule, and no rule order, can loosen another
rule or the effect's own settings. `auto_execute()` adds nothing; it
documents intent and cannot lift a requirement. Tests check that reversing
the rules changes nothing and that adding a rule never loosens.

| Requirement | Effect |
|---|---|
| `require_approval` | The effect waits in `AwaitingApproval` before its first attempt ([Approval](#approval)), and the request shows the risk. |
| `require_verification` | An effect without verification is refused with `RuntimeError::PolicyViolation` before anything is recorded. |
| `disable_automatic_retry` | The runtime never runs the effect again on its own. A transient failure ends `Failed` (or escalates if an earlier attempt may have applied). An unknown outcome is verified if it can be, else escalated, even for idempotent effects. A trusted "not applied" ends `Failed`. Verification checks and compensation retries are unaffected; an operator's `Resolution::Retry` still runs it. |

The policy is applied in `execute`, so closure effects, `submit` and
recovery of registered handlers all get it. The tracing span carries
`effect.risk_level`.

Tests: `tests/policy.rs` covers the requirements, refusal before recording,
an operator retry under no-retry, permissive rules not loosening, and a
handler's risk driving approval and retry. The model test randomly makes
effects `Critical` under a no-retry policy and checks that such an effect is
attempted at most once, through any mix of failures, crashes and recovery.

### Durable handlers

Implemented in `handler.rs`. Closure effects can only be finished by a
caller. A handler is registered under its effect name, and its input is
stored in full with the record, so recovery can rebuild the call and finish
the effect with nobody calling.

```rust
struct ChargeCustomer { stripe: Stripe }      // credentials live here, not in the input

impl EffectHandler for ChargeCustomer {
    const NAME: &'static str = "payment.charge";
    type Input = Charge;                       // stored; must stay deserializable
    type Output = Payment;
    type Error = EffectFailure;                // any Into<EffectFailure>
    fn kind(&self) -> EffectKind { EffectKind::IrreversibleWrite }
    fn remote_idempotency(&self) -> bool { true }
    async fn execute(&self, ctx: &EffectContext, charge: &Charge) -> Result<Payment, EffectFailure> { … }
    // optional: retry_policy, attempt_timeout, precondition
}

impl VerifiableEffect for ChargeCustomer {     // optional capability, as in spec §9
    async fn verify(&self, ctx: &EffectContext, charge: &Charge) -> Result<Verification<Payment>, EffectFailure> { … }
}

let runtime = Runtime::builder(store)
    .register(Handler::new(ChargeCustomer { stripe }).verifiable())
    .build();
let outcome = runtime.submit::<ChargeCustomer>(&order.id, charge).actor("agent:billing").await?;
```

- **Separate capability traits.** Capabilities are separate traits
  (`VerifiableEffect`; compensation is next). The `Handler` builder only
  offers `.verifiable()` for handlers that implement it, so an effect never
  claims a capability it lacks.
- **Same machinery as closures.** Handlers run through the closure-effect
  machinery: identity, fingerprints, retries, verification, leases and every
  crash guarantee are shared, not re-implemented.
- **Registration.** One handler per name; registering twice panics at
  startup. `submit` for a type that isn't registered under its name returns
  `RuntimeError::NotRegistered`.
- **Resuming.** A resumed effect reuses the record's stored fingerprint
  rather than recomputing it: it is the same effect by definition, and a
  future change to canonicalization must not block old records.

The exit test is in `tests/handlers.rs`. The submitting process crashes at
each fault point; no caller ever returns; a fresh runtime with the handler
registered runs `recover()`:

- verified and remote-idempotent effects end `Committed`, created exactly
  once;
- unprotected effects end `NeedsIntervention` wherever the outcome is in
  doubt, and are never re-run.

Mutation checks:

- Skipping the resume step breaks five tests.
- Blind re-running breaks the unprotected test.

### Retention

Implemented in `retention.rs` (N9). Records are kept forever by default. A
`RetentionPolicy` on the builder sets, per settled status, how long after
its last transition a record is kept:

```rust
Runtime::builder(store)
    .retention(RetentionPolicy::settled(30 * DAY).failed(7 * DAY))
    .build();
```

`runtime.prune()` deletes what the policy allows, in batches of 500 until
none is left, and returns a `PruneReport` (counts per status).
`run_recovery` calls it after every recovery pass.

- **Only settled records.** `EffectStatus::is_settled`: `Committed`,
  `Failed`, `Rejected`, `Compensated`. Everything else is work in progress
  or waits for a person (`Unknown`, `NeedsIntervention`,
  `AwaitingApproval`, `CompensationFailed`, …) and is kept however old.
- **Never under a live lease.** A committed effect whose compensation has
  just taken the lease is skipped.
- **Age is time since `updated_at`,** the last transition, so it measures
  how long the record has been settled.
- **The audit trail goes with the record.** Applications that must keep it
  longer export it first (an `EffectObserver`, or `events()`).
- **Pruning forgets.** The key is free: the next call with it inserts a
  new record and runs the effect again, and a pruned committed effect can
  no longer be compensated. Committed records should be kept at least as
  long as any caller might retry with the same key. This is the same
  contract a remote system's idempotency keys have, and why there is no
  default policy.

Tests (`tests/retention.rs`): every settled status pruned at its own age
and nothing unsettled even after 10,000 hours; the default keeping
everything; a pruned key running again (the documented trade-off); 1,201
records pruned in one pass across batches; and `run_recovery` pruning.
Mutation checks: pruning one batch per pass, dropping the builder's policy,
and a recovery loop that never prunes each fail a test.

## 8. Store contract

Lives in `agent-effects-store` (see [§13](#13-crate-layout)).

```rust
pub trait EffectStore: Send + Sync + 'static {
    fn insert_or_get(&self, new: NewEffect) -> impl Future<Output = Result<InsertOutcome, StoreError>> + Send;
    fn get(&self, id: EffectId) -> impl Future<Output = Result<Option<EffectRecord>, StoreError>> + Send;
    fn get_by_key(&self, key: &EffectKey) -> impl Future<Output = Result<Option<EffectRecord>, StoreError>> + Send;
    fn acquire_lease(&self, id: EffectId, owner: &WorkerId, now: SystemTime, ttl: Duration) -> impl Future<Output = Result<Lease, StoreError>> + Send;
    fn renew_lease(&self, lease: &Lease, now: SystemTime, ttl: Duration) -> impl Future<Output = Result<Lease, StoreError>> + Send;
    fn release_lease(&self, lease: &Lease) -> impl Future<Output = Result<(), StoreError>> + Send;
    fn transition(&self, request: TransitionRequest) -> impl Future<Output = Result<EffectRecord, StoreError>> + Send;
    fn list(&self, query: ListQuery) -> impl Future<Output = Result<Vec<EffectRecord>, StoreError>> + Send;
    fn events(&self, id: EffectId) -> impl Future<Output = Result<Vec<EffectEvent>, StoreError>> + Send;
    fn prune(&self, query: PruneQuery) -> impl Future<Output = Result<u64, StoreError>> + Send;
}
```

**The rules are pure functions on `EffectRecord`, not store code.** These
are `acquire_lease`, `renew_lease`, `release_lease` and `apply`. Each checks
a change against the record and applies it in memory. A store only has to run
"load, apply, persist the record and the returned audit event" atomically (a
lock, or a `BEGIN IMMEDIATE` / `SELECT … FOR UPDATE` transaction). Every
backend therefore enforces identical semantics:

- `insert_or_get` is atomic on the unique `(name, key)`. Of any number of
  racing callers, exactly one sees `inserted: true`. The existing record comes
  back untouched.
- A lease is exclusive while it is live, **even against its own owner**: two
  tasks of one worker must not run the same effect. Every acquisition
  increments `lease_epoch`, the fencing token. A lease is accepted only while
  the record still carries its owner and epoch and it has not expired. A
  worker whose lease lapsed or was taken over is rejected with `LeaseLost` on
  every write. Renewal is strict: an expired lease cannot be revived.
  Releasing a stale lease is a no-op, so it cannot free a successor's lease.
- `transition` checks, in order, the lease (or, for lease-less operator
  calls, that nobody holds a live one), compare-and-set on `version`, and the
  [transition table](#3-state-machine). On success it updates the
  bookkeeping (attempt count, `attempt_started_at`, `attempt_ended_at`,
  `next_attempt_at`, `compensation_attempts`, `committed_at`, output, last
  error), bumps `version`, and appends an audit
  event whose `sequence` equals the new version. On failure nothing changes.
- Lease operations do not bump `version`. A lease heartbeat therefore never
  conflicts with the transition that follows it, and the epoch alone fences
  lease holders.
- `list(ListQuery)` filters by status, by "no live lease at time T", and by
  an id cursor, ordered by id (UUIDv7, so creation order).
  `ListQuery::expired_leases(now)` is the recovery scan.
- `prune(PruneQuery)` deletes up to `limit` records, lowest id first, in
  one settled status, last changed at least `older_than` before `now` and
  with no live lease at `now` (`PruneQuery::matches`), with their audit
  events, atomically per record. A query for an unsettled status deletes
  nothing. The age is a duration, not a cutoff, so a store with its own
  clock applies it to that clock.
- `now` is passed in from the runtime's `Clock`. Stores must keep at least
  millisecond precision. `PostgresStore` uses the database's clock instead
  by default, removing cross-host clock skew ([§9b](#9b-storage-postgresql-via-sqlx)).

Every backend must pass `agent_effects_store::testkit::conformance` (the
`testkit` feature). It runs 17 cases: read-back of all fields, key
idempotency, 16-way concurrent inserts, missing records, lease exclusivity,
takeover fencing, strict renewal, release, a full transition history with its
events, rejected transitions leaving no trace, lease-less operator
resolution, terminal finality, listing/paging, and persisting
`may_have_applied` while refusing `FailedDefinitively` under it, and the
compensation lifecycle (attempt counter, retry schedule, operator retry), and
approval (`approved` persisted; denial rejects), and pruning (only settled,
idle, old records, lowest id first within the limit, events removed, the
key freed for a new record). Its sensitivity was checked
by breaking `MemoryStore` on purpose: ignoring the unique key, or persisting
the event without the record. The suite caught both.

## 9. Storage: SQLite via sqlx

`agent-effects-sqlite`, built in M6. It depends on `agent-effects-store`
only.

- **Connection settings** (`SqliteStore::open`):
  - WAL journal, so readers don't block the writer.
  - `synchronous = FULL`: the runtime persists an attempt *before* calling
    the remote system. Under `NORMAL`, a power cut could lose that write and
    leave a `Pending` record for an effect that may have run, which the next
    call would run again.
  - `busy_timeout` 5 s, foreign keys on, up to 8 pooled connections.
- **Writes.** Every write is "load → pure `EffectRecord` operation → save"
  inside a `BEGIN IMMEDIATE` transaction (`Pool::begin_with`). That takes the
  write lock up front. With a deferred `BEGIN`, two processes can both read
  and then fail to upgrade their lock. The multi-process test catches exactly
  that, and the in-process conformance suite does not. The `UPDATE` also
  checks the version it read, as a second guard.
- **Inserts** are `INSERT … ON CONFLICT (effect_name, logical_key) DO
  NOTHING`, then a read of the winner.
- **Queries and migrations.** Queries are checked at runtime, so no
  `DATABASE_URL` is needed at build time. Migrations are embedded
  (`sqlx::migrate!`, `migrations/0001_effects.sql`) and run on open. The
  sqlx migrations table records the schema version.
- **Encoding.** Times are Unix milliseconds; returned records are normalized
  to that precision so they equal what a later read returns. Ids are
  hyphenated UUID text, whose text order equals byte order, so `ORDER BY id`
  is creation order. JSON columns are stored as text.

Schema as built:

```sql
CREATE TABLE effects (
    id                 TEXT    PRIMARY KEY NOT NULL,
    effect_name        TEXT    NOT NULL,
    logical_key        TEXT    NOT NULL,
    kind               TEXT    NOT NULL,
    status             TEXT    NOT NULL,
    input              TEXT,             -- JSON
    input_fingerprint  TEXT,
    output             TEXT,             -- JSON
    last_error         TEXT,             -- JSON ErrorRecord
    created_by         TEXT,
    attempt_count      INTEGER NOT NULL,
    may_have_applied   INTEGER NOT NULL,  -- 0 or 1
    compensation_attempts INTEGER NOT NULL,
    approved           INTEGER NOT NULL,  -- 0 or 1
    next_attempt_at    INTEGER,
    attempt_started_at INTEGER,
    attempt_ended_at   INTEGER,
    lease_owner        TEXT,
    lease_epoch        INTEGER NOT NULL,
    lease_expires_at   INTEGER,
    version            INTEGER NOT NULL,
    created_at         INTEGER NOT NULL,
    updated_at         INTEGER NOT NULL,
    committed_at       INTEGER,
    UNIQUE (effect_name, logical_key)
);
CREATE INDEX effects_status_lease ON effects (status, lease_expires_at);
CREATE INDEX effects_status_updated ON effects (status, updated_at);  -- retention

CREATE TABLE effect_events (
    effect_id   TEXT    NOT NULL REFERENCES effects (id),
    sequence    INTEGER NOT NULL,
    transition  TEXT    NOT NULL,     -- Transition::as_str, e.g. effect.attempt_started
    from_status TEXT    NOT NULL,
    to_status   TEXT    NOT NULL,
    attempt     INTEGER NOT NULL,
    actor       TEXT,
    payload     TEXT,                 -- JSON
    at          INTEGER NOT NULL,
    PRIMARY KEY (effect_id, sequence)
);
```

Tests (`crates/agent-effects-sqlite/tests/`):

- **Conformance.** The full suite, each case on a fresh database file.
- **Reopening.** A committed effect and its audit trail survive closing and
  reopening the file.
- **Multiple processes.** The test binary re-runs itself as 3 worker
  processes on one database file. Each runs the same 150 effects in the same
  order, and every action appends a line to a shared log. Every effect ran
  exactly once, more than one process did real work, and every record is
  `Committed` after one attempt.

Mutation checks:

- A deferred `BEGIN` breaks the multi-process test.
- Dropping `ON CONFLICT` breaks conformance and reopening.
- Dropping the lease filter from `list` breaks conformance.
- Pruning selects and deletes under the same `BEGIN IMMEDIATE`. Dropping
  its lease filter, or leaving audit events behind (the foreign key
  refuses), breaks conformance.

## 9b. Storage: PostgreSQL via sqlx

`agent-effects-postgres`, built in N5 for services with many workers on many
hosts. It depends on `agent-effects-store` only, and runs the same pure
`EffectRecord` operations as every store.

- **The database's clock (default, `ClockSource::Database`).** Lease
  liveness, expiry and takeover, transition timestamps, and the "no live
  lease" filter of scans all use Postgres's `clock_timestamp()`, not the
  calling worker's clock. Workers with skewed clocks still agree on who
  holds a lease. This removes the clock-skew limit leases have on other
  stores. Retry schedules (`next_attempt_at`) are still the worker's.
  `ClockSource::Caller` restores caller time, which the conformance suite
  needs to drive time.
- **Writes.** Every write loads the row `SELECT … FOR UPDATE` (with
  `clock_timestamp()` in the same statement), applies the operation, and
  saves it in one transaction. The `UPDATE` also checks the version it read.
- **Scans.** Recovery and pending scans (queries with a lease filter) end
  `FOR UPDATE SKIP LOCKED`, so rows another worker is changing right now are
  skipped, not waited on.
- **Pruning.** One statement: a `FOR UPDATE SKIP LOCKED` selection, then
  deletes of its events and rows. With the database clock, both the age and
  lease liveness are measured against `clock_timestamp()`.
- **Schema and migrations.** Real types (`UUID`, `JSONB`, `TIMESTAMPTZ`,
  `BOOLEAN`), with times kept at millisecond precision like SQLite. Its own
  migrations (`crates/agent-effects-postgres/migrations`) run under sqlx's
  advisory lock, so concurrent startups are safe.
- **Tests.** They run against a real Postgres (`AGENT_EFFECTS_POSTGRES_URL`)
  and skip without one. CI's `postgres` job runs them against a service
  container with `AGENT_EFFECTS_REQUIRE_POSTGRES=1`, so a missing database
  fails rather than skips. Each test works in its own schema:
  - the conformance suite;
  - the database-clock test: workers an hour behind and ahead still see
    one lease;
  - a lock-skipping scan test;
  - pruning by the database's clock (a caller a day ahead prunes nothing
    just settled) that skips locked rows;
  - the runtime replaying across pools;
  - three worker processes running 150 effects with no duplicates.

  Mutation checks: the worker-clock-only, no-row-lock and no-`SKIP LOCKED`
  mutations (on scans and on pruning) each fail the matching test.

## 10. Sensitive data

Implemented in `redaction.rs` (N6). The runtime persists four kinds of
value, and each can leak:

| Value | Kept for | Read back by |
|---|---|---|
| input | identity (fingerprint), audit | handler recovery |
| output | audit | later callers (replay) |
| audit payloads | audit | operators |
| error messages | audit | callers and operators (they often echo tokens back) |

- **`Secret<T>` is the default protection.** It serializes as
  `"[REDACTED]"`, so a secret field in an input or output never reaches the
  store. Its `Debug` and `Display` print `[REDACTED]`. The action holds the
  real value in memory. Reading a `Secret` back from storage yields a
  redacted one (`expose()` → `None`, `is_redacted()`), for any stored shape.
- **A `Redactor` on the runtime** (`RuntimeBuilder::redactor`) rewrites all
  four kinds of value before anything is written. It receives the field
  (`Input`, `Output`, `AuditPayload`, `ErrorMessage`) and the effect name.
  Any `Fn(Field, &EffectName, &mut Value)` qualifies. `RedactKeys` masks
  named object fields at any depth, case-insensitively.
- **Where it applies.** Inputs are redacted in `execute`. Outputs, payloads
  and errors are redacted at the one place every transition passes through
  (`transition_leased`), and in recovery's marking, `resolve` and
  `approve`/`deny`. Approvers are shown the stored, redacted input.
- **Identity.** The fingerprint is taken after redaction, so secrets are not
  part of an effect's identity: a rotated token is the same effect, and a
  secret is never stored, not even hashed. A resumed effect brings its
  stored input and fingerprint, flagged as already stored, and is not
  redacted or fingerprinted again.
- **Limits.** What is redacted cannot be replayed or resumed: the caller
  that ran the effect gets the real output, a replay gets the stored one, and
  a resumed handler gets the redacted input. Credentials therefore belong in
  the closure's captured state or in the handler, never in the input.
- **Retention.** Records, including redacted ones, are kept until a
  [retention policy](#retention) prunes them with their audit trail.
  Pruning a key frees it, so a later call with that key starts a new
  effect, as remote idempotency keys expire too.

Tests (`tests/redaction.rs`):

- a secret input reaching the action but stored redacted;
- a rotated secret counting as the same effect, while another field still
  does not;
- a secret output reaching its caller but replaying redacted;
- `RedactKeys` on inputs, action outputs, `Resolution::Applied` outputs and
  operator notes;
- an error message echoing a key, redacted;
- the approver seeing only redacted input;
- a redacted field never hashed into the identity.

Each test scans everything stored about the effect (record and audit trail)
for the secret. Mutation checks, each failing a test: fingerprinting before
redaction, skipping redaction on transitions, and skipping it on operator
decisions.

## 11. Crash points

Every point has a tested recovery path (M7). `FaultPoint` names the points.
`FaultInjector` (feature `fault-injection`) stops the runtime at one of them:
`crash()` panics inside the runtime's task, and `abort()` kills the process.
Either way nothing after the point runs: no writes, no lease release, no
heartbeat. An action already spawned keeps running, like a request on the
wire.

| Crash point (`FaultPoint`) | Record left in | Recovery | verified / idempotent | unprotected |
|---|---|---|---|---|
| `BeforeInsert` | nothing | the next call starts fresh | created once | created once |
| `AfterInsert` | `Pending`, no lease | the next call runs it; nothing was sent | created once | created once |
| `AfterApprovalRequested` | `AwaitingApproval` | waits for a decision, durably; the next call asks the provider again; an operator can `approve`/`deny`; once approved, a call or (for a handler) recovery runs it | created once, after approval | created once, after approval |
| `AfterAttemptPersisted` (before the request is sent) | `Executing` | lease expiry → `recover()` → `Unknown` → unknown plan; the runtime cannot prove nothing was sent | created once | operator, created 0 times |
| `AfterActionStarted` (request in flight) | `Executing` | same | created once | operator, created ≤ 1 times |
| `AfterActionReturned` (after the remote commit, before persisting the result; this also covers "before the response") | `Executing` | same | created once | operator, created once |
| `AfterVerificationStarted` | `Verifying` | lease expiry → `Unknown` → verify again | created once | not on this path |
| `AfterCompensationStarted` (during compensation) | `Compensating` | lease expiry → the next call, or `recover()` for a compensable handler, runs the attempt again (compensations are idempotent) | undone once | undone once |

"verified" means irreversible with an authoritative lookup. "idempotent"
means irreversible with a remote that deduplicates on the key.
"unprotected" means neither. "operator" means the effect ends
`NeedsIntervention`, and a re-run never runs the action again.

Three suites cover it:

- **`tests/crash.rs`** (in-process). One test per point, each across all
  three protections. The runtime crashes; paused time runs out the lease;
  `recover()` runs; a fresh runtime over the same `MemoryStore` re-runs the
  effect. The test asserts that every point is reached on its path, so it
  cannot pass because nothing crashed.
- **`agent-effects-sqlite/tests/crash_subprocess.rs`** (real process death).
  The same matrix, with a child process that `abort()`s at the point on a
  SQLite file and a remote whose state is a file. The parent asserts the
  child died exactly when the point was reachable, then waits out the lease,
  recovers and re-runs. For a kill while the request is in flight, it allows
  that the request may not have left yet. Three more modes (N10):
  - **Durable handler**, the same matrix with nobody calling again: the
    parent only runs `recover()`, which must leave the effect `Committed`
    (created once) or, for an unprotected effect in doubt,
    `NeedsIntervention` (created at most once).
  - **Killed after asking for approval**: the request survives, recovery
    leaves it alone, an operator approves, and recovery runs it once.
  - **Killed mid-compensation**: recovery finishes the undo, exactly once.
- **`tests/model.rs`** (model-based, 512 cases per run). Proptest generates
  random effect configurations (closure or durable handler, kind,
  verification, remote idempotency, approval, a no-retry risk policy),
  `FakeRemote` scripts, and sequences of calls, crashes at every point
  including after an approval request, lease expiries, recovery passes,
  compensations (with crashes), operator approvals and denials, truthful
  operator resolutions, and pruning. A prune that removes the record starts
  a new generation against a fresh remote. Before each prune and after the
  last step:
  - the audit trail must replay through the transition table from `Pending`
    with contiguous sequence numbers, ending at the record's status and
    version;
  - an effect that is not naturally idempotent must have been created at
    most once;
  - `Committed` implies created, and `Failed` (for such effects) implies not
    created;
  - no automatic retry under a no-retry policy, no attempt before approval,
    nothing created after a denial, nothing cancelled before compensation
    started, `Compensated` implies the resource is gone;
  - observers saw exactly the audit trail, crashes or not (a transition is
    stored and observed in one step; crashes strike between steps);
  - a `Secret` in the input appears nowhere in the record or trail.

  Every prune must remove exactly the records that are settled and under no
  live lease. The model spells "settled" out rather than calling
  `is_settled`, so it does not share the code's definition.

Mutation checks:

- Disabling the checkpoints fails every crash test.
- Blind re-running of unknown outcomes fails exactly the three in-doubt
  points, in both crash suites, and the model test (shrunk to a duplicate).
- Recording ambiguous failures as `Failed` fails the model test.
- N10: a `Secret` that serializes its value, recovery marking `Unknown`
  without notifying observers, and an unobserved transition each fail the
  model test; recovery that never resumes, or leaves interrupted
  compensations alone, fails the subprocess suite; treating
  `NeedsIntervention` as settled fails conformance. A lease-blind prune
  survives the model (it never builds a committed record under a live
  lease) and fails conformance.

## 12. Observability

**Traces.** `tracing` spans `agent_effect.execute` (fields `effect.id`,
`effect.name`, `effect.kind`, `effect.risk_level`, `effect.status`,
`effect.logical_key`, `effect.attempt`) and `agent_effect.compensate`.
They export through `tracing-opentelemetry` like any other spans.

**Metrics (N7).** An `EffectObserver` (`RuntimeBuilder::observer`, any
number of them) receives two calls:

- `on_created(record)` when a new effect is recorded;
- `on_transition(observation)` after each transition is stored.

Every transition is written through one function, `commit_transition`
(redact, store, notify), so calls, recovery, compensation, approval and
operator decisions are all observed. An `Observation` carries the record
after the transition, the transition with its `from` and `to` status,
`in_previous_status` (an attempt's duration when leaving `Executing`, time
spent unknown when leaving `Unknown`) and `since_created`. Observers run
synchronously; one that panics is caught and logged, because the
transition is already stored. Levels (like effects awaiting approval) are
deltas since the observer started; `pending()` gives exact numbers.

`agent-effects-otel` implements the observer with OpenTelemetry metrics
(`OtelObserver::global()` or `::new(&meter)`). Its instruments are
`agent_effects.started`, `.completed`, `.failed`, `.rejected`, `.unknown`,
`.needs_intervention`, `.retry.count`, `.compensation.{started,completed,failed}`,
the `.pending_approval` up/down counter, and histograms in seconds:
`.duration`, `.attempt.duration` (tagged `outcome`) and `.unknown.duration`.
Attributes are `effect.name` and `effect.kind`, never the logical key, which
is unbounded and may identify customers. The most important signal is
`agent_effects.unknown`: a spike means remote systems are answering
ambiguously.

Tests:

- `tests/observer.rs`: exact sequences and durations on paused time, with
  replays observing nothing new; recovery, operator decisions and time spent
  unknown; approval and compensation; and a panicking observer that breaks
  nothing while other observers still see everything.
- `agent-effects-otel/tests/metrics.rs`: every instrument's value read back
  through the OpenTelemetry SDK's in-memory exporter.

Mutation checks: uncontained observer panics, and recovery bypassing
`commit_transition`, each fail a test.

## 12b. HTTP effects

`agent-effects-http` turns a `reqwest` request into an effect's action.
`HttpEffect::post(&client, url).json(&body)` builds it; `.send_json::<T>()`
or `.send()` makes the action, `.verify_json::<T>()` a lookup for
`.verify(...)`. `.idempotency_key_header()` sends `ctx.idempotency_key()`
as `Idempotency-Key` (`.idempotency_header(name)` for providers that call
it something else), the same value on every attempt, worker and restart.
The crate enables no reqwest features; TLS comes from the application's own
`reqwest` dependency and the `Client` it passes in.

Classification follows one rule: a failure is definite only when the
request provably never reached the server, or the server's answer says it
did not apply.

A connection failure proves that only if the client follows no redirects.
reqwest follows them inside `send()` by default, and a POST that applied and
answered `303` to an unreachable host fails with the same `is_connect()`
error, and the original URL, as a POST that never got through. The adapter
cannot inspect the caller's `Client`, so the caller declares it:
`.client_follows_no_redirects()` on a client built with
`redirect::Policy::none()`.

| Outcome | Class |
|---|---|
| connect, DNS or TLS failure | Ambiguous; Transient (`request_sent(false)`) only with `.client_follows_no_redirects()` |
| invalid URL or header | Validation, nothing sent |
| timeout or connection lost after sending | Ambiguous |
| 2xx with an unreadable or unparseable body | Ambiguous: it applied, the answer was lost |
| 3xx (only seen when the client does not follow redirects) | Ambiguous |
| 408, 425, 503 | Transient |
| 429, or 503 with `Retry-After` | RateLimited, honouring `Retry-After` (seconds or HTTP date) |
| 400, 422 | Validation |
| 401 / 403 | Authentication / Authorization |
| 409, 500, 502, 504, other 5xx | Ambiguous |
| 501, other 4xx | Permanent |

409 is ambiguous because idempotency-key providers answer it while a
request with the same key is still in flight. 500, 502 and 504 may come
from a gateway after the origin applied the request. `.classify_status(f)`
overrides the table per request for a provider that documents otherwise.
Error messages keep at most 512 bytes of the response body, and pass
through the runtime's `Redactor` like any other.

`verify_json` returns `NotApplied` only for 404 and 410. Any other failure
of the lookup (a 405, a 500, a timeout) is an error, which the runtime
treats as inconclusive: a lookup that is broken must never be read as
"the effect did not happen", since that re-runs it.

Tests (`agent-effects-http/tests/http.rs`) run against a small local
HTTP/1.1 server that applies POSTs, deduplicates them by
`Idempotency-Key`, and can drop the connection after applying or hang:

- a success sends the JSON body, headers and the effect's key;
- every status in the table reaches the runtime as its class;
- a refused connection is Transient for a client declared without
  redirects and Ambiguous otherwise; a POST that applied and redirected to
  an unreachable host is not re-sent; an unfollowed 303 is Ambiguous; a
  timeout after sending is Ambiguous;
- a dropped answer is re-sent under the same key and applied once;
- a lookup confirms a dropped answer without re-sending; a 404 lookup lets
  a lost request run again; a 405 lookup leaves the effect `Unknown`;
- a 2xx with an unparseable body is Ambiguous; an invalid header sends
  nothing.

Mutation checks: treating connect errors as sent, treating them as unsent
for a client that may follow redirects, reading a 3xx as permanent,
dropping the idempotency header, ignoring the timeout, and reading a 405 lookup or never reading a
404 lookup as not applied each fail a test.

## 13. Crate layout

```text
agent-effects/
├── crates/
│   ├── agent-effects            runtime: effect, runtime, state*, retry, policy,
│   │                            verification, compensation, retention, clock
│   ├── agent-effects-store      the contract: ids, kinds, failure classes, state
│   │                            machine, records, leases, EffectStore, testkit
│   ├── agent-effects-memory     MemoryStore
│   ├── agent-effects-sqlite     SqliteStore (sqlx)
│   ├── agent-effects-postgres   PostgresStore (sqlx), database-clock leases
│   ├── agent-effects-http       HttpEffect: reqwest actions, classification, Idempotency-Key
│   ├── agent-effects-otel       OtelObserver: OpenTelemetry metrics
│   └── agent-effects-mcp        (planned)
├── examples/                    unpublished workspace member: payment, agent_tool
├── (tests)                      per crate today: crates/*/tests (crash, model, multi-process, …)
└── docs/
```

Dependencies point inward. Store backends depend on `agent-effects-store`
only, never on the runtime. The runtime depends on the store crate. Adapters
(HTTP, OTel, MCP) depend on the runtime.

\* The state machine lives in `agent-effects-store`, because stores enforce
it and the runtime depends on the store crate. `agent-effects` re-exports it
as `agent_effects::state`, together with `id`, `kind` and `failure`, so users
only ever import `agent-effects`.

Crates are added when their milestone starts, not as empty placeholders.

## Decisions log

| # | Date | Decision | Why |
|---|---|---|---|
| D1 | 2026-10-05 | v0.1 ships the **closure API**; recovery of closure effects means re-attaching on the next call with the same key. A durable handler registry comes in v0.2 | Lowest adoption cost; recovery limits are explicit, not hidden |
| D2 | 2026-10-05 | **SQLite via sqlx** for the first durable store | One migration story shared with the v0.2 Postgres store |
| D3 | 2026-10-05 | Revised state set (§3): no `Executed`/`Approved` states; added `Pending`, `Rejected`, `NeedsIntervention`; compensation starts from Committed | The draft compensated Failed effects and had states missing from its enum |
| D4 | 2026-10-05 | `execute` returns `EffectOutcome<T>`; `Err` is infrastructure only | Unknown must not be swallowed by `?` |
| D5 | 2026-10-05 | Lease **epochs** fence every write | Stops a stalled worker and its replacement from both acting |
| D6 | 2026-10-05 | `VerificationMode` with a settle delay for eventually consistent lookups | "Not found" from a lagging index is not proof of not-applied |
| D7 | 2026-10-05 | Remote idempotency key = UUIDv5 of `(name, key)`, frozen | Stable across retries, workers and record loss |
| D8 | 2026-10-05 | Input fingerprint; same key with a different input is an error | A re-planning agent must not receive another request's result |
| D9 | 2026-10-05 | Store trait uses `-> impl Future + Send`, and the runtime is generic over the store; no `async_trait` | No boxing on the hot path; no `dyn` store needed |
| D10 | 2026-10-05 | MSRV 1.90, edition 2024 | `uuid` 1.27 requires 1.89 |
| D11 | 2026-10-05 | ~~Store trait and memory store inside the core crate~~ **Superseded the same day:** follow the spec's crate layout (§13): `agent-effects-store` holds the contract, `agent-effects-memory` the in-memory store | Backends depend only on a small, stable contract crate, never on the runtime |
| D12 | 2026-10-05 | ~~CI deferred until the repo is published~~ **Done 2026-10-06** (D27) | User decision |
| D13 | 2026-10-05 | Store rules are pure `EffectRecord` methods; stores only provide atomicity. Lease operations don't bump `version` | Identical semantics across backends; heartbeats can't conflict with transitions |
| D14 | 2026-10-05 | The builder defers identity and input errors to `run`; the effect key accepts any `Display` | One `?` per effect, and `.effect("x", order_id)` works for integer and UUID ids |
| D15 | 2026-10-05 | The whole call runs on a spawned task, and the action on a nested one | Cancellation safety for the whole write path; panics become ambiguous failures instead of crashing the call |
| D16 | 2026-10-05 | Waits between attempts are inline, holding the lease; `ScheduleRetry` is persisted before the wait | Answers the "Scheduled outcome?" question: simple for callers, and a crash mid-wait leaves a safely resumable `Pending` record |
| D17 | 2026-10-05 | Preconditions run only before the first attempt | A possibly-applied attempt can falsify its own precondition; re-checking would reject effects that happened |
| D18 | 2026-10-05 | Verification also runs after every success (postcondition); inconclusive checks leave the effect `Unknown`, not escalated | "200 OK" is not proof; an unreachable lookup is a reason to look again later, not to page an operator |
| D19 | 2026-10-05 | `max_attempts` is a lifetime budget per effect; it also caps checks per call | Restarts and re-attaching calls cannot reset the budget; every loop is bounded |
| D20 | 2026-10-05 | `TokioClock`, plus a `testkit` feature with `FakeRemote` | Paused-time tests run real backoff schedules instantly; a scripted provider makes duplicates countable |
| D21 | 2026-10-05 | ~~Recovery only marks expired attempts `Unknown`; it never verifies, re-runs or escalates~~ **Superseded by D29** for registered handlers | Without durable closures it has nothing safe to run; honest state plus `pending()` lets callers and operators act |
| D22 | 2026-10-05 | `Resolution::Retry` grants an attempt beyond the budget | It is an explicit human decision; refusing it would force a workaround |
| D23 | 2026-10-05 | SQLite runs with `synchronous = FULL` and `BEGIN IMMEDIATE` | Intent must be durable before the remote call; immediate locking avoids lock-upgrade failures between processes |
| D24 | 2026-10-05 | `agent-effects-sqlite` has its own MSRV, 1.94 (sqlx 0.9); the other crates stay at 1.90 | Users without SQLite are not forced onto a newer compiler |
| D25 | 2026-10-05 | Simulated crashes: a panic in the runtime's task in-process, `process::abort()` in subprocesses | Both stop at the exact point with no cleanup, the way a real crash does; no special shutdown path in the runtime to keep honest |
| D26 | 2026-10-05 | `FakeRemote` answers a replayed idempotency key with the original result even when scripted to fail | Real deduplicating providers check the key before evaluating; otherwise the model test reports duplicates that cannot happen |
| D27 | 2026-10-06 | GitHub Actions: `ci.yml` (format, clippy, tests on Linux + macOS, MSRV 1.90/1.94, docs, publish dry run, one `ci-pass` check) and tag-driven `release.yml` (preflight → CI at the tag → publish crate by crate, skipping versions already on crates.io → GitHub release from the CHANGELOG) | Mirrors the TalaDB release flow; a failed release can be re-run safely |
| D28 | 2026-10-06 | `EffectRecord.may_have_applied`: set on entering `Unknown`, cleared only by a trusted "not applied" (verification or operator); `FailedDefinitively` is refused while set (except `Read`). The runtime turns such a failure into `Unknown`, then verifies or escalates | Found by the model test in CI: a failed retry after an ambiguous attempt was recorded `Failed` although the earlier attempt applied. Enforced in the store, so no runtime path can make `Failed` lie |
| D29 | 2026-10-06 | Durable handlers per spec §9: `EffectHandler` + separate `VerifiableEffect`; the `Handler` builder gates capabilities by trait bounds. `submit::<H>` runs one; `recover()` resumes registered effects from stored input, reusing the stored fingerprint | Durability without a caller, with no second execution path: handlers ride the closure machinery and its tested guarantees |
| D30 | 2026-10-06 | The roadmap's v0.2 items ship in 0.1.0 | User decision: nothing was published yet, so the schema can still change in place |
| D31 | 2026-10-06 | Metrics through an `EffectObserver` trait in core; `agent-effects-otel` implements it | User decision: core stays dependency-free; any metrics backend can plug in |
| D32 | 2026-10-06 | Compensation: `Committed → Compensating → Compensated / CompensationFailed`, idempotent attempts retried with no `Unknown` state, a separate compensation idempotency key, `CompensableEffect` + closure API, recovery resumes compensable handlers; `Committed` is no longer terminal | Spec §16: compensation is a durable operation with attempts, timestamps, errors and an idempotency id, never "try once, ignore the error" |
| D33 | 2026-10-06 | Approval: `Pending → AwaitingApproval` after the precondition, before the first attempt; `ApprovalProvider` (`Approved`/`Denied`/`Deferred`) asked per call; operator `approve`/`deny`; `approved` persisted so it is asked once; precondition re-checked after approval; recovery never decides | Spec §24: approval survives restarts; a decision that took hours must not act on stale state |
| D34 | 2026-10-07 | Risk policy (spec §25): `RiskLevel` per effect; `RiskPolicy` rules select by risk, kind or both; requirements are the union of all matching rules plus the effect's own (monotonic, order-free); unmet `require_verification` refuses before recording; `disable_automatic_retry` gates every self-initiated re-run but not operator retries | "Defined precedence" that cannot surprise: no combination of rules can make an effect less guarded than any single rule says |
| D35 | 2026-10-07 | `agent-effects-postgres`: database clock by default (`ClockSource`), `FOR UPDATE` writes, `FOR UPDATE SKIP LOCKED` scans, MSRV 1.94; sqlx driver features moved into each store crate | Cross-host clock skew is the one lease weakness a shared database can remove; scans should never queue behind live work |
| D36 | 2026-10-07 | Redaction: `Secret<T>` (serializes as `"[REDACTED]"`, reads back redacted) plus a runtime `Redactor` over inputs, outputs, audit payloads and error messages; fingerprint after redaction; resumed inputs flagged as already stored | Spec §27: the audit log never stores secrets by default. A secret hashed into a fingerprint is still a stored secret |
| D37 | 2026-10-07 | Every transition goes through `commit_transition` (redact, store, notify); `EffectObserver` with `on_created` / `on_transition` and durations derived from `updated_at`; observer panics contained; `agent-effects-otel` instruments named per spec §29, never keyed by logical key | One choke point means no path can skip redaction or metrics; derived durations need no extra columns |
| D38 | 2026-10-07 | `agent-effects-http`: `HttpEffect` builder over a caller-supplied `reqwest::Client` with no reqwest features; definite failures only when provably not sent or answered as not applied (409/500/502/504 ambiguous); `verify_json` reads only 404/410 as not applied | Misclassifying in the definite direction duplicates effects; misclassifying toward ambiguous only costs a verification or an operator |
| D39 | 2026-10-07 | Retention: `RetentionPolicy` per settled status (none by default); `EffectStore::prune(PruneQuery)` with the age as a duration, never under a live lease, audit trail deleted with the record; `prune()` loops batches of 500; `run_recovery` prunes | Pruning forgets keys, so it must be opted into; a duration lets Postgres age by its own clock; unsettled records are work or wait for a person and are never pruned |
| D40 | 2026-10-07 | Settle delays count from `attempt_ended_at` (first transition out of `Executing`), measured in the store's clock | A slow request can write just before it returns; counting from its start trusted "not found" too early and duplicated it |
| D41 | 2026-10-07 | The runtime never judges lease liveness with its own clock; it calls `acquire_lease` and treats `LeaseHeld` as `InProgress` | A worker behind `PostgresStore`'s database clock reported `InProgress` for expired leases, delaying takeover |
| D42 | 2026-10-07 | `StartCompensationRetry` counts a compensation retry when it starts, not when it is scheduled | A retry that fell due while its worker was down was counted twice and spent the budget early |
| D43 | 2026-10-07 | `agent-effects-http`: connection failures are ambiguous unless the effect declares `.client_follows_no_redirects()`; unfollowed 3xx are ambiguous | A redirect's unreachable target fails exactly like the original host after the original request applied |

## Open questions

- Cached outputs are deserialized into the caller's `T`. If `T` changes shape
  between releases, old records stop deserializing. Should there be an output
  version tag, or a documented "don't do that"?
- A fenced worker whose action *succeeded* cannot record that, so the
  knowledge is lost and an operator resolves the effect blind. Should a fenced
  worker append a "late result" audit event, which leaves the status alone
  but gives the operator the evidence?
