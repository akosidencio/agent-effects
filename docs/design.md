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

**Bold** = terminal. Every transition is also an audit event
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
- a lost lease leads to `Unknown`, never to `Failed`.

Compensation (`Committed → Compensating → Compensated | CompensationFailed`)
arrives in v0.2. It starts from **Committed**, because a Failed effect changed
nothing. The enum is `#[non_exhaustive]` for this reason.

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
  `settle` has passed since the attempt started. Before that it counts as
  inconclusive and the runtime verifies again later.

Known limit: a request that a stalled worker sent long ago can still land
after verification said "not applied". Settle delays shrink this window and
remote idempotency closes it. Nothing else can. This is documented, not hidden.

## 6. Retry policy

`RetryPolicy { max_attempts, initial_delay, max_delay, multiplier, jitter }`.
`max_attempts` counts the first attempt. The backoff is exponential, capped at
`max_delay`. With jitter, each delay is drawn from `[d/2, d]` ("equal
jitter", so there is always some wait). A rate limit's `retry_after` is a
floor and may exceed the cap. The arithmetic is pure and takes the random
sample as an argument.

## 7. API (v0.1: closures)

Built in M3, except the parts marked *(M4)*.

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
    .retry(RetryPolicy::default())                 // (M4)
    .precondition(|ctx| async move { /* ... */ })  // (M4)
    .verify(VerificationMode::EventuallyConsistent { settle: Duration::from_secs(10) },
            |ctx| async move { /* ... */ })        // (M4)
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
  gets `InProgress { id }` immediately. *(M4: `runtime.wait(id, timeout)`.)*
- **Closures must be `Fn`**, not `FnOnce`, because an effect may be attempted
  more than once. They are `Send + Sync + 'static` because they run on a
  spawned task.
- **M3 interim behaviour.** With no retry policy yet, a retryable failure
  (`Transient`, `RateLimited`) is recorded as `Failed`. An unknown outcome
  that the current call produced is reported as `Unknown`. A safely
  repeatable effect is re-run by the *next* call. M4 adds in-call retries
  with a budget.

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
| Unknown | apply the [unknown plan](#5-unknown-outcomes) with this call's closures: re-run, escalate, or *(M4)* verify |
| NeedsIntervention | `NeedsIntervention { id }` |
| any non-terminal status, lease live | `InProgress` |

Every row is covered by a test in `crates/agent-effects/tests/runtime.rs`,
including a worker that stalls past its lease. The new holder escalates, and
the stalled worker's late success is refused by fencing and does not
overwrite that decision.

`runtime.recover()` *(M5)* runs in the background. It moves expired
`Executing`/`Verifying` records to `Unknown` and returns a report of what
still needs a caller or an operator. `runtime.pending()` lists those records,
so an application can re-drive them at startup. `runtime.resolve(id,
Resolution::{Applied(output), NotApplied, Retry})` is the operator path.

A durable handler registry, which would let the recovery worker finish
effects with no caller, is planned for v0.2. The storage format already
carries everything it needs.

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
  bookkeeping (attempt count, `attempt_started_at`, `next_attempt_at`,
  `committed_at`, output, last error), bumps `version`, and appends an audit
  event whose `sequence` equals the new version. On failure nothing changes.
- Lease operations do not bump `version`. A lease heartbeat therefore never
  conflicts with the transition that follows it, and the epoch alone fences
  lease holders.
- `list(ListQuery)` filters by status, by "no live lease at time T", and by
  an id cursor, ordered by id (UUIDv7, so creation order).
  `ListQuery::expired_leases(now)` is the recovery scan.
- `now` is passed in from the runtime's `Clock`. Stores must keep at least
  millisecond precision. *(v0.2: Postgres uses database `now()` to remove
  cross-host clock skew.)*

Every backend must pass `agent_effects_store::testkit::conformance` (the
`testkit` feature). It runs 13 cases: read-back of all fields, key
idempotency, 16-way concurrent inserts, missing records, lease exclusivity,
takeover fencing, strict renewal, release, a full transition history with its
events, rejected transitions leaving no trace, lease-less operator
resolution, terminal finality, and listing/paging. Its sensitivity was checked
by breaking `MemoryStore` on purpose: ignoring the unique key, or persisting
the event without the record. The suite caught both.

## 9. Storage: SQLite via sqlx *(planned, M6)*

- `sqlx` with the `sqlite` feature and runtime-checked queries (no
  `DATABASE_URL` at build time). Migrations are embedded with
  `sqlx::migrate!`. The same tooling serves Postgres in v0.2.
- WAL mode, `busy_timeout`. Compare-and-set transitions run in
  `BEGIN IMMEDIATE` transactions.
- Times are stored as integer Unix milliseconds, ids as text, payloads as
  JSON text.

```sql
CREATE TABLE effects (
    id                TEXT PRIMARY KEY,
    effect_name       TEXT NOT NULL,
    logical_key       TEXT NOT NULL,
    kind              TEXT NOT NULL,
    status            TEXT NOT NULL,
    input             TEXT,             -- JSON, redacted per §10
    input_fingerprint TEXT,
    output            TEXT,             -- JSON
    last_error        TEXT,             -- JSON
    attempt_count     INTEGER NOT NULL DEFAULT 0,
    next_attempt_at   INTEGER,
    current_attempt_started_at INTEGER,
    lease_owner       TEXT,
    lease_epoch       INTEGER NOT NULL DEFAULT 0,
    lease_expires_at  INTEGER,
    version           INTEGER NOT NULL DEFAULT 0,
    created_at        INTEGER NOT NULL,
    updated_at        INTEGER NOT NULL,
    committed_at      INTEGER,
    UNIQUE (effect_name, logical_key)
);

CREATE TABLE effect_events (
    effect_id  TEXT NOT NULL REFERENCES effects(id),
    sequence   INTEGER NOT NULL,
    event      TEXT NOT NULL,
    attempt    INTEGER NOT NULL,
    actor      TEXT,
    payload    TEXT,
    created_at INTEGER NOT NULL,
    PRIMARY KEY (effect_id, sequence)
);

CREATE INDEX effects_recoverable ON effects (status, lease_expires_at);
```

The schema version is recorded by sqlx's migrations table and is part of the
stability contract from 1.0.

## 10. Sensitive data

Inputs are persisted. With closures, persisting is only for fingerprinting and
audit. With the planned registry, it is also for re-execution. Therefore:

- **Credentials belong in the closure or handler, never in the input.**
- Audit payloads and outputs go through a redaction hook (`Secret<T>`
  serializes as `"[REDACTED]"`). A derive macro may come later. v0.1 has no
  macros.
- Records are kept until a retention policy prunes them *(planned)*. Pruning
  a key frees it, so a later call with that key starts a new effect. This
  matches how remote idempotency keys expire.

## 11. Crash points

| Crash point | Record left in | Recovery |
|---|---|---|
| before the record is inserted | nothing | caller retries, starts fresh |
| after insert, before `StartAttempt` | Pending | next call or lease expiry resumes; nothing was sent |
| after `StartAttempt`, before the request is sent | Executing | lease expiry → Unknown → unknown plan (the runtime cannot prove nothing was sent) |
| while the request is in flight | Executing | same |
| after remote commit, before the response | Executing | same; verification or idempotency resolves it, else escalate |
| after the response, before persisting it | Executing | same |
| during verification | Verifying | lease expiry → Unknown → verify again |
| during compensation | — | v0.2 |

Each row becomes a fault-injection test (M7), run both in-process (drop the
runtime, rebuild it on the same store) and as a killed subprocess on SQLite.

## 12. Observability

`tracing` spans named `agent_effect.execute`, with fields `effect.id`,
`effect.name`, `effect.kind`, `effect.status`, `effect.logical_key`,
`effect.attempt`. Metrics and OpenTelemetry come in v0.2. The most
important signal is the count of effects in `Unknown`.

## 13. Crate layout

```text
agent-effects/
├── crates/
│   ├── agent-effects            runtime: effect, runtime, state*, retry, policy,
│   │                            verification, compensation (v0.2), clock
│   ├── agent-effects-store      the contract: ids, kinds, failure classes, state
│   │                            machine, records, leases, EffectStore, testkit
│   ├── agent-effects-memory     MemoryStore
│   ├── agent-effects-sqlite     (M6)
│   ├── agent-effects-postgres   (v0.2)
│   ├── agent-effects-http       (v0.2)
│   ├── agent-effects-otel       (v0.2)
│   └── agent-effects-mcp        (v0.3)
├── examples/                    (M8)
├── tests/                       (M7: crash/fault suite)
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
| D12 | 2026-10-05 | CI (GitHub Actions) deferred until the repo is published | User decision |
| D13 | 2026-10-05 | Store rules are pure `EffectRecord` methods; stores only provide atomicity. Lease operations don't bump `version` | Identical semantics across backends; heartbeats can't conflict with transitions |
| D14 | 2026-10-05 | The builder defers identity and input errors to `run`; the effect key accepts any `Display` | One `?` per effect, and `.effect("x", order_id)` works for integer and UUID ids |
| D15 | 2026-10-05 | The whole call runs on a spawned task, and the action on a nested one | Cancellation safety for the whole write path; panics become ambiguous failures instead of crashing the call |

## Open questions

- Retries with long delays (e.g. a 2-minute rate limit) currently wait inline
  while holding the lease. Should the call return a `Scheduled` outcome
  instead?
- Cached outputs are deserialized into the caller's `T`. If `T` changes shape
  between releases, old records stop deserializing. Should there be an output
  version tag, or a documented "don't do that"?
- A fenced worker whose action *succeeded* cannot record that, so the
  knowledge is lost and an operator resolves the effect blind. Should a fenced
  worker append a "late result" audit event, which leaves the status alone
  but gives the operator the evidence?
