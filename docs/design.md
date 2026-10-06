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
  gets `InProgress { id }` immediately. `runtime.wait::<T>(id, timeout)` polls
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
    backoff, up to `max_attempts` checks per call. Then the effect stays
    `Unknown`, not escalated, and a later call or recovery checks again.

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
  worker grabs first are reported as `skipped`. It changes nothing else:
  `Pending`, `Unknown` and settled effects are left alone, because only a
  caller holding the closures can move them further. It is safe to run on
  several workers at once, and running it twice in a row is a no-op.
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
`testkit` feature). It runs 14 cases: read-back of all fields, key
idempotency, 16-way concurrent inserts, missing records, lease exclusivity,
takeover fencing, strict renewal, release, a full transition history with its
events, rejected transitions leaving no trace, lease-less operator
resolution, terminal finality, listing/paging, and persisting
`may_have_applied` while refusing `FailedDefinitively` under it. Its sensitivity was checked
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
    next_attempt_at    INTEGER,
    attempt_started_at INTEGER,
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
| `AfterAttemptPersisted` (before the request is sent) | `Executing` | lease expiry → `recover()` → `Unknown` → unknown plan; the runtime cannot prove nothing was sent | created once | operator, created 0 times |
| `AfterActionStarted` (request in flight) | `Executing` | same | created once | operator, created ≤ 1 times |
| `AfterActionReturned` (after the remote commit, before persisting the result; this also covers "before the response") | `Executing` | same | created once | operator, created once |
| `AfterVerificationStarted` | `Verifying` | lease expiry → `Unknown` → verify again | created once | not on this path |
| during compensation | — | v0.2 | | |

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
  that the request may not have left yet.
- **`tests/model.rs`** (model-based, 512 cases per run). Proptest generates
  random effect configurations, `FakeRemote` scripts, and sequences of
  calls, crashes at random points, lease expiries and recovery passes. After
  each case:
  - the audit trail must replay through the transition table from `Pending`
    with contiguous sequence numbers, ending at the record's status and
    version;
  - an effect that is not naturally idempotent must have been created at
    most once;
  - `Committed` implies created, and `Failed` (for such effects) implies not
    created.

Mutation checks:

- Disabling the checkpoints fails every crash test.
- Blind re-running of unknown outcomes fails exactly the three in-doubt
  points, in both crash suites, and the model test (shrunk to a duplicate).
- Recording ambiguous failures as `Failed` fails the model test.

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
│   ├── agent-effects-sqlite     SqliteStore (sqlx)
│   ├── agent-effects-postgres   (v0.2)
│   ├── agent-effects-http       (v0.2)
│   ├── agent-effects-otel       (v0.2)
│   └── agent-effects-mcp        (v0.3)
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
| D21 | 2026-10-05 | Recovery only marks expired attempts `Unknown`; it never verifies, re-runs or escalates | Without durable closures it has nothing safe to run; honest state plus `pending()` lets callers and operators act |
| D22 | 2026-10-05 | `Resolution::Retry` grants an attempt beyond the budget | It is an explicit human decision; refusing it would force a workaround |
| D23 | 2026-10-05 | SQLite runs with `synchronous = FULL` and `BEGIN IMMEDIATE` | Intent must be durable before the remote call; immediate locking avoids lock-upgrade failures between processes |
| D24 | 2026-10-05 | `agent-effects-sqlite` has its own MSRV, 1.94 (sqlx 0.9); the other crates stay at 1.90 | Users without SQLite are not forced onto a newer compiler |
| D25 | 2026-10-05 | Simulated crashes: a panic in the runtime's task in-process, `process::abort()` in subprocesses | Both stop at the exact point with no cleanup, the way a real crash does; no special shutdown path in the runtime to keep honest |
| D26 | 2026-10-05 | `FakeRemote` answers a replayed idempotency key with the original result even when scripted to fail | Real deduplicating providers check the key before evaluating; otherwise the model test reports duplicates that cannot happen |
| D27 | 2026-10-06 | GitHub Actions: `ci.yml` (format, clippy, tests on Linux + macOS, MSRV 1.90/1.94, docs, publish dry run, one `ci-pass` check) and tag-driven `release.yml` (preflight → CI at the tag → publish crate by crate, skipping versions already on crates.io → GitHub release from the CHANGELOG) | Mirrors the TalaDB release flow; a failed release can be re-run safely |
| D28 | 2026-10-06 | `EffectRecord.may_have_applied`: set on entering `Unknown`, cleared only by a trusted "not applied" (verification or operator); `FailedDefinitively` is refused while set (except `Read`). The runtime turns such a failure into `Unknown`, then verifies or escalates | Found by the model test in CI: a failed retry after an ambiguous attempt was recorded `Failed` although the earlier attempt applied. Enforced in the store, so no runtime path can make `Failed` lie |

## Open questions

- Cached outputs are deserialized into the caller's `T`. If `T` changes shape
  between releases, old records stop deserializing. Should there be an output
  version tag, or a documented "don't do that"?
- A fenced worker whose action *succeeded* cannot record that, so the
  knowledge is lost and an operator resolves the effect blind. Should a fenced
  worker append a "late result" audit event, which leaves the status alone
  but gives the operator the evidence?
