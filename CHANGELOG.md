# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project
adheres to [Semantic Versioning](https://semver.org/). Before 1.0, minor
versions may break the API and the storage format; the release notes say
when they do.

## [Unreleased]

## [0.1.0]

First release.

### `agent-effects`

- **Durable handlers.** `EffectHandler` (with `VerifiableEffect` as a
  separate capability) is registered via `RuntimeBuilder::register` and run
  with `runtime.submit::<H>(key, input)`. Inputs are stored, so `recover()`
  finishes registered effects after a crash with no caller.
- **Compensation.** `Committed → Compensating → Compensated /
  CompensationFailed`, durable and retried. Use `runtime.compensation(name,
  key).run(...)` for closure effects and `runtime.compensate::<H>(key)` for a
  `CompensableEffect`. Each compensation gets its own idempotency key;
  recovery resumes interrupted compensations; operators can retry or record
  a manual undo.
- **Closure API.** `runtime.effect(name, key)…run(action)` returns an
  `EffectOutcome`: `Committed`, `Failed`, `Rejected`, `Unknown`,
  `NeedsIntervention` or `InProgress`. `RuntimeError` is reserved for
  infrastructure problems.
- **One effect per key.**
  - Calls with the same name and key attach to one record and replay
    committed results.
  - Inputs are fingerprinted (SHA-256 of canonical JSON). Reusing a key with a
    different input or kind is an error.
  - Remote idempotency keys are derived deterministically from name and key.
- **Failure classification.** `EffectFailure` has `transient`, `permanent`,
  `ambiguous`, `rate_limited`, `authentication`, `authorization` and
  `validation`. `request_sent(false)` downgrades an ambiguous failure.
- **Retries.**
  - The attempt budget covers the effect's whole life, across calls and
    restarts.
  - Exponential backoff with jitter; `retry_after` is honoured.
  - Retries are persisted before waiting, so a crash mid-wait leaves a
    resumable record.
- **Per-attempt timeouts.** A timeout is an ambiguous failure.
- **Preconditions** run before the first attempt only.
- **Verification** runs as a postcondition after success and to reconcile
  unknown outcomes. `.verify` is for authoritative lookups;
  `.verify_eventually(settle, …)` for lagging ones.
- **Unknown outcomes** are verified, re-run only when provably safe, or
  escalated.
- **"Failed" never covers a possible application.** Once an attempt may have
  applied the effect, later definitive failures leave it `Unknown` or
  escalate it (`EffectRecord::may_have_applied`, enforced by the store).
- **Cancellation and panics.** Execution runs on spawned tasks: dropping the
  caller does not abort an attempt, and a panicking action counts as an
  ambiguous failure.
- **Leases.** Renewed by a heartbeat, with epoch fencing: a stalled worker's
  late writes are refused.
- **Recovery and operator API.** `recover()` and `run_recovery(interval)`
  mark abandoned attempts `Unknown`; `pending()` lists unsettled effects;
  `resolve()` records an operator's decision with an audit note;
  `wait(id, timeout)` waits for a concurrent caller.
- **Tracing.** `tracing` spans named `agent_effect.execute`.
- **Clocks.** `SystemClock`, `ManualClock`, and `TokioClock` for paused-time
  tests.
- **`testkit` feature.** `FakeRemote`, a scripted provider that can commit
  and then drop the connection, lose requests, hang, lag lookups and honour
  idempotency keys.
- **`fault-injection` feature.** `FaultInjector` crashes in-process or aborts
  the process at named `FaultPoint`s.

### `agent-effects-store`

- `EffectStore` trait, `EffectRecord` with the rules as pure operations
  (lease fencing, version checks, transition table, bookkeeping), audit
  events and `ListQuery`.
- The state machine `EffectStatus` × `Transition`, checked exhaustively and
  by property tests.
- The `testkit` feature's backend conformance suite.

### `agent-effects-memory`

- `MemoryStore`, for tests and development.

### `agent-effects-sqlite`

- `SqliteStore` on sqlx 0.9: embedded migrations, WAL,
  `synchronous = FULL`, and `BEGIN IMMEDIATE` around every change. Several
  processes may share one database file.

### Testing

- Store conformance on both backends.
- A crash suite covering every crash point, in-process and with real
  process death on SQLite.
- A model-based property test of the runtime against the transition table.
- A multi-process test on one SQLite file.
- Mutation checks for each safety rule.

[Unreleased]: https://github.com/akosidencio/agent-effects/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/akosidencio/agent-effects/releases/tag/v0.1.0
