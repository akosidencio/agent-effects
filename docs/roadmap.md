# Roadmap

Design reference: [design.md](design.md). Status as of 2026-10-05.

## v0.1: core runtime, memory and SQLite stores

| # | Milestone | Deliverables | Exit criteria | Status |
|---|---|---|---|---|
| M0 | Decisions + scaffolding | Design doc and decisions log, Cargo workspace (edition 2024, MSRV 1.90), MIT/Apache-2.0, lint config | Workspace builds; clippy pedantic and rustdoc clean | **done** |
| M1 | Core types | `EffectId`/`EffectKey`/`IdempotencyKey`, `EffectKind`, `EffectStatus` + pure transition table, `FailureClass`, `RetryPolicy`, unknown-outcome policy, `Clock` | Exhaustive and property tests on the transition table, backoff bounds and the retry-safety rule | **done** |
| M2 | Store contract + `MemoryStore` | `agent-effects-store` (trait, records, leases with epochs, audit events, `testkit` conformance suite) and `agent-effects-memory`, per the spec's crate layout (design §13) | MemoryStore passes the 13-case suite: insert races, stale version, stale epoch, expired-lease takeover, illegal transitions | **done** |
| M3 | Runtime happy path | `Runtime` builder, closure effect builder, `EffectOutcome`, input fingerprint, re-attach table (design §7), action on a spawned task, lease heartbeat, `tracing` spans, fault-hook points (no-ops) | Same key returns the cached result; a different input is rejected; dropping the caller's future doesn't abort the effect | **done** |
| M4 | Failure semantics | Classification → retry/fail/Unknown, per-attempt timeouts, preconditions, verification with settle delay, unknown plan, `InProgress` + `wait` | Testkit **fake remote service** (commits then drops the connection, eventually consistent lookup, honours idempotency keys); every `FailureClass` × `EffectKind` combination tested | **done** |
| M5 | Recovery + operator API | `recover()` (expired leases → Unknown, report), `pending()`, `resolve()` | Stalled-worker takeover scenario does not duplicate the effect | **done** |
| M6 | `agent-effects-sqlite` | sqlx + embedded migrations, WAL, `BEGIN IMMEDIATE` around the pure `EffectRecord` operations, schema per design §9; depends on `agent-effects-store` only | Passes the conformance suite; two processes sharing one database file | can start now |
| M7 | Crash + fault suite | `FaultInjector` behind a `fault-injection` feature; one test per crash point in design §11, in-process and as a killed subprocess; model-based property test of the runtime against the transition table | Every crash point has a tested recovery path | |
| M8 | Docs + release | README, `docs/crash-semantics.md`, examples (payment against the fake provider, agent tool), CHANGELOG, name reserved on crates.io | `cargo publish --dry-run` clean; 0.1.0 published | |

Dependency order: M2 → M3 → M4 → M5 → M7, with M6 in parallel after M2.
M8 last.

Deferred from v0.1 by decision: GitHub Actions CI (until the repo is
published), approval, compensation, Postgres, macros.

## v0.2: durability without a caller, and policy

- Durable **handler registry**, so the recovery worker can finish effects with
  no caller present (the trait API from the draft spec, §9).
- Compensation as a durable sub-lifecycle, starting from Committed.
- `ApprovalProvider` + CLI provider; `AwaitingApproval` survives restarts.
- Risk policy: `RiskLevel` × `EffectKind` with defined precedence.
- `agent-effects-postgres`: database-side `now()` for leases, and
  `FOR UPDATE SKIP LOCKED` for recovery scans.
- Redaction hook for audit payloads and outputs.
- Metrics; `agent-effects-otel`.
- `agent-effects-http` (reqwest): maps connect errors, timeouts and statuses
  to `FailureClass`; sends the `Idempotency-Key` header.
- Retention / pruning of settled records.

## v0.3: adapters

- `EffectGroup`: reverse-order compensation only, never a workflow language.
- rmcp tool wrapper (MCP tool annotations → `EffectKind`), Rig, a Tower
  layer, SQLx transactional effects.

## v1.0: stability

- Frozen state machine and storage format; migration tests from every
  released schema.
- Compatibility policy; crash semantics documented as part of the API
  contract.
- Long-running stress and fault fuzzing against Postgres.
