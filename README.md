# agent-effects: Reliable side effects for AI agents in Rust

<div align="center">

[![Version: 0.1.1](https://img.shields.io/badge/Version-v0.1.1-blue)](CHANGELOG.md)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/License-MIT_OR_Apache--2.0-blue.svg)](#license)
[![Rust: 2024 Edition](https://img.shields.io/badge/Rust-2024_Edition-orange?logo=rust)](Cargo.toml)
[![MSRV: Rust 1.90](https://img.shields.io/badge/MSRV-1.90%2B-orange?logo=rust)](#rust-crates-and-storage-backends)
<br/>
[![CI](https://github.com/akosidencio/agent-effects/actions/workflows/ci.yml/badge.svg)](https://github.com/akosidencio/agent-effects/actions/workflows/ci.yml)
[![Async: Tokio](https://img.shields.io/badge/Async-Tokio-purple)](crates/agent-effects/Cargo.toml)
[![Stores: Memory, SQLite, PostgreSQL](https://img.shields.io/badge/Stores-Memory%20%7C%20SQLite%20%7C%20PostgreSQL-green)](#rust-crates-and-storage-backends)
[![SQL stores: Rust 1.94](https://img.shields.io/badge/SQL_stores-Rust_1.94%2B-orange?logo=rust)](#rust-crates-and-storage-backends)

</div>

`agent-effects` is a Rust library for reliable AI agent tool calls and
side-effect execution. It combines idempotency keys, durable execution,
safe retries and crash recovery for payments, emails, cloud resources and
tickets initiated by LLM agents or other autonomous applications.

The runtime records intent before acting, distinguishes definite failures
from unknown outcomes, and uses remote verification or idempotent retry to
resolve uncertainty. Store effects in SQLite or PostgreSQL, and track their
lifecycle with tracing and OpenTelemetry metrics.

[Quick start](#quick-start) · [Features](#features) ·
[Examples](#examples) · [Documentation](#documentation)

## Why agent tool calls need idempotency

```text
agent decides to charge a customer
  → request reaches the payment provider → customer is charged
  → connection drops before the response
  → caller sees a timeout
```

Retrying charges the customer twice; giving up loses a payment that happened.
The honest answer is "unknown", and the runtime treats it that way:

```text
Unknown ─┬─ verification available ─→ ask the provider what happened
         ├─ idempotent / idempotency key ─→ safe to run again
         └─ neither ─→ needs an operator
```

## When to use agent-effects

- **AI agent and LLM tools:** attach repeated tool calls to the same effect
  and replay committed results.
- **Payments, emails and resource provisioning:** handle timeouts without
  blindly repeating an operation that may already have applied.
- **Rust services that need crash recovery:** resume registered handlers
  from durable storage after a worker restarts.
- **Operations that need human oversight:** require approval, retain an
  audit trail and compensate committed effects.

## Quick start

Add the runtime and a SQLite store to your Rust application:

```toml
[dependencies]
agent-effects = "0.1"
agent-effects-sqlite = "0.1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

```rust
use agent_effects::{EffectFailure, EffectKind, EffectOutcome, Runtime};
use agent_effects_sqlite::SqliteStore;

let runtime = Runtime::new(SqliteStore::open("effects.db").await?);

let outcome = runtime
    .effect("payment.charge", &order.id)        // one record per (name, key) while retained
    .kind(EffectKind::IrreversibleWrite)
    .input(&charge)                             // a reused key with another input is an error
    .remote_idempotency(true)                   // the provider deduplicates on our key
    .run(move |ctx| {
        let (provider, charge) = (provider.clone(), charge.clone());
        async move {
            provider
                .charge(&charge, ctx.idempotency_key()) // same key on every attempt
                .await
                .map_err(classify)                      // -> EffectFailure::transient / ambiguous / ...
        }
    })
    .await?;                                    // Err only for infrastructure problems

match outcome {
    EffectOutcome::Committed(payment) => { /* done; calling again replays it */ }
    EffectOutcome::Failed(error) => { /* definitely did not happen */ }
    EffectOutcome::Unknown { id } | EffectOutcome::NeedsIntervention { id } => {
        /* may have happened: do not retry blindly */
    }
    _ => {}
}
```

## Idempotency and outcome verification

Pick the protection your remote system allows:

| The remote… | Builder | After an unknown outcome |
|---|---|---|
| deduplicates on an idempotency key | `.remote_idempotency(true)` and forward `ctx.idempotency_key()` | re-sent; applied once |
| can be queried | `.verify(...)` or `.verify_eventually(settle, ...)` | checked; re-run only if verifiably not applied |
| neither | | escalated to an operator (`runtime.resolve`) |

## Features

- **Durable handlers.** Implement `EffectHandler`, `register` it, and
  `runtime.submit::<H>(key, input)`. Recovery then finishes the effect from
  its stored input even if the caller never comes back.
- **Metrics.** `.observer(OtelObserver::global())` exports the lifecycle as
  OpenTelemetry metrics; implement `EffectObserver` for anything else.
- **Redaction.** `Secret<T>` fields are stored as `"[REDACTED]"`, and a
  `Redactor` such as `RedactKeys` scrubs inputs, outputs, audit notes and
  error messages before anything is written.
- **Risk policy.** `.risk(RiskLevel::High)` plus a runtime `RiskPolicy`
  can require approval, verification, or no automatic retries. Rules only
  ever add requirements.
- **Approval.** `.require_approval()` waits durably for a human:
  an `ApprovalProvider` (a CLI prompt is included) or `runtime.approve` /
  `runtime.deny`.
- **Compensation.** `runtime.compensation(name, key).run(...)` or
  `runtime.compensate::<H>(key)` undoes a committed effect durably, with
  retries and its own idempotency key.
- **HTTP.** `agent-effects-http` turns a `reqwest` request into an action
  that sends `Idempotency-Key` and classifies every failure (a timeout after
  sending is ambiguous, not failed).
- **Retention.** `.retention(RetentionPolicy::settled(age))` prunes settled
  records once old enough; anything unresolved is kept. A pruned key is
  new again.
- **Retries and timeouts.** `.retry(policy)` sets a lifetime attempt budget
  and backoff with jitter, honouring `retry_after`; `.attempt_timeout(d)`
  bounds each action attempt.
- **Preconditions.** `.precondition(...)` rejects a stale decision before
  the first attempt.
- **Recovery and operator tools.** `runtime.run_recovery(interval)` resumes
  durable handlers; `runtime.pending(..)` lists unresolved effects and
  `runtime.resolve(..)` records an operator's decision. Use
  `runtime.wait(id, timeout)` to wait for another caller's effect.

## Examples

Run the payment reliability and LLM agent tool examples locally:

```sh
cargo run --example payment
cargo run --example agent_tool
```

- [Payment retries and verification](examples/payment.rs): a provider that
  charges and then drops the connection, shown under each protection, with
  the audit trail.
- [LLM agent refund tool](examples/agent_tool.rs): a
  `refund_order` tool for an LLM agent, handling duplicate calls, a changed
  amount and a stale decision.

## Reliability guarantees and limits

- Intent is persisted **before** the external call.
- One logical effect (`name` + application key) maps to one record, however
  many times agents, workers or restarts ask for it.
- An effect is only marked failed when it definitely did not apply.
- A retry that could duplicate an effect is never made automatically.
- Every crash point has a tested recovery path, in-process and with real
  process death. See [docs/crash-semantics.md](docs/crash-semantics.md),
  including the known limits.

`agent-effects` does **not** provide exactly-once side effects. Nothing can
without cooperation from the target system. Targets that honour an
idempotency key get the strongest guarantee, and the runtime derives a stable
key for every effect.

It is not a workflow engine, a job queue or an agent framework, and it does
not decide whether an agent is *allowed* to act. Application authorization
still applies.

## Rust crates and storage backends

| Crate | Purpose |
|---|---|
| [`agent-effects`](crates/agent-effects) | The runtime; the crate applications depend on. Features: `testkit` (`FakeRemote`), `fault-injection` (`FaultInjector`). |
| [`agent-effects-sqlite`](crates/agent-effects-sqlite) | SQLite store; several processes may share one file. |
| [`agent-effects-http`](crates/agent-effects-http) | HTTP requests as effects (reqwest): failure classification, `Idempotency-Key`. |
| [`agent-effects-otel`](crates/agent-effects-otel) | OpenTelemetry metrics through an `EffectObserver`. |
| [`agent-effects-postgres`](crates/agent-effects-postgres) | PostgreSQL store for many workers on many hosts; leases use the database's clock. |
| [`agent-effects-memory`](crates/agent-effects-memory) | In-memory store for tests and development. |
| [`agent-effects-store`](crates/agent-effects-store) | Storage contract and state machine, for writing new backends; includes the backend conformance suite (`testkit`). |

MSRV: Rust 1.90; `agent-effects-sqlite` and `agent-effects-postgres` need 1.94 (sqlx).

## Documentation

- [Crash recovery and production guidance](docs/crash-semantics.md): the contract when things fail,
  and how to run it in production
- [Runtime design and state machine](docs/design.md): failure model, store contract,
  decisions log
- [Integration roadmap](docs/roadmap.md): planned adapters and effect groups
- [Release changelog](CHANGELOG.md)

## Development

```sh
cargo test --workspace --all-features
cargo clippy --workspace --all-targets --all-features
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --all-features
cargo +1.90 test --workspace --exclude agent-effects-sqlite --exclude agent-effects-postgres --all-features   # MSRV
cargo +1.94 test -p agent-effects-sqlite -p agent-effects-postgres                                           # their MSRV (sqlx)

# PostgreSQL tests (they skip without a database):
podman run --rm -d -p 55432:5432 -e POSTGRES_PASSWORD=pw -e POSTGRES_DB=effects postgres:17
AGENT_EFFECTS_POSTGRES_URL=postgres://postgres:pw@localhost:55432/effects cargo test -p agent-effects-postgres
```

## Releasing

Bump `version` in `Cargo.toml`, move the CHANGELOG's `[Unreleased]` notes
under `## [x.y.z]`, merge to `main`, then push a tag:

```sh
git tag -a v0.1.0 -m "agent-effects 0.1.0" && git push origin v0.1.0
```

[`release.yml`](.github/workflows/release.yml) then:

1. checks that the tag, versions and changelog agree;
2. re-runs CI at the tag;
3. publishes each crate to crates.io (needs the `CARGO_REGISTRY_TOKEN`
   secret);
4. creates the GitHub release.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option.
