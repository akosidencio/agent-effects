# agent-effects

**The reliability layer between agents and the real world.**

`agent-effects` is a Rust runtime for executing real-world side effects
(payments, emails, cloud resources, tickets) initiated by AI agents and other
autonomous software. It records intent before acting, tells "failed" apart
from "don't know", and resolves unknown outcomes by verification or provably
safe retry instead of guessing.

> Let agents decide. Make effects deterministic.

## The problem

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

## Quick start

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
    .effect("payment.charge", &order.id)        // one record per (name, key), ever
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

Pick the protection your remote system allows:

| The remote… | Builder | After an unknown outcome |
|---|---|---|
| deduplicates on an idempotency key | `.remote_idempotency(true)` and forward `ctx.idempotency_key()` | re-sent; applied once |
| can be queried | `.verify(...)` or `.verify_eventually(settle, ...)` | checked; re-run only if verifiably not applied |
| neither | | escalated to an operator (`runtime.resolve`) |

Also available:

- `.retry(policy)`: lifetime attempt budget, backoff with jitter, honours
  `retry_after`.
- `.attempt_timeout(d)`.
- `.precondition(...)`: rejects a stale decision before the first attempt.
- `runtime.wait(id, timeout)`.
- Recovery: `runtime.run_recovery(interval)`, `runtime.pending(..)` and
  `runtime.resolve(..)`.

## Examples

```sh
cargo run --example payment
cargo run --example agent_tool
```

- [`payment`](examples/payment.rs): a provider that
  charges and then drops the connection, shown under each protection, with
  the audit trail.
- [`agent_tool`](examples/agent_tool.rs): a
  `refund_order` tool for an LLM agent, handling duplicate calls, a changed
  amount and a stale decision.

## Guarantees

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

## Crates

| Crate | Purpose |
|---|---|
| `agent-effects` | The runtime; the crate applications depend on. Features: `testkit` (`FakeRemote`), `fault-injection` (`FaultInjector`). |
| `agent-effects-sqlite` | SQLite store; several processes may share one file. |
| `agent-effects-memory` | In-memory store for tests and development. |
| `agent-effects-store` | Storage contract and state machine, for writing new backends; includes the backend conformance suite (`testkit`). |

MSRV: Rust 1.90; `agent-effects-sqlite` needs 1.94 (sqlx).

## Documentation

- [Crash semantics](docs/crash-semantics.md): the contract when things fail,
  and how to run it in production
- [Design](docs/design.md): state machine, failure model, store contract,
  decisions log
- [Roadmap](docs/roadmap.md): v0.2 adds a durable handler registry,
  compensation, approval, Postgres, HTTP and OpenTelemetry
- [Changelog](CHANGELOG.md)

## Development

```sh
cargo test --workspace --all-features
cargo clippy --workspace --all-targets --all-features
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --all-features
cargo +1.90 test --workspace --exclude agent-effects-sqlite --all-features   # MSRV
cargo +1.94 test -p agent-effects-sqlite                                      # its MSRV (sqlx)
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
