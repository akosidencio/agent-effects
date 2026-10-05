# agent-effects

**The reliability layer between agents and the real world.**

`agent-effects` is a Rust runtime for executing real-world side effects
(payments, emails, cloud resources, tickets) initiated by AI agents and other
autonomous software. It records intent before acting, tells "failed" apart
from "don't know", and resolves unknown outcomes by verification or provably
safe retry instead of guessing.

> **Status: pre-release.** On an in-memory store, the runtime runs effects
> with retries, timeouts, preconditions and verification. It records
> outcomes, replays them, re-attaches after crashes, and gives operators
> `recover`, `pending` and `resolve`. The SQLite store is in progress. See
> [docs/roadmap.md](docs/roadmap.md).

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

## Guarantees

- Intent is persisted **before** the external call.
- One logical effect (`name` + application key) maps to one record, however
  many times agents, workers or restarts ask for it.
- An effect is only marked failed when it definitely did not apply.
- A retry that could duplicate an effect is never made automatically.

`agent-effects` does **not** provide exactly-once side effects. Nothing can
without cooperation from the target system. Targets that honour an
idempotency key get the strongest guarantee, and the runtime derives a stable
key for every effect.

It is not a workflow engine, a job queue or an agent framework, and it does
not decide whether an agent is *allowed* to act. Application authorization
still applies.

## Documentation

- [Design](docs/design.md): state machine, failure model, store contract,
  crash semantics, decisions log
- [Roadmap](docs/roadmap.md)

## Development

```sh
cargo test --workspace --all-features
cargo clippy --workspace --all-targets --all-features
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --all-features
cargo +1.90 test --workspace --all-features   # MSRV
```

| Crate | Purpose |
|---|---|
| `agent-effects` | the runtime; the crate applications depend on |
| `agent-effects-store` | storage contract, state machine, backend conformance suite |
| `agent-effects-memory` | in-memory store for tests and development |

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option.
