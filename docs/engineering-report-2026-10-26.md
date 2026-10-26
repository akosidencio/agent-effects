# Agent Effects engineering report — 2026-10-26

This branch implements the first correctness review slice and prepares an
**unpublished 0.2.0 release candidate**. It does not complete the full initiative.
Milestone One remains partial because its full new fault-injection matrix is
not implemented. Operational features and ecosystem integrations remain pending.

## Implemented scope

| Requested work | Status | Result |
|---|---|---|
| 2.1 HTTP classification | Implemented | Ambiguous write responses cannot grant retry permission; retry advice is separate and durable. |
| 2.2 Execution contracts | Implemented | Immutable assumptions are persisted and checked before reattachment/replay and durable recovery. |
| 2.3 Payload compatibility | Implemented | Explicit schema versions, optional JSON migrations, version-aware recovery/compensation, and committed replay errors. |
| 2.4 Late-result evidence | Implemented minimal version | Append-only metadata, bounded inspection, no sensitive result payload and no authoritative state changes. |
| 2.5 Test expansion | Partial | Added HTTP, contract, schema-upgrade, fencing, evidence and property regressions; existing crash/model/process suites retained. |
| Milestone Two | Pending | No bounded recovery, scheduler, CLI, namespace, secret resolver, tombstone, metric expansion, or benchmark implementation. |
| Security validation | Limited remediation | Two existing locked-dependency advisories addressed; CI hardening, fuzzing and a comprehensive security assessment remain pending. |
| Milestone Three | Not started | MCP, Tower, EffectGroup, SQLx outbox and new framework examples are not implemented. |

## Discovery and baseline

The starting tree was clean on `main` at `6a115da` (0.1.3). Local and remote
branches were inspected: `main`, `fix/0.1.1-review-fixes`,
`feat/0.1.2-postgres-pgbouncer`, and `feat/0.1.3-strict-pgbouncer`. No separate
integration branch was established. Work uses `fix/execution-safety-contracts`;
`main` has not been modified or merged.

The existing architecture already supplied durable intent, input fingerprinting,
lease epochs, database-clock PostgreSQL leases, strict transaction-pooler
support, verification, lifetime attempt accounting, approval, compensation,
redaction, retention, observers, property tests and real crash/process tests.
These were extended rather than replaced. Recovery remains sequential and
backoff waits still retain leases; pruning still forgets logical keys.

Baseline formatting, Clippy with warnings denied, and documentation with
warnings denied passed. The first sandboxed test run could not bind HTTP test
servers; the same baseline suite passed with loopback access. PostgreSQL and
PgBouncer tests internally skip without their URLs, so that initial run was
not counted as database validation. An isolated PostgreSQL 17 service was then
used for actual baseline conformance/multiprocess checks, and final validation
requires both database and strict PgBouncer services.

## Architecture and reliability changes

### HTTP and retry advice

HTTP 408, 425, 429 and 503 now join 500/502/504 as ambiguous write outcomes.
`Retry-After` is parsed as delay seconds or an HTTP date without claiming that
the operation was rejected. The runtime persists a minimum retry timestamp
on unknown outcomes and carries it into otherwise safe re-execution. Immediate
verification can still confirm an already-applied operation.

A response URL cannot exclude a redirect back to the original URL. Failure
responses and negative verification therefore remain ambiguous unless the
application declares a client configured without redirects. Trusted provider
classification overrides remain available on that declared path. Definite
pre-send builder/connection failures retain their special handling.

Local HTTP regressions cover applied writes followed by errors or connection
loss, provider deduplication, verification, trusted rejection overrides,
Retry-After, refused connections and redirect failures, including same-URL
redirects. They assert remote application/request counts rather than just
matching error enums.

### Execution contracts

`ExecutionContract` format 1 stores kind, remote idempotency, verification mode
and settle delay, application semantic version, optional adapter identity,
effective approval/automatic retry requirements, and lifetime attempt ceiling.
Retry delays, jitter and attempt timeouts may change; the safety fields and
attempt ceiling must match. Unknown format versions fail closed.

Contracts are inserted atomically with intent and preserved on conflict,
leases and transitions. Recovery validates before deserializing inputs.
Registered compensation also validates; even a replayable legacy commit
cannot acquire an unattested durable compensator. Compatible releases keep
the application semantic version, rather than tying it to a crate release.

Legacy NULL contracts are deliberately unknown. Unresolved records return
`LegacyContract`; settled results may replay without execution. There is no
automatic legacy-adoption API. Pending legacy work needs reviewed migration
or completion using the earlier application before rollout. This restriction
is intentional and requires an operational upgrade plan.

### Payload versions and committed results

Input and output schema versions are independent of execution semantics.
Legacy payloads default to version 1. `PayloadSchema` optionally transforms
older JSON in memory before typed deserialization; migration does not rewrite
historical input, schema or fingerprint. Recovery can consume compatible old
inputs, while a new submission under a different input schema is explicitly
refused for an existing key.

Output schema metadata commits with output. A fresh unserializable success
still returns `Committed(value)`, keeps committed state and records an audit
note. Replay reports `OutputUnavailable`; unsupported schema/migration and
malformed typed output report `OutputPayload` or `Output`, explicitly naming a
committed effect. None reexecutes the action or represents external failure.
Durable and closure compensation use version-aware output decoding.

### Late-result evidence

`LateObservation` records only observation/effect identifiers, worker,
original lease epoch, action attempt, attempt start, worker observation time,
and success/failure/interruption category. It retains no request/response
payload or error message. Append is idempotent by observation id; inspection
is ordered and paginated. Evidence is distinct from transition history.

When fencing prevents an authoritative result, or a returned result cannot
be recorded, the runtime attempts a supplementary append. An action outliving
heartbeat fencing is monitored for its eventual return. Evidence never changes
status, version, output, lease or audit sequence, and never automatically
commits an effect. Operators must corroborate it with remote evidence.

Evidence is best effort: process death, database failure, or pruning before
append can lose it. Observation time is worker-reported, not a lease clock.
Foreign-key cascades remove evidence atomically with settled-record pruning;
unresolved effects retain the existing protection from pruning.

## Public API and compatibility

New APIs include:

- `EffectFailure::{with_retry_after, retry_after}`.
- `ExecutionContract`, `VerificationContract`, `NewEffect::with_contract`.
- `EffectBuilder::{contract_version, adapter_identity, input_schema_version, output_schema}`.
- Defaulted `EffectHandler::{contract_version, adapter_identity, input_schema, output_schema}`.
- `PayloadSchema`, `PayloadError`, `Runtime::wait_with_output_schema`,
  `Runtime::resolve_with_output_schema`, `CompensationBuilder::output_schema`,
  and compensation-context schema accessors.
- Explicit contract, legacy, input-schema, payload and missing-output errors.
- `LateObservation`, `LateOutcome`, and mandatory `EffectStore` append/list
  methods for supplementary evidence.

New record/request fields require updates to custom store implementations and
public struct literals. Existing handler implementations retain defaults.
HTTP response/negative-lookup behavior is deliberately stricter. No API was
deprecated, no unsafe Rust was introduced, and no new dependency package was
added. Core MSRV remains 1.90; SQL-store MSRV remains 1.94.

These storage/API compatibility changes justify **0.2.0**, now prepared in the
workspace manifests and lockfile. No crate has been uploaded and no release or
tag has been created.

## Database migrations

| Migration | SQLite | PostgreSQL | Legacy handling |
|---|---|---|---|
| 0002_execution_contract | Nullable TEXT JSON | Nullable JSONB | NULL stays unattested; no inferred capability. |
| 0003_payload_schemas | Integer input/output versions | BIGINT input/output versions | Both default to 1; existing JSON is unchanged. |
| 0004_late_observations | Separate JSON metadata table | Separate JSONB metadata table | No existing observations to backfill; indexed by effect/id. |

The original migration is unchanged. Contract and input schema are immutable
record identity metadata; output schema changes with its authoritative output.
Backend conformance checks cover nondefault fields, conflict preservation,
fencing, evidence immutability, pagination and pruning. SQLite/PostgreSQL tests
exercise upgrades from the original schema; SQLite also reopens a database and
migrates output for a newer reader.

Back up and stop old workers before rollout. PostgreSQL migration must use a
direct session before attaching through PgBouncer. Mixed old/new worker fleets
and downgrades with unresolved records are unsupported because old binaries
ignore contracts. No automatic down migration is supplied; restoring a database
cannot reverse an external action. See [storage-migrations.md](storage-migrations.md).

## Validation

All checks below ran against the 0.2.0 release candidate and patched lockfile.

| Command/check | Actual result |
|---|---|
| `cargo fmt --all --check` | Passed. |
| `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings` | Passed. |
| `cargo clippy -p agent-effects --lib --locked -- -D warnings` | Passed without optional features. |
| `cargo test --workspace --all-features --locked` | 247 passed, zero failed, three existing ignored documentation examples. |
| `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --all-features --locked` | Passed. |
| `cargo +1.90 test --workspace --exclude agent-effects-sqlite --exclude agent-effects-postgres --all-features --locked --target-dir target/msrv-1.90` | 212 passed, zero failed, three existing ignored documentation examples. |
| `cargo +1.94 test -p agent-effects-sqlite -p agent-effects-postgres --locked --target-dir target/msrv-1.94` | 35 passed, zero failed/ignored; PostgreSQL/PgBouncer required. |
| `cargo publish --workspace --dry-run --locked` | Seven package tarballs verified; all uploads aborted as expected for dry run. |
| `cargo audit --db /tmp/agent-effects-correctness-advisory-db --no-fetch --deny warnings --json` | Zero advisories affecting locked packages and zero warnings. |
| `cargo run --locked --example payment` and `cargo run --locked --example agent_tool` | Both passed. |

Native suite highlights: 23 HTTP integration tests, eight contract regressions,
seven payload regressions, ten recovery tests, nine SQLite subprocess crash
tests, both SQLite/PostgreSQL multiprocess suites, two strict PgBouncer tests,
and all three store-conformance implementations. The existing model-based test
and the new contract compatibility property test passed. Counts use parent test
suite summaries rather than double-counting child subprocess output. The three
ignored examples are existing documentation examples requiring application
setup, not skipped database integrations.

The environment was Linux x86_64 (kernel 7.2.10), rustc/cargo 1.98.0, with
installed 1.90 and 1.94 toolchains. Tests used temporary SQLite files and
isolated PostgreSQL 17 plus `edoburu/pgbouncer:v1.25.2-p0`, strict transaction
pooling and disabled prepared statements. Both required-service flags were
set for final native/SQL MSRV runs, preventing silent database skips. Separate
MSRV target directories avoid cross-toolchain artifact conflicts.

The original sandboxed loopback failure, an early MSRV run affected by source
changes during the run, and transient dependency-download DNS failures were
not counted as passes. Final checks were rerun after fixes. The initial 0.1.3
publication dry run found API mismatches against already-published 0.1.3 store
dependencies; the release candidate was bumped to 0.2.0 before repeating it.

No benchmark harness or smoke benchmark was added or executed. There are no
throughput, latency, CPU, memory, database-round-trip or superiority claims.
`cargo deny` was available but not run without a project policy; policy/CI
configuration and secret scanning/fuzz infrastructure remain future work.

## Dependency advisory remediation

The first dependency audit found locked `ctutils` 0.4.2 and a Tokio 1.53.2
unsoundness warning. Tokio's manifest floor and lock were raised to 1.53.3;
transitive `ctutils` was locked to 0.4.4. These are the patched ranges listed by
[RustSec for ctutils](https://rustsec.org/advisories/RUSTSEC-2026-0385.html)
and [RustSec for Tokio](https://rustsec.org/advisories/RUSTSEC-2026-0386.html).

A fresh isolated RustSec database contained 1,352 advisories; the fetched
revision was `21d909e4c8bd4e14c7aea58bf980c881a0053ab1`. The shared advisory cache
was busy, so the final check used that isolated cache. The post-fix audit,
with warnings denied, found **zero vulnerabilities and zero warnings**.
This is dependency advisory validation, not an exploitability assessment or
a comprehensive code security audit. Consumers must update/audit their own
lockfiles; a library lockfile does not constrain downstream resolution.

## Git and review

Branch: `fix/execution-safety-contracts`, based on `main` at `6a115da`.

| Commit | Scope |
|---|---|
| a9710bb | Conservative HTTP classification and durable retry advice. |
| 8555e10 | Immutable execution contracts and compatibility tests. |
| dc1308f | Payload versions, migration and explicit replay errors. |
| 0c46cda | Supplementary late-result evidence with strict fencing. |
| 6071d96 | Failure-response distrust when redirect history is unavailable. |
| e4f709a | 0.2.0 preparation, dependency patches and final legacy compensation guard. |

Draft PR: [#4 — Harden HTTP outcomes, execution contracts, and durable recovery compatibility](https://github.com/akosidencio/agent-effects/pull/4).

The final documentation/report update is included as a separate commit on the
same branch. The connector could not create the PR with its integration
permissions; the existing GitHub CLI login created it successfully.

Required review includes conservative HTTP behavior, legacy upgrade procedures,
custom-store API migration, schema-migration correctness and the limitations of
untrusted late-result evidence. No PR was merged and no administrative GitHub
protection/environment settings were changed.

## Remaining work and risks

The first review slice is buildable but does not finish Milestone One's entire
requested test expansion. Add deterministic database outages during renewal
and after remote success, cancellation across additional checkpoints,
simultaneous recovery stress, approval/resolution races, subprocess upgrades
between payload/contract versions and randomized deployment changes.

Operational work still needs bounded recovery, lease-free delayed scheduling,
CLI authorization and confirmation, namespaces, secret references, tombstones,
metric expansion, real benchmarks and CI/supply-chain/fuzz hardening. The
MCP/Tower/EffectGroup/SQLx integrations and their examples remain unstarted.
The corresponding production/security guide, operator runbook, observability
sample, benchmark publication and integration guides remain pending. The
existing refund/payment examples were retained, not counted as new integrations.

Provider idempotency and verification are still trusted application assertions;
this library cannot verify a provider's guarantee or key-retention window.
In-flight requests can outlive a lease and even a verification lookup. Only
remote cooperation closes that duplicate-application window. Pruning still
forgets keys. Redacted credentials are not recoverable without application
credential provision. Actor labels do not establish authorization. Payload
migrations are trusted application code and must preserve execution meaning.
No exactly-once external side-effect guarantee is claimed.

Review and complete the remaining correctness tests before treating the full
first milestone as complete, then proceed with the operational PRs in
[roadmap.md](roadmap.md). The next version is 0.2.0; publishing remains a
separate explicitly authorized action. Before tagging, finalize the changelog
from its unreleased section into `[0.2.0]` as required by the existing release
preflight, and obtain the required code/release review.
