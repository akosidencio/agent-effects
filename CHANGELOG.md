# Changelog

All notable changes are listed here, following
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and
[Semantic Versioning](https://semver.org/). Before 1.0, minor releases may
break the API or storage format.

## [Unreleased]

### Added

- Bounded-concurrency recovery: `RecoveryConfig` (`max_concurrency`,
  `batch_size`), `RuntimeBuilder::recovery` and `Runtime::recover_with`. A
  slow handler no longer holds up unrelated effects in the same pass, and a
  pass never has more than `max_concurrency` effects in flight or more than
  one page of records in memory. The default stays sequential, as in 0.2.0.
- `Runtime::run_recovery_until(interval, shutdown)`: a recovery worker that
  stops at a signal, starting nothing new and waiting for the effects it
  already started.
- `RecoveryReport::not_due` (scheduled retries a pass left for later) and
  `RecoveryReport::stopped` (the pass ended early for a shutdown).
- Lease-free retry scheduling. A retry backoff at least
  `RuntimeBuilder::lease_free_backoff` long (default: the lease TTL) is
  waited out without the lease: the retry's time is stored first,
  the lease released, and a fresh lease taken, with the record re-read, when
  it is due. No lease renewals during long backoffs, and a worker that dies
  meanwhile leaves nothing to expire. `Duration::MAX` keeps 0.2.0's
  behavior. Compensation retries are scheduled the same way.
- `FaultPoint::AfterRetryScheduled` and `FaultPoint::AfterBackoffLeaseReleased`
  for crash tests.
- Store conformance: a scheduled retry survives releasing and retaking the
  lease, and is visible to pending scans while nobody holds it.
- `agent-effects-cli`, an optional crate installing the `agent-effects`
  operator command for SQLite and PostgreSQL (direct or through PgBouncer):
  `list`, `pending`, `inspect`, `history`, `late`, `stats`, `approve`,
  `deny`, `resolve` and `retry`, with JSON output, paging, a read-only mode,
  read-only connections for inspection, confirmation of writes, operator
  policies by authenticated database identity, and documented exit codes.
  It never creates or migrates a database.
- `OperatorDecision` and `Runtime::{resolve_as, approve_as, deny_as}`: an
  operator's decision can require the record version the operator reviewed,
  so a decision is never applied to a state they did not see.
- `SqliteStore::attach`: use a pool without migrating it, for tools and
  read-only pools.
- `EffectObserver::on_event` with `RuntimeEvent` (non-exhaustive): lease
  conflicts, lease renewal failures, recovery passes and pruning, which are
  not stored transitions. The method has a default, so existing observers
  compile unchanged. `RecoveryReport::waiting_for_operator` lists the
  effects a pass left for a person.
- `agent-effects-otel`: instruments for attempts (first or retry), retry
  delay, verification starts and outcomes, approval decisions and latency,
  operator resolutions, effects awaiting intervention, lease expiries,
  conflicts and renewal failures, recovery passes, duration, per-effect
  outcomes and backlog, and pruned records. Every attribute is bounded: no
  ids, keys, actors or messages.
- `agent-effects-bench` (not published) and `bench/run-matrix.sh`:
  reproducible workloads for memory, SQLite and PostgreSQL; see
  `guides/benchmarks.md`.

### Security

- Automated RustSec audit, cargo-deny advisory/license/source checks, and
  redacted secret scans. CI covers stacked PRs and pins external actions.
- Fuzz targets for stored records/text, state transitions and retry delays.
- Stored enum/JSON decoding errors omit potentially sensitive corrupt values;
  CLI URL parsing errors omit credentials and query values.
- Security disclosure policy and a production operations guide.
- An unchanged v0.2.0 consumer/custom store and a SQLite database upgrade
  fixture run in CI without modifying the release tag or migration history.

### Fixed

- Retry jitter no longer panics when a very large duration rounds beyond
  the floating-point representation accepted by `Duration`. Found by fuzzing;
  normal delay rounding and retry authorization remain unchanged.
- SQLite: writers sharing a store and its clones now take turns (a first-come, first-served
  queue) instead of racing SQLite's busy handler, which is not fair. Under
  write contention on storage with slow `fsync`, a writer could be starved
  past the 5 s busy timeout and fail with "database is locked", leaving an
  effect whose action had succeeded unrecorded (it then became unknown). Found
  by the new benchmarks: 11 of 15 runs of 200 effects with 16 callers failed
  on a btrfs-on-LUKS disk; none fail now. Writers in other processes still
  wait on the busy timeout.

### Changed

- A recovery pass that hits a store error now waits for the effects it
  already started before returning the error.
- A recovery pass no longer waits through a long retry backoff while
  holding a slot: it stores the retry and reports the effect as `Pending`,
  and a later pass resumes it once due.
- The lease-free backoff threshold defaults to the lease TTL. Measured:
  for shorter waits, holding the lease (a renewal every third of the TTL)
  costs fewer store round trips than releasing and retaking it.
- A call that waited out a backoff without the lease and finds another
  worker running the retry waits for that worker and reports its outcome,
  instead of returning `InProgress`. While a retry waits without its lease,
  `Runtime::wait` reports it `InProgress` immediately.

## [0.2.0]

### Added

- Reliability regression matrices for store failures before/after durable
  operations, cancelled callers and recovery passes, lease-renewal outages,
  concurrent operator decisions, and multiprocess recovery on SQLite/PostgreSQL.
- Cross-version subprocess recovery tests for contracts, payload migrations,
  committed-output replay, and interrupted compensation.

- Persisted versioned execution contracts across memory, SQLite, and
  PostgreSQL. Reattachment and durable recovery refuse changed idempotency,
  verification, handler semantics, adapter identity, approval requirements,
  automatic retry policy, or lifetime attempt ceilings.
- `EffectBuilder::{contract_version, adapter_identity}`, matching optional
  `EffectHandler` methods, and explicit contract mismatch/legacy errors.
- SQL migration 0002 adds a nullable contract without rewriting legacy
  records. Unresolved unattested records require reconciliation; settled
  legacy results remain replayable. Stop older workers before upgrading.

- Explicit input/output payload schema versions (legacy version 1),
  `PayloadSchema` migration hooks, version-aware durable recovery and
  compensation, `wait_with_output_schema`, and versioned operator output.
- Missing committed output now returns `RuntimeError::OutputUnavailable`;
  schema failures report the committed status explicitly and never rerun the
  action. SQL migration 0003 preserves all existing payload interpretations.

- Append-only late-result metadata for fenced or unrecordable action results,
  with paginated store inspection and SQL migration 0004. Evidence records no
  response bodies or errors and never changes authoritative state.

### Compatibility

- Adding `execution_contract` to public `NewEffect` and `EffectRecord`
  requires custom store implementations and struct literals to be updated.
  Custom stores must also implement append/list methods for late observations.
  `NewEffect::new` remains available, and `with_contract` supplies assumptions.
- Retry timing and attempt timeouts may change across deployments; the
  lifetime attempt ceiling and safety declarations must stay unchanged.

### Security

- Raise Tokio's minimum to 1.53.3 for RUSTSEC-2026-0386 and update the lockfile's
  transitive `ctutils` to 0.4.4 for RUSTSEC-2026-0385. Downstream applications
  must audit/update their own lockfiles; a library lockfile does not constrain
  a downstream application's transitive resolution.

### Fixed

- HTTP 408, 425, 429, and 503 now default to ambiguous outcomes, preventing
  blind retries of writes that may already have applied. Trusted provider
  response semantics can still override classification.
- Failure responses and negative verification remain ambiguous for clients
  that may follow redirects, including redirects back to the original URL.
  Declare a client with redirects disabled to trust rejection/absence responses.
- Preserve `Retry-After` independently of failure classification, including
  a durable minimum retry time for ambiguous outcomes. It never authorizes
  a retry; verification, remote idempotency, or an idempotent kind must do so.

## [0.1.3]

### Changed

- PostgreSQL: documented the connect options a pool behind PgBouncer needs:
  `extra_float_digits(None)`, since sqlx sends that startup parameter by
  default and a PgBouncer that does not ignore it refuses the connection, and
  no `search_path` through `options`. The store has no float columns and does
  not depend on it.
- PostgreSQL: `PostgresOptions::connect` and `connect_with` are documented as
  needing a direct connection, since they migrate; through a pooler in
  transaction mode the migration lock can be left held. Use `migrate` over a
  direct connection and `attach` to the pooled pool.
- CI's PgBouncer ignores no startup parameters, so the PgBouncer tests run
  against the strictest configuration.

## [0.1.2]

### Added

- PostgreSQL: `PostgresOptions`, for a database shared with an application,
  including through PgBouncer:
  - `schema(name)` keeps the store's tables and its own migration history
    (`name._sqlx_migrations`) in one schema, named in every query, so the
    store no longer depends on `search_path` or collides with the
    application's `sqlx` migrations.
  - `migrate(&pool)` and `attach(pool)` are separate steps: migrate over a
    direct connection, attach to a pooled one without migrating. `attach`
    checks that every migration is applied and is the store's own.
  - `transaction_pooling(true)` for a pooler in transaction mode: statements
    are never prepared for reuse, and every store operation runs in a
    transaction, so a statement's parse and execution reach the same server.
    Unnamed statements alone are not enough: outside a transaction, PgBouncer
    may send the bind to a different server.
  - `max_connections(n)` for the pool `connect` opens, previously fixed at 10.

`PostgresStore::connect`, `connect_with` and `from_pool` behave as before.

## [0.1.1]

### Fixed

- HTTP: a failing status after the client followed a redirect is now
  `Ambiguous`, since the request may have applied before the redirect (a 503
  could re-send it, a 405 report it failed). A lookup redirected to a 404 is
  no longer read as "not applied". A redirect back to the same URL cannot be
  detected; use a client that does not follow redirects for irreversible
  writes.
- HTTP: an error response's body is read only up to the 512 bytes kept in
  the message, so a huge or endless body cannot exhaust memory or hang.
- A new attempt clears the stored output, so a replay never returns the
  output of an attempt that was shown not to have applied.
- Resuming an interrupted compensation attempt respects the retry budget;
  with none left it ends `CompensationFailed`.
- Verification checks are limited to `max_attempts` per call across all of
  its attempts, as documented, instead of per verification.
- PostgreSQL: the database clock is read after the row lock is acquired, so
  a lease that expired while waiting for the lock is no longer renewed.

## [0.1.0]

- Initial runtime with durable handlers, effect deduplication and result replay.
- Failure classification, retries, timeouts and outcome verification.
- Crash recovery, fenced leases and operator resolution.
- Preconditions, risk policies, human approval and compensation.
- Secret redaction, audit trails and record retention.
- In-memory, SQLite and PostgreSQL stores.
- HTTP integration, tracing and OpenTelemetry metrics.
- Test utilities, fault injection, crash tests and store conformance tests.
