# Changelog

All notable changes are listed here, following
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and
[Semantic Versioning](https://semver.org/). Before 1.0, minor releases may
break the API or storage format.

## [Unreleased — 0.2.0]

### Added

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
