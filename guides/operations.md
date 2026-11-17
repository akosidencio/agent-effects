# Production operations

How to deploy, run, monitor and upgrade agent-effects, and what to do when
something goes wrong. Pair it with the
[recovery guide](recovery.md), the [observability guide](observability.md)
and the [operator CLI](../crates/agent-effects-cli/README.md).

## Choosing a store

| | SQLite | PostgreSQL |
|---|---|---|
| Fits | One host: CLI agents, desktop apps, single-node services | Many workers on many hosts |
| Concurrency | Several processes share one file; writes serialize on it | Row locks; workers scale out |
| Leases judged by | The workers' clocks (one host) | The database's clock (`clock_timestamp()`) |
| Pooling | n/a | Direct, or PgBouncer in transaction mode |

The in-memory store is for tests: it loses everything on exit.

## SQLite

`SqliteStore::open(path)` creates the file if needed, migrates it, and sets
WAL, `synchronous = FULL` and a 5 s busy timeout. Keep those: `FULL` is what
makes "intent persisted before the call" true across a power cut.

- Put the file on a local disk, not a network filesystem: SQLite's locking
  is not reliable over NFS or SMB.
- Every process that opens the file must use agent-effects' migrations; the
  file holds nothing else.
- Concurrency: writes take the database lock (`BEGIN IMMEDIATE`). Recovery
  concurrency beyond 2–4 rarely helps unless handlers mostly wait on the
  network.

## PostgreSQL

```rust
use agent_effects_postgres::PostgresOptions;

// Once per deploy, over a direct connection (it takes a migration lock):
PostgresOptions::new().schema("effects").migrate(&direct_pool).await?;
// In every worker, possibly through PgBouncer:
let store = PostgresOptions::new()
    .schema("effects")
    .transaction_pooling(true) // only behind a transaction-mode pooler
    .attach(pooled_pool)
    .await?;
```

- **Schema of its own.** `schema("effects")` keeps the tables and their
  migration history apart from the application's.
- **Pool size.** Each in-flight effect needs a connection for its
  transitions, and its lease renewals need one too. Size the pool above the
  number of concurrent callers plus `RecoveryConfig::max_concurrency`.
- **Database clock.** Leases are judged by PostgreSQL's clock, so worker
  clock skew cannot hand one effect to two workers. Retry times
  (`next_attempt_at`) are computed by the workers: keep their clocks
  NTP-synchronized, or retries start early or late by the skew.
- **Roles.** Workers need `SELECT, INSERT, UPDATE, DELETE` on the store's
  tables (`DELETE` only if retention prunes). Operators need far less; see
  the CLI's grants.

### PgBouncer (transaction mode)

- Set `transaction_pooling(true)`: every store operation runs in a
  transaction and no statement is prepared for reuse. Unnamed statements
  alone are not enough: outside a transaction, PgBouncer may send a
  statement's bind to another server.
- Connect with `PgConnectOptions::extra_float_digits(None)`, and pass no
  `search_path` in `options`: a strict PgBouncer refuses startup parameters
  it does not know.
- Migrate over a direct connection, never through the pooler: the migration
  advisory lock would leak to another client.
- `max_prepared_statements = 0` is supported; CI runs against a strict
  PgBouncer that ignores no startup parameter.

## Worker lifecycle

A worker is a process with a `Runtime`, the application's handlers
registered, and a recovery loop:

```rust
let runtime = Runtime::builder(store)
    .worker_id(WorkerId::new(format!("{hostname}-{pid}")))
    .lease_ttl(Duration::from_secs(30))
    .recovery(RecoveryConfig::SEQUENTIAL.max_concurrency(8))
    .retention(RetentionPolicy::settled(Duration::from_secs(30 * 86_400)))
    .observer(agent_effects_otel::OtelObserver::global())
    .register(Handler::new(ChargeCard::new(client)).verifiable())
    .build();
let recovery = tokio::spawn({
    let runtime = runtime.clone();
    async move { runtime.run_recovery_until(Duration::from_secs(30), shutdown).await }
});
```

- **Worker ids** must be unique among live workers; a host name plus process
  id is enough. They appear in leases and audit events, never in metrics.
- **Every worker that may resume an effect must register its handler.** A
  worker without it reports the effect as `unhandled` and leaves it alone.
- **Lease TTL** (default 30 s) bounds how long a dead worker's effect waits
  before others treat it as abandoned. Shorter TTLs recover faster but renew
  more often (every third of the TTL) and are less tolerant of GC pauses and
  network blips. An action may run longer than the TTL: the lease is
  renewed while it runs.
- **Shutdown:** signal `run_recovery_until`, stop accepting new calls, and
  give in-flight calls a deadline. Anything cut off is recovered after its
  lease expires; nothing is lost or blindly repeated.

## Retry policies

`RetryPolicy { max_attempts, initial_delay, max_delay, multiplier, jitter }`
is a **lifetime** budget per effect: restarts and recovery do not reset it.
`max_attempts` is part of the persisted execution contract, so changing it
for effects already recorded makes them refuse to resume: introduce a new
`contract_version` or let old effects drain first.

Whether a failure is retried at all depends on its class and the effect's
protections, never on the policy alone: ambiguous outcomes are retried only
for idempotent, remotely idempotent or verified effects. A provider's
`Retry-After` sets when, never whether.

Backoffs of at least `lease_free_backoff` (the lease TTL by default) are
waited out without the lease. Keep jitter on when many effects fail together
(a provider outage), so they do not retry in lockstep.

## Monitoring

See the [observability guide](observability.md) for the instruments and
alert rules. The short list:

- `agent_effects.unknown` rate: ambiguous outcomes from remote systems.
- `agent_effects.needs_intervention`: work for operators.
- `agent_effects.recovery.backlog{state="due"}` and recovery pass failures.
- `agent_effects.lease.expired`: workers dying mid-attempt.

## Incident investigation

Start from the effect, not the logs:

```sh
agent-effects stats                                   # what is waiting, overall
agent-effects list --status unknown --status needs_intervention
agent-effects inspect <id>                            # state, attempts, lease, contract, last error
agent-effects history <id>                            # who did what, when
agent-effects late <id>                               # results that arrived after a worker lost its lease
```

Useful questions:

- **Did it apply?** `may_have_applied: true` means an attempt may have
  reached the remote system. Check the remote system with the effect's
  idempotency key (derived from its name and key) before deciding.
- **Why is it stuck?** A live lease means a worker is on it (`lease.owner`);
  an expired lease on `executing` means the worker died and the next
  recovery pass will mark it unknown; `pending` with a future
  `next_attempt_at` is a scheduled retry.
- **Late observations** say what a fenced-off attempt saw (`succeeded`,
  `failed`, `interrupted`). They are evidence, never proof: a late
  "succeeded" justifies checking the remote system, not resolving blindly.

Traces (`agent_effect.execute` spans) carry the effect id and attempt for
correlating with the remote system's logs.

## Operator resolution

Operators decide what the runtime cannot:

| Situation | Command |
|---|---|
| The remote system shows the effect happened | `agent-effects resolve <id> --applied [--output JSON] --reason …` |
| It shows the effect did not happen | `agent-effects resolve <id> --not-applied --reason …` |
| It did not happen and running it again is safe | `agent-effects retry <id> --reason …` |
| A compensation failed and was finished by hand | `agent-effects resolve <id> --compensated --reason …` |
| An effect awaits approval | `agent-effects approve <id> --reason …` / `deny` |

Each decision is checked against the version the operator was shown and
recorded with the operator's identity and reason. `retry` on an effect that
did apply duplicates it: verify first. Applications that build their own
admin tools use `Runtime::resolve_as` / `approve_as` / `deny_as` with an
`OperatorDecision`, after authorizing the operator themselves.

## Backups

The store is the source of truth for what was attempted. Back it up like
any business data, and remember what a restore means: **effects recorded
after the backup are forgotten**, so their keys are free again and the next
call with one of them runs the effect again. After restoring:

1. Treat every effect recorded after the backup time as possibly applied.
2. Prefer remotely idempotent effects (the remote system deduplicates on the
   same idempotency key, which is derived from the key, not the record).
3. Reconcile anything else against the remote systems before restarting
   callers.

SQLite: back up with `sqlite3 effects.db ".backup backup.db"` or the online
backup API, not by copying the file while it is open (the WAL holds recent
commits). PostgreSQL: any consistent method (`pg_dump`, base backups with
WAL archiving).

## Schema migrations and safe upgrades

- Migrations are additive and never rewrite existing records. 0.2.1 adds
  none.
- PostgreSQL: run `PostgresOptions::migrate` once per deploy over a direct
  connection, before starting new workers. `attach` refuses a database that
  lacks a migration, so a worker never runs against an older schema.
- SQLite: `SqliteStore::open` migrates on open; the CLI and `attach` never
  do.
- Upgrade order: migrate, deploy new workers, then retire old ones. When a
  release notes that old workers must stop before upgrading (0.2.0 did, for
  execution contracts), stop them first.
- Changing a handler: keep its name, `contract_version`, kind, retry ceiling
  and verification mode for effects already recorded; change
  `contract_version` (and keep the old code path until old effects drain)
  when execution semantics change. Payload schema versions migrate stored
  inputs and outputs; see the payload documentation.

## Known limitations

- **Not exactly-once.** Without the remote system's cooperation (idempotency
  keys or a lookup to verify), an ambiguous outcome can only be escalated.
- **In-flight requests can outlive a lease.** A worker that stalls past its
  lease is fenced off from writing, but its request may still reach the
  remote system; remote idempotency is the only full protection.
- **Retry times use worker clocks**, even on PostgreSQL.
- **Up/down counters are per process** and drift across restarts; use the
  recovery backlog gauge or `agent-effects stats` for exact levels.
- **SQLite is single-host.** Several processes may share a file on one
  machine, not across machines.
- **Pruning forgets.** A pruned key runs again on its next call; keep
  committed records at least as long as callers may retry.
- **The CLI's authority is its database credentials.** Its policy narrows
  what it does, not what the database role allows.

## Data exposure and resource limits

The default runtime retains inputs, outputs, audit notes and errors. Configure
an application `Redactor` before using sensitive payloads, and restrict access
to database backups and audit history. The CLI hides input/output bodies unless
`inspect --show-payloads` is requested; history, keys, notes and stored errors
can still contain application data. Actor text is an audit label, never a
credential. Prefer a database role restricted to the required schema and use
read-only credentials for inspection.

Tracing spans include effect identifiers and names. Keep names static and never
put customer identifiers or secrets into them. Exported metric labels contain
no payloads, keys or actors; an application that dynamically generates effect
names can still create unbounded cardinality. Apply exporter attribute limits
and retention appropriate to your deployment.

Bound recovery concurrency, provider request timeouts, database pool sizes and
payload sizes in the host application. A handler that never returns can delay
graceful shutdown indefinitely; use a shutdown deadline and allow subsequent
recovery to reconcile interrupted work. Database renewal errors do not prove an
external failure. Never turn them into permission to repeat a write.
