# Storage and migration guide

Migration 0002 adds nullable `execution_contract` (SQLite TEXT JSON;
PostgreSQL JSONB). Existing rows, keys, idempotency keys, payloads, attempt
counts and audit histories are preserved. The memory store uses the same
record structure. No dependencies or MSRV changes are required.

Stop old workers and take a backup before upgrading. Apply SQLite migrations
through `SqliteStore::open` or `from_pool`. For PostgreSQL, run
`PostgresOptions::migrate` on a direct session, then attach through PgBouncer.
Named schemas keep their own migration history. Do not migrate through a
transaction-mode pooler; follow the existing PostgreSQL deployment guidance.

A NULL contract is deliberately unknown. Settled legacy records can replay,
but unresolved records cannot execute or compensate through a durable handler
until reconciled. Unknown or intervention-required records can be resolved by
an authorized operator using remote evidence. Pending legacy records need a
reviewed application migration or completion using the previous runtime before
the upgrade; this release has no automatic adoption API. Never manufacture
idempotency support from the new handler's configuration. Preserve the original
logical key: a new key can repeat an already applied operation.

New records persist format 1 contracts with kind, remote idempotency,
verification mode and settle delay, handler semantic version, optional adapter
identity, effective approval and retry restrictions, and lifetime attempt
ceiling. These must compare equal on reattachment, recovery, and durable
compensation. Backoff, jitter and attempt timeout can change. A changed attempt
ceiling is refused; consumed attempts remain durable. Compatible releases keep
the application contract version unchanged. Provider rotation is compatible
only if its recorded adapter identity and guarantees remain valid.

Unknown contract format versions fail closed. New application semantics should
use a new version and a reviewed migration/reconciliation strategy, rather than
silently promoting unresolved effects to safer capabilities.

Migration rollback is not provided. Keep the added column and restore a backup
only after reconciling external actions that happened since it was taken.
Old binaries ignore the contract and cannot enforce it, so mixed old/new worker
fleets and downgrades with unresolved effects are unsupported. Database rollback
cannot undo an external side effect.

Custom store implementations must persist `NewEffect::execution_contract`
atomically with insertion, return it unchanged on conflict, and preserve it on
every transition and lease operation. `NewEffect::new` creates an unattested
record; `with_contract` records explicit assumptions. Run the shared store
conformance suite, which now checks nondefault contracts and conflict behavior.
