# Roadmap

Agent Effects remains an embeddable reliability layer for consequential actions.
It is not a workflow engine or a hosted control plane. Implementation status
below is for the engineering initiative requested on 2026-10-26.

## Correctness: implemented in the first review branch

- Conservative HTTP write response handling, including rate-limit hints and
  redirects that return to the original URL. A declared no-redirect client is
  needed for definite response rejection and negative lookup evidence.
- Immutable versioned execution contracts, validation before reattachment and
  handler recovery, and explicit safe handling of legacy records.
- Versioned input/output payloads with application-supplied migrations, explicit
  committed-but-unreplayable output errors, and version-aware compensation.
- Append-only late-result metadata, bounded inspection, and strict separation
  from authoritative state and fencing. No response payloads are retained.
- New regressions and contract properties, plus the existing model, subprocess
  crash, multiprocess, approval, compensation, and store conformance suites.

Milestone One as a whole remains **partial**. The full requested test expansion
still needs deterministic storage outages during renewal/commit, cancellation
at every additional checkpoint, simultaneous recovery stress, operator/approval
races, subprocess schema upgrades, and randomized contract/payload deployment
changes. Passing existing suites is not evidence that every new matrix was added.

## Operations: not started by this branch

Implement in dependency order after correctness review:

1. Bounded recovery concurrency with backpressure, fairness, cancellation,
   multi-worker fencing, and recovery statistics.
2. Retry scheduling that releases idle leases while respecting durable budgets,
   persisted due times, backoff, and provider rate limits.
3. Optional SQLite/PostgreSQL operator CLI with read-only mode, JSON, explicit
   action confirmation, actor/notes, and real application authorization hooks.
4. Tenant/application namespaces across identity, retrieval, remote keys, scans,
   and migrations; retain unnamespaced identity compatibility.
5. Secret references and application credential resolution for durable handlers,
   with rotation and no persisted plaintext credentials.
6. Optional deduplication tombstones during atomic settled-record pruning.
7. Additional low-cardinality operational metrics and a production configuration.
8. Reproducible memory/SQLite/PostgreSQL and multiprocess benchmark harnesses,
   actual 100/1,000/10,000-effect measurements, and environment documentation.
9. RustSec/dependency policy and secret scanning in CI, pinned action review,
   release/provenance hardening, repeatable fuzz targets, and documented manual
   GitHub environment/protection settings. A local dependency check is not CI.

## Integrations: not started by this branch

- MCP adapter with a trusted application capability registry; untrusted server
  annotations never establish idempotency, authorization, or safe retries.
- Tower middleware that preserves service semantics and reports unknown outcomes.
- Small `EffectGroup` API with ordered execution, persistent progress where
  needed, reverse compensation, and explicit partial outcomes.
- SQLx transaction integration with a precise boundary between database commit
  and external side effects; no cross-system atomicity claims.
- Optional agent framework examples once the core adapters are stable.

The production/security integration guide, operator runbook, observability
sample, benchmark guide/results, MCP guide, and new-crate examples remain tied
to those future implementations. Existing general guidance is in
[crash-semantics.md](crash-semantics.md); new compatibility guidance is in
[storage-migrations.md](storage-migrations.md),
[payload-schemas.md](payload-schemas.md), and
[retry-idempotency.md](retry-idempotency.md).
