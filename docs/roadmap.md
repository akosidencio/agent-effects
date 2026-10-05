# Roadmap

Design reference: [design.md](design.md). Status as of 2026-10-05.

## v0.1: core runtime, memory and SQLite stores

- done

## v0.2: durability without a caller, and policy

- Durable **handler registry**, so the recovery worker can finish effects with
  no caller present.
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
