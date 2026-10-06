# Roadmap

Design reference: [design.md](design.md). Status as of 2026-10-05.

## v0.1: core runtime, memory and SQLite stores

- done

## Next: durability without a caller, and policy (ships in 0.1.0)

| # | Milestone | Status |
|---|---|---|
| N1 | Durable **handler registry** (`EffectHandler`, `VerifiableEffect`, `submit`); recovery finishes registered effects with no caller | **done** |
| N2 | Compensation as a durable sub-lifecycle from `Committed` (`CompensableEffect`) | **done** |
| N3 | `ApprovalProvider` + CLI provider; `AwaitingApproval` survives restarts | **done** |
| N4 | Risk policy: `RiskLevel` × `EffectKind`, defined precedence | **done** |
| N5 | `agent-effects-postgres`: database-side `now()`, `FOR UPDATE SKIP LOCKED` scans | |
| N6 | Redaction hook for audit payloads and outputs | |
| N7 | `EffectObserver` metrics in core; `agent-effects-otel` | |
| N8 | `agent-effects-http` (reqwest): failure classification, `Idempotency-Key` | |
| N9 | Retention / pruning of settled records | |
| N10 | Model test and crash suite cover the new features; docs | |

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
