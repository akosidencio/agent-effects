# Crash semantics

What `agent-effects` guarantees when processes crash, networks fail and
workers stall, what it leaves to you, and where its limits are. This is part
of the API contract; [design.md](design.md) has the mechanism behind each
point.

## The guarantee

For an effect identified by `(name, key)`:

1. **Intent is durable before the outside world is touched.** The attempt is
   committed to the store before the action is called.
2. **One logical effect, one record.** Every call with the same name and key,
   from any task, process or restart, attaches to the same record. A
   committed result is replayed, never re-run.
3. **"Failed" means it did not happen.** An effect is only `Failed` when it
   definitely did not apply. Anything that may have applied is `Unknown`,
   and then resolved by evidence or by a person. This holds across
   attempts: once any attempt may have applied the effect, a later attempt
   that definitely failed does not make it `Failed`, because it proves
   nothing about the earlier one. The store enforces this (the record's
   `may_have_applied` flag) until a verification or an operator shows the
   effect did not apply.
4. **No blind retries.** The runtime re-runs an effect whose last attempt may
   have applied only when that is provably harmless. That means one of:
   - the kind is `Read` or `IdempotentWrite`;
   - the remote deduplicates on `ctx.idempotency_key()`;
   - a verification found that the effect did not apply.

It does **not** guarantee exactly-once side effects. No library can, without
cooperation from the remote system. The strongest protection is a remote
that honours the idempotency key. Verification is next. With neither, an
uncertain outcome goes to a person.

## What every outcome asks of you

| Outcome | Meaning | What to do |
|---|---|---|
| `Committed(t)` | It happened; `t` is the result | Proceed. Calling again returns the same `t`. |
| `Failed(error)` | It definitely did not happen and won't be retried | Report it; a new attempt needs a new key. |
| `Rejected(error)` | A precondition refused it before it ran | Nothing happened; usually the decision was stale. |
| `Unknown { id }` | It may or may not have happened | **Do not treat as failed.** Call again later with the same key (the runtime verifies or re-runs if safe), or leave it to recovery and an operator. |
| `NeedsIntervention { id }` | The runtime cannot resolve it safely | **Do not retry.** A person checks the remote system and calls `runtime.resolve`. |
| `InProgress { id }` | Someone else is running it right now | `runtime.wait(id, timeout)`, or call again later. |
| `Err(RuntimeError)` | The runtime itself failed (store, bad input, mismatched key) | Fix the cause. If the store failed after the action ran, the effect is in doubt and recovery will mark it `Unknown`. |

For an agent tool, say "unknown" out loud: tell the model the status is
unknown and not to retry. The [`agent_tool`](../examples/agent_tool.rs)
example does this.

## Crash by crash

| The process dies… | The record is left… | What happens next |
|---|---|---|
| before the record is inserted | absent | The next call starts fresh. |
| after insert, before an attempt starts | `Pending`, unleased | The next call runs it. Nothing was sent. |
| after the attempt is recorded, before the request is sent | `Executing`, leased | After the lease expires, it becomes `Unknown`. The runtime cannot know nothing was sent. |
| while the request is in flight | `Executing` | Same. The request may still land. |
| after the remote applied it, before the result is recorded | `Executing` | Same. Verification finds it; an idempotency key deduplicates the re-send. |
| during verification | `Verifying` | After the lease expires, `Unknown`, then verified again. |
| while waiting to retry | `Pending` with `next_attempt_at` | The next call waits out the schedule, then runs it. Nothing is in flight. |

From `Unknown`, a later call with the same key does one of these:

- **Verifies**, if the effect has verification.
- **Re-runs**, if that is safe and retry budget remains.
- **Escalates** to `NeedsIntervention`, if neither applies.

## Running it in production

- **Run recovery.** Spawn `runtime.run_recovery(interval)` on at least one
  worker. It marks effects whose worker died mid-attempt as `Unknown`, then
  finishes every effect that has a registered handler from its stored input:
  verified, re-run only if safe, or escalated.
- **Prefer handlers for effects that must finish.** Implement
  `EffectHandler` and register it, and an effect completes even if its
  caller never returns. Closure effects cannot be resumed: recovery reports
  them as `unhandled`. Use `runtime.pending(after, limit)` at startup to
  call each one again with its original key and input.
- **Give operators `resolve`.**
  `runtime.resolve(id, Resolution::{Applied, NotApplied, Retry}, actor, note)`
  records a person's decision and the reason in the audit trail.
- **Prefer idempotency keys.** Forward `ctx.idempotency_key()`. It is stable
  across attempts, workers and restarts. Then declare
  `.remote_idempotency(true)`.
- **Verify with the right mode.** Use `.verify(...)` only if the lookup
  reads its own writes. For search or list APIs that lag, use
  `.verify_eventually(settle, ...)`, with `settle` longer than the lag.
  Treating a lagging lookup as authoritative re-runs effects that already
  happened. A test demonstrates exactly this.
- **Size the lease.** A worker that stops renewing for `lease_ttl` (30 s by
  default) is presumed dead. Keep it well above any pause your process can
  have (GC, VM migration, a blocked executor), and above the clock skew
  between machines.
- **Use a durable store.** `MemoryStore` loses everything with the process.
  It is for tests. `SqliteStore` commits with `synchronous = FULL`, because
  the record of an attempt must survive a power cut too.
- **Keep credentials out of inputs.** Inputs are stored as given, for
  fingerprinting and audit. Capture credentials in the action closure
  instead.

## Known limits

- **A stalled request can outrun verification.** Suppose worker A stalls
  past its lease while its request is still in flight. B takes over and
  verifies; the remote says "not applied", so B re-runs. A's request then
  lands, and the effect has happened twice. Settle delays shorten this
  window, and attempt timeouts plus a long lease make it rarer. Only a
  remote idempotency key closes it.
- **Leases trust the workers' clocks.** Each worker compares lease expiry
  against its own clock. A worker whose clock runs ahead may take over a
  live lease early. Fencing still rejects every write of the original
  holder, but the effect may then be re-run under the rules above. Keep
  clocks synchronized (NTP) and the lease TTL far above the skew.
- **A fenced worker's late success is not recorded.** If A's action
  succeeds after B took over, A's result is refused. The operator resolving
  the effect does not see it. This is an open design question.
- **Closures need a caller.** Recovery can only mark closure effects
  `Unknown`. Finishing them takes a call with the same key, or an operator.
  Registered handlers do not have this limit.
- **Stored inputs must stay readable.** A handler's input is stored so
  recovery can rebuild the call. If the input type changes incompatibly, old
  effects land in `RecoveryReport::resume_errors` instead of running.
- **Outputs are replayed as stored.** If the output type changes between
  releases, replaying an old result fails with `RuntimeError::Output`
  instead of returning wrong data.

## How this is tested

| Suite | What it covers |
|---|---|
| `crates/agent-effects/tests/crash.rs` | Every crash point × {verified, remote-idempotent, unprotected}: crash, lease expiry, recovery, re-run. Asserts every point is actually reached. |
| `crates/agent-effects-sqlite/tests/crash_subprocess.rs` | The same matrix, with a child process that `abort()`s on a SQLite file. |
| `crates/agent-effects/tests/model.rs` | Random configurations, remote failures, crashes, lease expiries and recovery passes. Checks that every audit trail follows the state machine, and that a non-idempotent effect is never created twice. |
| `crates/agent-effects/tests/recovery.rs` | Stalled-worker takeover without duplicates; operator resolution. |
| `crates/agent-effects-sqlite/tests/multi_process.rs` | Three processes on one database file run each effect exactly once. |

Each safety rule above was also broken on purpose to confirm a test fails
(see [design.md](design.md), §11).
