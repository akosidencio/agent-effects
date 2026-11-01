# Recovery configuration and worker lifecycle

Recovery is how effects that lost their caller get finished: a process
crashed mid-attempt, a worker was stopped during a retry backoff, or an
effect was recorded and never started. This guide covers running recovery
in production: how often, how many effects at once, and how to stop and
restart workers safely.

## What a recovery pass does

`Runtime::recover()` runs one pass, in two steps:

1. **Mark abandoned attempts unknown.** An effect that is `Executing` or
   `Verifying` with an expired lease belonged to a worker that died. It may
   have changed the outside world, so it becomes `Unknown`, never `Failed`.
2. **Resume what has a handler.** Every unsettled effect nobody holds, whose
   name has a registered handler, is driven forward from its stored input:
   verified, re-run if its execution contract makes that safe, or escalated
   to `NeedsIntervention`. Effects waiting for an operator or an approval are
   left alone, and so are retries scheduled for later (`report.not_due`).
   Closure effects have no stored handler and are reported as `unhandled`;
   only a caller can run them again.

A pass never decides an outcome the store's state does not support.
Concurrency, restarts and multiple workers change how fast a backlog drains,
not what happens to any one effect.

## Running recovery continuously

```rust
use std::time::Duration;
use agent_effects::RecoveryConfig;

let runtime = Runtime::builder(store)
    .register(handler)
    .recovery(RecoveryConfig::SEQUENTIAL.max_concurrency(8).batch_size(200))
    .build();

let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
let worker = tokio::spawn({
    let runtime = runtime.clone();
    async move {
        runtime
            .run_recovery_until(Duration::from_secs(30), async {
                let _ = stopped.await;
            })
            .await
    }
});
```

`run_recovery(interval)` is the same loop without a stop signal. Every round
runs one pass and then prunes settled records if the runtime has a retention
policy. A failed pass is logged and retried at the next tick.

**Interval.** A dead worker's effect becomes recoverable only after its
lease expires (`lease_ttl`, 30 s by default), so an interval shorter than the
lease TTL mostly finds nothing new. 15–60 s suits most deployments. Shorter
intervals mainly speed up retries that were scheduled by a worker that has
since stopped.

## Concurrency

| Setting | Default | Meaning |
|---|---|---|
| `max_concurrency` | 1 | Effects a pass works on at once. |
| `batch_size` | 100 | Records read per store scan. |

The default is sequential, exactly as in 0.2.0: one slow handler (a
verification that waits for a settle delay, a provider that times out)
delays every effect behind it. With `max_concurrency(n)` a slow effect holds
one of `n` slots and the others keep moving.

Bounds a pass always keeps:

- at most `max_concurrency` effects in flight;
- at most one page of `batch_size` records in memory, plus the in-flight
  effects;
- no record is resumed twice in one pass (scans page by id).

**Sizing.** Each in-flight effect uses a store connection for its
transitions, and its lease renewals need one too. Keep `max_concurrency`
comfortably below the connection pool size (PostgreSQL `max_connections`
defaults to 10; behind PgBouncer, below the pool the role may use). Starting
points:

- SQLite: 2–4. Writes serialize on the database file; more concurrency only
  helps handlers that spend their time waiting on the network.
- PostgreSQL: 4–16, below the pool size.
- Handler-bound backlogs (slow remote APIs): raise concurrency first.
- Store-bound backlogs (many cheap effects): raise `batch_size` to cut scans.

Several workers, in one process or many, may run recovery against the same
store at once. Every change happens under a lease, and PostgreSQL scans skip
rows another worker has locked, so workers split a backlog between them.
A pass may report an effect as `skipped`, or resume it and find another
worker already finished it; the next pass reconciles both.

## Scheduled retries

A retry is stored with the time it may start (`next_attempt_at`) before
anyone waits for it. Backoffs of at least `lease_free_backoff` (a third of
the lease TTL by default) are waited out without the lease, so:

- a recovery pass stores the retry and moves on; the effect shows in
  `report.not_due` until a later pass finds it due;
- a worker that dies during a backoff holds nothing, and the retry is
  resumed as soon as it is due rather than after a lease expiry;
- several workers finding one due retry run it once: each takes the lease
  and re-reads the record before starting it.

A released lease never makes a retry more permissive: whether an effect may
run again is decided under the lease before the wait. See the
[`retry` module documentation](https://docs.rs/agent-effects/latest/agent_effects/retry/index.html#scheduling).

## Errors during a pass

- **A store error while scanning** stops the pass from starting anything new.
  The pass waits for the effects it already started, then returns the error.
  Their progress is kept; the next pass continues.
- **A store error while resuming one effect** is reported in
  `report.resume_errors` and does not stop the pass. The effect keeps its
  lease until it expires and is picked up again after that.
- **A handler that cannot run** (input that no longer deserializes, a changed
  execution contract) is reported in `resume_errors`; nothing is executed.

## Shutdown and restart

`run_recovery_until(interval, shutdown)` handles a graceful stop:

- between passes, it returns at once;
- during a pass, it starts nothing new, waits for the effects already
  started, and returns. No pruning runs after a stopped pass.

Waiting for started effects can take as long as their handlers do. Bound
the shutdown with a timeout if the process has a deadline:

```rust
let _ = stop.send(());
if tokio::time::timeout(Duration::from_secs(20), worker).await.is_err() {
    tracing::warn!("recovery still finishing effects at shutdown");
}
```

Dropping or aborting a recovery future is also safe. Each effect runs on a
task of its own, so cancelling the pass never cuts an effect off between its
action and the record of its result; it only stops waiting. If the process
then exits, an effect that was mid-attempt is recovered like any crash: its
lease expires, the next pass marks it `Unknown`, and it is verified,
re-run only if that is safe, or escalated.

**Restart checklist**

1. Stop recovery workers with the shutdown signal, then stop callers.
2. Deploy. Keep `lease_ttl`, handler names, contract versions and payload
   schemas compatible, or follow the migration notes in the changelog.
3. Start workers. The first pass after the old leases expire picks up
   whatever the old processes left.

## Reading a recovery report

| Field | Meaning | Action |
|---|---|---|
| `marked_unknown` | Attempts whose worker died | None; the pass continues with them. A sustained rate means workers are dying. |
| `resumed` | Effects driven forward, with the status reached | `NeedsIntervention` needs an operator. |
| `skipped` | Taken by another worker first | None. |
| `unhandled` | Closure effects with no registered handler | Only a caller can re-run them; investigate if they accumulate. |
| `resume_errors` | Effects that could not be resumed | Investigate: store errors, contract mismatches, payload schema errors. |
| `not_due` | Retries scheduled for later | None; a later pass resumes them. |
| `stopped` | The pass ended early for a shutdown | None. |

With `max_concurrency` above 1, effects appear in the order they finished.
