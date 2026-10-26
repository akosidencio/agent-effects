# Retry and idempotency

Intent deduplication in the store prevents duplicate callers from starting
independent logical effects. Remote idempotency is a separate guarantee:
the provider must deduplicate the stable key forwarded by the application.
Set `remote_idempotency(true)` only when that provider contract applies to
the operation, account, and full recovery window.

HTTP 408, 425, 429, 500, 502, 503, and 504 default to `Ambiguous`. A gateway
can fail after a write; `Retry-After` is not proof of rejection. The adapter
parses delay seconds and HTTP dates, clamps past dates to zero and ignores
invalid values. The runtime persists the minimum retry time independently
of the failure class, including for unknown outcomes. It waits only when
re-execution is safe; verification can confirm immediately.

Use a client with redirects disabled and declare
`HttpEffect::client_follows_no_redirects()` to classify connection failures
that prove the request was never sent as transient. With redirect following,
a connection failure may concern a later target after the original write.
The same applies to failure statuses and negative verification: URL equality
cannot exclude a redirect back to the original URL. Declare a client without
redirects to use definite status classifications or 404/410 lookup absence;
otherwise these remain ambiguous. This also applies to provider overrides
that would classify a response as a definite rejection.

Provider-specific `classify_status` overrides are trusted application code.
Return `Transient`, `RateLimited`, or a definitive failure only when the
provider's response semantics prove non-application. Returning `None` uses
the conservative default. Sending an idempotency header alone does not
establish provider support. A 425 normally rejects TLS early data, but a
generic adapter does not attest the response's origin or early-data path.

For ambiguous outcomes, the runtime verifies, safely re-executes under an
idempotency guarantee, or escalates. Without a usable guarantee, surface
`Unknown` or `NeedsIntervention` to the agent and operator. Reusing the same
logical key attaches to the recorded effect; choosing a new key can
repeat the action.

The HTTP specifications describe response and retry semantics, not an
application transaction guarantee: [RFC 9110](https://www.rfc-editor.org/rfc/rfc9110.html),
[429](https://www.rfc-editor.org/rfc/rfc6585.html#section-4), and
[425](https://www.rfc-editor.org/rfc/rfc8470.html#section-5.2).

No configuration provides exactly-once external effects without remote
cooperation. Provider key expiration and requests outliving leases remain
application concerns; see [crash-semantics.md](crash-semantics.md).
