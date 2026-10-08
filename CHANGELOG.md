# Changelog

All notable changes are listed here, following
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and
[Semantic Versioning](https://semver.org/). Before 1.0, minor releases may
break the API or storage format.

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
