# Changelog

All notable changes are listed here, following
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and
[Semantic Versioning](https://semver.org/). Before 1.0, minor releases may
break the API or storage format.

## [0.1.0]

- Initial runtime with durable handlers, effect deduplication and result replay.
- Failure classification, retries, timeouts and outcome verification.
- Crash recovery, fenced leases and operator resolution.
- Preconditions, risk policies, human approval and compensation.
- Secret redaction, audit trails and record retention.
- In-memory, SQLite and PostgreSQL stores.
- HTTP integration, tracing and OpenTelemetry metrics.
- Test utilities, fault injection, crash tests and store conformance tests.
