# Security policy

## Supported versions

Security fixes go into the latest release of the newest minor version.
Before 1.0 that is the latest `0.2.x`; earlier minor versions are not
patched.

## Reporting a vulnerability

Please report vulnerabilities privately through GitHub: on the repository's
**Security** tab, choose **Report a vulnerability**
(<https://github.com/akosidencio/agent-effects/security/advisories/new>).
Do not open a public issue for a suspected vulnerability.

Include what you found, how to reproduce it, the affected crates and
versions, and what an attacker could achieve. You can expect an
acknowledgement within a week. Once a fix is released, it is credited in the
advisory unless you ask otherwise.

## Scope

In scope: the published crates in this repository (`agent-effects`,
`agent-effects-store`, `agent-effects-memory`, `agent-effects-sqlite`,
`agent-effects-postgres`, `agent-effects-http`, `agent-effects-otel`,
`agent-effects-cli`). Of particular interest:

- an effect running more often than its protections allow (a duplicated
  non-idempotent side effect), or recorded as failed when it may have
  applied;
- a fenced-off worker or a stale lease changing an effect's state;
- secrets or inputs leaking past a configured `Redactor` into storage, logs,
  metrics or CLI output;
- SQL injection or other misuse of stored data;
- the CLI acting beyond the database permissions it runs with.

Out of scope: vulnerabilities in an application's own handlers, approval
providers or authorization; denial of service by someone who can already
write to the store; and anything requiring access the deployment already
grants (database administrator, the host running the workers).

## Known advisories

- **RUSTSEC-2026-0459 / CVE-2026-119375** (SQLite before 3.54, bundled by
  `libsqlite3-sys` < 0.39 through sqlx): opening a crafted database file
  together with a crafted `-wal` file can return uninitialized process memory.
  It affects `agent-effects-sqlite` and the CLI's SQLite support. The attack
  is local: someone must be able to write both files the process opens.
  Anyone who can write your effects database can already forge its records,
  so for the store's own database the advisory adds little. **Do not open a
  SQLite database from an untrusted source** with the CLI or the store.
  No patched build is possible until sqlx allows `libsqlite3-sys` 0.39; CI
  ignores this one advisory until then, and the ignore will be removed with
  that upgrade. PostgreSQL deployments are not affected.

## Hardening in this repository

- Every pull request runs `cargo deny` (advisories, licenses, sources) and a
  secret scan; `cargo audit` also runs daily against the lockfile.
- Third-party GitHub Actions are pinned to commit SHAs, and workflows run
  with read-only repository permissions.
- Fuzz targets in `fuzz/` cover stored-record parsing, the state machine and
  retry arithmetic.
