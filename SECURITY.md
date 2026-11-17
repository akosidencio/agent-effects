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

## Hardening in this repository

- Every pull request runs `cargo deny` (advisories, licenses, sources) and a
  secret scan; `cargo audit` also runs daily against the lockfile.
- Third-party GitHub Actions are pinned to commit SHAs, and workflows run
  with read-only repository permissions.
- Fuzz targets in `fuzz/` cover stored-record parsing, the state machine and
  retry arithmetic.
