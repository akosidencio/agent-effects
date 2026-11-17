# agent-effects-cli

The `agent-effects` command: inspect and reconcile
[agent-effects](https://crates.io/crates/agent-effects) records in a SQLite or
PostgreSQL store, without editing database rows by hand.

```sh
cargo install agent-effects-cli
export AGENT_EFFECTS_DATABASE_URL=postgres://ops@db.internal/app
export AGENT_EFFECTS_SCHEMA=effects          # if the store has a schema of its own

agent-effects stats
agent-effects list --status unknown --status needs_intervention
agent-effects inspect 01a19666-f0ca-7376-b3f9-97f88570b82b
agent-effects history 01a19666-f0ca-7376-b3f9-97f88570b82b
agent-effects resolve 01a19666-f0ca-7376-b3f9-97f88570b82b --not-applied \
    --reason "no charge in the provider dashboard (OPS-142)"
```

The core library does not need the CLI, and the CLI never runs effects.

## Commands

| Command | What it does | Changes records |
|---|---|---|
| `list [--status S]... [--scheduled]` | Effects oldest first, optionally by status; `--scheduled` shows pending retries with their time | no |
| `pending` | Effects waiting for a caller, recovery or an operator, with nobody working on them | no |
| `inspect ID [--show-payloads]` | One effect: status, attempts, retry schedule, lease, execution contract, last error, late-result evidence | no |
| `history ID` | The audit trail: every transition, actor and note | no |
| `late ID` | Late-result evidence from fenced-off or unrecorded attempts (untrusted, never proof) | no |
| `stats` | Counts by status, scheduled and due retries, live leases | no |
| `approve ID --reason R` | Approves an effect awaiting approval | yes |
| `deny ID --reason R` | Denies it; it ends rejected | yes |
| `resolve ID --applied [--output JSON \| --output-file F] --reason R` | Records that an unknown or escalated effect applied | yes |
| `resolve ID --not-applied --reason R` | Records that it did not apply; it ends failed | yes |
| `resolve ID --compensated --reason R` | A failed compensation was finished by hand | yes |
| `retry ID --reason R` | Lets an unknown or escalated effect (or a failed compensation) run again | yes |

Listings page by id: `--limit N` (1–1000, default 50) and `--after ID` with the
`next_after` cursor of the previous page.

`resolve --applied` and `retry` assert facts about the outside world. Check
the remote system first: `retry` on an effect that did apply duplicates it.

## Configuration

| Option | Environment | Meaning |
|---|---|---|
| `--database-url` | `AGENT_EFFECTS_DATABASE_URL` | `postgres://…`, `sqlite://path` or a SQLite file path |
| `--schema` | `AGENT_EFFECTS_SCHEMA` | PostgreSQL schema of the store (`PostgresOptions::schema`) |
| `--transaction-pooling` | `AGENT_EFFECTS_TRANSACTION_POOLING` | The URL goes through PgBouncer in transaction mode |
| `--read-only` | `AGENT_EFFECTS_READ_ONLY` | Refuse every write command |
| `--policy` | `AGENT_EFFECTS_OPERATOR_POLICY` | Operator policy file, see below |
| `--format text\|json` | `AGENT_EFFECTS_FORMAT` | Output format |
| `--actor` | `AGENT_EFFECTS_OPERATOR` | Operator label recorded with each decision |

The CLI never creates or migrates a database: a missing file or an unmigrated
schema is a connection error. Passwords in URLs are never printed.

## Safety of write commands

Every write command:

1. checks the effect is in a state the command applies to (`approve` needs
   `awaiting_approval`, `resolve` needs `unknown` or `needs_intervention`, …)
   and that no worker holds its lease;
2. shows the effect and asks for confirmation (type `yes`). Without a
   terminal, it refuses unless `--yes` is given;
3. applies the decision only if the effect is still at the version that was
   shown. If anything changed in between, nothing is written and the command
   exits with code 4. `--expect-version N` pins an explicit version, for
   scripts that inspected first.

The decision is recorded in the effect's audit trail with the actor and the
reason. Inspecting never changes an effect: read commands open read-only
connections (`mode=ro` for SQLite; `default_transaction_read_only` for a
direct PostgreSQL connection) and never retry, verify or recover anything.

## Authorization and trust boundary

The CLI is a local process. It opens no network listener and exposes no
operation remotely.

Its authority is the database credentials it connects with: whoever can run
it against a database can do what that database role, or for SQLite the
file permissions, allow. That is the trust boundary. Recommended roles for
PostgreSQL:

```sql
-- Inspection only
CREATE ROLE effects_viewer LOGIN;
GRANT USAGE ON SCHEMA effects TO effects_viewer;
GRANT SELECT ON ALL TABLES IN SCHEMA effects TO effects_viewer;

-- Decisions: read, and update records with their audit events
CREATE ROLE effects_operator LOGIN;
GRANT USAGE ON SCHEMA effects TO effects_operator;
GRANT SELECT ON ALL TABLES IN SCHEMA effects TO effects_operator;
GRANT UPDATE ON effects.effects TO effects_operator;
GRANT INSERT ON effects.effect_events TO effects_operator;
```

An **operator policy** narrows what the CLI will do, by the identity the
database authenticated, never by the `--actor` label:

```json
{
  "operators": {
    "effects_oncall": ["approve", "deny"],
    "effects_operator": ["approve", "deny", "resolve", "retry"]
  }
}
```

For PostgreSQL the identity is `current_user`. For SQLite it is the local user
name from the environment, which is advisory only: the file permissions
decide. Unknown fields or command names in a policy are errors. A policy is a
guardrail for the CLI, not a replacement for grants: a role that may update
the tables can do so without the CLI.

The recorded actor combines both, e.g.
`operator:alice (agent-effects-cli, database role effects_oncall)`.

## Output

Text output is for people; `--format json` prints one JSON document on
standard output for scripts, and errors as `{"error": …, "exit_code": …}` on
standard error.

Stored inputs and outputs are omitted unless `inspect --show-payloads` is
given: they are where personal or secret data usually is. Keys, errors and
audit notes are shown. Configure a redactor in the application so that what
is stored is safe to show.

## Exit codes

| Code | Meaning |
|---|---|
| 0 | Success |
| 1 | Unexpected error |
| 2 | Invalid arguments or configuration |
| 3 | No effect with that id |
| 4 | Conflict: wrong state, a worker holds it, or it changed since it was shown |
| 5 | Refused: read-only mode, not confirmed, or not allowed by the policy |
| 6 | Cannot reach the database, or it is not an agent-effects store |

MSRV: Rust 1.94 (sqlx). License: MIT OR Apache-2.0.
