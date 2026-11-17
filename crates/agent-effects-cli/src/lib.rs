//! Operator CLI for [`agent-effects`](https://crates.io/crates/agent-effects).
//!
//! Installs the `agent-effects` binary, which inspects and reconciles
//! effects in a SQLite or PostgreSQL store without editing records by hand.
//! See the crate's README for commands, configuration, exit codes and the
//! authorization model. The library exposes [`run`] for embedding the same
//! commands, and [`policy`] for the operator policy format.
//!
//! The CLI is a local process: it opens no network listener and exposes no
//! operation remotely.

pub mod cli;
mod connect;
mod output;
pub mod policy;

use std::fmt::Write as _;
use std::io::Write;
use std::time::SystemTime;

use agent_effects::store::{EffectStore, ListQuery, StoreError};
use agent_effects::{
    EffectId, EffectRecord, EffectStatus, OperatorDecision, Resolution, Runtime, RuntimeError,
};
use serde_json::{Value, json};

use crate::cli::{Cli, Command, Decision, Format, Outcome, Page};
use crate::connect::{Identity, Store};
use crate::policy::{Permission, Policy};

/// Exit codes, also documented in the README.
pub mod exit {
    /// Success.
    pub const OK: u8 = 0;
    /// An unexpected error.
    pub const ERROR: u8 = 1;
    /// Invalid arguments or configuration.
    pub const USAGE: u8 = 2;
    /// No effect with that id.
    pub const NOT_FOUND: u8 = 3;
    /// The effect is not in a state the command applies to, a worker holds
    /// it, or it changed since it was shown.
    pub const CONFLICT: u8 = 4;
    /// Refused: read-only mode, not confirmed, or not allowed by the policy.
    pub const REFUSED: u8 = 5;
    /// The database could not be reached or is not an agent-effects store.
    pub const CONNECTION: u8 = 6;
}

/// Why a command did not succeed; each maps to an [exit code](exit).
#[derive(Debug)]
pub enum Failure {
    /// [`exit::USAGE`].
    Usage(String),
    /// [`exit::NOT_FOUND`].
    NotFound(String),
    /// [`exit::CONFLICT`].
    Conflict(String),
    /// [`exit::REFUSED`].
    Refused(String),
    /// [`exit::CONNECTION`].
    Connection(String),
    /// [`exit::ERROR`].
    Other(String),
}

impl Failure {
    /// The process exit code.
    pub fn code(&self) -> u8 {
        match self {
            Self::Usage(_) => exit::USAGE,
            Self::NotFound(_) => exit::NOT_FOUND,
            Self::Conflict(_) => exit::CONFLICT,
            Self::Refused(_) => exit::REFUSED,
            Self::Connection(_) => exit::CONNECTION,
            Self::Other(_) => exit::ERROR,
        }
    }

    fn message(&self) -> &str {
        match self {
            Self::Usage(m)
            | Self::NotFound(m)
            | Self::Conflict(m)
            | Self::Refused(m)
            | Self::Connection(m)
            | Self::Other(m) => m,
        }
    }
}

impl From<StoreError> for Failure {
    fn from(error: StoreError) -> Self {
        match error {
            StoreError::NotFound(id) => Self::NotFound(format!("no effect {id}")),
            StoreError::VersionConflict { expected, actual } => Self::Conflict(format!(
                "the effect changed since it was shown (version {expected}, now {actual}); \
                 inspect it again"
            )),
            StoreError::LeaseHeld { owner, .. } => Self::Conflict(format!(
                "worker {owner} is working on the effect; try again when it is done"
            )),
            StoreError::InvalidTransition(e) => Self::Conflict(e.to_string()),
            // The database's grants, not the CLI, refused it.
            other if other.to_string().contains("permission denied") => {
                Self::Refused(format!("the database refused it: {other}"))
            }
            other => Self::Other(other.to_string()),
        }
    }
}

impl From<RuntimeError> for Failure {
    fn from(error: RuntimeError) -> Self {
        match error {
            RuntimeError::Store(e) => e.into(),
            other => Self::Other(other.to_string()),
        }
    }
}

/// Asks the operator to confirm a write.
pub trait Confirm {
    /// Shows `prompt` and returns the operator's answer, or `None` if
    /// nobody can be asked (input is not a terminal).
    fn confirm(&mut self, prompt: &str) -> Option<bool>;
}

/// Where a command's output and questions go.
pub struct Io<'a> {
    /// Results.
    pub out: &'a mut (dyn Write + Send),
    /// Messages, prompts and errors.
    pub err: &'a mut (dyn Write + Send),
    /// Confirmation of writes.
    pub confirm: &'a mut (dyn Confirm + Send),
}

/// Runs a parsed command and returns its exit code. Errors are written to
/// `io.err` (as JSON with `--format json`).
pub async fn run(cli: Cli, io: &mut Io<'_>) -> u8 {
    match execute(&cli, io).await {
        Ok(()) => exit::OK,
        Err(failure) => {
            let _ = match cli.format {
                Format::Json => writeln!(
                    io.err,
                    "{}",
                    json!({ "error": failure.message(), "exit_code": failure.code() })
                ),
                Format::Text => writeln!(io.err, "error: {}", failure.message()),
            };
            failure.code()
        }
    }
}

async fn execute(cli: &Cli, io: &mut Io<'_>) -> Result<(), Failure> {
    let write = matches!(
        cli.command,
        Command::Approve { .. }
            | Command::Deny { .. }
            | Command::Resolve { .. }
            | Command::Retry { .. }
    );
    if write && cli.read_only {
        return Err(Failure::Refused(
            "read-only mode: write commands are disabled".into(),
        ));
    }
    let policy = load_policy(cli)?;
    let (store, identity) = connect::open(cli, write).await?;
    match store {
        Store::Sqlite(store) => dispatch(cli, io, store, &identity, policy.as_ref()).await,
        Store::Postgres(store) => dispatch(cli, io, store, &identity, policy.as_ref()).await,
    }
}

fn load_policy(cli: &Cli) -> Result<Option<Policy>, Failure> {
    let Some(path) = &cli.policy else {
        return Ok(None);
    };
    let text = std::fs::read_to_string(path)
        .map_err(|e| Failure::Usage(format!("cannot read policy {}: {e}", path.display())))?;
    Policy::from_json(&text)
        .map(Some)
        .map_err(|e| Failure::Usage(format!("invalid policy {}: {e}", path.display())))
}

async fn dispatch<S: EffectStore>(
    cli: &Cli,
    io: &mut Io<'_>,
    store: S,
    identity: &Identity,
    policy: Option<&Policy>,
) -> Result<(), Failure> {
    let now = SystemTime::now();
    let format = cli.format;
    match &cli.command {
        Command::List {
            statuses,
            scheduled,
            page,
        } => {
            let mut statuses = statuses.clone();
            if *scheduled {
                statuses = vec![EffectStatus::Pending];
            }
            let mut query = ListQuery::statuses(statuses).limit(usize::from(page.limit));
            query.after = page.after;
            let mut records = store.list(query).await?;
            let next = next_cursor(&records, page);
            if *scheduled {
                records.retain(|r| r.next_attempt_at.is_some());
            }
            print_list(io, format, &records, next.as_deref(), now)
        }
        Command::Pending { page } => {
            let runtime = Runtime::new(store);
            let records = runtime.pending(page.after, usize::from(page.limit)).await?;
            let next = next_cursor(&records, page);
            print_list(io, format, &records, next.as_deref(), now)
        }
        Command::Inspect { id, show_payloads } => {
            let record = get(&store, *id).await?;
            let late = store.late_observations(*id, None, 100).await?;
            let view = output::detail(&record, &late, now, *show_payloads);
            print_value(io, format, &view, |v| output::fields(v, 0))
        }
        Command::History { id } => {
            get(&store, *id).await?;
            let events: Vec<Value> = store
                .events(*id)
                .await?
                .iter()
                .map(output::event_view)
                .collect();
            let view = json!({ "effect": id.to_string(), "events": events });
            print_value(io, format, &view, |v| {
                output::table(
                    v["events"].as_array().map_or(&[], Vec::as_slice),
                    &[
                        "sequence",
                        "at",
                        "transition",
                        "from",
                        "to",
                        "attempt",
                        "actor",
                        "payload",
                    ],
                )
            })
        }
        Command::Late { id, page } => {
            get(&store, *id).await?;
            let late = store
                .late_observations(*id, page.after, usize::from(page.limit))
                .await?;
            let next = (late.len() == usize::from(page.limit))
                .then(|| late.last().map(|l| l.id.to_string()))
                .flatten();
            let rows: Vec<Value> = late.iter().map(output::late_view).collect();
            let view =
                json!({ "effect": id.to_string(), "observations": rows, "next_after": next });
            print_value(io, format, &view, |v| {
                output::table(
                    v["observations"].as_array().map_or(&[], Vec::as_slice),
                    &[
                        "id",
                        "worker",
                        "lease_epoch",
                        "attempt",
                        "observed_at",
                        "outcome",
                    ],
                )
            })
        }
        Command::Stats => {
            let view = stats(&store, now).await?;
            print_value(io, format, &view, |v| output::fields(v, 0))
        }
        Command::Approve { .. }
        | Command::Deny { .. }
        | Command::Resolve { .. }
        | Command::Retry { .. } => dispatch_write(cli, io, store, identity, policy, now).await,
    }
}

async fn dispatch_write<S: EffectStore>(
    cli: &Cli,
    io: &mut Io<'_>,
    store: S,
    identity: &Identity,
    policy: Option<&Policy>,
    now: SystemTime,
) -> Result<(), Failure> {
    let format = cli.format;
    match &cli.command {
        Command::Approve { id, decision } => {
            let operator = Operator::new(identity, policy, Permission::Approve, decision)?;
            let decision = write_command(io, format, &store, *id, operator, "approve", |record| {
                require(record, &[EffectStatus::AwaitingApproval], "approve")
            })
            .await?;
            let runtime = Runtime::new(store);
            let record = runtime.approve_as(*id, decision).await?;
            print_written(io, format, &record, now)
        }
        Command::Deny { id, decision } => {
            let operator = Operator::new(identity, policy, Permission::Deny, decision)?;
            let decision = write_command(io, format, &store, *id, operator, "deny", |record| {
                require(record, &[EffectStatus::AwaitingApproval], "deny")
            })
            .await?;
            let runtime = Runtime::new(store);
            let record = runtime.deny_as(*id, decision).await?;
            print_written(io, format, &record, now)
        }
        Command::Resolve {
            id,
            outcome,
            output,
            output_file,
            output_schema_version,
            decision,
        } => {
            let operator = Operator::new(identity, policy, Permission::Resolve, decision)?;
            let resolution = resolution(
                outcome,
                output.as_deref(),
                output_file.as_deref(),
                *output_schema_version,
            )?;
            let (allowed, what): (&[EffectStatus], _) = match resolution {
                Resolution::Compensated => {
                    (&[EffectStatus::CompensationFailed], "resolve --compensated")
                }
                _ => (
                    &[EffectStatus::Unknown, EffectStatus::NeedsIntervention],
                    "resolve",
                ),
            };
            let operator = Operator {
                decision: operator
                    .decision
                    .output_schema_version(output_schema_version.unwrap_or(1)),
                ..operator
            };
            let decision = write_command(io, format, &store, *id, operator, what, |record| {
                require(record, allowed, what)
            })
            .await?;
            let runtime = Runtime::new(store);
            let record = runtime.resolve_as(*id, resolution, decision).await?;
            print_written(io, format, &record, now)
        }
        Command::Retry { id, decision } => {
            let operator = Operator::new(identity, policy, Permission::Retry, decision)?;
            let decision = write_command(io, format, &store, *id, operator, "retry", |record| {
                require(
                    record,
                    &[
                        EffectStatus::Unknown,
                        EffectStatus::NeedsIntervention,
                        EffectStatus::CompensationFailed,
                    ],
                    "retry",
                )
            })
            .await?;
            let runtime = Runtime::new(store);
            let record = runtime.resolve_as(*id, Resolution::Retry, decision).await?;
            print_written(io, format, &record, now)
        }
        _ => Err(Failure::Other("not a write command".into())),
    }
}

/// An authorized operator about to decide.
struct Operator {
    decision: OperatorDecision,
    yes: bool,
}

impl Operator {
    fn new(
        identity: &Identity,
        policy: Option<&Policy>,
        permission: Permission,
        decision: &Decision,
    ) -> Result<Self, Failure> {
        if let Some(policy) = policy
            && !policy.allows(&identity.name, permission)
        {
            return Err(Failure::Refused(format!(
                "{} {} may not {} (operator policy)",
                identity.kind,
                identity.name,
                permission.as_str()
            )));
        }
        if decision.reason.trim().is_empty() {
            return Err(Failure::Usage("--reason must not be empty".into()));
        }
        // The label is recorded next to the identity the database
        // authenticated, never instead of it.
        let actor = match &decision.actor {
            Some(label) => format!(
                "operator:{label} (agent-effects-cli, {} {})",
                identity.kind, identity.name
            ),
            None => format!(
                "operator:{} (agent-effects-cli, {})",
                identity.name, identity.kind
            ),
        };
        let mut operator_decision = OperatorDecision::new(actor, decision.reason.clone());
        if let Some(version) = decision.expect_version {
            operator_decision = operator_decision.expect_version(version);
        }
        Ok(Self {
            decision: operator_decision,
            yes: decision.yes,
        })
    }
}

/// Shows the effect, checks it is in a state the command applies to, asks
/// for confirmation, and returns the decision pinned to the version shown.
async fn write_command<S: EffectStore>(
    io: &mut Io<'_>,
    format: Format,
    store: &S,
    id: EffectId,
    operator: Operator,
    what: &str,
    check: impl FnOnce(&EffectRecord) -> Result<(), Failure>,
) -> Result<OperatorDecision, Failure> {
    let record = get(store, id).await?;
    if let Some(expected) = operator.decision.expected_version
        && expected != record.version
    {
        return Err(StoreError::VersionConflict {
            expected,
            actual: record.version,
        }
        .into());
    }
    check(&record)?;
    if let Some(holder) = record.live_lease_owner(SystemTime::now()) {
        return Err(Failure::Conflict(format!(
            "worker {holder} is working on the effect; try again when it is done"
        )));
    }
    if !operator.yes {
        if format == Format::Text {
            let _ = writeln!(
                io.err,
                "{}",
                output::fields(&output::summary(&record, SystemTime::now()), 2)
            );
        }
        let prompt = format!(
            "{what} effect {id} ({} {}, version {})? Type 'yes' to confirm: ",
            record.key, record.status, record.version
        );
        match io.confirm.confirm(&prompt) {
            Some(true) => {}
            Some(false) => return Err(Failure::Refused("not confirmed".into())),
            None => {
                return Err(Failure::Refused(
                    "input is not a terminal: pass --yes to confirm".into(),
                ));
            }
        }
    }
    // The decision applies only while the record is still at the version
    // shown; the store re-checks it atomically with the write.
    Ok(operator.decision.expect_version(record.version))
}

fn require(record: &EffectRecord, allowed: &[EffectStatus], what: &str) -> Result<(), Failure> {
    if allowed.contains(&record.status) {
        return Ok(());
    }
    let allowed: Vec<_> = allowed.iter().map(|s| s.as_str()).collect();
    Err(Failure::Conflict(format!(
        "effect {} is {}; {what} applies to {}",
        record.id,
        record.status,
        allowed.join(" or ")
    )))
}

fn resolution(
    outcome: &Outcome,
    output: Option<&str>,
    output_file: Option<&std::path::Path>,
    output_schema_version: Option<u32>,
) -> Result<Resolution, Failure> {
    let output_given = output.is_some() || output_file.is_some() || output_schema_version.is_some();
    if output_given && !outcome.applied {
        return Err(Failure::Usage(
            "--output, --output-file and --output-schema-version go with --applied only".into(),
        ));
    }
    if outcome.not_applied {
        return Ok(Resolution::NotApplied);
    }
    if outcome.compensated {
        return Ok(Resolution::Compensated);
    }
    let text = match (output, output_file) {
        (Some(text), _) => Some(text.to_owned()),
        (None, Some(path)) => Some(
            std::fs::read_to_string(path)
                .map_err(|e| Failure::Usage(format!("cannot read {}: {e}", path.display())))?,
        ),
        (None, None) => None,
    };
    let output = text
        .map(|t| serde_json::from_str(&t))
        .transpose()
        .map_err(|e| Failure::Usage(format!("the output is not valid JSON: {e}")))?;
    Ok(Resolution::Applied { output })
}

async fn get<S: EffectStore>(store: &S, id: EffectId) -> Result<EffectRecord, Failure> {
    store
        .get(id)
        .await?
        .ok_or_else(|| Failure::NotFound(format!("no effect {id}")))
}

fn next_cursor(records: &[EffectRecord], page: &Page) -> Option<String> {
    (records.len() == usize::from(page.limit))
        .then(|| records.last().map(|r| r.id.to_string()))
        .flatten()
}

async fn stats<S: EffectStore>(store: &S, now: SystemTime) -> Result<Value, Failure> {
    let mut by_status: std::collections::BTreeMap<&'static str, u64> =
        agent_effects::state::ALL_STATUSES
            .iter()
            .map(|s| (s.as_str(), 0))
            .collect();
    let (mut total, mut scheduled, mut due, mut leased, mut doubtful) =
        (0u64, 0u64, 0u64, 0u64, 0u64);
    let mut after = None;
    loop {
        let mut query = ListQuery::statuses([]).limit(1000);
        query.after = after;
        let page = store.list(query).await?;
        for record in &page {
            total += 1;
            *by_status.entry(record.status.as_str()).or_default() += 1;
            if let Some(at) = record.next_attempt_at
                && matches!(
                    record.status,
                    EffectStatus::Pending | EffectStatus::Compensating
                )
            {
                scheduled += 1;
                if at <= now {
                    due += 1;
                }
            }
            if record.live_lease_owner(now).is_some() {
                leased += 1;
            }
            if record.may_have_applied && !record.status.is_settled() {
                doubtful += 1;
            }
        }
        if page.len() < 1000 {
            break;
        }
        after = page.last().map(|r| r.id);
    }
    Ok(json!({
        "total": total,
        "by_status": by_status,
        "retries_scheduled": scheduled,
        "retries_due": due,
        "leased_now": leased,
        "unsettled_and_may_have_applied": doubtful,
        "measured_at": output::timestamp(now),
    }))
}

fn print_list(
    io: &mut Io<'_>,
    format: Format,
    records: &[EffectRecord],
    next: Option<&str>,
    now: SystemTime,
) -> Result<(), Failure> {
    let rows: Vec<Value> = records.iter().map(|r| output::summary(r, now)).collect();
    let view = json!({ "effects": rows, "next_after": next });
    print_value(io, format, &view, |v| {
        let mut text = output::table(
            v["effects"].as_array().map_or(&[], Vec::as_slice),
            &[
                "id",
                "name",
                "key",
                "status",
                "attempts",
                "updated_at",
                "next_attempt_at",
            ],
        );
        if let Some(next) = v["next_after"].as_str() {
            let _ = writeln!(text, "more: --after {next}");
        }
        text
    })
}

fn print_written(
    io: &mut Io<'_>,
    format: Format,
    record: &EffectRecord,
    now: SystemTime,
) -> Result<(), Failure> {
    let view = output::summary(record, now);
    print_value(io, format, &view, |v| output::fields(v, 0))
}

fn print_value(
    io: &mut Io<'_>,
    format: Format,
    value: &Value,
    text: impl FnOnce(&Value) -> String,
) -> Result<(), Failure> {
    let rendered = match format {
        Format::Json => {
            serde_json::to_string_pretty(value).map_err(|e| Failure::Other(e.to_string()))? + "\n"
        }
        Format::Text => text(value),
    };
    io.out
        .write_all(rendered.as_bytes())
        .map_err(|e| Failure::Other(format!("cannot write output: {e}")))
}
