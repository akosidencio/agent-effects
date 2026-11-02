//! Command-line arguments.

use std::path::PathBuf;

use agent_effects::{EffectId, EffectStatus};
use clap::{Args, Parser, Subcommand, ValueEnum};

/// Inspect and reconcile agent-effects records in a SQLite or PostgreSQL
/// store.
///
/// Read commands open read-only connections and never change a record.
/// Write commands (approve, deny, resolve, retry) show the effect, ask for
/// confirmation, and apply only if the effect has not changed since it was
/// shown. Authority comes from the database credentials, optionally narrowed
/// by an operator policy file; the --actor label is recorded, never trusted.
#[derive(Debug, Parser)]
#[command(name = "agent-effects", version, max_term_width = 100)]
pub struct Cli {
    /// The store: a PostgreSQL URL (postgres://...), a SQLite URL
    /// (`sqlite://path`) or a SQLite file path. The database must already be
    /// migrated; the CLI never creates or migrates one.
    #[arg(
        long,
        env = "AGENT_EFFECTS_DATABASE_URL",
        global = true,
        hide_env_values = true
    )]
    pub database_url: Option<String>,

    /// PostgreSQL schema holding the store's tables, as set with
    /// `PostgresOptions::schema`.
    #[arg(long, env = "AGENT_EFFECTS_SCHEMA", global = true)]
    pub schema: Option<String>,

    /// The PostgreSQL URL goes through PgBouncer (or another pooler) in
    /// transaction mode.
    #[arg(long, env = "AGENT_EFFECTS_TRANSACTION_POOLING", global = true)]
    pub transaction_pooling: bool,

    /// Refuse every command that changes a record.
    #[arg(long, env = "AGENT_EFFECTS_READ_ONLY", global = true)]
    pub read_only: bool,

    /// Operator policy (JSON): which authenticated identities may run which
    /// write commands. See the CLI's README.
    #[arg(long, env = "AGENT_EFFECTS_OPERATOR_POLICY", global = true)]
    pub policy: Option<PathBuf>,

    /// Output format.
    #[arg(
        long,
        value_enum,
        default_value_t = Format::Text,
        env = "AGENT_EFFECTS_FORMAT",
        global = true
    )]
    pub format: Format,

    /// What to do.
    #[command(subcommand)]
    pub command: Command,
}

/// Output formats.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum Format {
    /// Aligned text for people.
    Text,
    /// One JSON document, for scripts.
    Json,
}

/// The commands.
#[derive(Debug, Subcommand)]
pub enum Command {
    /// List effects, oldest first, optionally by status.
    List {
        /// Only effects in this status (repeatable), e.g. `unknown`.
        #[arg(long = "status", value_parser = parse_status)]
        statuses: Vec<EffectStatus>,
        /// Only `pending` effects with a scheduled retry.
        #[arg(long)]
        scheduled: bool,
        /// Paging.
        #[command(flatten)]
        page: Page,
    },
    /// Effects waiting for a caller, recovery or an operator, with nobody
    /// working on them: unknown, needs intervention, awaiting approval,
    /// scheduled retries, interrupted attempts.
    Pending {
        /// Paging.
        #[command(flatten)]
        page: Page,
    },
    /// One effect in full: status, attempts, retry schedule, lease, contract,
    /// last error and late-result evidence.
    Inspect {
        /// The effect id.
        #[arg(value_parser = parse_id)]
        id: EffectId,
        /// Also print the stored input and output. They may contain personal
        /// or sensitive data the application did not redact.
        #[arg(long)]
        show_payloads: bool,
    },
    /// The audit trail of one effect: every transition, who made it and
    /// why.
    History {
        /// The effect id.
        #[arg(value_parser = parse_id)]
        id: EffectId,
    },
    /// Late-result evidence: what fenced-off or unrecorded attempts reported.
    /// Untrusted, never proof that an effect applied.
    Late {
        /// The effect id.
        #[arg(value_parser = parse_id)]
        id: EffectId,
        /// Paging.
        #[command(flatten)]
        page: Page,
    },
    /// Counts by status, retry schedules and leases.
    Stats,
    /// Approve an effect awaiting approval. Its next attempt may then run.
    Approve {
        /// The effect id.
        #[arg(value_parser = parse_id)]
        id: EffectId,
        /// Who decides and why.
        #[command(flatten)]
        decision: Decision,
    },
    /// Deny an effect awaiting approval. It ends rejected.
    Deny {
        /// The effect id.
        #[arg(value_parser = parse_id)]
        id: EffectId,
        /// Who decides and why.
        #[command(flatten)]
        decision: Decision,
    },
    /// Record what happened to an unknown or escalated effect, after
    /// checking the remote system yourself.
    Resolve {
        /// The effect id.
        #[arg(value_parser = parse_id)]
        id: EffectId,
        /// What happened.
        #[command(flatten)]
        outcome: Outcome,
        /// For --applied: the effect's output as JSON. Later calls receive
        /// it as the result.
        #[arg(long, conflicts_with = "output_file")]
        output: Option<String>,
        /// For --applied: a file holding the output as JSON.
        #[arg(long)]
        output_file: Option<PathBuf>,
        /// For --applied: the schema version of the output (default 1).
        #[arg(long)]
        output_schema_version: Option<u32>,
        /// Who decides and why.
        #[command(flatten)]
        decision: Decision,
    },
    /// Allow an unknown or escalated effect (or a failed compensation) to
    /// run again. Only when you know repeating it is safe.
    Retry {
        /// The effect id.
        #[arg(value_parser = parse_id)]
        id: EffectId,
        /// Who decides and why.
        #[command(flatten)]
        decision: Decision,
    },
}

/// Paging through results by id.
#[derive(Debug, Args)]
pub struct Page {
    /// Continue after this effect id (from the previous page).
    #[arg(long, value_parser = parse_id)]
    pub after: Option<EffectId>,
    /// At most this many results (1 to 1000).
    #[arg(long, default_value_t = 50, value_parser = clap::value_parser!(u16).range(1..=1000))]
    pub limit: u16,
}

/// What an operator found for `resolve`.
#[derive(Debug, Args)]
#[group(required = true, multiple = false)]
pub struct Outcome {
    /// The effect applied.
    #[arg(long)]
    pub applied: bool,
    /// The effect did not apply. It ends failed.
    #[arg(long)]
    pub not_applied: bool,
    /// For a failed compensation: it was undone by hand.
    #[arg(long)]
    pub compensated: bool,
}

/// Who decides and why, for write commands.
#[derive(Debug, Args)]
pub struct Decision {
    /// Why, recorded in the audit trail (and as the error of a denial or a
    /// not-applied resolution).
    #[arg(long)]
    pub reason: String,
    /// A label for the operator, recorded with the authenticated identity.
    /// Not used to authorize anything.
    #[arg(long, env = "AGENT_EFFECTS_OPERATOR")]
    pub actor: Option<String>,
    /// Apply only if the effect is still at this version (as shown by
    /// inspect). Defaults to the version shown before confirming.
    #[arg(long)]
    pub expect_version: Option<u64>,
    /// Do not ask for confirmation. Required when input is not a terminal.
    #[arg(long, short = 'y')]
    pub yes: bool,
}

fn parse_id(text: &str) -> Result<EffectId, String> {
    uuid::Uuid::parse_str(text.trim())
        .map(EffectId::from_uuid)
        .map_err(|_| format!("`{text}` is not an effect id (a UUID)"))
}

fn parse_status(text: &str) -> Result<EffectStatus, String> {
    EffectStatus::parse(text.trim()).ok_or_else(|| {
        let known: Vec<_> = agent_effects::state::ALL_STATUSES
            .iter()
            .map(|s| s.as_str())
            .collect();
        format!("unknown status `{text}`; one of: {}", known.join(", "))
    })
}
