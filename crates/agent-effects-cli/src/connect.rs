//! Opening the store, never creating or migrating it.

use std::str::FromStr;
use std::time::Duration;

use agent_effects_postgres::{PostgresOptions, PostgresStore};
use agent_effects_sqlite::SqliteStore;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};

use crate::Failure;
use crate::cli::Cli;

/// An opened store, and who the database says we are.
pub(crate) enum Store {
    Sqlite(SqliteStore),
    Postgres(PostgresStore),
}

/// The identity a policy is checked against: the database role PostgreSQL
/// authenticated, or the local user for a SQLite file (whose permissions are
/// the real boundary).
#[derive(Clone, Debug)]
pub(crate) struct Identity {
    pub(crate) kind: &'static str,
    pub(crate) name: String,
}

/// `write` opens a connection that may change records; otherwise the
/// connection is read-only where the database supports it.
pub(crate) async fn open(cli: &Cli, write: bool) -> Result<(Store, Identity), Failure> {
    let url = cli.database_url.as_deref().ok_or_else(|| {
        Failure::Usage("no database: pass --database-url or set AGENT_EFFECTS_DATABASE_URL".into())
    })?;
    if url.starts_with("postgres://") || url.starts_with("postgresql://") {
        open_postgres(cli, url, write).await
    } else {
        if cli.schema.is_some() || cli.transaction_pooling {
            return Err(Failure::Usage(
                "--schema and --transaction-pooling apply to PostgreSQL only".into(),
            ));
        }
        open_sqlite(url, write).await
    }
}

async fn open_sqlite(url: &str, write: bool) -> Result<(Store, Identity), Failure> {
    let options = if url.starts_with("sqlite:") {
        SqliteConnectOptions::from_str(url)
            .map_err(|e| Failure::Usage(format!("invalid SQLite URL: {e}")))?
    } else {
        SqliteConnectOptions::new().filename(url)
    };
    // Never create a database: a typo must not leave an empty store behind.
    let options = options
        .create_if_missing(false)
        .read_only(!write)
        .journal_mode(SqliteJournalMode::Wal)
        .synchronous(SqliteSynchronous::Full)
        .busy_timeout(Duration::from_secs(5))
        .foreign_keys(true);
    let pool = SqlitePoolOptions::new()
        .max_connections(2)
        .acquire_timeout(Duration::from_secs(10))
        .connect_with(options)
        .await
        .map_err(|e| Failure::Connection(format!("cannot open SQLite database {url}: {e}")))?;
    let store = SqliteStore::attach(pool)
        .await
        .map_err(|e| Failure::Connection(format!("{url} is not an agent-effects store: {e}")))?;
    let name = std::env::var("USER")
        .or_else(|_| std::env::var("USERNAME"))
        .unwrap_or_else(|_| "unknown".into());
    Ok((
        Store::Sqlite(store),
        Identity {
            kind: "local user",
            name,
        },
    ))
}

async fn open_postgres(cli: &Cli, url: &str, write: bool) -> Result<(Store, Identity), Failure> {
    let shown = redact_url(url);
    let mut options = PgConnectOptions::from_str(url)
        .map_err(|e| Failure::Usage(format!("invalid PostgreSQL URL {shown}: {e}")))?;
    if cli.transaction_pooling {
        // A strict PgBouncer refuses startup parameters it does not know.
        options = options.extra_float_digits(None);
    } else if !write {
        // Every transaction on this connection is read-only, enforced by
        // the server. Not available through a transaction pooler, where
        // the role's grants are the guard.
        options = options.options([("default_transaction_read_only", "on")]);
    }
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .acquire_timeout(Duration::from_secs(10))
        .connect_with(options)
        .await
        .map_err(|e| Failure::Connection(format!("cannot connect to {shown}: {e}")))?;
    let mut store_options = PostgresOptions::new().transaction_pooling(cli.transaction_pooling);
    if let Some(schema) = &cli.schema {
        store_options = store_options.schema(schema);
    }
    // In a transaction, so a transaction pooler sends it to one server.
    let identity = async {
        let mut tx = pool.begin().await?;
        let name: String = sqlx::query_scalar("SELECT current_user::text")
            .persistent(false)
            .fetch_one(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok::<_, sqlx::Error>(name)
    }
    .await
    .map_err(|e| Failure::Connection(format!("cannot query {shown}: {e}")))?;
    let store = store_options.attach(pool).await.map_err(|e| {
        Failure::Connection(format!("{shown} has no agent-effects store here: {e}"))
    })?;
    Ok((
        Store::Postgres(store),
        Identity {
            kind: "database role",
            name: identity,
        },
    ))
}

/// The URL with any password replaced, for messages.
pub(crate) fn redact_url(url: &str) -> String {
    let Some((scheme, rest)) = url.split_once("://") else {
        return url.to_owned();
    };
    let (authority, path) = rest.split_once('/').map_or((rest, ""), |(a, p)| (a, p));
    let shown_authority = match authority.rsplit_once('@') {
        Some((credentials, host)) => match credentials.split_once(':') {
            Some((user, _password)) => format!("{user}:***@{host}"),
            None => format!("{credentials}@{host}"),
        },
        None => authority.to_owned(),
    };
    let path = path.split_once('?').map_or(path, |(p, _)| p);
    if rest.contains('/') {
        format!("{scheme}://{shown_authority}/{path}")
    } else {
        format!("{scheme}://{shown_authority}")
    }
}

#[cfg(test)]
mod tests {
    use super::redact_url;

    #[test]
    fn passwords_and_query_strings_never_appear_in_messages() {
        assert_eq!(
            redact_url("postgres://app:s3cret@db.internal:5432/effects?sslmode=require&password=x"),
            "postgres://app:***@db.internal:5432/effects"
        );
        assert_eq!(
            redact_url("postgresql://app@db/effects"),
            "postgresql://app@db/effects"
        );
        assert_eq!(redact_url("postgres://db"), "postgres://db");
        assert_eq!(redact_url("postgres://u:p:q@h/d"), "postgres://u:***@h/d");
    }
}
