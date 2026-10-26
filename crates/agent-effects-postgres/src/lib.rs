//! PostgreSQL store for [`agent-effects`](https://crates.io/crates/agent-effects).
//!
//! For production services: many workers on many hosts sharing one database.
//!
//! ```no_run
//! # async fn demo() -> Result<(), agent_effects_store::StoreError> {
//! let store = agent_effects_postgres::PostgresStore::connect("postgres://localhost/effects").await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Sharing an application's database
//!
//! [`PostgresOptions`] fits the store into a database an application already
//! uses, including through PgBouncer:
//!
//! - [`schema`](PostgresOptions::schema) keeps the store's tables and its
//!   migration history in a schema of their own, named in every query. The
//!   application's own `sqlx` migrations and `search_path` are left alone.
//! - [`migrate`](PostgresOptions::migrate) and
//!   [`attach`](PostgresOptions::attach) are separate steps: migrate once over
//!   a direct connection, then attach to the application's pool without
//!   migrating.
//! - [`transaction_pooling`](PostgresOptions::transaction_pooling) makes every
//!   query safe through a pooler in transaction mode.
//!
//! ```no_run
//! # async fn demo(direct: sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
//! use agent_effects_postgres::PostgresOptions;
//! use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
//!
//! let options = PostgresOptions::new()
//!     .schema("agent_effects")
//!     .transaction_pooling(true);
//! // Once, at deploy or startup, over a direct connection to Postgres.
//! options.migrate(&direct).await?;
//! // The application's pool, which goes through PgBouncer. sqlx sends the
//! // `extra_float_digits` startup parameter unless told not to, and a
//! // PgBouncer that does not ignore it refuses the connection.
//! let pooled = PgPoolOptions::new()
//!     .connect_with(
//!         "postgres://app@pgbouncer:6432/app"
//!             .parse::<PgConnectOptions>()?
//!             .extra_float_digits(None),
//!     )
//!     .await?;
//! let store = options.attach(pooled).await?;
//! # Ok(())
//! # }
//! ```
//!
//! # The database's clock
//!
//! By default ([`ClockSource::Database`]), every time the store uses comes
//! from Postgres's `clock_timestamp()`, not from the calling worker:
//!
//! - lease liveness, lease expiry and takeover;
//! - the times recorded on transitions;
//! - the "no live lease" filter of recovery scans.
//!
//! Workers whose clocks disagree therefore still agree on who holds a lease.
//! This removes the clock-skew limit that leases otherwise have. Retry
//! schedules (`next_attempt_at`) are still computed by the worker.
//! [`ClockSource::Caller`] uses the times the runtime passes in instead, as
//! the in-memory and SQLite stores do; the conformance suite needs it to
//! drive time.
//!
//! # Concurrency
//!
//! Every change is "load the row `FOR UPDATE`, apply the pure
//! `EffectRecord` operation, save" in one transaction, so concurrent writers
//! serialize on the row. The update also checks the version it read.
//! Recovery and pending scans read with `FOR UPDATE SKIP LOCKED`: rows
//! another worker is changing right now are skipped, not waited on.

use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use agent_effects_store::{
    EffectEvent, EffectId, EffectKey, EffectKind, EffectName, EffectRecord, EffectStatus,
    EffectStore, ErrorRecord, InsertOutcome, Lease, ListQuery, LogicalKey, NewEffect, PruneQuery,
    StoreError, Transition, TransitionRequest, WorkerId,
};
use serde_json::Value;
use sqlx::migrate::Migrator;
use sqlx::pool::PoolConnection;
use sqlx::postgres::{PgArguments, PgConnectOptions, PgConnection, PgPool, PgPoolOptions, PgRow};
use sqlx::query::Query;
use sqlx::{AssertSqlSafe, Connection, Executor, Postgres, Row, Transaction};
use time::OffsetDateTime;

static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

/// sqlx's default name for the migration history table.
const HISTORY: &str = "_sqlx_migrations";

const COLUMNS: &str = "id, effect_name, logical_key, kind, status, input, input_fingerprint, \
     output, last_error, created_by, attempt_count, may_have_applied, compensation_attempts, \
     approved, next_attempt_at, attempt_started_at, attempt_ended_at, lease_owner, lease_epoch, \
     lease_expires_at, version, created_at, updated_at, committed_at, execution_contract";

/// Whose clock the store trusts for leases and recorded times.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ClockSource {
    /// Postgres's `clock_timestamp()`: one clock for every worker.
    #[default]
    Database,
    /// The times the runtime passes in, from each worker's own clock.
    Caller,
}

/// Where the store keeps its tables and how it talks to the database.
///
/// The defaults are what [`PostgresStore::connect`] uses: tables found
/// through `search_path`, migration history in `_sqlx_migrations`, prepared
/// statements cached per connection, and 10 connections.
#[derive(Clone, Debug)]
pub struct PostgresOptions {
    schema: Option<String>,
    transaction_pooling: bool,
    max_connections: u32,
}

impl Default for PostgresOptions {
    fn default() -> Self {
        Self {
            schema: None,
            transaction_pooling: false,
            max_connections: 10,
        }
    }
}

impl PostgresOptions {
    /// The defaults.
    pub fn new() -> Self {
        Self::default()
    }

    /// Keeps the store's tables in `schema` and names it in every query, so
    /// `search_path` does not matter. The migration history goes in
    /// `schema._sqlx_migrations`, apart from any other `sqlx` migrations in
    /// the database.
    ///
    /// `schema` is a name, used exactly as written (case included), not SQL.
    /// [`migrate`](Self::migrate) creates it if it does not exist.
    #[must_use]
    pub fn schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = Some(schema.into());
        self
    }

    /// For a pool that reaches Postgres through PgBouncer, or another pooler,
    /// in transaction mode.
    ///
    /// Such a pooler may hand each transaction, and each statement outside
    /// one, to a different server connection. So no statement is prepared
    /// for reuse, and every store operation runs inside a transaction, so
    /// that a statement's parse, bind and execution all reach the same
    /// server. Reads that would otherwise run alone cost a `BEGIN` and a
    /// `COMMIT` more.
    ///
    /// The pool is the application's, so its connect options are too. Two of
    /// sqlx's matter behind a pooler: turn off the `extra_float_digits`
    /// startup parameter (`PgConnectOptions::extra_float_digits(None)`)
    /// unless the pooler ignores it, and do not set `search_path` through
    /// `PgConnectOptions::options`, which a pooler does not pass on; use
    /// [`schema`](Self::schema) instead. The store has no float columns, so
    /// it does not depend on `extra_float_digits`.
    #[must_use]
    pub fn transaction_pooling(mut self, enabled: bool) -> Self {
        self.transaction_pooling = enabled;
        self
    }

    /// The size of the pool [`connect`](Self::connect) and
    /// [`connect_with`](Self::connect_with) open; 10 by default.
    /// [`attach`](Self::attach) uses the pool it is given as it is.
    #[must_use]
    pub fn max_connections(mut self, max: u32) -> Self {
        self.max_connections = max;
        self
    }

    /// Connects to `url`, applies migrations, and attaches.
    ///
    /// Migrating needs a direct connection (see [`migrate`](Self::migrate)),
    /// so `url` must not go through a pooler in transaction mode; for that,
    /// [`migrate`](Self::migrate) directly and [`attach`](Self::attach) to the
    /// pooled pool.
    ///
    /// # Errors
    ///
    /// [`StoreError::Backend`] if connecting or migrating fails.
    pub async fn connect(&self, url: &str) -> Result<PostgresStore, StoreError> {
        let options: PgConnectOptions = url.parse().map_err(StoreError::backend)?;
        self.connect_with(options).await
    }

    /// Connects with `options`, applies migrations, and attaches. Like
    /// [`connect`](Self::connect), it needs a direct connection.
    ///
    /// # Errors
    ///
    /// [`StoreError::Backend`] if connecting or migrating fails.
    pub async fn connect_with(
        &self,
        options: PgConnectOptions,
    ) -> Result<PostgresStore, StoreError> {
        let pool = PgPoolOptions::new()
            .max_connections(self.max_connections)
            .connect_with(options)
            .await
            .map_err(StoreError::backend)?;
        self.migrate(&pool).await?;
        self.attach(pool).await
    }

    /// Applies the store's migrations through `pool`.
    ///
    /// `pool` must reach Postgres directly, not through a pooler in
    /// transaction mode: migrations hold a session-level advisory lock, so
    /// concurrent callers are safe. Through such a pooler the lock can be
    /// left held on a server connection, blocking every later migration in
    /// the database, the application's included. With a [schema](Self::schema), the
    /// connection used is closed afterwards instead of going back to the
    /// pool, so `pool` keeps its own `search_path`.
    ///
    /// # Errors
    ///
    /// [`StoreError::Backend`] if migrating fails, or if the history table
    /// holds a different migration under one of the store's versions (as
    /// when it is shared with an application's migrations; give the store a
    /// schema of its own).
    pub async fn migrate(&self, pool: &PgPool) -> Result<(), StoreError> {
        let Some(schema) = &self.schema else {
            return MIGRATOR.run(pool).await.map_err(StoreError::backend);
        };
        let mut conn = pool.acquire().await.map_err(StoreError::backend)?.detach();
        let result = migrate_into(&mut conn, schema).await;
        // A failed close leaves only a connection the server will drop.
        let _ = conn.close().await;
        result
    }

    /// Uses `pool` without migrating; `pool` may go through a pooler (see
    /// [`transaction_pooling`](Self::transaction_pooling) for its connect
    /// options).
    ///
    /// Checks that every migration this version needs has been applied, so a
    /// missing [`migrate`](Self::migrate) fails here rather than on the first
    /// effect.
    ///
    /// # Errors
    ///
    /// [`StoreError::Backend`] if the database is unreachable or a migration
    /// is missing or not the store's own.
    pub async fn attach(&self, pool: PgPool) -> Result<PostgresStore, StoreError> {
        let store = PostgresStore {
            pool,
            clock: ClockSource::Database,
            sql: Arc::new(Statements::new(self)),
        };
        store.check_migrated().await?;
        Ok(store)
    }
}

/// Runs the migrations on `conn` with `schema` as its only `search_path`, so
/// the unqualified names in the migration files land in it.
async fn migrate_into(conn: &mut PgConnection, schema: &str) -> Result<(), StoreError> {
    let quoted = quote_ident(schema);
    // CREATE SCHEMA IF NOT EXISTS needs CREATE on the database even when the
    // schema exists, so leave a schema an administrator made alone.
    let exists: bool =
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = $1)")
            .bind(schema)
            .fetch_one(&mut *conn)
            .await
            .map_err(StoreError::backend)?;
    if !exists {
        conn.execute(AssertSqlSafe(format!(
            "CREATE SCHEMA IF NOT EXISTS {quoted}"
        )))
        .await
        .map_err(StoreError::backend)?;
    }
    conn.execute(AssertSqlSafe(format!("SET search_path TO {quoted}")))
        .await
        .map_err(StoreError::backend)?;
    let mut migrator = Migrator::with_migrations(MIGRATOR.iter().cloned().collect());
    migrator.dangerous_set_table_name(format!("{quoted}.{HISTORY}"));
    migrator.run(conn).await.map_err(StoreError::backend)
}

/// `name` as a quoted SQL identifier.
fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// The store's SQL, with its table names filled in.
#[derive(Debug)]
struct Statements {
    transaction_pooling: bool,
    history: String,
    effects: String,
    events: String,
    lock: Arc<str>,
    insert: Arc<str>,
    get: Arc<str>,
    get_by_key: Arc<str>,
    save: Arc<str>,
    insert_event: Arc<str>,
    list_events: Arc<str>,
}

impl Statements {
    fn new(options: &PostgresOptions) -> Self {
        let qualify = |table: &str| match &options.schema {
            Some(schema) => format!("{}.{table}", quote_ident(schema)),
            None => table.to_owned(),
        };
        let (history, effects, events) = (
            qualify(HISTORY),
            qualify("effects"),
            qualify("effect_events"),
        );
        Self {
            transaction_pooling: options.transaction_pooling,
            lock: format!("SELECT {COLUMNS} FROM {effects} WHERE id = $1 FOR UPDATE").into(),
            insert: format!(
                "INSERT INTO {effects} ({COLUMNS}) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, \
                 $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25) \
                 ON CONFLICT (effect_name, logical_key) DO NOTHING"
            )
            .into(),
            get: format!("SELECT {COLUMNS} FROM {effects} WHERE id = $1").into(),
            get_by_key: format!(
                "SELECT {COLUMNS} FROM {effects} WHERE effect_name = $1 AND logical_key = $2"
            )
            .into(),
            save: format!(
                "UPDATE {effects} SET status = $1, output = $2, last_error = $3, \
                 attempt_count = $4, may_have_applied = $5, compensation_attempts = $6, \
                 approved = $7, next_attempt_at = $8, attempt_started_at = $9, \
                 attempt_ended_at = $10, lease_owner = $11, lease_epoch = $12, \
                 lease_expires_at = $13, version = $14, updated_at = $15, committed_at = $16 \
                 WHERE id = $17 AND version = $18"
            )
            .into(),
            insert_event: format!(
                "INSERT INTO {events} \
                 (effect_id, sequence, transition, from_status, to_status, attempt, actor, \
                 payload, at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)"
            )
            .into(),
            list_events: format!(
                "SELECT effect_id, sequence, transition, from_status, to_status, attempt, \
                 actor, payload, at FROM {events} WHERE effect_id = $1 ORDER BY sequence"
            )
            .into(),
            history,
            effects,
            events,
        }
    }

    /// A query for `sql`, prepared for reuse unless behind a pooler.
    fn query<'q>(&self, sql: impl Into<Arc<str>>) -> Query<'q, Postgres, PgArguments> {
        sqlx::query(AssertSqlSafe(sql.into())).persistent(!self.transaction_pooling)
    }
}

/// The connection one store operation runs on: in a transaction when the
/// pool goes through a transaction-mode pooler, so every statement of the
/// operation reaches the same server.
enum Session {
    Direct(PoolConnection<Postgres>),
    Pinned(Transaction<'static, Postgres>),
}

impl Session {
    /// Ends the operation, committing its transaction if it has one.
    async fn finish(self) -> Result<(), StoreError> {
        match self {
            Self::Direct(_) => Ok(()),
            Self::Pinned(tx) => tx.commit().await.map_err(StoreError::backend),
        }
    }
}

impl Deref for Session {
    type Target = PgConnection;

    fn deref(&self) -> &PgConnection {
        match self {
            Self::Direct(conn) => conn,
            Self::Pinned(tx) => tx,
        }
    }
}

impl DerefMut for Session {
    fn deref_mut(&mut self) -> &mut PgConnection {
        match self {
            Self::Direct(conn) => conn,
            Self::Pinned(tx) => tx,
        }
    }
}

/// An [`EffectStore`] in a PostgreSQL database.
///
/// Cheap to clone; clones share the connection pool.
#[derive(Clone, Debug)]
pub struct PostgresStore {
    pool: PgPool,
    clock: ClockSource,
    sql: Arc<Statements>,
}

impl PostgresStore {
    /// Connects to `url` and applies migrations, with the default
    /// [`PostgresOptions`].
    ///
    /// # Errors
    ///
    /// [`StoreError::Backend`] if connecting or migrating fails.
    pub async fn connect(url: &str) -> Result<Self, StoreError> {
        PostgresOptions::new().connect(url).await
    }

    /// Connects with `options` and applies migrations, with the default
    /// [`PostgresOptions`]. For a schema of the store's own, see
    /// [`PostgresOptions::schema`].
    ///
    /// # Errors
    ///
    /// [`StoreError::Backend`] if connecting or migrating fails.
    pub async fn connect_with(options: PgConnectOptions) -> Result<Self, StoreError> {
        PostgresOptions::new().connect_with(options).await
    }

    /// Uses an existing pool and applies migrations, with the default
    /// [`PostgresOptions`]. Concurrent callers are safe: migrations run under
    /// an advisory lock. To use a pool without migrating, as through
    /// PgBouncer, see [`PostgresOptions::attach`].
    ///
    /// # Errors
    ///
    /// [`StoreError::Backend`] if migrating fails.
    pub async fn from_pool(pool: PgPool) -> Result<Self, StoreError> {
        let options = PostgresOptions::new();
        options.migrate(&pool).await?;
        options.attach(pool).await
    }

    /// Chooses whose clock to trust; see [`ClockSource`].
    #[must_use]
    pub fn with_clock_source(mut self, clock: ClockSource) -> Self {
        self.clock = clock;
        self
    }

    /// The underlying pool.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    async fn session(&self) -> Result<Session, StoreError> {
        if self.sql.transaction_pooling {
            self.pool.begin().await.map(Session::Pinned)
        } else {
            self.pool.acquire().await.map(Session::Direct)
        }
        .map_err(StoreError::backend)
    }

    /// Fails unless every migration is applied, and is the store's own.
    async fn check_migrated(&self) -> Result<(), StoreError> {
        let mut session = self.session().await?;
        let applied = self
            .sql
            .query(format!(
                "SELECT version, checksum FROM {} WHERE success",
                self.sql.history
            ))
            .fetch_all(&mut *session)
            .await;
        let applied = match applied {
            Ok(rows) => rows,
            Err(e)
                if e.as_database_error()
                    .and_then(sqlx::error::DatabaseError::code)
                    .as_deref()
                    == Some("42P01") =>
            {
                return Err(StoreError::backend(format!(
                    "no migration history at {}: run PostgresOptions::migrate first",
                    self.sql.history
                )));
            }
            Err(e) => return Err(StoreError::backend(e)),
        };
        session.finish().await?;
        for migration in MIGRATOR.iter() {
            if migration.migration_type.is_down_migration() {
                continue;
            }
            let found = applied.iter().find_map(|row| {
                (row.try_get::<i64, _>("version").ok()? == migration.version)
                    .then(|| row.try_get::<Vec<u8>, _>("checksum").ok())
            });
            match found {
                None => {
                    return Err(StoreError::backend(format!(
                        "migration {} ({}) is not applied in {}: run PostgresOptions::migrate \
                         first",
                        migration.version, migration.description, self.sql.history
                    )));
                }
                Some(checksum) if checksum.as_deref() != Some(&*migration.checksum) => {
                    return Err(StoreError::backend(format!(
                        "migration {} in {} is not agent-effects' own; give the store a \
                         schema of its own with PostgresOptions::schema",
                        migration.version, self.sql.history
                    )));
                }
                Some(_) => {}
            }
        }
        Ok(())
    }

    async fn database_now(&self, conn: &mut PgConnection) -> Result<SystemTime, StoreError> {
        let now: OffsetDateTime = sqlx::query_scalar("SELECT clock_timestamp()")
            .persistent(!self.sql.transaction_pooling)
            .fetch_one(conn)
            .await
            .map_err(StoreError::backend)?;
        Ok(SystemTime::from(now))
    }

    /// The time to use: the database's when it is the source, else `caller`.
    fn effective(&self, database: SystemTime, caller: SystemTime) -> SystemTime {
        match self.clock {
            ClockSource::Database => database,
            ClockSource::Caller => caller,
        }
    }

    async fn fetch_by_key(
        &self,
        conn: &mut PgConnection,
        key: &EffectKey,
    ) -> Result<Option<EffectRecord>, StoreError> {
        self.sql
            .query(Arc::clone(&self.sql.get_by_key))
            .bind(key.name.as_str())
            .bind(key.key.as_str())
            .fetch_optional(conn)
            .await
            .map_err(StoreError::backend)?
            .as_ref()
            .map(decode_record)
            .transpose()
    }

    async fn fetch(
        &self,
        conn: &mut PgConnection,
        id: EffectId,
    ) -> Result<Option<EffectRecord>, StoreError> {
        self.sql
            .query(Arc::clone(&self.sql.get))
            .bind(*id.as_uuid())
            .fetch_optional(conn)
            .await
            .map_err(StoreError::backend)?
            .as_ref()
            .map(decode_record)
            .transpose()
    }

    /// Loads `id` locked `FOR UPDATE`, applies `change` (given the
    /// database's current time once the lock is held), and saves the
    /// record. The transaction stays open for further writes.
    async fn modify<T>(
        &self,
        id: EffectId,
        change: impl FnOnce(&mut EffectRecord, SystemTime) -> Result<T, StoreError>,
    ) -> Result<(T, EffectRecord, Transaction<'static, Postgres>), StoreError> {
        let mut tx = self.pool.begin().await.map_err(StoreError::backend)?;
        let row = self
            .sql
            .query(Arc::clone(&self.sql.lock))
            .bind(*id.as_uuid())
            .fetch_optional(&mut *tx)
            .await
            .map_err(StoreError::backend)?
            .ok_or(StoreError::NotFound(id))?;
        let mut record = decode_record(&row)?;
        // Read only now: a clock read in the locking statement is taken
        // before waiting for the lock, and can be stale by the whole wait.
        let db_now = self.database_now(&mut tx).await?;
        let read_version = record.version;
        let result = change(&mut record, db_now)?;
        normalize(&mut record);
        self.save(&mut tx, &record, read_version).await?;
        Ok((result, record, tx))
    }

    /// Writes `record` over the row last read at `read_version`.
    async fn save(
        &self,
        tx: &mut Transaction<'static, Postgres>,
        record: &EffectRecord,
        read_version: u64,
    ) -> Result<(), StoreError> {
        let updated = self
            .sql
            .query(Arc::clone(&self.sql.save))
            .bind(record.status.as_str())
            .bind(record.output.clone())
            .bind(json(record.last_error.as_ref())?)
            .bind(to_i32(record.attempt_count)?)
            .bind(record.may_have_applied)
            .bind(to_i32(record.compensation_attempts)?)
            .bind(record.approved)
            .bind(record.next_attempt_at.map(timestamp))
            .bind(record.attempt_started_at.map(timestamp))
            .bind(record.attempt_ended_at.map(timestamp))
            .bind(record.lease_owner.as_ref().map(|w| w.as_str().to_owned()))
            .bind(to_i64(record.lease_epoch)?)
            .bind(record.lease_expires_at.map(timestamp))
            .bind(to_i64(record.version)?)
            .bind(timestamp(record.updated_at))
            .bind(record.committed_at.map(timestamp))
            .bind(*record.id.as_uuid())
            .bind(to_i64(read_version)?)
            .execute(&mut **tx)
            .await
            .map_err(StoreError::backend)?
            .rows_affected();
        if updated == 1 {
            Ok(())
        } else {
            // Unreachable under FOR UPDATE; kept as a second guard.
            Err(StoreError::backend(format!(
                "effect {} changed under a row lock",
                record.id
            )))
        }
    }
}

impl EffectStore for PostgresStore {
    async fn insert_or_get(&self, mut new: NewEffect) -> Result<InsertOutcome, StoreError> {
        let mut session = self.session().await?;
        if self.clock == ClockSource::Database {
            new.now = self.database_now(&mut session).await?;
        }
        let mut record = EffectRecord::new(new);
        normalize(&mut record);
        let inserted = self
            .sql
            .query(Arc::clone(&self.sql.insert))
            .bind(*record.id.as_uuid())
            .bind(record.key.name.as_str())
            .bind(record.key.key.as_str())
            .bind(record.kind.as_str())
            .bind(record.status.as_str())
            .bind(record.input.clone())
            .bind(record.input_fingerprint.clone())
            .bind(record.output.clone())
            .bind(json(record.last_error.as_ref())?)
            .bind(record.created_by.clone())
            .bind(to_i32(record.attempt_count)?)
            .bind(record.may_have_applied)
            .bind(to_i32(record.compensation_attempts)?)
            .bind(record.approved)
            .bind(record.next_attempt_at.map(timestamp))
            .bind(record.attempt_started_at.map(timestamp))
            .bind(record.attempt_ended_at.map(timestamp))
            .bind(record.lease_owner.as_ref().map(|w| w.as_str().to_owned()))
            .bind(to_i64(record.lease_epoch)?)
            .bind(record.lease_expires_at.map(timestamp))
            .bind(to_i64(record.version)?)
            .bind(timestamp(record.created_at))
            .bind(timestamp(record.updated_at))
            .bind(record.committed_at.map(timestamp))
            .bind(json(record.execution_contract.as_ref())?)
            .execute(&mut *session)
            .await
            .map_err(StoreError::backend)?
            .rows_affected()
            == 1;
        if inserted {
            session.finish().await?;
            return Ok(InsertOutcome {
                record,
                inserted: true,
            });
        }
        let existing = self
            .fetch_by_key(&mut session, &record.key)
            .await?
            .ok_or_else(|| StoreError::backend("conflicting insert, but no existing record"))?;
        session.finish().await?;
        Ok(InsertOutcome {
            record: existing,
            inserted: false,
        })
    }

    async fn get(&self, id: EffectId) -> Result<Option<EffectRecord>, StoreError> {
        let mut session = self.session().await?;
        let record = self.fetch(&mut session, id).await?;
        session.finish().await?;
        Ok(record)
    }

    async fn get_by_key(&self, key: &EffectKey) -> Result<Option<EffectRecord>, StoreError> {
        let mut session = self.session().await?;
        let record = self.fetch_by_key(&mut session, key).await?;
        session.finish().await?;
        Ok(record)
    }

    async fn acquire_lease(
        &self,
        id: EffectId,
        owner: &WorkerId,
        now: SystemTime,
        ttl: Duration,
    ) -> Result<Lease, StoreError> {
        let (mut lease, _, tx) = self
            .modify(id, |record, db_now| {
                record.acquire_lease(owner, self.effective(db_now, now), ttl)
            })
            .await?;
        tx.commit().await.map_err(StoreError::backend)?;
        lease.expires_at = round_trip(lease.expires_at);
        Ok(lease)
    }

    async fn renew_lease(
        &self,
        lease: &Lease,
        now: SystemTime,
        ttl: Duration,
    ) -> Result<Lease, StoreError> {
        let (mut renewed, _, tx) = self
            .modify(lease.effect_id, |record, db_now| {
                record.renew_lease(lease, self.effective(db_now, now), ttl)
            })
            .await?;
        tx.commit().await.map_err(StoreError::backend)?;
        renewed.expires_at = round_trip(renewed.expires_at);
        Ok(renewed)
    }

    async fn release_lease(&self, lease: &Lease) -> Result<(), StoreError> {
        let (released, _, tx) = self
            .modify(lease.effect_id, |record, _| Ok(record.release_lease(lease)))
            .await?;
        if released {
            tx.commit().await.map_err(StoreError::backend)?;
        } else {
            tx.rollback().await.map_err(StoreError::backend)?;
        }
        Ok(())
    }

    async fn transition(&self, mut request: TransitionRequest) -> Result<EffectRecord, StoreError> {
        let id = request.id;
        let (mut event, record, mut tx) = self
            .modify(id, |record, db_now| {
                request.now = self.effective(db_now, request.now);
                record.apply(request)
            })
            .await?;
        event.at = round_trip(event.at);
        self.sql
            .query(Arc::clone(&self.sql.insert_event))
            .bind(*event.effect_id.as_uuid())
            .bind(to_i64(event.sequence)?)
            .bind(event.transition.as_str())
            .bind(event.from.as_str())
            .bind(event.to.as_str())
            .bind(to_i32(event.attempt)?)
            .bind(event.actor.clone())
            .bind(event.payload.clone())
            .bind(timestamp(event.at))
            .execute(&mut *tx)
            .await
            .map_err(StoreError::backend)?;
        tx.commit().await.map_err(StoreError::backend)?;
        Ok(record)
    }

    async fn list(&self, query: ListQuery) -> Result<Vec<EffectRecord>, StoreError> {
        let mut sql = format!("SELECT {COLUMNS} FROM {} WHERE TRUE", self.sql.effects);
        let mut next = 1;
        let mut placeholder = || {
            let p = format!("${next}");
            next += 1;
            p
        };
        if !query.statuses.is_empty() {
            let marks: Vec<String> = query.statuses.iter().map(|_| placeholder()).collect();
            sql.push_str(" AND status IN (");
            sql.push_str(&marks.join(", "));
            sql.push(')');
        }
        let lease_filter = query.lease_expired_at.is_some();
        if lease_filter {
            let now = match self.clock {
                ClockSource::Database => "clock_timestamp()".to_owned(),
                ClockSource::Caller => placeholder(),
            };
            sql.push_str(
                " AND (lease_owner IS NULL OR lease_expires_at IS NULL OR lease_expires_at <= ",
            );
            sql.push_str(&now);
            sql.push(')');
        }
        if query.after.is_some() {
            sql.push_str(" AND id > ");
            sql.push_str(&placeholder());
        }
        sql.push_str(" ORDER BY id LIMIT ");
        sql.push_str(&placeholder());
        if lease_filter {
            // A work-finding scan: skip rows another worker is changing.
            sql.push_str(" FOR UPDATE SKIP LOCKED");
        }

        // Only placeholders and the table name are interpolated; every value
        // is bound.
        let mut statement = self.sql.query(sql);
        for status in &query.statuses {
            statement = statement.bind(status.as_str());
        }
        if let (Some(now), ClockSource::Caller) = (query.lease_expired_at, self.clock) {
            statement = statement.bind(timestamp(now));
        }
        if let Some(after) = query.after {
            statement = statement.bind(*after.as_uuid());
        }
        statement = statement.bind(i64::try_from(query.limit).unwrap_or(i64::MAX));
        let mut session = self.session().await?;
        let records = statement
            .fetch_all(&mut *session)
            .await
            .map_err(StoreError::backend)?
            .iter()
            .map(decode_record)
            .collect::<Result<_, _>>()?;
        session.finish().await?;
        Ok(records)
    }

    async fn events(&self, id: EffectId) -> Result<Vec<EffectEvent>, StoreError> {
        let mut session = self.session().await?;
        if self.fetch(&mut session, id).await?.is_none() {
            return Err(StoreError::NotFound(id));
        }
        let events = self
            .sql
            .query(Arc::clone(&self.sql.list_events))
            .bind(*id.as_uuid())
            .fetch_all(&mut *session)
            .await
            .map_err(StoreError::backend)?
            .iter()
            .map(decode_event)
            .collect::<Result<_, _>>()?;
        session.finish().await?;
        Ok(events)
    }

    async fn prune(&self, query: PruneQuery) -> Result<u64, StoreError> {
        let Some(cutoff) = query.cutoff().filter(|_| query.status.is_settled()) else {
            return Ok(0);
        };
        // Rows another worker is changing are skipped, and the ones selected
        // stay locked until both deletes commit.
        let (cutoff_sql, now_sql, limit) = match self.clock {
            ClockSource::Database => (
                "clock_timestamp() - $2 * interval '1 microsecond'",
                "clock_timestamp()",
                "$3",
            ),
            ClockSource::Caller => ("$2", "$3", "$4"),
        };
        let (effects, events) = (&self.sql.effects, &self.sql.events);
        let sql = format!(
            "WITH doomed AS (\
                SELECT id FROM {effects} WHERE status = $1 AND updated_at <= {cutoff_sql} \
                AND (lease_owner IS NULL OR lease_expires_at IS NULL \
                     OR lease_expires_at <= {now_sql}) \
                ORDER BY id LIMIT {limit} FOR UPDATE SKIP LOCKED), \
             events AS (DELETE FROM {events} WHERE effect_id IN (SELECT id FROM doomed)) \
             DELETE FROM {effects} WHERE id IN (SELECT id FROM doomed)"
        );
        // Only fixed fragments and table names are interpolated; every value
        // is bound.
        let mut statement = self.sql.query(sql).bind(query.status.as_str());
        statement = match self.clock {
            ClockSource::Database => statement
                .bind(i64::try_from(query.older_than.as_micros()).map_err(StoreError::backend)?),
            ClockSource::Caller => statement.bind(timestamp(cutoff)).bind(timestamp(query.now)),
        };
        let mut session = self.session().await?;
        let deleted = statement
            .bind(i64::try_from(query.limit).unwrap_or(i64::MAX))
            .execute(&mut *session)
            .await
            .map_err(StoreError::backend)?;
        session.finish().await?;
        Ok(deleted.rows_affected())
    }
}

fn decode_record(row: &PgRow) -> Result<EffectRecord, StoreError> {
    let kind: String = get(row, "kind")?;
    let status: String = get(row, "status")?;
    Ok(EffectRecord {
        id: EffectId::from_uuid(get(row, "id")?),
        key: EffectKey::new(
            EffectName::new(get::<String>(row, "effect_name")?).map_err(StoreError::backend)?,
            LogicalKey::new(get::<String>(row, "logical_key")?).map_err(StoreError::backend)?,
        ),
        kind: EffectKind::parse(&kind).ok_or_else(|| corrupt("kind", &kind))?,
        status: EffectStatus::parse(&status).ok_or_else(|| corrupt("status", &status))?,
        execution_contract: get::<Option<Value>>(row, "execution_contract")?
            .map(serde_json::from_value)
            .transpose()
            .map_err(StoreError::backend)?,
        input: get(row, "input")?,
        input_fingerprint: get(row, "input_fingerprint")?,
        output: get(row, "output")?,
        last_error: get::<Option<Value>>(row, "last_error")?
            .map(serde_json::from_value::<ErrorRecord>)
            .transpose()
            .map_err(StoreError::backend)?,
        created_by: get(row, "created_by")?,
        attempt_count: to_u32(get(row, "attempt_count")?)?,
        may_have_applied: get(row, "may_have_applied")?,
        compensation_attempts: to_u32(get(row, "compensation_attempts")?)?,
        approved: get(row, "approved")?,
        next_attempt_at: get::<Option<OffsetDateTime>>(row, "next_attempt_at")?
            .map(SystemTime::from),
        attempt_started_at: get::<Option<OffsetDateTime>>(row, "attempt_started_at")?
            .map(SystemTime::from),
        attempt_ended_at: get::<Option<OffsetDateTime>>(row, "attempt_ended_at")?
            .map(SystemTime::from),
        lease_owner: get::<Option<String>>(row, "lease_owner")?.map(WorkerId::new),
        lease_epoch: to_u64(get(row, "lease_epoch")?)?,
        lease_expires_at: get::<Option<OffsetDateTime>>(row, "lease_expires_at")?
            .map(SystemTime::from),
        version: to_u64(get(row, "version")?)?,
        created_at: SystemTime::from(get::<OffsetDateTime>(row, "created_at")?),
        updated_at: SystemTime::from(get::<OffsetDateTime>(row, "updated_at")?),
        committed_at: get::<Option<OffsetDateTime>>(row, "committed_at")?.map(SystemTime::from),
    })
}

fn decode_event(row: &PgRow) -> Result<EffectEvent, StoreError> {
    let transition: String = get(row, "transition")?;
    let from: String = get(row, "from_status")?;
    let to: String = get(row, "to_status")?;
    Ok(EffectEvent {
        effect_id: EffectId::from_uuid(get(row, "effect_id")?),
        sequence: to_u64(get(row, "sequence")?)?,
        transition: Transition::parse(&transition)
            .ok_or_else(|| corrupt("transition", &transition))?,
        from: EffectStatus::parse(&from).ok_or_else(|| corrupt("from_status", &from))?,
        to: EffectStatus::parse(&to).ok_or_else(|| corrupt("to_status", &to))?,
        attempt: to_u32(get(row, "attempt")?)?,
        actor: get(row, "actor")?,
        payload: get(row, "payload")?,
        at: SystemTime::from(get::<OffsetDateTime>(row, "at")?),
    })
}

fn get<'r, T>(row: &'r PgRow, column: &str) -> Result<T, StoreError>
where
    T: sqlx::Decode<'r, Postgres> + sqlx::Type<Postgres>,
{
    row.try_get(column).map_err(StoreError::backend)
}

fn corrupt(column: &str, value: &str) -> StoreError {
    StoreError::backend(format!(
        "unreadable {column} in effects database: {value:?}"
    ))
}

fn json<T: serde::Serialize>(value: Option<&T>) -> Result<Option<Value>, StoreError> {
    value
        .map(serde_json::to_value)
        .transpose()
        .map_err(StoreError::backend)
}

fn to_i32(value: u32) -> Result<i32, StoreError> {
    i32::try_from(value).map_err(StoreError::backend)
}

fn to_u32(value: i32) -> Result<u32, StoreError> {
    u32::try_from(value).map_err(StoreError::backend)
}

fn to_i64(value: u64) -> Result<i64, StoreError> {
    i64::try_from(value).map_err(StoreError::backend)
}

fn to_u64(value: i64) -> Result<u64, StoreError> {
    u64::try_from(value).map_err(StoreError::backend)
}

fn timestamp(time: SystemTime) -> OffsetDateTime {
    OffsetDateTime::from(round_trip(time))
}

/// `time` at millisecond precision, the precision the store keeps.
fn round_trip(time: SystemTime) -> SystemTime {
    match time.duration_since(UNIX_EPOCH) {
        Ok(after) => {
            UNIX_EPOCH + Duration::from_millis(u64::try_from(after.as_millis()).unwrap_or(u64::MAX))
        }
        Err(before) => {
            let millis = u64::try_from(before.duration().as_millis()).unwrap_or(u64::MAX);
            UNIX_EPOCH - Duration::from_millis(millis)
        }
    }
}

/// Gives every time in `record` its stored precision, so returned records
/// equal what a later read returns.
fn normalize(record: &mut EffectRecord) {
    for time in [
        &mut record.next_attempt_at,
        &mut record.attempt_started_at,
        &mut record.attempt_ended_at,
        &mut record.lease_expires_at,
        &mut record.committed_at,
    ]
    .into_iter()
    .flatten()
    {
        *time = round_trip(*time);
    }
    record.created_at = round_trip(record.created_at);
    record.updated_at = round_trip(record.updated_at);
}
