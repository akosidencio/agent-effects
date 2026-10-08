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

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use agent_effects_store::{
    EffectEvent, EffectId, EffectKey, EffectKind, EffectName, EffectRecord, EffectStatus,
    EffectStore, ErrorRecord, InsertOutcome, Lease, ListQuery, LogicalKey, NewEffect, PruneQuery,
    StoreError, Transition, TransitionRequest, WorkerId,
};
use serde_json::Value;
use sqlx::postgres::{PgConnectOptions, PgPool, PgPoolOptions, PgRow};
use sqlx::{AssertSqlSafe, Postgres, Row, Transaction};
use time::OffsetDateTime;

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

const COLUMNS: &str = "id, effect_name, logical_key, kind, status, input, input_fingerprint, \
     output, last_error, created_by, attempt_count, may_have_applied, compensation_attempts, \
     approved, next_attempt_at, attempt_started_at, attempt_ended_at, lease_owner, lease_epoch, \
     lease_expires_at, version, created_at, updated_at, committed_at";

/// Whose clock the store trusts for leases and recorded times.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ClockSource {
    /// Postgres's `clock_timestamp()`: one clock for every worker.
    #[default]
    Database,
    /// The times the runtime passes in, from each worker's own clock.
    Caller,
}

/// An [`EffectStore`] in a PostgreSQL database.
///
/// Cheap to clone; clones share the connection pool.
#[derive(Clone, Debug)]
pub struct PostgresStore {
    pool: PgPool,
    clock: ClockSource,
}

impl PostgresStore {
    /// Connects to `url` and applies migrations.
    ///
    /// # Errors
    ///
    /// [`StoreError::Backend`] if connecting or migrating fails.
    pub async fn connect(url: &str) -> Result<Self, StoreError> {
        let options: PgConnectOptions = url.parse().map_err(StoreError::backend)?;
        Self::connect_with(options).await
    }

    /// Connects with `options` (e.g. a `search_path` for a schema) and
    /// applies migrations.
    ///
    /// # Errors
    ///
    /// [`StoreError::Backend`] if connecting or migrating fails.
    pub async fn connect_with(options: PgConnectOptions) -> Result<Self, StoreError> {
        let pool = PgPoolOptions::new()
            .max_connections(10)
            .connect_with(options)
            .await
            .map_err(StoreError::backend)?;
        Self::from_pool(pool).await
    }

    /// Uses an existing pool and applies migrations. Concurrent callers are
    /// safe: migrations run under an advisory lock.
    ///
    /// # Errors
    ///
    /// [`StoreError::Backend`] if migrating fails.
    pub async fn from_pool(pool: PgPool) -> Result<Self, StoreError> {
        MIGRATOR.run(&pool).await.map_err(StoreError::backend)?;
        Ok(Self {
            pool,
            clock: ClockSource::Database,
        })
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

    async fn database_now(&self) -> Result<SystemTime, StoreError> {
        let now: OffsetDateTime = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&self.pool)
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

    /// Loads `id` locked `FOR UPDATE`, applies `change` (given the
    /// database's current time once the lock is held), and saves the
    /// record. The transaction stays open for further writes.
    async fn modify<T>(
        &self,
        id: EffectId,
        change: impl FnOnce(&mut EffectRecord, SystemTime) -> Result<T, StoreError>,
    ) -> Result<(T, EffectRecord, Transaction<'static, Postgres>), StoreError> {
        let mut tx = self.pool.begin().await.map_err(StoreError::backend)?;
        let row = sqlx::query(AssertSqlSafe(format!(
            "SELECT {COLUMNS} FROM effects WHERE id = $1 FOR UPDATE"
        )))
        .bind(*id.as_uuid())
        .fetch_optional(&mut *tx)
        .await
        .map_err(StoreError::backend)?
        .ok_or(StoreError::NotFound(id))?;
        let mut record = decode_record(&row)?;
        // Read only now: a clock read in the locking statement is taken
        // before waiting for the lock, and can be stale by the whole wait.
        let db_now: OffsetDateTime = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *tx)
            .await
            .map_err(StoreError::backend)?;
        let read_version = record.version;
        let result = change(&mut record, SystemTime::from(db_now))?;
        normalize(&mut record);
        save(&mut tx, &record, read_version).await?;
        Ok((result, record, tx))
    }
}

impl EffectStore for PostgresStore {
    async fn insert_or_get(&self, mut new: NewEffect) -> Result<InsertOutcome, StoreError> {
        if self.clock == ClockSource::Database {
            new.now = self.database_now().await?;
        }
        let mut record = EffectRecord::new(new);
        normalize(&mut record);
        let inserted = sqlx::query(AssertSqlSafe(format!(
            "INSERT INTO effects ({COLUMNS}) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, \
             $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24) \
             ON CONFLICT (effect_name, logical_key) DO NOTHING"
        )))
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
        .execute(&self.pool)
        .await
        .map_err(StoreError::backend)?
        .rows_affected()
            == 1;
        if inserted {
            return Ok(InsertOutcome {
                record,
                inserted: true,
            });
        }
        let existing = self
            .get_by_key(&record.key)
            .await?
            .ok_or_else(|| StoreError::backend("conflicting insert, but no existing record"))?;
        Ok(InsertOutcome {
            record: existing,
            inserted: false,
        })
    }

    async fn get(&self, id: EffectId) -> Result<Option<EffectRecord>, StoreError> {
        sqlx::query(AssertSqlSafe(format!(
            "SELECT {COLUMNS} FROM effects WHERE id = $1"
        )))
        .bind(*id.as_uuid())
        .fetch_optional(&self.pool)
        .await
        .map_err(StoreError::backend)?
        .as_ref()
        .map(decode_record)
        .transpose()
    }

    async fn get_by_key(&self, key: &EffectKey) -> Result<Option<EffectRecord>, StoreError> {
        sqlx::query(AssertSqlSafe(format!(
            "SELECT {COLUMNS} FROM effects WHERE effect_name = $1 AND logical_key = $2"
        )))
        .bind(key.name.as_str())
        .bind(key.key.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(StoreError::backend)?
        .as_ref()
        .map(decode_record)
        .transpose()
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
        sqlx::query(
            "INSERT INTO effect_events \
             (effect_id, sequence, transition, from_status, to_status, attempt, actor, payload, at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
        )
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
        let mut sql = format!("SELECT {COLUMNS} FROM effects WHERE TRUE");
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

        // Only placeholders are interpolated; every value is bound.
        let mut statement = sqlx::query(AssertSqlSafe(sql));
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
        statement
            .fetch_all(&self.pool)
            .await
            .map_err(StoreError::backend)?
            .iter()
            .map(decode_record)
            .collect()
    }

    async fn events(&self, id: EffectId) -> Result<Vec<EffectEvent>, StoreError> {
        if self.get(id).await?.is_none() {
            return Err(StoreError::NotFound(id));
        }
        sqlx::query(
            "SELECT effect_id, sequence, transition, from_status, to_status, attempt, actor, \
             payload, at FROM effect_events WHERE effect_id = $1 ORDER BY sequence",
        )
        .bind(*id.as_uuid())
        .fetch_all(&self.pool)
        .await
        .map_err(StoreError::backend)?
        .iter()
        .map(decode_event)
        .collect()
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
        let sql = format!(
            "WITH doomed AS (\
                SELECT id FROM effects WHERE status = $1 AND updated_at <= {cutoff_sql} \
                AND (lease_owner IS NULL OR lease_expires_at IS NULL \
                     OR lease_expires_at <= {now_sql}) \
                ORDER BY id LIMIT {limit} FOR UPDATE SKIP LOCKED), \
             events AS (DELETE FROM effect_events WHERE effect_id IN (SELECT id FROM doomed)) \
             DELETE FROM effects WHERE id IN (SELECT id FROM doomed)"
        );
        // Only fixed fragments are interpolated; every value is bound.
        let mut statement = sqlx::query(AssertSqlSafe(sql)).bind(query.status.as_str());
        statement = match self.clock {
            ClockSource::Database => statement
                .bind(i64::try_from(query.older_than.as_micros()).map_err(StoreError::backend)?),
            ClockSource::Caller => statement.bind(timestamp(cutoff)).bind(timestamp(query.now)),
        };
        let deleted = statement
            .bind(i64::try_from(query.limit).unwrap_or(i64::MAX))
            .execute(&self.pool)
            .await
            .map_err(StoreError::backend)?;
        Ok(deleted.rows_affected())
    }
}

/// Writes `record` over the row last read at `read_version`.
async fn save(
    tx: &mut Transaction<'static, Postgres>,
    record: &EffectRecord,
    read_version: u64,
) -> Result<(), StoreError> {
    let updated = sqlx::query(
        "UPDATE effects SET status = $1, output = $2, last_error = $3, attempt_count = $4, \
         may_have_applied = $5, compensation_attempts = $6, approved = $7, \
         next_attempt_at = $8, attempt_started_at = $9, attempt_ended_at = $10, \
         lease_owner = $11, lease_epoch = $12, lease_expires_at = $13, version = $14, \
         updated_at = $15, committed_at = $16 \
         WHERE id = $17 AND version = $18",
    )
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

fn json(error: Option<&ErrorRecord>) -> Result<Option<Value>, StoreError> {
    error
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
