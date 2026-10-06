//! SQLite store for [`agent-effects`](https://crates.io/crates/agent-effects).
//!
//! For CLI agents, desktop apps and single-node services. Several processes
//! may share one database file.
//!
//! ```no_run
//! # async fn demo() -> Result<(), agent_effects_store::StoreError> {
//! let store = agent_effects_sqlite::SqliteStore::open("effects.db").await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Durability and concurrency
//!
//! [`SqliteStore::open`] configures:
//!
//! - **WAL** journal, so readers do not block the writer.
//! - **`synchronous = FULL`**: a commit is on disk before it returns. The
//!   runtime persists an attempt *before* calling the remote system. Losing
//!   that write in a power cut would leave a `Pending` record for an effect
//!   that may have run, and the next call would run it again. `NORMAL` would
//!   allow exactly that.
//! - **`busy_timeout`** of 5 seconds, so concurrent writers wait instead of
//!   failing.
//!
//! Every change runs as "load, apply the pure `EffectRecord` operation, save"
//! inside a `BEGIN IMMEDIATE` transaction. That takes the write lock up
//! front, so two writers, even in different processes, cannot both read a
//! record and then both write it. The record update also checks the version
//! it read, as a second guard.
//!
//! Times are stored as Unix milliseconds; sub-millisecond precision is
//! dropped, as the [`EffectStore`] contract allows.

use std::path::Path;
use std::str::FromStr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use agent_effects_store::{
    EffectEvent, EffectId, EffectKey, EffectKind, EffectName, EffectRecord, EffectStatus,
    EffectStore, ErrorRecord, InsertOutcome, Lease, ListQuery, LogicalKey, NewEffect, StoreError,
    Transition, TransitionRequest, WorkerId,
};
use serde::Serialize;
use serde::de::DeserializeOwned;
use sqlx::sqlite::{
    SqliteConnectOptions, SqliteJournalMode, SqlitePool, SqlitePoolOptions, SqliteRow,
    SqliteSynchronous,
};
use sqlx::{AssertSqlSafe, Row, Sqlite, Transaction};
use uuid::Uuid;

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

const COLUMNS: &str = "id, effect_name, logical_key, kind, status, input, input_fingerprint, \
     output, last_error, created_by, attempt_count, may_have_applied, next_attempt_at, \
     attempt_started_at, \
     lease_owner, lease_epoch, lease_expires_at, version, created_at, updated_at, committed_at";

/// An [`EffectStore`] in a SQLite database.
///
/// Cheap to clone; clones share the connection pool.
#[derive(Clone, Debug)]
pub struct SqliteStore {
    pool: SqlitePool,
}

impl SqliteStore {
    /// Opens (creating if needed) the database at `path`, configures it for
    /// durability (see the [crate docs](crate)) and applies migrations.
    ///
    /// # Errors
    ///
    /// [`StoreError::Backend`] if the file cannot be opened or migrated.
    pub async fn open(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Full)
            .busy_timeout(Duration::from_secs(5))
            .foreign_keys(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(8)
            .connect_with(options)
            .await
            .map_err(StoreError::backend)?;
        Self::from_pool(pool).await
    }

    /// Uses an existing pool and applies migrations. The pool's connection
    /// options are the caller's responsibility; see the
    /// [crate docs](crate) for what [`Self::open`] sets and why.
    ///
    /// # Errors
    ///
    /// [`StoreError::Backend`] if migrating fails.
    pub async fn from_pool(pool: SqlitePool) -> Result<Self, StoreError> {
        MIGRATOR.run(&pool).await.map_err(StoreError::backend)?;
        Ok(Self { pool })
    }

    /// The underlying pool.
    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    async fn begin(&self) -> Result<Transaction<'static, Sqlite>, StoreError> {
        self.pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(StoreError::backend)
    }

    /// Loads `id` under a write transaction, applies `change`, and saves the
    /// record. Returns what `change` returned and the saved record, with the
    /// transaction still open for further writes.
    async fn modify<T>(
        &self,
        id: EffectId,
        change: impl FnOnce(&mut EffectRecord) -> Result<T, StoreError>,
    ) -> Result<(T, EffectRecord, Transaction<'static, Sqlite>), StoreError> {
        let mut tx = self.begin().await?;
        let row = sqlx::query(AssertSqlSafe(format!(
            "SELECT {COLUMNS} FROM effects WHERE id = ?"
        )))
        .bind(id.to_string())
        .fetch_optional(&mut *tx)
        .await
        .map_err(StoreError::backend)?;
        let mut record = decode_record(&row.ok_or(StoreError::NotFound(id))?)?;
        let read_version = record.version;
        let result = change(&mut record)?;
        normalize(&mut record);
        save(&mut tx, &record, read_version).await?;
        Ok((result, record, tx))
    }
}

impl EffectStore for SqliteStore {
    async fn insert_or_get(&self, new: NewEffect) -> Result<InsertOutcome, StoreError> {
        let mut record = EffectRecord::new(new);
        normalize(&mut record);
        let inserted = bind_record(
            sqlx::query(AssertSqlSafe(format!(
                "INSERT INTO effects ({COLUMNS}) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
                 ON CONFLICT (effect_name, logical_key) DO NOTHING"
            ))),
            &record,
        )?
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
            "SELECT {COLUMNS} FROM effects WHERE id = ?"
        )))
        .bind(id.to_string())
        .fetch_optional(&self.pool)
        .await
        .map_err(StoreError::backend)?
        .as_ref()
        .map(decode_record)
        .transpose()
    }

    async fn get_by_key(&self, key: &EffectKey) -> Result<Option<EffectRecord>, StoreError> {
        sqlx::query(AssertSqlSafe(format!(
            "SELECT {COLUMNS} FROM effects WHERE effect_name = ? AND logical_key = ?"
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
            .modify(id, |record| record.acquire_lease(owner, now, ttl))
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
            .modify(lease.effect_id, |record| {
                record.renew_lease(lease, now, ttl)
            })
            .await?;
        tx.commit().await.map_err(StoreError::backend)?;
        renewed.expires_at = round_trip(renewed.expires_at);
        Ok(renewed)
    }

    async fn release_lease(&self, lease: &Lease) -> Result<(), StoreError> {
        let (released, _, tx) = self
            .modify(lease.effect_id, |record| Ok(record.release_lease(lease)))
            .await?;
        if released {
            tx.commit().await.map_err(StoreError::backend)?;
        } else {
            tx.rollback().await.map_err(StoreError::backend)?;
        }
        Ok(())
    }

    async fn transition(&self, request: TransitionRequest) -> Result<EffectRecord, StoreError> {
        let id = request.id;
        let (mut event, record, mut tx) = self.modify(id, |record| record.apply(request)).await?;
        event.at = round_trip(event.at);
        sqlx::query(
            "INSERT INTO effect_events \
             (effect_id, sequence, transition, from_status, to_status, attempt, actor, payload, at) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(event.effect_id.to_string())
        .bind(to_i64(event.sequence)?)
        .bind(event.transition.as_str())
        .bind(event.from.as_str())
        .bind(event.to.as_str())
        .bind(i64::from(event.attempt))
        .bind(event.actor.as_deref())
        .bind(json(event.payload.as_ref())?)
        .bind(to_ms(event.at))
        .execute(&mut *tx)
        .await
        .map_err(StoreError::backend)?;
        tx.commit().await.map_err(StoreError::backend)?;
        Ok(record)
    }

    async fn list(&self, query: ListQuery) -> Result<Vec<EffectRecord>, StoreError> {
        let mut sql = format!("SELECT {COLUMNS} FROM effects WHERE 1 = 1");
        if !query.statuses.is_empty() {
            let marks = vec!["?"; query.statuses.len()].join(", ");
            sql.push_str(" AND status IN (");
            sql.push_str(&marks);
            sql.push(')');
        }
        if query.lease_expired_at.is_some() {
            sql.push_str(
                " AND (lease_owner IS NULL OR lease_expires_at IS NULL OR lease_expires_at <= ?)",
            );
        }
        if query.after.is_some() {
            sql.push_str(" AND id > ?");
        }
        sql.push_str(" ORDER BY id LIMIT ?");

        // Only placeholders are interpolated; every value is bound.
        let mut statement = sqlx::query(AssertSqlSafe(sql));
        for status in &query.statuses {
            statement = statement.bind(status.as_str());
        }
        if let Some(now) = query.lease_expired_at {
            statement = statement.bind(to_ms(now));
        }
        if let Some(after) = query.after {
            statement = statement.bind(after.to_string());
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
             payload, at FROM effect_events WHERE effect_id = ? ORDER BY sequence",
        )
        .bind(id.to_string())
        .fetch_all(&self.pool)
        .await
        .map_err(StoreError::backend)?
        .iter()
        .map(decode_event)
        .collect()
    }
}

/// Writes `record` over the row last read at `read_version`.
async fn save(
    tx: &mut Transaction<'static, Sqlite>,
    record: &EffectRecord,
    read_version: u64,
) -> Result<(), StoreError> {
    let updated = sqlx::query(
        "UPDATE effects SET status = ?, output = ?, last_error = ?, attempt_count = ?, \
         may_have_applied = ?, next_attempt_at = ?, attempt_started_at = ?, lease_owner = ?, lease_epoch = ?, \
         lease_expires_at = ?, version = ?, updated_at = ?, committed_at = ? \
         WHERE id = ? AND version = ?",
    )
    .bind(record.status.as_str())
    .bind(json(record.output.as_ref())?)
    .bind(json(record.last_error.as_ref())?)
    .bind(i64::from(record.attempt_count))
    .bind(record.may_have_applied)
    .bind(record.next_attempt_at.map(to_ms))
    .bind(record.attempt_started_at.map(to_ms))
    .bind(record.lease_owner.as_ref().map(WorkerId::as_str))
    .bind(to_i64(record.lease_epoch)?)
    .bind(record.lease_expires_at.map(to_ms))
    .bind(to_i64(record.version)?)
    .bind(to_ms(record.updated_at))
    .bind(record.committed_at.map(to_ms))
    .bind(record.id.to_string())
    .bind(to_i64(read_version)?)
    .execute(&mut **tx)
    .await
    .map_err(StoreError::backend)?
    .rows_affected();
    if updated == 1 {
        Ok(())
    } else {
        // Unreachable under BEGIN IMMEDIATE; kept as a second guard.
        Err(StoreError::backend(format!(
            "effect {} changed under a write lock",
            record.id
        )))
    }
}

type Query<'q> = sqlx::query::Query<'q, Sqlite, sqlx::sqlite::SqliteArguments>;

fn bind_record<'q>(query: Query<'q>, r: &EffectRecord) -> Result<Query<'q>, StoreError> {
    Ok(query
        .bind(r.id.to_string())
        .bind(r.key.name.as_str().to_owned())
        .bind(r.key.key.as_str().to_owned())
        .bind(r.kind.as_str())
        .bind(r.status.as_str())
        .bind(json(r.input.as_ref())?)
        .bind(r.input_fingerprint.clone())
        .bind(json(r.output.as_ref())?)
        .bind(json(r.last_error.as_ref())?)
        .bind(r.created_by.clone())
        .bind(i64::from(r.attempt_count))
        .bind(r.may_have_applied)
        .bind(r.next_attempt_at.map(to_ms))
        .bind(r.attempt_started_at.map(to_ms))
        .bind(r.lease_owner.as_ref().map(|w| w.as_str().to_owned()))
        .bind(to_i64(r.lease_epoch)?)
        .bind(r.lease_expires_at.map(to_ms))
        .bind(to_i64(r.version)?)
        .bind(to_ms(r.created_at))
        .bind(to_ms(r.updated_at))
        .bind(r.committed_at.map(to_ms)))
}

fn decode_record(row: &SqliteRow) -> Result<EffectRecord, StoreError> {
    let id: String = get(row, "id")?;
    let kind: String = get(row, "kind")?;
    let status: String = get(row, "status")?;
    Ok(EffectRecord {
        id: parse_id(&id)?,
        key: EffectKey::new(
            EffectName::new(get::<String>(row, "effect_name")?).map_err(StoreError::backend)?,
            LogicalKey::new(get::<String>(row, "logical_key")?).map_err(StoreError::backend)?,
        ),
        kind: EffectKind::parse(&kind).ok_or_else(|| corrupt("kind", &kind))?,
        status: EffectStatus::parse(&status).ok_or_else(|| corrupt("status", &status))?,
        input: from_json(get(row, "input")?)?,
        input_fingerprint: get(row, "input_fingerprint")?,
        output: from_json(get(row, "output")?)?,
        last_error: from_json::<ErrorRecord>(get(row, "last_error")?)?,
        created_by: get(row, "created_by")?,
        attempt_count: u32::try_from(get::<i64>(row, "attempt_count")?)
            .map_err(StoreError::backend)?,
        may_have_applied: get(row, "may_have_applied")?,
        next_attempt_at: get::<Option<i64>>(row, "next_attempt_at")?.map(from_ms),
        attempt_started_at: get::<Option<i64>>(row, "attempt_started_at")?.map(from_ms),
        lease_owner: get::<Option<String>>(row, "lease_owner")?.map(WorkerId::new),
        lease_epoch: to_u64(get(row, "lease_epoch")?)?,
        lease_expires_at: get::<Option<i64>>(row, "lease_expires_at")?.map(from_ms),
        version: to_u64(get(row, "version")?)?,
        created_at: from_ms(get(row, "created_at")?),
        updated_at: from_ms(get(row, "updated_at")?),
        committed_at: get::<Option<i64>>(row, "committed_at")?.map(from_ms),
    })
}

fn decode_event(row: &SqliteRow) -> Result<EffectEvent, StoreError> {
    let id: String = get(row, "effect_id")?;
    let transition: String = get(row, "transition")?;
    let from: String = get(row, "from_status")?;
    let to: String = get(row, "to_status")?;
    Ok(EffectEvent {
        effect_id: parse_id(&id)?,
        sequence: to_u64(get(row, "sequence")?)?,
        transition: Transition::parse(&transition)
            .ok_or_else(|| corrupt("transition", &transition))?,
        from: EffectStatus::parse(&from).ok_or_else(|| corrupt("from_status", &from))?,
        to: EffectStatus::parse(&to).ok_or_else(|| corrupt("to_status", &to))?,
        attempt: u32::try_from(get::<i64>(row, "attempt")?).map_err(StoreError::backend)?,
        actor: get(row, "actor")?,
        payload: from_json(get(row, "payload")?)?,
        at: from_ms(get(row, "at")?),
    })
}

fn get<'r, T>(row: &'r SqliteRow, column: &str) -> Result<T, StoreError>
where
    T: sqlx::Decode<'r, Sqlite> + sqlx::Type<Sqlite>,
{
    row.try_get(column).map_err(StoreError::backend)
}

fn parse_id(id: &str) -> Result<EffectId, StoreError> {
    Uuid::from_str(id)
        .map(EffectId::from_uuid)
        .map_err(|_| corrupt("id", id))
}

fn corrupt(column: &str, value: &str) -> StoreError {
    StoreError::backend(format!(
        "unreadable {column} in effects database: {value:?}"
    ))
}

fn json<T: Serialize>(value: Option<&T>) -> Result<Option<String>, StoreError> {
    value
        .map(serde_json::to_string)
        .transpose()
        .map_err(StoreError::backend)
}

fn from_json<T: DeserializeOwned>(text: Option<String>) -> Result<Option<T>, StoreError> {
    text.map(|t| serde_json::from_str(&t))
        .transpose()
        .map_err(StoreError::backend)
}

fn to_i64(value: u64) -> Result<i64, StoreError> {
    i64::try_from(value).map_err(StoreError::backend)
}

fn to_u64(value: i64) -> Result<u64, StoreError> {
    u64::try_from(value).map_err(StoreError::backend)
}

fn to_ms(time: SystemTime) -> i64 {
    match time.duration_since(UNIX_EPOCH) {
        Ok(after) => i64::try_from(after.as_millis()).unwrap_or(i64::MAX),
        Err(before) => -i64::try_from(before.duration().as_millis()).unwrap_or(i64::MAX),
    }
}

fn from_ms(ms: i64) -> SystemTime {
    if ms >= 0 {
        UNIX_EPOCH + Duration::from_millis(ms.unsigned_abs())
    } else {
        UNIX_EPOCH - Duration::from_millis(ms.unsigned_abs())
    }
}

/// `time` at the precision it is stored with.
fn round_trip(time: SystemTime) -> SystemTime {
    from_ms(to_ms(time))
}

/// Gives every time in `record` its stored precision, so returned records
/// equal what a later read returns.
fn normalize(record: &mut EffectRecord) {
    for time in [
        &mut record.next_attempt_at,
        &mut record.attempt_started_at,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn times_round_trip_at_millisecond_precision() {
        let t = UNIX_EPOCH + Duration::new(1_700_000_000, 123_456_789);
        assert_eq!(
            round_trip(t),
            UNIX_EPOCH + Duration::new(1_700_000_000, 123_000_000)
        );
        let before = UNIX_EPOCH - Duration::from_millis(1500);
        assert_eq!(round_trip(before), before);
    }
}
