//! Versioned stored payloads and application-supplied JSON migrations.

use std::sync::Arc;

use serde_json::Value;

/// Why a stored payload cannot be read by the current application.
#[derive(Debug, thiserror::Error)]
pub enum PayloadError {
    /// No migration supports this stored schema.
    #[error("unsupported payload schema {stored}; current schema is {current}")]
    UnsupportedVersion {
        /// The version written by the original application.
        stored: u32,
        /// The version expected by the current application.
        current: u32,
    },
    /// The application's migration rejected the payload.
    #[error("payload migration failed: {0}")]
    Migration(String),
}

type Migration = Arc<dyn Fn(u32, Value) -> Result<Value, PayloadError> + Send + Sync>;

/// A current schema version and optional migration from older stored JSON.
///
/// Migrations run only when versions differ. They must be deterministic,
/// perform no external side effects, and avoid including secrets in errors.
/// The runtime deserializes the returned JSON into the current Rust type;
/// stored historical payloads remain unchanged.
#[derive(Clone)]
pub struct PayloadSchema {
    version: u32,
    migration: Option<Migration>,
}

impl Default for PayloadSchema {
    fn default() -> Self {
        Self::new(1)
    }
}

impl PayloadSchema {
    /// Uses `version` for newly serialized payloads. Legacy rows are version 1.
    pub fn new(version: u32) -> Self {
        Self {
            version,
            migration: None,
        }
    }

    /// The version this application writes and expects to read.
    pub fn version(&self) -> u32 {
        self.version
    }

    /// Adds a migration receiving the stored version and JSON. Return an
    /// explicit error for versions the application cannot safely interpret.
    #[must_use]
    pub fn migrate(
        mut self,
        migration: impl Fn(u32, Value) -> Result<Value, PayloadError> + Send + Sync + 'static,
    ) -> Self {
        self.migration = Some(Arc::new(migration));
        self
    }

    /// Reads stored JSON, migrating it if needed. This does not update the
    /// record, its input fingerprint, or its authoritative effect status.
    ///
    /// # Errors
    ///
    /// [`PayloadError::UnsupportedVersion`] when no migration is available,
    /// or an application migration error.
    pub fn decode(&self, stored: u32, value: Value) -> Result<Value, PayloadError> {
        if stored == self.version {
            return Ok(value);
        }
        match &self.migration {
            Some(migrate) => migrate(stored, value),
            None => Err(PayloadError::UnsupportedVersion {
                stored,
                current: self.version,
            }),
        }
    }
}
