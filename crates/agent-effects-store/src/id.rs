//! Effect identity: record ids, logical keys and remote idempotency keys.

use std::fmt;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Maximum length, in bytes, of an [`EffectName`].
pub const MAX_NAME_LEN: usize = 200;

/// Maximum length, in bytes, of a [`LogicalKey`].
pub const MAX_KEY_LEN: usize = 512;

/// Namespace for deriving remote idempotency keys.
///
/// Part of the stability contract: changing it changes every derived key, so
/// a retry issued by a new version would no longer deduplicate against an
/// attempt issued by an old one.
const IDEMPOTENCY_NAMESPACE: Uuid = Uuid::from_u128(0x5b2f_9c1e_7a4d_4e0b_9f3a_6c8d_1e2f_4a70);

/// Durable identity of one effect record.
///
/// A version 7 UUID, so ids sort by creation time.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct EffectId(Uuid);

impl EffectId {
    /// Generates a new time-ordered id.
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    /// Wraps an existing UUID, e.g. one read back from a store.
    pub const fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }

    /// The underlying UUID.
    pub const fn as_uuid(&self) -> &Uuid {
        &self.0
    }
}

impl Default for EffectId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for EffectId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Why an [`EffectName`] or [`LogicalKey`] was rejected.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum IdentityError {
    /// The value was empty.
    #[error("{0} must not be empty")]
    Empty(&'static str),
    /// The value exceeded its maximum length.
    #[error("{what} is {len} bytes, maximum is {max}")]
    TooLong {
        /// Which value was too long.
        what: &'static str,
        /// Its length in bytes.
        len: usize,
        /// The allowed maximum.
        max: usize,
    },
    /// The value contained a control character.
    #[error("{0} must not contain control characters")]
    ControlCharacter(&'static str),
}

fn validate(what: &'static str, value: &str, max: usize) -> Result<(), IdentityError> {
    if value.is_empty() {
        return Err(IdentityError::Empty(what));
    }
    if value.len() > max {
        return Err(IdentityError::TooLong {
            what,
            len: value.len(),
            max,
        });
    }
    if value.chars().any(char::is_control) {
        return Err(IdentityError::ControlCharacter(what));
    }
    Ok(())
}

/// The type of an effect, e.g. `payment.charge`.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct EffectName(String);

impl EffectName {
    /// Validates and wraps a name.
    ///
    /// # Errors
    ///
    /// Rejects empty names, names over [`MAX_NAME_LEN`] bytes and names
    /// containing control characters.
    pub fn new(name: impl Into<String>) -> Result<Self, IdentityError> {
        let name = name.into();
        validate("effect name", &name, MAX_NAME_LEN)?;
        Ok(Self(name))
    }

    /// The name as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for EffectName {
    type Error = IdentityError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<EffectName> for String {
    fn from(value: EffectName) -> Self {
        value.0
    }
}

impl fmt::Display for EffectName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// The application's identifier for one logical occurrence of an effect,
/// e.g. the order id for `payment.charge`.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct LogicalKey(String);

impl LogicalKey {
    /// Validates and wraps a key.
    ///
    /// # Errors
    ///
    /// Rejects empty keys, keys over [`MAX_KEY_LEN`] bytes and keys containing
    /// control characters.
    pub fn new(key: impl Into<String>) -> Result<Self, IdentityError> {
        let key = key.into();
        validate("logical key", &key, MAX_KEY_LEN)?;
        Ok(Self(key))
    }

    /// The key as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for LogicalKey {
    type Error = IdentityError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<LogicalKey> for String {
    fn from(value: LogicalKey) -> Self {
        value.0
    }
}

impl fmt::Display for LogicalKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// The unique identity of a logical effect: `(name, logical key)`.
///
/// Stores enforce that at most one record exists per `EffectKey`.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct EffectKey {
    /// The effect type.
    pub name: EffectName,
    /// The logical occurrence.
    pub key: LogicalKey,
}

impl EffectKey {
    /// Pairs a name with a logical key.
    pub const fn new(name: EffectName, key: LogicalKey) -> Self {
        Self { name, key }
    }

    /// The idempotency key to forward to remote systems.
    ///
    /// Derived deterministically (a version 5 UUID) from the name and logical key, so
    /// every attempt of the same logical effect sends the same value, even if
    /// its record is lost and recreated.
    pub fn idempotency_key(&self) -> IdempotencyKey {
        // Length-prefixing the name keeps ("a:b", "c") and ("a", "b:c") apart.
        let material = format!("{}:{}:{}", self.name.0.len(), self.name.0, self.key.0);
        IdempotencyKey(Uuid::new_v5(&IDEMPOTENCY_NAMESPACE, material.as_bytes()))
    }
}

impl fmt::Display for EffectKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.name, self.key)
    }
}

/// A key a remote system can use to deduplicate requests, such as an HTTP
/// `Idempotency-Key` header.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct IdempotencyKey(Uuid);

impl IdempotencyKey {
    /// The underlying UUID.
    pub const fn as_uuid(&self) -> &Uuid {
        &self.0
    }
}

impl fmt::Display for IdempotencyKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Identifies a runtime instance that holds execution leases.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct WorkerId(String);

impl WorkerId {
    /// Wraps a caller-chosen worker id, e.g. a hostname plus process id.
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// Generates a random worker id.
    pub fn random() -> Self {
        Self(Uuid::now_v7().to_string())
    }

    /// The id as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for WorkerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(name: &str, key: &str) -> EffectKey {
        EffectKey::new(
            EffectName::new(name).unwrap(),
            LogicalKey::new(key).unwrap(),
        )
    }

    #[test]
    fn idempotency_key_is_stable() {
        // Pinned value: if this changes, deployed retries stop deduplicating.
        assert_eq!(
            key("payment.charge", "order_5824")
                .idempotency_key()
                .to_string(),
            "ab211c89-566b-5012-bd33-3a7f81351a08"
        );
    }

    #[test]
    fn idempotency_key_separates_ambiguous_splits() {
        assert_ne!(
            key("a:b", "c").idempotency_key(),
            key("a", "b:c").idempotency_key()
        );
    }

    #[test]
    fn rejects_invalid_identity() {
        assert_eq!(
            EffectName::new(""),
            Err(IdentityError::Empty("effect name"))
        );
        assert!(matches!(
            LogicalKey::new("x".repeat(MAX_KEY_LEN + 1)),
            Err(IdentityError::TooLong { .. })
        ));
        assert_eq!(
            LogicalKey::new("a\nb"),
            Err(IdentityError::ControlCharacter("logical key"))
        );
    }

    #[test]
    fn deserialization_validates() {
        assert!(serde_json::from_str::<EffectName>("\"\"").is_err());
        let name: EffectName = serde_json::from_str("\"payment.charge\"").unwrap();
        assert_eq!(name.as_str(), "payment.charge");
    }

    #[test]
    fn effect_ids_are_time_ordered() {
        let a = EffectId::new();
        let b = EffectId::new();
        assert!(a < b);
    }
}
