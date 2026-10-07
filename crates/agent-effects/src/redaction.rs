//! Keeping secrets out of the store.
//!
//! Everything the runtime persists can leak: the input (kept for identity,
//! audit and handler recovery), the output (replayed to later callers),
//! audit payloads, and error messages, which often echo tokens back. Two
//! tools keep secrets out:
//!
//! - [`Secret<T>`], the default. It serializes as `"[REDACTED]"`, so a
//!   secret field in an input or output never reaches the store. Its `Debug`
//!   and `Display` print `[REDACTED]` too. The action sees the real value
//!   (it holds it in memory); what is read back from the store is a redacted
//!   `Secret` ([`Secret::expose`] returns `None`).
//! - A [`Redactor`] on the runtime
//!   ([`RuntimeBuilder::redactor`](crate::RuntimeBuilder::redactor)). It
//!   rewrites every input, output, audit payload and error message before it
//!   is written. [`RedactKeys`] masks fields by name at any depth.
//!
//! Redaction happens before the input is fingerprinted, so secrets are not
//! part of an effect's identity and are never stored, not even hashed.
//!
//! What is redacted cannot be replayed or resumed:
//!
//! - a later caller gets the redacted output;
//! - a handler resumed by recovery gets the redacted input.
//!
//! Keep credentials in the action's captured state or in the handler, not in
//! inputs.

use std::collections::HashSet;
use std::fmt;

use serde::de::{Deserialize, Deserializer, IgnoredAny};
use serde::ser::{Serialize, Serializer};
use serde_json::Value;

use crate::id::EffectName;

/// What redacted values are replaced with.
pub const REDACTED: &str = "[REDACTED]";

/// A value that must never be stored or logged.
///
/// ```
/// use agent_effects::redaction::Secret;
///
/// #[derive(serde::Serialize)]
/// struct Charge {
///     account: String,
///     card_token: Secret<String>,
/// }
///
/// let charge = Charge { account: "acct_1".into(), card_token: Secret::new("tok_live_x".into()) };
/// assert_eq!(
///     serde_json::to_string(&charge).unwrap(),
///     r#"{"account":"acct_1","card_token":"[REDACTED]"}"#
/// );
/// assert_eq!(format!("{:?}", charge.card_token), "[REDACTED]");
/// assert_eq!(charge.card_token.expose().map(String::as_str), Some("tok_live_x"));
/// ```
#[derive(Clone, Default, PartialEq, Eq)]
pub struct Secret<T>(Option<T>);

impl<T> Secret<T> {
    /// Wraps `value`.
    pub const fn new(value: T) -> Self {
        Self(Some(value))
    }

    /// The value, or `None` if this `Secret` was read back from storage,
    /// where only `"[REDACTED]"` was kept.
    pub const fn expose(&self) -> Option<&T> {
        self.0.as_ref()
    }

    /// The value, consuming the wrapper; `None` as for [`Self::expose`].
    pub fn into_inner(self) -> Option<T> {
        self.0
    }

    /// Whether the value is gone because it was read back from storage.
    pub const fn is_redacted(&self) -> bool {
        self.0.is_none()
    }
}

impl<T> From<T> for Secret<T> {
    fn from(value: T) -> Self {
        Self::new(value)
    }
}

impl<T> fmt::Debug for Secret<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(REDACTED)
    }
}

impl<T> fmt::Display for Secret<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(REDACTED)
    }
}

impl<T> Serialize for Secret<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(REDACTED)
    }
}

/// Reading a `Secret` back always yields a redacted one: the value was never
/// stored.
impl<'de, T> Deserialize<'de> for Secret<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        IgnoredAny::deserialize(deserializer)?;
        Ok(Self(None))
    }
}

/// Which persisted value a [`Redactor`] is looking at.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Field {
    /// An effect's input, before it is fingerprinted and stored.
    Input,
    /// An action's or verification's output, or an operator's
    /// `Resolution::Applied` output.
    Output,
    /// An audit event's payload, e.g. an operator's note.
    AuditPayload,
    /// An error message, as a JSON string.
    ErrorMessage,
}

/// Rewrites values before the runtime stores them. Applies to every effect
/// the runtime runs, every transition it records and every operator
/// decision.
///
/// Any `Fn(Field, &EffectName, &mut Value)` is a redactor.
pub trait Redactor: Send + Sync + 'static {
    /// Rewrites `value`, of `field`, of an effect named `effect`, in place.
    fn redact(&self, field: Field, effect: &EffectName, value: &mut Value);
}

impl<F> Redactor for F
where
    F: Fn(Field, &EffectName, &mut Value) + Send + Sync + 'static,
{
    fn redact(&self, field: Field, effect: &EffectName, value: &mut Value) {
        self(field, effect, value);
    }
}

/// Replaces the value of every object field with one of these names
/// (case-insensitively, at any depth) with `"[REDACTED]"`.
///
/// ```
/// use agent_effects::redaction::{Field, RedactKeys, Redactor};
/// use agent_effects::EffectName;
/// use serde_json::json;
///
/// let redact = RedactKeys::new(["password", "card_number"]);
/// let mut value = json!({ "user": "ada", "auth": { "Password": "hunter2" }, "cards": [{ "card_number": "4242" }] });
/// redact.redact(Field::Input, &EffectName::new("signup").unwrap(), &mut value);
/// assert_eq!(value, json!({ "user": "ada", "auth": { "Password": "[REDACTED]" }, "cards": [{ "card_number": "[REDACTED]" }] }));
/// ```
#[derive(Clone, Debug, Default)]
pub struct RedactKeys {
    keys: HashSet<String>,
}

impl RedactKeys {
    /// Masks fields with any of `keys`.
    pub fn new<K: AsRef<str>>(keys: impl IntoIterator<Item = K>) -> Self {
        Self {
            keys: keys
                .into_iter()
                .map(|k| k.as_ref().to_ascii_lowercase())
                .collect(),
        }
    }

    fn mask(&self, value: &mut Value) {
        match value {
            Value::Object(map) => {
                for (key, value) in map.iter_mut() {
                    if self.keys.contains(&key.to_ascii_lowercase()) {
                        *value = Value::String(REDACTED.into());
                    } else {
                        self.mask(value);
                    }
                }
            }
            Value::Array(items) => items.iter_mut().for_each(|item| self.mask(item)),
            _ => {}
        }
    }
}

impl Redactor for RedactKeys {
    fn redact(&self, _field: Field, _effect: &EffectName, value: &mut Value) {
        self.mask(value);
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn a_secret_never_serializes_its_value_and_reads_back_redacted() {
        let secret = Secret::new("tok_live_x".to_string());
        let stored = serde_json::to_value(&secret).unwrap();
        assert_eq!(stored, json!(REDACTED));
        let back: Secret<String> = serde_json::from_value(stored).unwrap();
        assert!(back.is_redacted());
        assert_eq!(back.expose(), None);
        let anything: Secret<u64> = serde_json::from_value(json!({ "x": 1 })).unwrap();
        assert!(
            anything.is_redacted(),
            "any stored shape reads back redacted"
        );
    }

    #[test]
    fn a_closure_is_a_redactor() {
        let redactor = |field: Field, _: &EffectName, value: &mut Value| {
            if field == Field::ErrorMessage {
                *value = json!("hidden");
            }
        };
        let mut message = json!("token sk_live_1 rejected");
        redactor.redact(
            Field::ErrorMessage,
            &EffectName::new("x").unwrap(),
            &mut message,
        );
        assert_eq!(message, json!("hidden"));
    }
}
