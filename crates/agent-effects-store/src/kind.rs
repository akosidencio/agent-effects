//! Behavioral classification of effects.

use serde::{Deserialize, Serialize};

/// What re-executing an effect does to the outside world.
///
/// The kind decides how much the runtime may retry on its own (see
/// `Capabilities::unknown_plan` in `agent-effects`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EffectKind {
    /// No externally visible mutation, e.g. fetching a balance.
    Read,
    /// Repeating the same request leaves the same end state, e.g.
    /// `PUT /users/123`.
    IdempotentWrite,
    /// Changes external state, but a compensating operation exists, e.g.
    /// reserving inventory.
    ReversibleWrite,
    /// Changes external state and cannot be reliably undone, e.g. sending an
    /// email.
    IrreversibleWrite,
}

impl EffectKind {
    /// Whether executing the same request twice is harmless on its own,
    /// without help from the remote system.
    pub const fn is_naturally_idempotent(self) -> bool {
        matches!(self, Self::Read | Self::IdempotentWrite)
    }

    /// The stable storage representation.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::IdempotentWrite => "idempotent_write",
            Self::ReversibleWrite => "reversible_write",
            Self::IrreversibleWrite => "irreversible_write",
        }
    }

    /// Parses the storage representation produced by [`Self::as_str`].
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "read" => Self::Read,
            "idempotent_write" => Self::IdempotentWrite,
            "reversible_write" => Self::ReversibleWrite,
            "irreversible_write" => Self::IrreversibleWrite,
            _ => return None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn storage_representation_round_trips_and_matches_serde() {
        for kind in [
            EffectKind::Read,
            EffectKind::IdempotentWrite,
            EffectKind::ReversibleWrite,
            EffectKind::IrreversibleWrite,
        ] {
            assert_eq!(EffectKind::parse(kind.as_str()), Some(kind));
            assert_eq!(
                serde_json::to_string(&kind).unwrap(),
                format!("\"{}\"", kind.as_str())
            );
        }
    }
}
