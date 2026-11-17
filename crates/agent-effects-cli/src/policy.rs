//! Who may run which write command.
//!
//! The CLI's authority is the database credentials it connects with: anyone
//! who can run it against a database can do what that database role (or,
//! for SQLite, the file permissions) allows. A policy narrows that, mapping
//! the identity the database authenticated to the write commands it may
//! run:
//!
//! ```json
//! {
//!   "operators": {
//!     "effects_oncall": ["approve", "deny"],
//!     "effects_admin": ["approve", "deny", "resolve", "retry"]
//!   }
//! }
//! ```
//!
//! For PostgreSQL the identity is `current_user`, the role the server
//! authenticated. For SQLite it is the local user name from the environment,
//! which is only advisory: file permissions decide who can write. The
//! `--actor` label is recorded in the audit trail and never consulted here.
//!
//! A policy is a guardrail for the CLI, not a substitute for grants: a role
//! that may update the store's tables can do so without the CLI. Give
//! read-only operators a role with `SELECT` only.

use std::collections::{BTreeMap, BTreeSet};

use serde::Deserialize;

/// The write commands a policy can grant.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Permission {
    /// `approve`.
    Approve,
    /// `deny`.
    Deny,
    /// `resolve` (applied, not applied, compensated).
    Resolve,
    /// `retry`.
    Retry,
}

impl Permission {
    /// The command's name.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Approve => "approve",
            Self::Deny => "deny",
            Self::Resolve => "resolve",
            Self::Retry => "retry",
        }
    }
}

/// Write permissions by authenticated identity.
#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Policy {
    /// Identity → the write commands it may run.
    pub operators: BTreeMap<String, BTreeSet<Permission>>,
}

impl Policy {
    /// Parses a policy from JSON.
    ///
    /// # Errors
    ///
    /// If `json` is not a valid policy; unknown fields and commands are
    /// errors, so a typo cannot silently grant or drop a permission.
    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }

    /// Whether `identity` may run `permission`'s command.
    pub fn allows(&self, identity: &str, permission: Permission) -> bool {
        self.operators
            .get(identity)
            .is_some_and(|granted| granted.contains(&permission))
    }
}

#[cfg(test)]
mod tests {
    use super::{Permission, Policy};

    #[test]
    fn grants_only_what_is_listed() {
        let policy = Policy::from_json(
            r#"{"operators": {"oncall": ["approve", "deny"], "admin": ["approve", "deny", "resolve", "retry"]}}"#,
        )
        .unwrap();
        assert!(policy.allows("oncall", Permission::Approve));
        assert!(!policy.allows("oncall", Permission::Resolve));
        assert!(policy.allows("admin", Permission::Retry));
        assert!(!policy.allows("someone", Permission::Approve));
        assert!(!Policy::default().allows("admin", Permission::Approve));
    }

    #[test]
    fn typos_are_errors_not_silent_grants() {
        assert!(Policy::from_json(r#"{"operators": {"a": ["aprove"]}}"#).is_err());
        assert!(Policy::from_json(r#"{"operator": {"a": ["approve"]}}"#).is_err());
    }
}
