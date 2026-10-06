//! Policy: what the runtime may do on its own.
//!
//! Two parts:
//!
//! - [`Capabilities::unknown_plan`], the rule that keeps the runtime from
//!   turning "I don't know" into a duplicate side effect;
//! - [`RiskPolicy`], which adds requirements by risk level and effect kind:
//!   approval, verification, no automatic retries.
//!
//! Both are pure, so they can be tested exhaustively.

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::kind::EffectKind;
use crate::verification::VerificationMode;

/// The properties of an effect that decide how its unknown outcomes are
/// resolved.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Capabilities {
    /// The effect's kind.
    pub kind: EffectKind,
    /// Whether the remote system deduplicates on the effect's
    /// [`IdempotencyKey`](crate::id::IdempotencyKey), so a repeated request
    /// cannot apply twice.
    pub remote_idempotency: bool,
    /// Whether, and how reliably, the effect can be verified.
    pub verification: VerificationMode,
}

/// What to do with an effect whose last attempt has an unknown outcome.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UnknownPlan {
    /// Ask the remote system what happened.
    Verify,
    /// Run the action again; repeating it cannot duplicate the effect.
    Reexecute,
    /// Nothing the runtime can do safely; an operator must resolve it.
    Escalate,
}

impl Capabilities {
    /// What to do when an attempt's outcome is unknown.
    ///
    /// Verification is preferred even when re-executing would be safe: it
    /// usually costs less than the action and never repeats it.
    pub const fn unknown_plan(&self) -> UnknownPlan {
        if !matches!(self.verification, VerificationMode::None) {
            UnknownPlan::Verify
        } else if self.kind.is_naturally_idempotent() || self.remote_idempotency {
            UnknownPlan::Reexecute
        } else {
            UnknownPlan::Escalate
        }
    }

    /// Whether every unknown outcome of this effect will need an operator.
    ///
    /// True for writes that are neither idempotent nor verifiable. Such
    /// effects are allowed, but the runtime warns when one is built.
    pub const fn unknown_always_escalates(&self) -> bool {
        matches!(self.unknown_plan(), UnknownPlan::Escalate)
    }
}

/// How much damage an effect could do if it went wrong. Set with
/// [`EffectBuilder::risk`](crate::EffectBuilder::risk) or
/// [`EffectHandler::risk`](crate::EffectHandler::risk); defaults to `Low`.
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum RiskLevel {
    /// Routine and cheap to get wrong.
    #[default]
    Low,
    /// Visible to customers or costly to undo.
    Medium,
    /// Hard to undo, or affects money or data.
    High,
    /// Irreversible damage at scale, e.g. deleting production.
    Critical,
}

impl fmt::Display for RiskLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
            Self::Critical => "critical",
        })
    }
}

/// What a policy demands of an effect. Requirements only ever accumulate.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct Requirements {
    /// A human must approve the effect before its first attempt.
    pub approval: bool,
    /// The effect must be verifiable; one that is not is refused before
    /// anything is recorded.
    pub verification: bool,
    /// The runtime must not retry or re-run the effect on its own.
    pub no_automatic_retry: bool,
}

impl Requirements {
    /// Both sets of requirements: the stricter of each.
    #[must_use]
    pub const fn and(self, other: Self) -> Self {
        Self {
            approval: self.approval || other.approval,
            verification: self.verification || other.verification,
            no_automatic_retry: self.no_automatic_retry || other.no_automatic_retry,
        }
    }
}

/// Which effects a rule applies to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Selector {
    risk: Option<RiskLevel>,
    kind: Option<EffectKind>,
}

impl Selector {
    fn matches(self, risk: RiskLevel, kind: EffectKind) -> bool {
        self.risk.is_none_or(|r| r == risk) && self.kind.is_none_or(|k| k == kind)
    }
}

/// Requirements by risk level and effect kind, applied to every effect the
/// runtime runs. Built with [`PolicyBuilder`]; set with
/// [`RuntimeBuilder::risk_policy`](crate::RuntimeBuilder::risk_policy).
///
/// **Precedence:** requirements only accumulate. An effect gets its own
/// settings plus the requirements of *every* rule that matches it, so the
/// strictest one always wins, and no rule, nor its order, can loosen another.
///
/// ```
/// use agent_effects::{EffectKind, PolicyBuilder, RiskLevel};
///
/// let policy = PolicyBuilder::new()
///     .for_risk(RiskLevel::Low).auto_execute()
///     .for_risk(RiskLevel::Medium).require_verification()
///     .for_risk(RiskLevel::High).require_approval()
///     .for_risk(RiskLevel::Critical).require_approval().disable_automatic_retry()
///     .for_kind(EffectKind::IrreversibleWrite).require_verification()
///     .build();
///
/// let critical = policy.requirements(RiskLevel::Critical, EffectKind::ReversibleWrite);
/// assert!(critical.approval && critical.no_automatic_retry && !critical.verification);
/// ```
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RiskPolicy {
    rules: Vec<(Selector, Requirements)>,
}

impl RiskPolicy {
    /// What the policy demands of an effect of `risk` and `kind`: the union
    /// of every matching rule.
    pub fn requirements(&self, risk: RiskLevel, kind: EffectKind) -> Requirements {
        self.rules
            .iter()
            .filter(|(selector, _)| selector.matches(risk, kind))
            .fold(Requirements::default(), |all, (_, rule)| all.and(*rule))
    }
}

/// Builds a [`RiskPolicy`]: start a rule with `for_risk`, `for_kind` or
/// `for_risk_and_kind`, then add requirements to it.
#[derive(Clone, Debug, Default)]
#[must_use]
pub struct PolicyBuilder {
    rules: Vec<(Selector, Requirements)>,
}

impl PolicyBuilder {
    /// An empty policy.
    pub fn new() -> Self {
        Self::default()
    }

    /// Starts a rule for every effect of `risk`.
    pub fn for_risk(self, risk: RiskLevel) -> Self {
        self.rule(Some(risk), None)
    }

    /// Starts a rule for every effect of `kind`.
    pub fn for_kind(self, kind: EffectKind) -> Self {
        self.rule(None, Some(kind))
    }

    /// Starts a rule for effects of both `risk` and `kind`.
    pub fn for_risk_and_kind(self, risk: RiskLevel, kind: EffectKind) -> Self {
        self.rule(Some(risk), Some(kind))
    }

    /// Adds no requirement: the rule's effects run without extra checks.
    /// Documents intent; it cannot lift another rule's requirements.
    pub fn auto_execute(self) -> Self {
        self
    }

    /// The rule's effects need approval before their first attempt.
    pub fn require_approval(self) -> Self {
        self.require(|r| r.approval = true)
    }

    /// The rule's effects must be verifiable.
    pub fn require_verification(self) -> Self {
        self.require(|r| r.verification = true)
    }

    /// The runtime must never retry or re-run the rule's effects on its
    /// own. An operator's explicit retry is still allowed.
    pub fn disable_automatic_retry(self) -> Self {
        self.require(|r| r.no_automatic_retry = true)
    }

    /// The policy.
    pub fn build(self) -> RiskPolicy {
        RiskPolicy { rules: self.rules }
    }

    fn rule(mut self, risk: Option<RiskLevel>, kind: Option<EffectKind>) -> Self {
        self.rules
            .push((Selector { risk, kind }, Requirements::default()));
        self
    }

    /// Adds to the latest rule, or to a rule for every effect if none was
    /// started.
    fn require(mut self, add: impl FnOnce(&mut Requirements)) -> Self {
        if self.rules.is_empty() {
            self = self.rule(None, None);
        }
        if let Some((_, requirements)) = self.rules.last_mut() {
            add(requirements);
        }
        self
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    const KINDS: [EffectKind; 4] = [
        EffectKind::Read,
        EffectKind::IdempotentWrite,
        EffectKind::ReversibleWrite,
        EffectKind::IrreversibleWrite,
    ];

    const MODES: [VerificationMode; 3] = [
        VerificationMode::None,
        VerificationMode::Authoritative,
        VerificationMode::EventuallyConsistent {
            settle: Duration::from_secs(5),
        },
    ];

    fn all_capabilities() -> impl Iterator<Item = Capabilities> {
        KINDS.into_iter().flat_map(|kind| {
            [false, true]
                .into_iter()
                .flat_map(move |remote_idempotency| {
                    MODES.into_iter().map(move |verification| Capabilities {
                        kind,
                        remote_idempotency,
                        verification,
                    })
                })
        })
    }

    #[test]
    fn non_idempotent_writes_never_blindly_reexecute() {
        for caps in all_capabilities() {
            if caps.unknown_plan() == UnknownPlan::Reexecute {
                assert!(
                    caps.kind.is_naturally_idempotent() || caps.remote_idempotency,
                    "{caps:?} re-executes blindly"
                );
            }
        }
    }

    #[test]
    fn escalation_happens_only_without_any_safe_option() {
        for caps in all_capabilities() {
            let no_safe_option = caps.verification == VerificationMode::None
                && !caps.kind.is_naturally_idempotent()
                && !caps.remote_idempotency;
            assert_eq!(caps.unknown_always_escalates(), no_safe_option, "{caps:?}");
        }
    }

    const RISKS: [RiskLevel; 4] = [
        RiskLevel::Low,
        RiskLevel::Medium,
        RiskLevel::High,
        RiskLevel::Critical,
    ];

    fn sample_policy() -> RiskPolicy {
        PolicyBuilder::new()
            .for_risk(RiskLevel::Low)
            .auto_execute()
            .for_risk(RiskLevel::Medium)
            .require_verification()
            .for_risk(RiskLevel::High)
            .require_approval()
            .for_risk(RiskLevel::Critical)
            .require_approval()
            .disable_automatic_retry()
            .for_kind(EffectKind::IrreversibleWrite)
            .require_verification()
            .build()
    }

    #[test]
    fn requirements_are_the_union_of_matching_rules() {
        let policy = sample_policy();
        let low_read = policy.requirements(RiskLevel::Low, EffectKind::Read);
        assert_eq!(low_read, Requirements::default());
        let high_irreversible = policy.requirements(RiskLevel::High, EffectKind::IrreversibleWrite);
        assert!(high_irreversible.approval && high_irreversible.verification);
        assert!(!high_irreversible.no_automatic_retry);
    }

    #[test]
    fn rule_order_never_matters() {
        let forward = sample_policy();
        let mut reversed = forward.clone();
        reversed.rules.reverse();
        for risk in RISKS {
            for kind in KINDS {
                assert_eq!(
                    forward.requirements(risk, kind),
                    reversed.requirements(risk, kind)
                );
            }
        }
    }

    #[test]
    fn adding_a_rule_never_loosens() {
        let base = sample_policy();
        let mut extended = base.clone();
        extended.rules.push((
            Selector {
                risk: None,
                kind: None,
            },
            Requirements::default(),
        ));
        for risk in RISKS {
            for kind in KINDS {
                let (before, after) = (
                    base.requirements(risk, kind),
                    extended.requirements(risk, kind),
                );
                assert_eq!(before.and(after), after, "{risk} {kind:?}");
            }
        }
    }

    #[test]
    fn requirements_without_a_rule_apply_to_everything() {
        let policy = PolicyBuilder::new().disable_automatic_retry().build();
        for risk in RISKS {
            assert!(
                policy
                    .requirements(risk, EffectKind::Read)
                    .no_automatic_retry
            );
        }
    }
}
