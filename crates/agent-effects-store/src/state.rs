//! The effect state machine.
//!
//! [`EffectStatus::apply`] is the single source of truth for which
//! transitions are legal. Stores and the runtime never change a status
//! without going through it. The table is documented in
//! `docs/design.md#state-machine`.

use std::fmt;

use serde::{Deserialize, Serialize};

/// Where an effect is in its lifecycle.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum EffectStatus {
    /// Recorded and waiting to run, either for the first time or for a
    /// scheduled retry.
    Pending,
    /// Waiting for a human or policy decision before it may run.
    AwaitingApproval,
    /// An attempt is in flight under a lease.
    Executing,
    /// An attempt may or may not have applied.
    Unknown,
    /// Checking the remote system for the effect's outcome.
    Verifying,
    /// The effect applied. Terminal.
    Committed,
    /// The effect definitely did not apply and will not be retried. Terminal.
    Failed,
    /// A precondition or approval decision refused the effect before it ran.
    /// Terminal.
    Rejected,
    /// The runtime cannot resolve the outcome on its own; an operator must.
    NeedsIntervention,
}

/// Every status, for exhaustive tests and store migrations.
pub const ALL_STATUSES: [EffectStatus; 9] = [
    EffectStatus::Pending,
    EffectStatus::AwaitingApproval,
    EffectStatus::Executing,
    EffectStatus::Unknown,
    EffectStatus::Verifying,
    EffectStatus::Committed,
    EffectStatus::Failed,
    EffectStatus::Rejected,
    EffectStatus::NeedsIntervention,
];

impl EffectStatus {
    /// Whether no further transition is possible.
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Committed | Self::Failed | Self::Rejected)
    }

    /// Whether the effect may have changed the outside world without the
    /// runtime knowing the result, so it must not simply be started again.
    pub const fn is_in_doubt(self) -> bool {
        matches!(
            self,
            Self::Executing | Self::Unknown | Self::Verifying | Self::NeedsIntervention
        )
    }

    /// The stable storage representation.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::AwaitingApproval => "awaiting_approval",
            Self::Executing => "executing",
            Self::Unknown => "unknown",
            Self::Verifying => "verifying",
            Self::Committed => "committed",
            Self::Failed => "failed",
            Self::Rejected => "rejected",
            Self::NeedsIntervention => "needs_intervention",
        }
    }

    /// Parses the storage representation produced by [`Self::as_str`].
    pub fn parse(s: &str) -> Option<Self> {
        ALL_STATUSES.into_iter().find(|status| status.as_str() == s)
    }

    /// The status after `event`, or an error if the event is not legal here.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidTransition`] for any pair not in the transition table.
    pub const fn apply(self, event: Transition) -> Result<Self, InvalidTransition> {
        use EffectStatus as S;
        use Transition as T;

        let next = match (self, event) {
            (S::Pending, T::RequestApproval) => S::AwaitingApproval,
            (S::AwaitingApproval, T::Approve) => S::Pending,
            (S::AwaitingApproval, T::Deny) | (S::Pending, T::PreconditionRejected) => S::Rejected,

            (S::Pending, T::StartAttempt) => S::Executing,

            (S::Executing, T::Succeeded)
            | (S::Verifying, T::VerificationConfirmed)
            | (S::Unknown | S::NeedsIntervention, T::ResolvedApplied) => S::Committed,

            (S::Executing, T::FailedDefinitively)
            | (S::Verifying, T::VerificationNotApplied)
            | (S::Unknown | S::NeedsIntervention, T::ResolvedNotApplied) => S::Failed,

            (S::Executing | S::Verifying | S::Unknown, T::ScheduleRetry)
            | (S::Unknown | S::NeedsIntervention, T::ResolvedRetry) => S::Pending,

            (S::Executing | S::Verifying, T::OutcomeUnknown | T::LeaseExpired) => S::Unknown,

            (S::Executing | S::Unknown, T::StartVerification) => S::Verifying,

            (S::Unknown, T::Escalate) | (S::Verifying, T::VerificationConflict) => {
                S::NeedsIntervention
            }

            _ => {
                return Err(InvalidTransition { from: self, event });
            }
        };
        Ok(next)
    }
}

impl fmt::Display for EffectStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Something that happens to an effect and may change its status.
///
/// Each variant is also an audit event; see [`Transition::as_str`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum Transition {
    /// Policy requires approval before the first attempt.
    RequestApproval,
    /// Approval was granted.
    Approve,
    /// Approval was refused.
    Deny,
    /// A precondition permanently refused the effect.
    PreconditionRejected,
    /// An attempt begins. Persisted before the action is invoked.
    StartAttempt,
    /// The action succeeded and no verification is configured.
    Succeeded,
    /// The action failed and the effect definitely did not apply, with no
    /// retry left or allowed.
    FailedDefinitively,
    /// Another attempt is scheduled, because the effect definitely did not
    /// apply or re-executing it is known to be safe.
    ScheduleRetry,
    /// The attempt ended without a definitive answer.
    OutcomeUnknown,
    /// The lease holder disappeared mid-attempt (recovery).
    LeaseExpired,
    /// Verification begins, after a success or to reconcile an unknown outcome.
    StartVerification,
    /// Verification found the effect applied.
    VerificationConfirmed,
    /// Verification found, authoritatively, that the effect did not apply,
    /// and it will not be retried.
    VerificationNotApplied,
    /// Verification found remote state that contradicts the effect.
    VerificationConflict,
    /// The runtime gave up resolving an unknown outcome on its own.
    Escalate,
    /// An operator confirmed the effect applied.
    ResolvedApplied,
    /// An operator confirmed the effect did not apply.
    ResolvedNotApplied,
    /// An operator asserted it is safe to run the effect again.
    ResolvedRetry,
}

/// Every transition, for exhaustive tests.
pub const ALL_TRANSITIONS: [Transition; 18] = [
    Transition::RequestApproval,
    Transition::Approve,
    Transition::Deny,
    Transition::PreconditionRejected,
    Transition::StartAttempt,
    Transition::Succeeded,
    Transition::FailedDefinitively,
    Transition::ScheduleRetry,
    Transition::OutcomeUnknown,
    Transition::LeaseExpired,
    Transition::StartVerification,
    Transition::VerificationConfirmed,
    Transition::VerificationNotApplied,
    Transition::VerificationConflict,
    Transition::Escalate,
    Transition::ResolvedApplied,
    Transition::ResolvedNotApplied,
    Transition::ResolvedRetry,
];

impl Transition {
    /// Parses the audit-event name produced by [`Self::as_str`].
    pub fn parse(s: &str) -> Option<Self> {
        ALL_TRANSITIONS.into_iter().find(|t| t.as_str() == s)
    }

    /// The stable audit-event name, e.g. `effect.attempt_started`.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RequestApproval => "effect.approval_requested",
            Self::Approve => "effect.approved",
            Self::Deny => "effect.denied",
            Self::PreconditionRejected => "effect.precondition_rejected",
            Self::StartAttempt => "effect.attempt_started",
            Self::Succeeded => "effect.succeeded",
            Self::FailedDefinitively => "effect.failed",
            Self::ScheduleRetry => "effect.retry_scheduled",
            Self::OutcomeUnknown => "effect.outcome_unknown",
            Self::LeaseExpired => "effect.lease_expired",
            Self::StartVerification => "effect.verification_started",
            Self::VerificationConfirmed => "effect.verification_confirmed",
            Self::VerificationNotApplied => "effect.verification_not_applied",
            Self::VerificationConflict => "effect.verification_conflict",
            Self::Escalate => "effect.escalated",
            Self::ResolvedApplied => "effect.resolved_applied",
            Self::ResolvedNotApplied => "effect.resolved_not_applied",
            Self::ResolvedRetry => "effect.resolved_retry",
        }
    }
}

impl fmt::Display for Transition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A transition that is not legal from the current status.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error("transition {event} is not allowed from status {from}")]
pub struct InvalidTransition {
    /// The status the effect was in.
    pub from: EffectStatus,
    /// The rejected transition.
    pub event: Transition,
}

#[cfg(test)]
mod tests {
    use std::collections::{HashSet, VecDeque};

    use proptest::prelude::*;

    use super::*;

    fn successors(status: EffectStatus) -> impl Iterator<Item = EffectStatus> {
        ALL_TRANSITIONS
            .into_iter()
            .filter_map(move |t| status.apply(t).ok())
    }

    fn reachable_from(start: EffectStatus) -> HashSet<EffectStatus> {
        let mut seen = HashSet::from([start]);
        let mut queue = VecDeque::from([start]);
        while let Some(status) = queue.pop_front() {
            for next in successors(status) {
                if seen.insert(next) {
                    queue.push_back(next);
                }
            }
        }
        seen
    }

    fn sources_of(target: EffectStatus) -> HashSet<(EffectStatus, Transition)> {
        ALL_STATUSES
            .into_iter()
            .flat_map(|s| ALL_TRANSITIONS.into_iter().map(move |t| (s, t)))
            .filter(|(s, t)| s.apply(*t) == Ok(target))
            .collect()
    }

    #[test]
    fn terminal_states_have_no_exits() {
        for status in ALL_STATUSES.into_iter().filter(|s| s.is_terminal()) {
            assert_eq!(successors(status).count(), 0, "{status} has an exit");
        }
    }

    #[test]
    fn every_state_is_reachable_from_pending() {
        let reachable = reachable_from(EffectStatus::Pending);
        for status in ALL_STATUSES {
            assert!(reachable.contains(&status), "{status} is unreachable");
        }
    }

    #[test]
    fn every_state_can_settle() {
        for status in ALL_STATUSES {
            assert!(
                reachable_from(status).iter().any(|s| s.is_terminal()),
                "{status} can never reach a terminal state"
            );
        }
    }

    #[test]
    fn only_pending_starts_an_attempt() {
        let sources = sources_of(EffectStatus::Executing);
        assert_eq!(
            sources,
            HashSet::from([(EffectStatus::Pending, Transition::StartAttempt)])
        );
    }

    #[test]
    fn in_doubt_states_never_restart_without_a_retry_decision() {
        // Leaving doubt for Pending must always be an explicit decision that
        // re-executing is safe, never a side effect of some other event.
        for status in ALL_STATUSES.into_iter().filter(|s| s.is_in_doubt()) {
            for t in ALL_TRANSITIONS {
                if status.apply(t) == Ok(EffectStatus::Pending) {
                    assert!(
                        matches!(t, Transition::ScheduleRetry | Transition::ResolvedRetry),
                        "{status} --{t}--> pending"
                    );
                }
            }
        }
    }

    #[test]
    fn committed_requires_evidence() {
        let events: HashSet<Transition> = sources_of(EffectStatus::Committed)
            .into_iter()
            .map(|(_, t)| t)
            .collect();
        assert_eq!(
            events,
            HashSet::from([
                Transition::Succeeded,
                Transition::VerificationConfirmed,
                Transition::ResolvedApplied,
            ])
        );
    }

    #[test]
    fn a_lost_lease_never_counts_as_failure() {
        for status in ALL_STATUSES {
            if let Ok(next) = status.apply(Transition::LeaseExpired) {
                assert_eq!(next, EffectStatus::Unknown);
            }
        }
    }

    #[test]
    fn storage_representation_round_trips_and_matches_serde() {
        for status in ALL_STATUSES {
            assert_eq!(EffectStatus::parse(status.as_str()), Some(status));
            assert_eq!(
                serde_json::to_string(&status).unwrap(),
                format!("\"{}\"", status.as_str())
            );
        }
        let names: HashSet<_> = ALL_TRANSITIONS.iter().map(|t| t.as_str()).collect();
        assert_eq!(names.len(), ALL_TRANSITIONS.len());
        for transition in ALL_TRANSITIONS {
            assert_eq!(Transition::parse(transition.as_str()), Some(transition));
        }
    }

    fn transition() -> impl Strategy<Value = Transition> {
        proptest::sample::select(ALL_TRANSITIONS.to_vec())
    }

    proptest! {
        #[test]
        fn random_histories_respect_invariants(events in prop::collection::vec(transition(), 0..64)) {
            let mut status = EffectStatus::Pending;
            for event in events {
                match status.apply(event) {
                    Ok(next) => {
                        prop_assert!(!status.is_terminal());
                        if status == EffectStatus::Committed {
                            prop_assert_ne!(next, EffectStatus::Executing);
                        }
                        status = next;
                    }
                    Err(err) => {
                        prop_assert_eq!(err.from, status);
                    }
                }
            }
        }
    }
}
