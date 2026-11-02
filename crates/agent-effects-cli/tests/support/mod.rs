//! Seeds a store with effects in every state an operator deals with, and
//! runs the CLI in-process against it.
#![allow(dead_code)] // Each test binary uses part of it.

use std::collections::VecDeque;
use std::time::{Duration, SystemTime};

use agent_effects::store::{
    EffectStore, LateObservation, LateOutcome, Transition, TransitionRequest,
};
use agent_effects::{EffectFailure, EffectId, EffectOutcome, Runtime, WorkerId};
use agent_effects_cli::cli::Cli;
use agent_effects_cli::{Confirm, Io, run};
use clap::Parser;
use serde_json::Value;

/// The seeded effects, by the state they were left in.
#[derive(Clone, Copy, Debug)]
pub struct Seeded {
    pub committed: EffectId,
    pub escalated: EffectId,
    pub awaiting: EffectId,
    pub scheduled: EffectId,
}

pub async fn seed<S: EffectStore + Clone>(store: &S) -> Seeded {
    let rt = Runtime::new(store.clone());
    let committed = match rt
        .effect("payment.charge", "order-1")
        .input(&serde_json::json!({ "card": "4111-1111", "amount": 10 }))
        .run(|_| async { Ok::<_, EffectFailure>(serde_json::json!({ "charge": "ch_1" })) })
        .await
        .unwrap()
    {
        EffectOutcome::Committed(_) => {
            rt.store()
                .get_by_key(&key("payment.charge", "order-1"))
                .await
                .unwrap()
                .unwrap()
                .id
        }
        other => panic!("{other:?}"),
    };
    // Neither idempotent nor verifiable, and the answer was lost: only an
    // operator can say what happened.
    let escalated = match rt
        .effect("payment.charge", "order-2")
        .run(|_| async { Err::<Value, _>(EffectFailure::ambiguous("connection reset")) })
        .await
        .unwrap()
    {
        EffectOutcome::NeedsIntervention { id } => id,
        other => panic!("{other:?}"),
    };
    let awaiting = match rt
        .effect("refund.issue", "order-3")
        .require_approval()
        .run(|_| async { Ok::<_, EffectFailure>(1) })
        .await
        .unwrap()
    {
        EffectOutcome::AwaitingApproval { id } => id,
        other => panic!("{other:?}"),
    };
    let scheduled = scheduled_retry(store).await;

    // Evidence from a fenced-off attempt of the escalated effect.
    let record = store.get(escalated).await.unwrap().unwrap();
    let lease = agent_effects::store::Lease {
        effect_id: escalated,
        owner: WorkerId::new("stale-worker"),
        epoch: 1,
        expires_at: SystemTime::now(),
    };
    store
        .append_late_observation(LateObservation::new(
            &record,
            &lease,
            LateOutcome::Succeeded,
            SystemTime::now(),
        ))
        .await
        .unwrap();
    Seeded {
        committed,
        escalated,
        awaiting,
        scheduled,
    }
}

/// A retry stored for an hour from now, with the lease released.
async fn scheduled_retry<S: EffectStore>(store: &S) -> EffectId {
    let now = SystemTime::now();
    let record = store
        .insert_or_get(
            agent_effects::store::NewEffect::new(
                key("email.send", "welcome-4"),
                agent_effects::EffectKind::IrreversibleWrite,
                now,
            )
            .with_contract(agent_effects::store::ExecutionContract::new(
                agent_effects::EffectKind::IrreversibleWrite,
                5,
            )),
        )
        .await
        .unwrap()
        .record;
    let lease = store
        .acquire_lease(record.id, &WorkerId::new("w"), now, Duration::from_secs(30))
        .await
        .unwrap();
    let record = store
        .transition(TransitionRequest::new(
            &record,
            Some(&lease),
            Transition::StartAttempt,
            now,
        ))
        .await
        .unwrap();
    let mut retry = TransitionRequest::new(&record, Some(&lease), Transition::ScheduleRetry, now);
    retry.next_attempt_at = Some(now + Duration::from_secs(3600));
    let record = store.transition(retry).await.unwrap();
    store.release_lease(&lease).await.unwrap();
    record.id
}

pub fn key(name: &str, key: &str) -> agent_effects::EffectKey {
    agent_effects::EffectKey::new(
        agent_effects::EffectName::new(name).unwrap(),
        agent_effects::LogicalKey::new(key).unwrap(),
    )
}

/// Answers confirmations from a script; `None` stands for "not a terminal".
#[derive(Default)]
pub struct Script {
    pub answers: VecDeque<Option<bool>>,
    pub prompts: Vec<String>,
    /// Runs when asked, before answering: lets a test change the effect
    /// between the moment it is shown and the decision.
    pub meanwhile: Option<Box<dyn FnMut() + Send>>,
}

impl Confirm for Script {
    fn confirm(&mut self, prompt: &str) -> Option<bool> {
        self.prompts.push(prompt.to_owned());
        if let Some(meanwhile) = &mut self.meanwhile {
            meanwhile();
        }
        self.answers.pop_front().flatten()
    }
}

pub struct Ran {
    pub code: u8,
    pub out: String,
    pub err: String,
}

impl Ran {
    pub fn json(&self) -> Value {
        serde_json::from_str(&self.out)
            .unwrap_or_else(|e| panic!("not JSON ({e}): {}\nstderr: {}", self.out, self.err))
    }
}

/// Runs `args` (after the program name) with `script` answering prompts.
pub async fn cli(args: &[&str], script: &mut Script) -> Ran {
    let parsed = Cli::try_parse_from(std::iter::once("agent-effects").chain(args.iter().copied()))
        .unwrap_or_else(|e| panic!("{e}"));
    let (mut out, mut err) = (Vec::new(), Vec::new());
    let code = run(
        parsed,
        &mut Io {
            out: &mut out,
            err: &mut err,
            confirm: script,
        },
    )
    .await;
    Ran {
        code,
        out: String::from_utf8(out).unwrap(),
        err: String::from_utf8(err).unwrap(),
    }
}
