//! Arbitrary sequences of lease operations and transitions against one
//! record, with arbitrary times and leases (stale ones included). Checks
//! the invariants every store relies on:
//!
//! - a refused change leaves the record exactly as it was;
//! - an accepted transition bumps the version by one and returns an event
//!   numbered by it;
//! - a terminal status never changes again;
//! - the attempt count never decreases;
//! - an effect that may have applied never becomes `Failed` through a
//!   failed attempt (unless it is a read);
//! - only the current lease holder's writes are accepted.
#![no_main]

use std::time::{Duration, SystemTime};

use agent_effects::state::ALL_TRANSITIONS;
use agent_effects::store::{EffectRecord, Lease, NewEffect, TransitionRequest};
use agent_effects::{
    EffectKey, EffectKind, EffectName, EffectStatus, LogicalKey, Transition, WorkerId,
};
use arbitrary::{Arbitrary, Unstructured};
use libfuzzer_sys::fuzz_target;

const KINDS: [EffectKind; 4] = [
    EffectKind::Read,
    EffectKind::IdempotentWrite,
    EffectKind::ReversibleWrite,
    EffectKind::IrreversibleWrite,
];

fn at(seconds: u16) -> SystemTime {
    SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000 + u64::from(seconds))
}

fn run(u: &mut Unstructured<'_>) -> arbitrary::Result<()> {
    let key = EffectKey::new(
        EffectName::new("fuzz.effect").unwrap(),
        LogicalKey::new("k").unwrap(),
    );
    let kind = *u.choose(&KINDS)?;
    let mut record = EffectRecord::new(NewEffect::new(key, kind, at(0)));
    let mut leases: Vec<Lease> = Vec::new();
    let workers = [WorkerId::new("a"), WorkerId::new("b")];
    for _ in 0..u.int_in_range(0..=64)? {
        let before = record.clone();
        let now = at(u16::arbitrary(u)?);
        match u.int_in_range(0..=3)? {
            0 => {
                let ttl = Duration::from_secs(u64::from(u8::arbitrary(u)?));
                if let Ok(lease) = record.acquire_lease(u.choose(&workers)?, now, ttl) {
                    assert_eq!(lease.epoch, before.lease_epoch + 1);
                    leases.push(lease);
                } else {
                    assert_eq!(record, before);
                }
            }
            1 if !leases.is_empty() => {
                let lease = u.choose(&leases)?.clone();
                if record
                    .renew_lease(&lease, now, Duration::from_secs(30))
                    .is_err()
                {
                    assert_eq!(record, before);
                }
            }
            2 if !leases.is_empty() => {
                let lease = u.choose(&leases)?.clone();
                let current = record.lease_epoch == lease.epoch;
                assert_eq!(
                    record.release_lease(&lease),
                    current && before.lease_owner.is_some()
                );
            }
            _ => {
                let transition: Transition = *u.choose(&ALL_TRANSITIONS)?;
                let lease = if bool::arbitrary(u)? && !leases.is_empty() {
                    Some(u.choose(&leases)?.clone())
                } else {
                    None
                };
                let mut request = TransitionRequest::new(&record, lease.as_ref(), transition, now);
                if bool::arbitrary(u)? {
                    request.expected_version = u64::arbitrary(u)?;
                }
                if bool::arbitrary(u)? {
                    request.next_attempt_at = Some(at(u16::arbitrary(u)?));
                }
                match record.apply(request) {
                    Ok(event) => {
                        assert!(
                            !before.status.is_terminal(),
                            "{} left a terminal status",
                            before.status
                        );
                        assert_eq!(record.version, before.version + 1);
                        assert_eq!(event.sequence, record.version);
                        assert_eq!((event.from, event.to), (before.status, record.status));
                        assert!(record.attempt_count >= before.attempt_count);
                        if let Some(lease) = &lease {
                            assert_eq!(lease.epoch, before.lease_epoch, "a stale lease wrote");
                        }
                        if transition == Transition::FailedDefinitively {
                            assert!(!before.may_have_applied || kind == EffectKind::Read);
                        }
                        if record.status == EffectStatus::Unknown {
                            assert!(record.may_have_applied);
                        }
                    }
                    Err(_) => {
                        assert_eq!(record, before, "a refused {transition} changed the record")
                    }
                }
            }
        }
    }
    Ok(())
}

fuzz_target!(|data: &[u8]| {
    let _ = run(&mut Unstructured::new(data));
});
