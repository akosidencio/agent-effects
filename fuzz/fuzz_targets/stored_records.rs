//! Stored JSON (records, contracts, evidence, events, errors, policies)
//! from a damaged or hostile database: parsing never panics, and whatever
//! parses survives a round trip unchanged.
#![no_main]

use agent_effects::RetryPolicy;
use agent_effects::store::{
    EffectEvent, EffectRecord, ErrorRecord, ExecutionContract, LateObservation,
};
use libfuzzer_sys::fuzz_target;
use serde::{Serialize, de::DeserializeOwned};

fn round_trip<T: Serialize + DeserializeOwned + PartialEq + std::fmt::Debug>(data: &[u8]) {
    let Ok(value) = serde_json::from_slice::<T>(data) else {
        return;
    };
    let json = serde_json::to_vec(&value).expect("a parsed value serializes");
    let again: T = serde_json::from_slice(&json).expect("a serialized value parses");
    assert_eq!(again, value);
}

fuzz_target!(|data: &[u8]| {
    round_trip::<EffectRecord>(data);
    round_trip::<ExecutionContract>(data);
    round_trip::<LateObservation>(data);
    round_trip::<EffectEvent>(data);
    round_trip::<ErrorRecord>(data);
    if let Ok(policy) = serde_json::from_slice::<RetryPolicy>(data) {
        // Floats may not round-trip bit for bit (NaN); the policy must
        // still compute delays without panicking.
        let _ = policy.delay(3, agent_effects::FailureClass::Transient, 0.5);
    }
});
