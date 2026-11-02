//! Text columns read back from a store: statuses, transitions, kinds and
//! identities. Parsing never panics, and a parsed value prints back as the
//! text it came from.
#![no_main]

use agent_effects::{EffectKind, EffectName, EffectStatus, LogicalKey, Transition};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let Ok(text) = std::str::from_utf8(data) else {
        return;
    };
    if let Some(status) = EffectStatus::parse(text) {
        assert_eq!(status.as_str(), text);
    }
    if let Some(transition) = Transition::parse(text) {
        assert_eq!(transition.as_str(), text);
    }
    if let Some(kind) = EffectKind::parse(text) {
        assert_eq!(kind.as_str(), text);
    }
    let (name, key) = text.split_once('\n').unwrap_or((text, ""));
    if let (Ok(name), Ok(key)) = (EffectName::new(name), LogicalKey::new(key)) {
        let key = agent_effects::EffectKey::new(name, key);
        // Stable, and distinct for effect and compensation.
        assert_eq!(key.idempotency_key(), key.clone().idempotency_key());
        assert_ne!(key.idempotency_key(), key.compensation_idempotency_key());
    }
});
