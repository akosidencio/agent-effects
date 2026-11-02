//! Retry delays from arbitrary policies and provider `Retry-After` values:
//! no panic, and never longer than the policy's cap or the provider's ask.
#![no_main]

use std::time::Duration;

use agent_effects::{FailureClass, RetryPolicy};
use arbitrary::{Arbitrary, Unstructured};
use libfuzzer_sys::fuzz_target;

fn duration(u: &mut Unstructured<'_>) -> arbitrary::Result<Duration> {
    Ok(Duration::new(
        u64::arbitrary(u)?,
        u32::arbitrary(u)? % 1_000_000_000,
    ))
}

fuzz_target!(|data: &[u8]| {
    let mut u = Unstructured::new(data);
    let Ok(policy) = (|| {
        Ok::<_, arbitrary::Error>(RetryPolicy {
            max_attempts: u32::arbitrary(&mut u)?,
            initial_delay: duration(&mut u)?,
            max_delay: duration(&mut u)?,
            multiplier: f64::arbitrary(&mut u)?,
            jitter: bool::arbitrary(&mut u)?,
        })
    })() else {
        return;
    };
    let (Ok(retry), Ok(sample), Ok(asked)) = (
        u32::arbitrary(&mut u),
        f64::arbitrary(&mut u),
        duration(&mut u),
    ) else {
        return;
    };
    for class in [
        FailureClass::Transient,
        FailureClass::Ambiguous,
        FailureClass::RateLimited {
            retry_after: Some(asked),
        },
        FailureClass::RateLimited { retry_after: None },
    ] {
        let delay = policy.delay(retry, class, sample);
        assert!(delay <= policy.max_delay.max(asked));
    }
});
