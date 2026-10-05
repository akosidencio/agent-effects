//! Charging a card through a provider that sometimes charges and then drops
//! the connection before answering: the case where a blind retry charges
//! the customer twice.
//!
//! ```sh
//! cargo run --example payment
//! ```
//!
//! The same failure is shown under each protection: an idempotency key the
//! provider honours, a lookup that verifies the charge, and neither (an
//! operator decides). A replayed call at the end shows that a committed
//! effect is never run again.

// One scenario after another in `main`, so it reads as a walkthrough.
#![allow(clippy::too_many_lines)]

use std::time::Duration;

use agent_effects::store::EffectStore;
use agent_effects::testkit::{Behavior, FakeRemote};
use agent_effects::{
    EffectFailure, EffectId, EffectOutcome, Resolution, RetryPolicy, Runtime, SystemClock,
    Verification,
};
use agent_effects_memory::MemoryStore;
use serde::{Deserialize, Serialize};

#[derive(Serialize)]
struct Charge {
    order: &'static str,
    cents: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct Payment {
    id: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryStore::new();
    let runtime = Runtime::builder(store.clone())
        .retry_policy(RetryPolicy {
            initial_delay: Duration::from_millis(50),
            ..RetryPolicy::default()
        })
        .build();

    println!("1. The provider honours idempotency keys");
    let provider = FakeRemote::new(SystemClock).script([Behavior::CommitThenDrop]);
    let charge = Charge {
        order: "order-1",
        cents: 4200,
    };
    let outcome = runtime
        .effect("payment.charge", charge.order)
        .input(&charge)
        .remote_idempotency(true)
        .run({
            let provider = provider.clone();
            move |ctx| {
                let provider = provider.clone();
                async move {
                    let id = provider
                        .create("order-1", Some(ctx.idempotency_key()))
                        .await?;
                    Ok::<_, EffectFailure>(Payment { id })
                }
            }
        })
        .await?;
    report(&store, &outcome, "order-1", &provider).await?;

    println!("\n2. No idempotency key, but the charge can be looked up");
    let provider = FakeRemote::new(SystemClock).script([Behavior::CommitThenDrop]);
    let charge = Charge {
        order: "order-2",
        cents: 1999,
    };
    let outcome = runtime
        .effect("payment.charge", charge.order)
        .input(&charge)
        .verify({
            let provider = provider.clone();
            move |_| {
                let provider = provider.clone();
                async move {
                    Ok::<_, EffectFailure>(match provider.find("order-2").await? {
                        Some(id) => Verification::Confirmed(Payment { id }),
                        None => Verification::NotApplied,
                    })
                }
            }
        })
        .run({
            let provider = provider.clone();
            move |_| {
                let provider = provider.clone();
                async move {
                    let id = provider.create("order-2", None).await?;
                    Ok::<_, EffectFailure>(Payment { id })
                }
            }
        })
        .await?;
    report(&store, &outcome, "order-2", &provider).await?;

    println!("\n3. Neither: the runtime will not guess");
    let provider = FakeRemote::new(SystemClock).script([Behavior::CommitThenDrop]);
    let charge = Charge {
        order: "order-3",
        cents: 500,
    };
    let unprotected = |provider: FakeRemote| {
        runtime
            .effect("payment.charge", charge.order)
            .input(&charge)
            .run(move |_| {
                let provider = provider.clone();
                async move {
                    let id = provider.create("order-3", None).await?;
                    Ok::<_, EffectFailure>(Payment { id })
                }
            })
    };
    let outcome = unprotected(provider.clone()).await?;
    report(&store, &outcome, "order-3", &provider).await?;
    if let EffectOutcome::NeedsIntervention { id } = outcome {
        // An operator finds the charge in the provider's dashboard.
        let found = Payment {
            id: "order-3#1".into(),
        };
        runtime
            .resolve(
                id,
                Resolution::applied(&found)?,
                "operator:alice",
                "charge found in the provider dashboard",
            )
            .await?;
        let replay = unprotected(provider.clone()).await?;
        println!("   after the operator's decision: {replay:?}");
    }

    println!("\n4. The agent asks again for order-1");
    let provider = FakeRemote::new(SystemClock);
    let charge = Charge {
        order: "order-1",
        cents: 4200,
    };
    let replay = runtime
        .effect("payment.charge", charge.order)
        .input(&charge)
        .remote_idempotency(true)
        .run({
            let provider = provider.clone();
            move |ctx| {
                let provider = provider.clone();
                async move {
                    let id = provider
                        .create("order-1", Some(ctx.idempotency_key()))
                        .await?;
                    Ok::<_, EffectFailure>(Payment { id })
                }
            }
        })
        .await?;
    println!("   outcome: {replay:?}");
    println!("   provider requests this time: {}", provider.requests());
    Ok(())
}

async fn report(
    store: &MemoryStore,
    outcome: &EffectOutcome<Payment>,
    order: &str,
    provider: &FakeRemote,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("   outcome: {outcome:?}");
    println!(
        "   provider: {} request(s), customer charged {} time(s)",
        provider.requests(),
        provider.applications(order)
    );
    let id = match outcome {
        EffectOutcome::Unknown { id } | EffectOutcome::NeedsIntervention { id } => *id,
        _ => effect_id(store, order).await?,
    };
    let trail: Vec<_> = store
        .events(id)
        .await?
        .iter()
        .map(|e| e.transition.as_str())
        .collect();
    println!("   audit trail: {}", trail.join(" → "));
    Ok(())
}

async fn effect_id(
    store: &MemoryStore,
    order: &str,
) -> Result<EffectId, Box<dyn std::error::Error>> {
    let key = agent_effects::EffectKey::new(
        agent_effects::EffectName::new("payment.charge")?,
        agent_effects::LogicalKey::new(order)?,
    );
    Ok(store.get_by_key(&key).await?.ok_or("no record")?.id)
}
