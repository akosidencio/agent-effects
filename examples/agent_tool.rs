//! A `refund_order` tool for an LLM agent, built on the effect runtime.
//!
//! ```sh
//! cargo run --example agent_tool
//! ```
//!
//! The model decides *what* should happen; the runtime controls *how* the
//! refund reaches the payment provider. The transcript below replays things
//! agents really do:
//!
//! - call the tool, then call it again because the reply got lost;
//! - call it with a different amount for the same order;
//! - act on a stale decision after a human already refunded the order.
//!
//! The tool turns every outcome into a reply the model can act on, including
//! "status unknown, do not retry".

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use agent_effects::testkit::FakeRemote;
use agent_effects::{
    EffectFailure, EffectKind, EffectOutcome, Precondition, Runtime, RuntimeError, SystemClock,
};
use agent_effects_memory::MemoryStore;
use serde::Serialize;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Order {
    Paid,
    Refunded,
}

/// The application's own state, which the precondition reads.
type Orders = Arc<Mutex<HashMap<String, Order>>>;

#[derive(Serialize)]
struct RefundRequest<'a> {
    order: &'a str,
    cents: u64,
}

/// The tool. Returns the text handed back to the model.
async fn refund_order(
    runtime: &Runtime<MemoryStore>,
    payments: &FakeRemote,
    orders: &Orders,
    order: &str,
    cents: u64,
) -> String {
    let outcome = runtime
        .effect("payment.refund", order)
        .kind(EffectKind::IrreversibleWrite)
        .input(&RefundRequest { order, cents })
        .actor("agent:support-bot")
        .remote_idempotency(true)
        .precondition({
            let (orders, order) = (Arc::clone(orders), order.to_owned());
            move |_| {
                let state = orders.lock().unwrap().get(&order).copied();
                async move {
                    match state {
                        Some(Order::Paid) => Precondition::Satisfied,
                        Some(Order::Refunded) => Precondition::reject("order is already refunded"),
                        None => Precondition::reject("no such order"),
                    }
                }
            }
        })
        .run({
            let (payments, order) = (payments.clone(), order.to_owned());
            move |ctx| {
                let (payments, order) = (payments.clone(), order.clone());
                async move {
                    let refund = payments
                        .create(&format!("refund:{order}"), Some(ctx.idempotency_key()))
                        .await?;
                    Ok::<_, EffectFailure>(refund)
                }
            }
        })
        .await;

    match outcome {
        Ok(EffectOutcome::Committed(refund)) => {
            orders
                .lock()
                .unwrap()
                .insert(order.to_owned(), Order::Refunded);
            format!("refunded {order}: {refund}")
        }
        Ok(EffectOutcome::Rejected(why)) => format!("not refunded: {}", why.message),
        Ok(EffectOutcome::Failed(why)) => format!("refund failed: {}", why.message),
        Ok(EffectOutcome::InProgress { .. }) => {
            "a refund for this order is already in progress; wait".into()
        }
        Ok(EffectOutcome::Unknown { id } | EffectOutcome::NeedsIntervention { id }) => format!(
            "refund status unknown (effect {id}); a human will check. Do not retry or \
             tell the customer it failed."
        ),
        Ok(_) => "refund status unavailable".into(),
        Err(RuntimeError::InputMismatch { .. }) => format!(
            "a different refund for {order} was already requested; \
             ask a human instead of changing the amount"
        ),
        Err(e) => format!("tool error: {e}"),
    }
}

#[tokio::main]
async fn main() {
    let runtime = Runtime::new(MemoryStore::new());
    let payments = FakeRemote::new(SystemClock);
    let orders: Orders = Arc::new(Mutex::new(HashMap::from([
        ("order-42".to_owned(), Order::Paid),
        ("order-7".to_owned(), Order::Paid),
    ])));

    let call = async |order: &str, cents: u64| {
        let reply = refund_order(&runtime, &payments, &orders, order, cents).await;
        println!("agent → refund_order({order}, {cents})\n      ← {reply}");
    };

    call("order-42", 2500).await;
    // The agent never saw the reply and calls again.
    call("order-42", 2500).await;
    // The agent second-guesses the amount.
    call("order-42", 3000).await;

    // Meanwhile a human refunds order-7 by hand.
    orders
        .lock()
        .unwrap()
        .insert("order-7".into(), Order::Refunded);
    call("order-7", 1200).await;

    println!(
        "\nprovider received {} request(s) and issued {} refund(s) for order-42, \
         {} for order-7",
        payments.requests(),
        payments.applications("refund:order-42"),
        payments.applications("refund:order-7"),
    );
}
