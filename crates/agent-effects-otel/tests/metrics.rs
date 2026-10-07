//! The observer's metrics, read back through the OpenTelemetry SDK.

use std::collections::HashMap;

use agent_effects::testkit::{Behavior, FakeRemote};
use agent_effects::{
    EffectFailure, EffectKind, EffectOutcome, FailureClass, RetryPolicy, Runtime, TokioClock,
};
use agent_effects_memory::MemoryStore;
use agent_effects_otel::OtelObserver;
use opentelemetry::metrics::MeterProvider;
use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, PeriodicReader, SdkMeterProvider};

/// Every metric's value after one flush: sums for counters, (count, sum)
/// for histograms, keyed by name.
struct Collected {
    sums: HashMap<String, i64>,
    histograms: HashMap<String, (u64, f64)>,
    attributes: HashMap<String, Vec<(String, String)>>,
}

fn collect(provider: &SdkMeterProvider, exporter: &InMemoryMetricExporter) -> Collected {
    provider.force_flush().unwrap();
    let batches = exporter.get_finished_metrics().unwrap();
    let mut collected = Collected {
        sums: HashMap::new(),
        histograms: HashMap::new(),
        attributes: HashMap::new(),
    };
    let last = batches.last().expect("one export");
    for scope in last.scope_metrics() {
        for metric in scope.metrics() {
            let name = metric.name().to_owned();
            let mut attrs = Vec::new();
            match metric.data() {
                AggregatedMetrics::U64(MetricData::Sum(sum)) => {
                    let mut total = 0;
                    for point in sum.data_points() {
                        total += i64::try_from(point.value()).unwrap();
                        attrs.extend(
                            point
                                .attributes()
                                .map(|kv| (kv.key.to_string(), kv.value.to_string())),
                        );
                    }
                    collected.sums.insert(name.clone(), total);
                }
                AggregatedMetrics::I64(MetricData::Sum(sum)) => {
                    let total = sum
                        .data_points()
                        .map(opentelemetry_sdk::metrics::data::SumDataPoint::value)
                        .sum();
                    collected.sums.insert(name.clone(), total);
                }
                AggregatedMetrics::F64(MetricData::Histogram(histogram)) => {
                    let (mut count, mut total) = (0, 0.0);
                    for point in histogram.data_points() {
                        count += point.count();
                        total += point.sum();
                        attrs.extend(
                            point
                                .attributes()
                                .map(|kv| (kv.key.to_string(), kv.value.to_string())),
                        );
                    }
                    collected.histograms.insert(name.clone(), (count, total));
                }
                _ => {}
            }
            collected.attributes.insert(name, attrs);
        }
    }
    collected
}

#[tokio::test(start_paused = true)]
async fn the_lifecycle_becomes_metrics() {
    let exporter = InMemoryMetricExporter::default();
    let provider = SdkMeterProvider::builder()
        .with_reader(PeriodicReader::builder(exporter.clone()).build())
        .build();
    let clock = TokioClock::new();
    let rt = Runtime::builder(MemoryStore::new())
        .clock(clock)
        .retry_policy(RetryPolicy {
            jitter: false,
            ..RetryPolicy::default()
        })
        .observer(OtelObserver::new(&provider.meter("test")))
        .build();

    // Committed after one retry.
    let flaky = FakeRemote::new(clock).script([Behavior::Fail(FailureClass::Transient)]);
    let outcome = rt
        .effect("payment.charge", "order-1")
        .kind(EffectKind::ReversibleWrite)
        .run(move |_| {
            let flaky = flaky.clone();
            async move { flaky.create("charge", None).await }
        })
        .await
        .unwrap();
    assert_eq!(outcome, EffectOutcome::Committed("charge#1".into()));
    // Failed for good.
    rt.effect("payment.charge", "order-2")
        .run(|_| async { Err::<(), _>(EffectFailure::permanent("card declined")) })
        .await
        .unwrap();
    // Unknown, then escalated.
    rt.effect("email.send", "welcome-1")
        .run(|_| async { Err::<(), _>(EffectFailure::ambiguous("timed out")) })
        .await
        .unwrap();
    // Awaits approval, approved, committed.
    let approved = || {
        rt.effect("cluster.delete", "prod")
            .require_approval()
            .run(|_| async { Ok::<_, EffectFailure>(()) })
    };
    let EffectOutcome::AwaitingApproval { id } = approved().await.unwrap() else {
        panic!("expected to wait for approval");
    };
    let waiting = collect(&provider, &exporter);
    assert_eq!(waiting.sums["agent_effects.pending_approval"], 1);
    rt.approve(id, "operator:dennis", "ok").await.unwrap();
    approved().await.unwrap();
    // Compensated.
    rt.compensation("payment.charge", "order-1")
        .run(|_, _: Option<String>| async { Ok(()) })
        .await
        .unwrap();

    let metrics = collect(&provider, &exporter);
    let sum = |name: &str| metrics.sums.get(name).copied().unwrap_or(0);
    assert_eq!(sum("agent_effects.started"), 4);
    assert_eq!(sum("agent_effects.completed"), 2);
    assert_eq!(sum("agent_effects.failed"), 1);
    assert_eq!(sum("agent_effects.unknown"), 1);
    assert_eq!(sum("agent_effects.needs_intervention"), 1);
    assert_eq!(sum("agent_effects.retry.count"), 1);
    assert_eq!(sum("agent_effects.compensation.started"), 1);
    assert_eq!(sum("agent_effects.compensation.completed"), 1);
    assert_eq!(
        sum("agent_effects.pending_approval"),
        0,
        "approved, so no longer pending"
    );

    let (settled, _) = metrics.histograms["agent_effects.duration"];
    assert_eq!(settled, 3, "two commits and one failure settled");
    let (attempts, _) = metrics.histograms["agent_effects.attempt.duration"];
    assert_eq!(
        attempts, 5,
        "two for the flaky charge, one each for the others"
    );
    let (_, backoff) = metrics.histograms["agent_effects.duration"];
    assert!(
        backoff >= 1.0,
        "the flaky charge waited a 1 s backoff: {backoff}"
    );

    let attrs = &metrics.attributes["agent_effects.attempt.duration"];
    assert!(attrs.contains(&("effect.name".into(), "payment.charge".into())));
    assert!(attrs.contains(&("effect.kind".into(), "reversible_write".into())));
    assert!(attrs.contains(&("outcome".into(), "committed".into())));
    assert!(
        metrics
            .attributes
            .values()
            .flatten()
            .all(|(_, value)| !value.contains("order-")),
        "the logical key is never an attribute"
    );
}
