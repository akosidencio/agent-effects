#!/usr/bin/env bash
# Runs the benchmark matrix and writes one JSON line per measured run.
#
#   AGENT_EFFECTS_POSTGRES_URL=postgres://... bench/run-matrix.sh [stores...]
#
# Stores default to "memory sqlite" plus "postgres" when the URL is set.
# SIZES (default "100 1000 10000") sets the effect counts, RUNS (default 3)
# the measured runs, DATA_DIR and LABEL where SQLite files go and how the
# results are labeled. Results go to target/bench-results/<UTC time>.jsonl;
# a summary table is printed at the end. Point the URL at a dedicated,
# isolated database: every run creates and drops a schema of its own.
set -euo pipefail

cd "$(dirname "$0")/.."
cargo build --release --locked -p agent-effects-bench
bench=target/release/agent-effects-bench
mkdir -p target/bench-results
out="target/bench-results/$(date -u +%Y%m%dT%H%M%SZ).jsonl"

stores=("$@")
if [ ${#stores[@]} -eq 0 ]; then
    stores=(memory sqlite)
    [ -n "${AGENT_EFFECTS_POSTGRES_URL:-}" ] && stores+=(postgres)
fi

run() {
    echo "== $*" >&2
    "$bench" --runs "${RUNS:-3}" --warmup 1 \
        --data-dir "${DATA_DIR:-target/bench-data}" --label "${LABEL:-}" "$@" >>"$out"
}

for store in "${stores[@]}"; do
    for n in ${SIZES:-100 1000 10000}; do
        run --store "$store" --workload unique --effects "$n" --concurrency 16
        run --store "$store" --workload duplicate --effects "$n" --concurrency 16
        run --store "$store" --workload transitions --effects "$n" --concurrency 16
        run --store "$store" --workload lease-contention --effects "$n" --concurrency 16
        run --store "$store" --workload recovery --effects "$n" --concurrency 16
        run --store "$store" --workload compensation --effects "$n" --concurrency 16
        run --store "$store" --workload prune --effects "$n" --concurrency 16
    done
    # Recovery with simulated 10 ms remote calls: sequential versus bounded
    # concurrency, where handler latency dominates.
    for n in ${SIZES:-100 1000}; do
        [ "$n" -gt 1000 ] && continue
        for c in 1 16; do
            run --store "$store" --workload recovery --effects "$n" --concurrency "$c" --work-ms 10
        done
    done
    # A one-second backoff per effect, lease-free versus holding the lease.
    for n in ${SIZES:-100 1000}; do
        [ "$n" -gt 1000 ] && continue
        run --store "$store" --workload retry --effects "$n" --concurrency 100
        run --store "$store" --workload retry-holding-leases --effects "$n" --concurrency 100
    done
done

echo "results: $out" >&2
jq -rs '
  group_by([.store, .label, .workload, .effects, .concurrency, .work_ms])[]
  | sort_by(.throughput_per_s) as $runs
  | $runs[($runs | length) / 2 | floor] as $median
  | [ ($median.store + (if $median.label != "" then "/" + $median.label else "" end)), $median.workload, $median.effects, $median.concurrency, $median.work_ms,
      ($median.throughput_per_s | floor),
      (if $median.latency_ms.p50 then ($median.latency_ms.p50 * 100 | round / 100) else "-" end),
      (if $median.latency_ms.p95 then ($median.latency_ms.p95 * 100 | round / 100) else "-" end),
      (if $median.latency_ms.p99 then ($median.latency_ms.p99 * 100 | round / 100) else "-" end),
      ($median.store_ops_per_effect * 10 | round / 10),
      ($median.cpu_percent_of_one_core // "-" | if type == "number" then round else . end),
      ($median.peak_rss_mib | round),
      ([$runs[].duplicate_side_effects] | add),
      ([$runs[].unresolved] | add) ]
  | @tsv' "$out" | column -t -N store,workload,effects,conc,work_ms,per_s,p50_ms,p95_ms,p99_ms,ops/effect,cpu%,rss_mib,dups,unresolved
