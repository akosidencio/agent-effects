# Safety fuzzing

These are optional developer tools, outside the published workspace. They
add no dependencies to the runtime. Use nightly and cargo-fuzz 0.13.2:

```sh
cargo +stable install cargo-fuzz --version 0.13.2 --locked
cargo +nightly fuzz build
cargo +nightly fuzz run state_machine -- -max_total_time=60 -timeout=2 -rss_limit_mb=1024
```

Targets:

- `state_machine`: arbitrary lease operations, timestamps, stale tokens and
  transitions; refused writes leave records unchanged, successful writes
  obey fencing, monotone counters and ambiguous-outcome rules.
- `stored_records`: JSON decoding and round trips of persisted records,
  contracts, events, evidence and errors.
- `stored_text`: database enum and identity text, including stable effect
  and compensation idempotency keys.
- `retry_delays`: arbitrary policies, floating-point multipliers, attempt
  counts and provider delays; arithmetic must not panic or exceed its bounds.

CI runs bounded smoke campaigns with AddressSanitizer. Keep longer campaigns
on isolated developer machines. A smoke campaign is not a proof of safety.
Corpus, sanitizer builds and crash artifacts are ignored. Preserve and
minimize any failure locally, then add a deterministic regression test.
Do not upload real stored payloads or credentials as seeds or artifacts.
On environments that prohibit LeakSanitizer's ptrace checks, use
`ASAN_OPTIONS=detect_leaks=0`; AddressSanitizer remains enabled, but leak
checking is not performed.
