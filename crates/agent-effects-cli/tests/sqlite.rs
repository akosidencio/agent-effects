//! The CLI against a real SQLite store: reads, every write command, the
//! authorization boundary, confirmation, operator races and errors.
mod support;

use std::sync::Arc;

use agent_effects::store::{EffectStore, Transition};
use agent_effects::{EffectStatus, Runtime};
use agent_effects_cli::exit;
use agent_effects_sqlite::SqliteStore;
use support::{Script, Seeded, cli, seed};

struct Fixture {
    _dir: tempfile::TempDir,
    url: String,
    store: SqliteStore,
    ids: Seeded,
}

async fn fixture() -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("effects.db");
    let store = SqliteStore::open(&path).await.unwrap();
    let ids = seed(&store).await;
    Fixture {
        url: path.display().to_string(),
        _dir: dir,
        store,
        ids,
    }
}

impl Fixture {
    async fn run(&self, args: &[&str]) -> support::Ran {
        self.run_with(args, &mut Script::default()).await
    }

    async fn run_with(&self, args: &[&str], script: &mut Script) -> support::Ran {
        let mut full = vec!["--database-url", self.url.as_str()];
        full.extend_from_slice(args);
        cli(&full, script).await
    }

    async fn status(&self, id: agent_effects::EffectId) -> EffectStatus {
        self.store.get(id).await.unwrap().unwrap().status
    }
}

#[tokio::test]
async fn inspecting_extreme_stored_timestamps_does_not_panic() {
    let fixture = fixture().await;
    for millis in [i64::MIN, i64::MAX] {
        sqlx::query("UPDATE effects SET updated_at = ?, next_attempt_at = ? WHERE id = ?")
            .bind(millis)
            .bind(millis)
            .bind(fixture.ids.awaiting.to_string())
            .execute(fixture.store.pool())
            .await
            .unwrap();
        let result = fixture
            .run(&[
                "--format",
                "json",
                "inspect",
                &fixture.ids.awaiting.to_string(),
            ])
            .await;
        assert_eq!(result.code, exit::OK, "{}", result.err);
        assert!(result.json()["updated_at"].is_string());
    }
}

#[tokio::test]
async fn list_filters_by_status_and_pages_by_id() {
    let f = fixture().await;
    let all = f.run(&["--format", "json", "list"]).await;
    assert_eq!(all.code, exit::OK, "{}", all.err);
    assert_eq!(all.json()["effects"].as_array().unwrap().len(), 4);

    let first = f
        .run(&["--format", "json", "list", "--limit", "2"])
        .await
        .json();
    let cursor = first["next_after"].as_str().unwrap().to_owned();
    let second = f
        .run(&[
            "--format", "json", "list", "--limit", "2", "--after", &cursor,
        ])
        .await
        .json();
    let ids: Vec<_> = first["effects"]
        .as_array()
        .unwrap()
        .iter()
        .chain(second["effects"].as_array().unwrap())
        .map(|e| e["id"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(ids.len(), 4);
    assert_eq!(
        ids.iter().collect::<std::collections::BTreeSet<_>>().len(),
        4
    );

    let escalated = f
        .run(&["--format", "json", "list", "--status", "needs_intervention"])
        .await
        .json();
    let rows = escalated["effects"].as_array().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["id"], f.ids.escalated.to_string());

    let scheduled = f
        .run(&["--format", "json", "list", "--scheduled"])
        .await
        .json();
    let rows = scheduled["effects"].as_array().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["id"], f.ids.scheduled.to_string());
    assert_eq!(rows[0]["retry_due"], false);
    assert!(rows[0]["next_attempt_at"].as_str().is_some());
}

#[tokio::test]
async fn pending_lists_what_waits_for_someone() {
    let f = fixture().await;
    let pending = f.run(&["--format", "json", "pending"]).await.json();
    let ids: Vec<_> = pending["effects"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["id"].as_str().unwrap().to_owned())
        .collect();
    assert!(ids.contains(&f.ids.escalated.to_string()));
    assert!(ids.contains(&f.ids.awaiting.to_string()));
    assert!(ids.contains(&f.ids.scheduled.to_string()));
    assert!(!ids.contains(&f.ids.committed.to_string()));
}

#[tokio::test]
async fn inspect_hides_payloads_unless_asked_and_shows_evidence() {
    let f = fixture().await;
    let id = f.ids.committed.to_string();
    let ran = f.run(&["inspect", &id]).await;
    assert_eq!(ran.code, exit::OK, "{}", ran.err);
    assert!(!ran.out.contains("4111"), "card number leaked: {}", ran.out);
    assert!(ran.out.contains("input_stored: true"), "{}", ran.out);

    let shown = f
        .run(&["--format", "json", "inspect", &id, "--show-payloads"])
        .await
        .json();
    assert_eq!(shown["input"]["card"], "4111-1111");
    assert_eq!(shown["output"]["charge"], "ch_1");

    let escalated = f
        .run(&["--format", "json", "inspect", &f.ids.escalated.to_string()])
        .await
        .json();
    assert_eq!(escalated["status"], "needs_intervention");
    assert_eq!(escalated["may_have_applied"], true);
    assert_eq!(escalated["last_error"]["message"], "connection reset");
    let late = escalated["late_observations"].as_array().unwrap();
    assert_eq!(late.len(), 1);
    assert_eq!(late[0]["outcome"], "succeeded");
    assert_eq!(late[0]["worker"], "stale-worker");
    assert!(escalated.get("input").is_none());

    let scheduled = f
        .run(&["--format", "json", "inspect", &f.ids.scheduled.to_string()])
        .await
        .json();
    assert_eq!(scheduled["retry"]["due"], false);
    assert_eq!(scheduled["lease"]["live"], false);
}

#[tokio::test]
async fn inspecting_never_moves_an_effect() {
    let f = fixture().await;
    let before = f.store.get(f.ids.escalated).await.unwrap().unwrap();
    for command in ["inspect", "history", "late"] {
        let ran = f.run(&[command, &f.ids.escalated.to_string()]).await;
        assert_eq!(ran.code, exit::OK, "{command}: {}", ran.err);
    }
    f.run(&["stats"]).await;
    f.run(&["pending"]).await;
    assert_eq!(f.store.get(f.ids.escalated).await.unwrap().unwrap(), before);
}

#[tokio::test]
async fn history_and_late_evidence() {
    let f = fixture().await;
    let id = f.ids.escalated.to_string();
    let history = f.run(&["--format", "json", "history", &id]).await.json();
    let transitions: Vec<_> = history["events"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["transition"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(
        transitions,
        [
            Transition::StartAttempt.as_str(),
            Transition::OutcomeUnknown.as_str(),
            Transition::Escalate.as_str()
        ]
    );
    let text = f.run(&["history", &id]).await;
    assert!(text.out.starts_with("SEQUENCE"), "{}", text.out);

    let late = f.run(&["--format", "json", "late", &id]).await.json();
    assert_eq!(late["observations"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn stats_count_statuses_and_schedules() {
    let f = fixture().await;
    let stats = f.run(&["--format", "json", "stats"]).await.json();
    assert_eq!(stats["total"], 4);
    assert_eq!(stats["by_status"]["committed"], 1);
    assert_eq!(stats["by_status"]["needs_intervention"], 1);
    assert_eq!(stats["by_status"]["awaiting_approval"], 1);
    assert_eq!(stats["by_status"]["pending"], 1);
    assert_eq!(stats["retries_scheduled"], 1);
    assert_eq!(stats["retries_due"], 0);
    assert_eq!(stats["unsettled_and_may_have_applied"], 1);
}

#[tokio::test]
async fn approve_records_the_authenticated_identity_and_the_reason() {
    let f = fixture().await;
    let id = f.ids.awaiting.to_string();
    let ran = f
        .run(&[
            "approve",
            &id,
            "--reason",
            "ticket OPS-42",
            "--actor",
            "alice",
            "--yes",
        ])
        .await;
    assert_eq!(ran.code, exit::OK, "{}", ran.err);
    let record = f.store.get(f.ids.awaiting).await.unwrap().unwrap();
    assert_eq!(record.status, EffectStatus::Pending);
    assert!(record.approved);
    let event = f.store.events(f.ids.awaiting).await.unwrap().pop().unwrap();
    assert_eq!(event.transition, Transition::Approve);
    let actor = event.actor.unwrap();
    assert!(
        actor.starts_with("operator:alice (agent-effects-cli, local user "),
        "{actor}"
    );
    assert_eq!(event.payload.unwrap()["note"], "ticket OPS-42");
}

#[tokio::test]
async fn deny_ends_the_effect_rejected_with_the_reason() {
    let f = fixture().await;
    let ran = f
        .run(&[
            "deny",
            &f.ids.awaiting.to_string(),
            "--reason",
            "duplicate refund",
            "--yes",
        ])
        .await;
    assert_eq!(ran.code, exit::OK, "{}", ran.err);
    let record = f.store.get(f.ids.awaiting).await.unwrap().unwrap();
    assert_eq!(record.status, EffectStatus::Rejected);
    assert_eq!(record.last_error.unwrap().message, "duplicate refund");
}

#[tokio::test]
async fn resolve_applied_stores_the_output_for_later_calls() {
    let f = fixture().await;
    let ran = f
        .run(&[
            "--format",
            "json",
            "resolve",
            &f.ids.escalated.to_string(),
            "--applied",
            "--output",
            r#"{"charge":"ch_2"}"#,
            "--reason",
            "provider dashboard shows ch_2",
            "--yes",
        ])
        .await;
    assert_eq!(ran.code, exit::OK, "{}", ran.err);
    assert_eq!(ran.json()["status"], "committed");
    let rt = Runtime::new(f.store.clone());
    let outcome = rt
        .effect("payment.charge", "order-2")
        .run(|_| async { panic!("an operator resolved it; it must not run") })
        .await
        .unwrap();
    assert_eq!(
        outcome,
        agent_effects::EffectOutcome::Committed(serde_json::json!({ "charge": "ch_2" }))
    );
}

#[tokio::test]
async fn resolve_not_applied_and_retry() {
    let f = fixture().await;
    let id = f.ids.escalated.to_string();
    let retried = f
        .run(&[
            "retry",
            &id,
            "--reason",
            "provider confirmed no charge",
            "--yes",
        ])
        .await;
    assert_eq!(retried.code, exit::OK, "{}", retried.err);
    assert_eq!(f.status(f.ids.escalated).await, EffectStatus::Pending);

    // A fresh escalated effect for not-applied.
    let g = fixture().await;
    let ran = g
        .run(&[
            "resolve",
            &g.ids.escalated.to_string(),
            "--not-applied",
            "--reason",
            "no charge in the ledger",
            "--yes",
        ])
        .await;
    assert_eq!(ran.code, exit::OK, "{}", ran.err);
    let record = g.store.get(g.ids.escalated).await.unwrap().unwrap();
    assert_eq!(record.status, EffectStatus::Failed);
    assert!(!record.may_have_applied);
}

#[tokio::test]
async fn read_only_mode_refuses_writes() {
    let f = fixture().await;
    let ran = f
        .run(&[
            "--read-only",
            "approve",
            &f.ids.awaiting.to_string(),
            "--reason",
            "x",
            "--yes",
        ])
        .await;
    assert_eq!(ran.code, exit::REFUSED);
    assert!(ran.err.contains("read-only"), "{}", ran.err);
    assert_eq!(
        f.status(f.ids.awaiting).await,
        EffectStatus::AwaitingApproval
    );
}

#[tokio::test]
async fn the_policy_decides_by_identity_not_by_actor_label() {
    let f = fixture().await;
    let me = std::env::var("USER")
        .or_else(|_| std::env::var("USERNAME"))
        .unwrap_or_else(|_| "unknown".into());
    let dir = tempfile::tempdir().unwrap();
    let deny_all = dir.path().join("deny.json");
    // The actor label names a granted operator; the identity is not granted.
    std::fs::write(
        &deny_all,
        r#"{"operators": {"alice": ["approve", "deny", "resolve", "retry"]}}"#,
    )
    .unwrap();
    let id = f.ids.awaiting.to_string();
    let ran = f
        .run(&[
            "--policy",
            deny_all.to_str().unwrap(),
            "approve",
            &id,
            "--reason",
            "x",
            "--actor",
            "alice",
            "--yes",
        ])
        .await;
    if me != "alice" {
        assert_eq!(ran.code, exit::REFUSED, "{}", ran.err);
        assert!(ran.err.contains("operator policy"), "{}", ran.err);
        assert_eq!(
            f.status(f.ids.awaiting).await,
            EffectStatus::AwaitingApproval
        );
    }

    let approve_only = dir.path().join("approve.json");
    std::fs::write(
        &approve_only,
        format!(r#"{{"operators": {{"{me}": ["approve"]}}}}"#),
    )
    .unwrap();
    let policy = approve_only.to_str().unwrap();
    let escalated = f.ids.escalated.to_string();
    let refused = f
        .run(&[
            "--policy", policy, "retry", &escalated, "--reason", "x", "--yes",
        ])
        .await;
    assert_eq!(refused.code, exit::REFUSED, "{}", refused.err);
    assert_eq!(
        f.status(f.ids.escalated).await,
        EffectStatus::NeedsIntervention
    );
    let allowed = f
        .run(&["--policy", policy, "approve", &id, "--reason", "x", "--yes"])
        .await;
    assert_eq!(allowed.code, exit::OK, "{}", allowed.err);

    let broken = dir.path().join("broken.json");
    std::fs::write(&broken, r#"{"operators": {"a": ["aprove"]}}"#).unwrap();
    let ran = f
        .run(&["--policy", broken.to_str().unwrap(), "stats"])
        .await;
    assert_eq!(ran.code, exit::USAGE, "{}", ran.err);
}

#[tokio::test]
async fn writes_need_confirmation() {
    let f = fixture().await;
    let id = f.ids.awaiting.to_string();
    let args = ["approve", id.as_str(), "--reason", "ok"];

    let mut not_a_terminal = Script::default();
    not_a_terminal.answers.push_back(None);
    let ran = f.run_with(&args, &mut not_a_terminal).await;
    assert_eq!(ran.code, exit::REFUSED);
    assert!(ran.err.contains("--yes"), "{}", ran.err);

    let mut declined = Script::default();
    declined.answers.push_back(Some(false));
    assert_eq!(f.run_with(&args, &mut declined).await.code, exit::REFUSED);
    assert!(
        declined.prompts[0].contains("version"),
        "{:?}",
        declined.prompts
    );
    assert_eq!(
        f.status(f.ids.awaiting).await,
        EffectStatus::AwaitingApproval
    );

    let mut confirmed = Script::default();
    confirmed.answers.push_back(Some(true));
    let ran = f.run_with(&args, &mut confirmed).await;
    assert_eq!(ran.code, exit::OK, "{}", ran.err);
    assert_eq!(f.status(f.ids.awaiting).await, EffectStatus::Pending);
}

#[tokio::test]
async fn a_change_between_showing_and_confirming_aborts_the_decision() {
    let f = fixture().await;
    let id = f.ids.awaiting;
    // While the operator reads the prompt, another operator denies it.
    let path = f.url.clone();
    let mut script = Script::default();
    script.answers.push_back(Some(true));
    script.meanwhile = Some(Box::new(move || {
        let path = path.clone();
        std::thread::spawn(move || {
            tokio::runtime::Runtime::new().unwrap().block_on(async {
                let store = SqliteStore::open(&path).await.unwrap();
                Runtime::new(store)
                    .deny(id, "operator:bob", "too late")
                    .await
                    .unwrap();
            });
        })
        .join()
        .unwrap();
    }));
    let ran = f
        .run_with(&["approve", &id.to_string(), "--reason", "ok"], &mut script)
        .await;
    assert_eq!(ran.code, exit::CONFLICT, "{}", ran.err);
    assert!(
        ran.err.contains("changed since it was shown"),
        "{}",
        ran.err
    );
    assert_eq!(
        f.status(id).await,
        EffectStatus::Rejected,
        "bob's decision stands"
    );
    let approvals = f
        .store
        .events(id)
        .await
        .unwrap()
        .iter()
        .filter(|e| e.transition == Transition::Approve)
        .count();
    assert_eq!(approvals, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_operators_cannot_both_decide() {
    let f = Arc::new(fixture().await);
    let id = f.ids.awaiting.to_string();
    let runs: Vec<_> = ["approve", "deny"]
        .into_iter()
        .map(|command| {
            let (f, id) = (Arc::clone(&f), id.clone());
            tokio::spawn(async move {
                f.run(&[command, &id, "--reason", "race", "--yes"])
                    .await
                    .code
            })
        })
        .collect();
    let mut codes = Vec::new();
    for run in runs {
        codes.push(run.await.unwrap());
    }
    codes.sort_unstable();
    assert_eq!(codes, [exit::OK, exit::CONFLICT], "exactly one decision");
    let decided = f
        .store
        .events(f.ids.awaiting)
        .await
        .unwrap()
        .iter()
        .filter(|e| matches!(e.transition, Transition::Approve | Transition::Deny))
        .count();
    assert_eq!(decided, 1);
}

#[tokio::test]
async fn wrong_states_versions_and_ids_are_refused_with_distinct_codes() {
    let f = fixture().await;
    let committed = f.ids.committed.to_string();
    let ran = f
        .run(&["approve", &committed, "--reason", "x", "--yes"])
        .await;
    assert_eq!(ran.code, exit::CONFLICT);
    assert!(ran.err.contains("awaiting_approval"), "{}", ran.err);

    let ran = f
        .run(&[
            "resolve",
            &committed,
            "--compensated",
            "--reason",
            "x",
            "--yes",
        ])
        .await;
    assert_eq!(ran.code, exit::CONFLICT, "{}", ran.err);

    let ran = f
        .run(&[
            "approve",
            &f.ids.awaiting.to_string(),
            "--reason",
            "x",
            "--expect-version",
            "99",
            "--yes",
        ])
        .await;
    assert_eq!(ran.code, exit::CONFLICT, "{}", ran.err);
    assert_eq!(
        f.status(f.ids.awaiting).await,
        EffectStatus::AwaitingApproval
    );

    let missing = agent_effects::EffectId::new().to_string();
    let ran = f.run(&["--format", "json", "inspect", &missing]).await;
    assert_eq!(ran.code, exit::NOT_FOUND);
    let error: serde_json::Value = serde_json::from_str(&ran.err).unwrap();
    assert_eq!(error["exit_code"], exit::NOT_FOUND);

    let ran = f
        .run(&[
            "resolve",
            &f.ids.escalated.to_string(),
            "--applied",
            "--output",
            "{not json",
            "--reason",
            "x",
            "--yes",
        ])
        .await;
    assert_eq!(ran.code, exit::USAGE, "{}", ran.err);
    assert_eq!(
        f.status(f.ids.escalated).await,
        EffectStatus::NeedsIntervention
    );

    let ran = f
        .run(&[
            "resolve",
            &f.ids.escalated.to_string(),
            "--not-applied",
            "--output",
            "{}",
            "--reason",
            "x",
            "--yes",
        ])
        .await;
    assert_eq!(ran.code, exit::USAGE, "an output only goes with --applied");
    assert_eq!(
        f.status(f.ids.escalated).await,
        EffectStatus::NeedsIntervention
    );

    let ran = f
        .run(&[
            "retry",
            &f.ids.escalated.to_string(),
            "--reason",
            "  ",
            "--yes",
        ])
        .await;
    assert_eq!(ran.code, exit::USAGE);
}

#[tokio::test]
async fn a_held_effect_is_not_decided_over_a_worker() {
    let f = fixture().await;
    let lease = f
        .store
        .acquire_lease(
            f.ids.escalated,
            &agent_effects::WorkerId::new("busy"),
            std::time::SystemTime::now(),
            std::time::Duration::from_secs(60),
        )
        .await
        .unwrap();
    let ran = f
        .run(&[
            "retry",
            &f.ids.escalated.to_string(),
            "--reason",
            "x",
            "--yes",
        ])
        .await;
    assert_eq!(ran.code, exit::CONFLICT, "{}", ran.err);
    assert!(ran.err.contains("busy"), "{}", ran.err);
    f.store.release_lease(&lease).await.unwrap();
}

#[tokio::test]
async fn missing_and_foreign_databases_are_connection_errors_and_never_created() {
    let dir = tempfile::tempdir().unwrap();
    let missing = dir.path().join("typo.db");
    let ran = cli(
        &["--database-url", missing.to_str().unwrap(), "stats"],
        &mut Script::default(),
    )
    .await;
    assert_eq!(ran.code, exit::CONNECTION, "{}", ran.err);
    assert!(!missing.exists(), "the CLI must not create a database");

    let foreign = dir.path().join("other.db");
    let pool = sqlx::SqlitePool::connect(&format!("sqlite://{}?mode=rwc", foreign.display()))
        .await
        .unwrap();
    // WAL, like every agent-effects store, so only the missing history
    // tells it apart.
    sqlx::query("PRAGMA journal_mode = WAL")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("CREATE TABLE t (x INTEGER)")
        .execute(&pool)
        .await
        .unwrap();
    pool.close().await;
    let ran = cli(
        &["--database-url", foreign.to_str().unwrap(), "list"],
        &mut Script::default(),
    )
    .await;
    assert_eq!(ran.code, exit::CONNECTION, "{}", ran.err);
    assert!(
        ran.err.contains("not an agent-effects store"),
        "{}",
        ran.err
    );

    let ran = cli(&["stats"], &mut Script::default()).await;
    if std::env::var_os("AGENT_EFFECTS_DATABASE_URL").is_none() {
        assert_eq!(ran.code, exit::USAGE, "{}", ran.err);
    }
}

#[tokio::test]
async fn a_read_only_connection_cannot_write_even_if_asked() {
    let f = fixture().await;
    // What read commands get: the store attached to a read-only pool.
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .connect_with(
            sqlx::sqlite::SqliteConnectOptions::new()
                .filename(&f.url)
                .read_only(true),
        )
        .await
        .unwrap();
    let read_only = SqliteStore::attach(pool).await.unwrap();
    assert!(read_only.get(f.ids.escalated).await.unwrap().is_some());
    let error = Runtime::new(read_only)
        .resolve(
            f.ids.escalated,
            agent_effects::Resolution::Retry,
            "operator:x",
            "x",
        )
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("readonly")
            || error.to_string().contains("read-only")
            || error.to_string().contains("read only"),
        "{error}"
    );
    assert_eq!(
        f.status(f.ids.escalated).await,
        EffectStatus::NeedsIntervention
    );
}
