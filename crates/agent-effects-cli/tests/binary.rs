//! The installed binary end to end: exit codes, output streams, and
//! refusing to act without a terminal or --yes.
mod support;

use std::process::{Command, Output, Stdio};

use agent_effects::store::EffectStore;
use agent_effects_cli::exit;
use agent_effects_sqlite::SqliteStore;

fn agent_effects(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_agent-effects"))
        .args(args)
        .env_remove("AGENT_EFFECTS_DATABASE_URL")
        .env_remove("AGENT_EFFECTS_READ_ONLY")
        .env_remove("AGENT_EFFECTS_OPERATOR_POLICY")
        .env_remove("AGENT_EFFECTS_FORMAT")
        .stdin(Stdio::null())
        .output()
        .unwrap()
}

fn code(output: &Output) -> u8 {
    u8::try_from(output.status.code().unwrap()).unwrap()
}

#[tokio::test]
async fn the_binary_reads_json_and_refuses_unconfirmed_writes() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("effects.db");
    let store = SqliteStore::open(&path).await.unwrap();
    let ids = support::seed(&store).await;
    let db = path.to_str().unwrap();

    let help = agent_effects(&["--help"]);
    assert_eq!(code(&help), exit::OK);
    assert!(String::from_utf8_lossy(&help.stdout).contains("approve"));

    let list = agent_effects(&["--database-url", db, "--format", "json", "list"]);
    assert_eq!(
        code(&list),
        exit::OK,
        "{}",
        String::from_utf8_lossy(&list.stderr)
    );
    let json: serde_json::Value = serde_json::from_slice(&list.stdout).unwrap();
    assert_eq!(json["effects"].as_array().unwrap().len(), 4);

    // Standard input is not a terminal: no --yes, no write.
    let id = ids.awaiting.to_string();
    let refused = agent_effects(&["--database-url", db, "approve", &id, "--reason", "ok"]);
    assert_eq!(code(&refused), exit::REFUSED);
    assert!(String::from_utf8_lossy(&refused.stderr).contains("--yes"));
    assert_eq!(
        store.get(ids.awaiting).await.unwrap().unwrap().status,
        agent_effects::EffectStatus::AwaitingApproval
    );

    let approved = agent_effects(&[
        "--database-url",
        db,
        "approve",
        &id,
        "--reason",
        "ok",
        "--yes",
    ]);
    assert_eq!(
        code(&approved),
        exit::OK,
        "{}",
        String::from_utf8_lossy(&approved.stderr)
    );
    assert_eq!(
        String::from_utf8_lossy(&approved.stderr),
        "",
        "no prompt with --yes"
    );

    let usage = agent_effects(&["--database-url", db, "inspect", "nope"]);
    assert_eq!(code(&usage), exit::USAGE);
    let missing = agent_effects(&["stats"]);
    assert_eq!(code(&missing), exit::USAGE);
    assert!(String::from_utf8_lossy(&missing.stderr).contains("AGENT_EFFECTS_DATABASE_URL"));
}

#[test]
fn connection_errors_never_print_the_password() {
    let output = agent_effects(&[
        "--database-url",
        "postgres://app:hunter2-secret@127.0.0.1:9/effects",
        "--format",
        "json",
        "stats",
    ]);
    assert_eq!(code(&output), exit::CONNECTION);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!stderr.contains("hunter2"), "{stderr}");
    assert!(stderr.contains("app:***@127.0.0.1"), "{stderr}");
    let error: serde_json::Value = serde_json::from_str(&stderr).unwrap();
    assert_eq!(error["exit_code"], exit::CONNECTION);
}
