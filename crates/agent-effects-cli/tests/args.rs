//! Argument parsing: what the CLI accepts and refuses before touching a
//! database.
use agent_effects_cli::cli::{Cli, Command, Format};
use clap::Parser;

fn parse(args: &[&str]) -> Result<Cli, clap::Error> {
    Cli::try_parse_from(std::iter::once("agent-effects").chain(args.iter().copied()))
}

const ID: &str = "01a19666-f0ca-7376-b3f9-97f88570b82b";

#[test]
fn global_options_go_anywhere() {
    let cli = parse(&[
        "list",
        "--format",
        "json",
        "--database-url",
        "x.db",
        "--read-only",
    ])
    .unwrap();
    assert_eq!(cli.format, Format::Json);
    assert!(cli.read_only);
    assert_eq!(cli.database_url.as_deref(), Some("x.db"));
}

#[test]
fn statuses_and_ids_are_validated() {
    let cli = parse(&[
        "list",
        "--status",
        "unknown",
        "--status",
        "needs_intervention",
    ])
    .unwrap();
    let Command::List { statuses, .. } = cli.command else {
        panic!()
    };
    assert_eq!(statuses.len(), 2);
    let error = parse(&["list", "--status", "lost"])
        .unwrap_err()
        .to_string();
    assert!(error.contains("one of: pending"), "{error}");
    let error = parse(&["inspect", "not-a-uuid"]).unwrap_err().to_string();
    assert!(error.contains("is not an effect id"), "{error}");
}

#[test]
fn page_sizes_are_bounded() {
    assert!(parse(&["list", "--limit", "0"]).is_err());
    assert!(parse(&["list", "--limit", "1001"]).is_err());
    assert!(parse(&["list", "--limit", "1000"]).is_ok());
}

#[test]
fn writes_need_a_reason() {
    assert!(parse(&["approve", ID]).is_err());
    assert!(parse(&["approve", ID, "--reason", "ok"]).is_ok());
}

#[test]
fn resolve_takes_exactly_one_outcome_and_one_output_source() {
    assert!(parse(&["resolve", ID, "--reason", "x"]).is_err());
    assert!(parse(&["resolve", ID, "--applied", "--not-applied", "--reason", "x"]).is_err());
    assert!(
        parse(&[
            "resolve",
            ID,
            "--applied",
            "--output",
            "{}",
            "--output-file",
            "f",
            "--reason",
            "x"
        ])
        .is_err()
    );
    assert!(
        parse(&[
            "resolve",
            ID,
            "--applied",
            "--output",
            "{}",
            "--reason",
            "x"
        ])
        .is_ok()
    );
    assert!(parse(&["resolve", ID, "--compensated", "--reason", "x"]).is_ok());
}

#[test]
fn every_command_has_help() {
    for command in [
        "list", "pending", "inspect", "history", "late", "stats", "approve", "deny", "resolve",
        "retry",
    ] {
        let error = parse(&[command, "--help"]).unwrap_err();
        assert_eq!(
            error.kind(),
            clap::error::ErrorKind::DisplayHelp,
            "{command}"
        );
    }
}
