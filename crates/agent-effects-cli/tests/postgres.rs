//! The CLI against PostgreSQL, directly and through a strict PgBouncer in
//! transaction mode. Skipped unless `AGENT_EFFECTS_POSTGRES_URL` (and
//! `AGENT_EFFECTS_PGBOUNCER_URL`) are set; required when the matching
//! `AGENT_EFFECTS_REQUIRE_*` variable is.
mod support;

use agent_effects::EffectStatus;
use agent_effects::store::{EffectStore, Transition};
use agent_effects_cli::exit;
use agent_effects_postgres::{PostgresOptions, PostgresStore};
use support::{Script, cli, seed};

fn env(name: &str, require: &str) -> Option<String> {
    let url = std::env::var(name).ok();
    assert!(
        url.is_some() || std::env::var_os(require).is_none(),
        "{require} is set but {name} is not"
    );
    if url.is_none() {
        eprintln!("skipped: set {name}");
    }
    url
}

async fn fresh(url: &str) -> (PostgresStore, String) {
    let schema = format!(
        "cli_{}",
        agent_effects::EffectId::new().to_string().replace('-', "")
    );
    let store = PostgresOptions::new()
        .schema(&schema)
        .connect(url)
        .await
        .unwrap();
    (store, schema)
}

#[tokio::test]
async fn postgres_inspect_and_decide_with_the_database_role_as_identity() {
    let Some(url) = env(
        "AGENT_EFFECTS_POSTGRES_URL",
        "AGENT_EFFECTS_REQUIRE_POSTGRES",
    ) else {
        return;
    };
    let (store, schema) = fresh(&url).await;
    let ids = seed(&store).await;
    let base: Vec<String> = ["--database-url", &url, "--schema", &schema]
        .map(str::to_owned)
        .to_vec();
    let run = |args: &[&str]| {
        let mut full = base.clone();
        full.extend(args.iter().map(|a| (*a).to_owned()));
        async move {
            let full: Vec<&str> = full.iter().map(String::as_str).collect();
            cli(&full, &mut Script::default()).await
        }
    };

    let stats = run(&["--format", "json", "stats"]).await;
    assert_eq!(stats.code, exit::OK, "{}", stats.err);
    assert_eq!(stats.json()["total"], 4);
    let inspect = run(&["--format", "json", "inspect", &ids.escalated.to_string()]).await;
    assert_eq!(
        inspect.json()["late_observations"]
            .as_array()
            .unwrap()
            .len(),
        1
    );

    let ran = run(&[
        "resolve",
        &ids.escalated.to_string(),
        "--not-applied",
        "--reason",
        "ledger checked",
        "--actor",
        "alice",
        "--yes",
    ])
    .await;
    assert_eq!(ran.code, exit::OK, "{}", ran.err);
    let event = store.events(ids.escalated).await.unwrap().pop().unwrap();
    assert_eq!(event.transition, Transition::ResolvedNotApplied);
    // The authenticated role, not the label, is the identity.
    assert_eq!(
        event.actor.as_deref(),
        Some("operator:alice (agent-effects-cli, database role postgres)")
    );
    assert_eq!(
        store.get(ids.escalated).await.unwrap().unwrap().status,
        EffectStatus::Failed
    );

    let wrong_schema = cli(
        &["--database-url", &url, "--schema", "no_such_schema", "list"],
        &mut Script::default(),
    )
    .await;
    assert_eq!(wrong_schema.code, exit::CONNECTION, "{}", wrong_schema.err);
}

#[tokio::test]
async fn postgres_read_commands_use_read_only_transactions() {
    let Some(url) = env(
        "AGENT_EFFECTS_POSTGRES_URL",
        "AGENT_EFFECTS_REQUIRE_POSTGRES",
    ) else {
        return;
    };
    let (store, schema) = fresh(&url).await;
    let ids = seed(&store).await;
    // The connection a read command opens: the server refuses writes on it.
    let options: sqlx::postgres::PgConnectOptions = url.parse().unwrap();
    let pool = sqlx::postgres::PgPoolOptions::new()
        .connect_with(options.options([("default_transaction_read_only", "on")]))
        .await
        .unwrap();
    let read_only = PostgresOptions::new()
        .schema(&schema)
        .attach(pool)
        .await
        .unwrap();
    assert!(read_only.get(ids.escalated).await.unwrap().is_some());
    let error = agent_effects::Runtime::new(read_only)
        .resolve(ids.escalated, agent_effects::Resolution::Retry, "x", "x")
        .await
        .unwrap_err();
    assert!(error.to_string().contains("read-only"), "{error}");
    assert_eq!(
        store.get(ids.escalated).await.unwrap().unwrap().status,
        EffectStatus::NeedsIntervention
    );
}

#[tokio::test]
async fn postgres_through_a_strict_pgbouncer() {
    let Some(direct) = env(
        "AGENT_EFFECTS_POSTGRES_URL",
        "AGENT_EFFECTS_REQUIRE_POSTGRES",
    ) else {
        return;
    };
    let Some(pooled) = env(
        "AGENT_EFFECTS_PGBOUNCER_URL",
        "AGENT_EFFECTS_REQUIRE_PGBOUNCER",
    ) else {
        return;
    };
    let (store, schema) = fresh(&direct).await;
    let ids = seed(&store).await;
    let base = [
        "--database-url",
        pooled.as_str(),
        "--schema",
        schema.as_str(),
        "--transaction-pooling",
    ];
    for args in [
        &["--format", "json", "list"][..],
        &["--format", "json", "pending"],
        &["--format", "json", "stats"],
    ] {
        let mut full = base.to_vec();
        full.extend_from_slice(args);
        let ran = cli(&full, &mut Script::default()).await;
        assert_eq!(ran.code, exit::OK, "{args:?}: {}", ran.err);
    }
    let mut full = base.to_vec();
    let id = ids.awaiting.to_string();
    full.extend_from_slice(&["approve", &id, "--reason", "pooled", "--yes"]);
    let ran = cli(&full, &mut Script::default()).await;
    assert_eq!(ran.code, exit::OK, "{}", ran.err);
    assert_eq!(
        store.get(ids.awaiting).await.unwrap().unwrap().status,
        EffectStatus::Pending
    );
}

#[tokio::test]
async fn postgres_grants_are_the_boundary() {
    let Some(url) = env(
        "AGENT_EFFECTS_POSTGRES_URL",
        "AGENT_EFFECTS_REQUIRE_POSTGRES",
    ) else {
        return;
    };
    let (store, schema) = fresh(&url).await;
    let ids = seed(&store).await;
    let suffix = &schema[4..16];
    let (viewer, operator) = (format!("viewer_{suffix}"), format!("operator_{suffix}"));
    // The roles the README recommends.
    let admin = sqlx::PgPool::connect(&url).await.unwrap();
    for statement in [
        format!("CREATE ROLE {viewer} LOGIN PASSWORD 'pw'"),
        format!("GRANT USAGE ON SCHEMA {schema} TO {viewer}"),
        format!("GRANT SELECT ON ALL TABLES IN SCHEMA {schema} TO {viewer}"),
        format!("CREATE ROLE {operator} LOGIN PASSWORD 'pw'"),
        format!("GRANT USAGE ON SCHEMA {schema} TO {operator}"),
        format!("GRANT SELECT ON ALL TABLES IN SCHEMA {schema} TO {operator}"),
        format!("GRANT UPDATE ON {schema}.effects TO {operator}"),
        format!("GRANT INSERT ON {schema}.effect_events TO {operator}"),
    ] {
        sqlx::query(sqlx::AssertSqlSafe(statement))
            .execute(&admin)
            .await
            .unwrap();
    }
    let as_role = |role: &str| {
        let (protocol, rest) = url.split_once("://").unwrap();
        let host = rest.rsplit_once('@').map_or(rest, |(_, host)| host);
        format!("{protocol}://{role}:pw@{host}")
    };
    let id = ids.awaiting.to_string();
    let viewer_url = as_role(&viewer);
    let read = cli(
        &[
            "--database-url",
            &viewer_url,
            "--schema",
            &schema,
            "inspect",
            &id,
        ],
        &mut Script::default(),
    )
    .await;
    assert_eq!(read.code, exit::OK, "{}", read.err);
    let write = cli(
        &[
            "--database-url",
            &viewer_url,
            "--schema",
            &schema,
            "approve",
            &id,
            "--reason",
            "x",
            "--yes",
        ],
        &mut Script::default(),
    )
    .await;
    assert_eq!(write.code, exit::REFUSED, "{}", write.err);
    assert!(write.err.contains("permission denied"), "{}", write.err);
    assert_eq!(
        store.get(ids.awaiting).await.unwrap().unwrap().status,
        EffectStatus::AwaitingApproval
    );

    let operator_url = as_role(&operator);
    let write = cli(
        &[
            "--database-url",
            &operator_url,
            "--schema",
            &schema,
            "approve",
            &id,
            "--reason",
            "x",
            "--yes",
        ],
        &mut Script::default(),
    )
    .await;
    assert_eq!(write.code, exit::OK, "{}", write.err);
    let event = store.events(ids.awaiting).await.unwrap().pop().unwrap();
    assert_eq!(
        event.actor.as_deref(),
        Some(format!("operator:{operator} (agent-effects-cli, database role)").as_str())
    );
}
