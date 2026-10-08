//! HTTP effects against a real (local) HTTP server.

use std::collections::{HashMap, VecDeque};
use std::fmt::Write as _;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use agent_effects::store::{EffectStore, ErrorRecord};
use agent_effects::{
    EffectKey, EffectKind, EffectName, EffectOutcome, FailureClass, LogicalKey, RetryPolicy,
    Runtime,
};
use agent_effects_http::HttpEffect;
use agent_effects_memory::MemoryStore;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// Extra response headers.
type Headers = Vec<(&'static str, String)>;

/// What the server does with the next scripted POST.
#[derive(Clone, Debug)]
enum Reply {
    /// Answer with this status (applying the POST first if it is 2xx).
    Status(u16, Headers, String),
    /// Apply the POST, then close the connection without answering.
    ApplyThenDrop,
    /// Apply the POST, then answer `303 See Other` to this location.
    ApplyThenRedirect(String),
    /// Never answer.
    Hang,
    /// Answer `500` with a body that never ends.
    EndlessError,
}

#[derive(Clone, Debug)]
struct Recorded {
    method: String,
    path: String,
    headers: HashMap<String, String>,
    body: String,
}

#[derive(Default)]
struct State {
    script: VecDeque<Reply>,
    requests: Vec<Recorded>,
    applied: u32,
    /// Idempotency key → the id it created.
    keys: HashMap<String, String>,
}

/// A tiny HTTP/1.1 server: POSTs create a resource (deduplicated on the
/// `Idempotency-Key`), `GET /lookup` finds it, `GET /broken` answers 405,
/// `GET /unavailable` 503, `GET /missing` 404, and `GET /moved` redirects
/// to `/missing`.
#[derive(Clone)]
struct Server {
    base: String,
    state: Arc<Mutex<State>>,
}

impl Server {
    async fn start(script: impl IntoIterator<Item = Reply>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let state = Arc::new(Mutex::new(State {
            script: script.into_iter().collect(),
            ..State::default()
        }));
        let shared = Arc::clone(&state);
        tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                tokio::spawn(handle(stream, Arc::clone(&shared)));
            }
        });
        Self { base, state }
    }

    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base)
    }

    fn requests(&self) -> Vec<Recorded> {
        self.state.lock().unwrap().requests.clone()
    }

    fn applied(&self) -> u32 {
        self.state.lock().unwrap().applied
    }
}

/// Applies a POST once per idempotency key; returns the resource id.
fn apply(state: &mut State, key: Option<&String>) -> String {
    if let Some(id) = key.and_then(|k| state.keys.get(k)) {
        return id.clone();
    }
    state.applied += 1;
    let id = format!("res#{}", state.applied);
    if let Some(key) = key {
        state.keys.insert(key.clone(), id.clone());
    }
    id
}

/// The answer to a `GET` of `path`.
fn lookup(path: &str, applied: u32) -> (u16, Headers, String) {
    match (path, applied) {
        ("/broken", _) => (405, Vec::new(), "no".to_owned()),
        ("/unavailable", _) => (503, Vec::new(), "down".to_owned()),
        ("/missing", _) => (404, Vec::new(), "not found".to_owned()),
        ("/moved", _) => (
            303,
            vec![("location", "/missing".to_owned())],
            String::new(),
        ),
        (_, 0) => (404, Vec::new(), "not found".to_owned()),
        (_, _) => (200, Vec::new(), r#"{"id":"res#1"}"#.to_owned()),
    }
}

/// Answers `500` with a body that goes on until the client hangs up.
async fn endless_error(stream: &mut TcpStream) {
    let head = "HTTP/1.1 500 X\r\ntransfer-encoding: chunked\r\n\r\n";
    let chunk = format!("{:x}\r\n{}\r\n", 4096, "x".repeat(4096));
    if stream.write_all(head.as_bytes()).await.is_ok() {
        while stream.write_all(chunk.as_bytes()).await.is_ok() {}
    }
}

async fn handle(mut stream: TcpStream, state: Arc<Mutex<State>>) {
    let mut buffer = Vec::new();
    let mut chunk = [0u8; 4096];
    let head_end = loop {
        let n = stream.read(&mut chunk).await.unwrap_or(0);
        if n == 0 {
            return;
        }
        buffer.extend_from_slice(&chunk[..n]);
        if let Some(i) = buffer.windows(4).position(|w| w == b"\r\n\r\n") {
            break i + 4;
        }
    };
    let head = String::from_utf8_lossy(&buffer[..head_end]).into_owned();
    let mut lines = head.lines();
    let mut request_line = lines.next().unwrap().split(' ');
    let (method, path) = (
        request_line.next().unwrap().to_owned(),
        request_line.next().unwrap().to_owned(),
    );
    let headers: HashMap<String, String> = lines
        .filter_map(|l| l.split_once(':'))
        .map(|(k, v)| (k.trim().to_ascii_lowercase(), v.trim().to_owned()))
        .collect();
    let length: usize = headers
        .get("content-length")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    while buffer.len() < head_end + length {
        let n = stream.read(&mut chunk).await.unwrap_or(0);
        if n == 0 {
            break;
        }
        buffer.extend_from_slice(&chunk[..n]);
    }
    let body = String::from_utf8_lossy(&buffer[head_end..]).into_owned();
    let key = headers.get("idempotency-key").cloned();

    let mut endless = false;
    let answer = {
        let mut state = state.lock().unwrap();
        state.requests.push(Recorded {
            method: method.clone(),
            path: path.clone(),
            headers: headers.clone(),
            body,
        });
        if method == "GET" {
            Some(lookup(&path, state.applied))
        } else {
            match state.script.pop_front() {
                None => {
                    let id = apply(&mut state, key.as_ref());
                    Some((200, Vec::new(), format!(r#"{{"id":"{id}"}}"#)))
                }
                Some(Reply::Status(code, extra, payload)) => {
                    if (200..300).contains(&code) {
                        apply(&mut state, key.as_ref());
                    }
                    Some((code, extra, payload))
                }
                Some(Reply::ApplyThenDrop) => {
                    apply(&mut state, key.as_ref());
                    return; // drop the connection unanswered
                }
                Some(Reply::ApplyThenRedirect(location)) => {
                    apply(&mut state, key.as_ref());
                    Some((303, vec![("location", location)], String::new()))
                }
                Some(Reply::Hang) => None,
                Some(Reply::EndlessError) => {
                    endless = true;
                    None
                }
            }
        }
    };
    if endless {
        return endless_error(&mut stream).await;
    }
    let Some((status, extra, payload)) = answer else {
        return std::future::pending().await;
    };
    let mut response = format!(
        "HTTP/1.1 {status} X\r\ncontent-length: {}\r\nconnection: close\r\n",
        payload.len()
    );
    for (name, value) in extra {
        write!(response, "{name}: {value}\r\n").unwrap();
    }
    response.push_str("\r\n");
    response.push_str(&payload);
    let _ = stream.write_all(response.as_bytes()).await;
    let _ = stream.shutdown().await;
}

#[derive(Serialize)]
struct Charge {
    amount: u64,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Payment {
    id: String,
}

const NO_WAIT: RetryPolicy = RetryPolicy {
    max_attempts: 3,
    initial_delay: Duration::ZERO,
    max_delay: Duration::ZERO,
    multiplier: 1.0,
    jitter: false,
};

fn runtime(store: &MemoryStore, retry: RetryPolicy) -> Runtime<MemoryStore> {
    Runtime::builder(store.clone()).retry_policy(retry).build()
}

async fn last_error(store: &MemoryStore, logical: &str) -> ErrorRecord {
    let key = EffectKey::new(
        EffectName::new("http.call").unwrap(),
        LogicalKey::new(logical).unwrap(),
    );
    store
        .get_by_key(&key)
        .await
        .unwrap()
        .unwrap()
        .last_error
        .unwrap()
}

#[tokio::test]
async fn a_success_sends_json_and_the_idempotency_key() {
    let server = Server::start([]).await;
    let client = reqwest::Client::new();
    let rt = runtime(&MemoryStore::new(), NO_WAIT);
    let outcome = rt
        .effect("http.call", "order-1")
        .remote_idempotency(true)
        .run(
            HttpEffect::post(&client, server.url("/charges"))
                .json(&Charge { amount: 4200 })
                .header("authorization", "Bearer test")
                .idempotency_key_header()
                .send_json::<Payment>(),
        )
        .await
        .unwrap();
    assert_eq!(
        outcome,
        EffectOutcome::Committed(Payment { id: "res#1".into() })
    );

    let request = &server.requests()[0];
    assert_eq!(
        (request.method.as_str(), request.path.as_str()),
        ("POST", "/charges")
    );
    assert_eq!(request.body, r#"{"amount":4200}"#);
    assert_eq!(request.headers["content-type"], "application/json");
    assert_eq!(request.headers["authorization"], "Bearer test");
    let expected = EffectKey::new(
        EffectName::new("http.call").unwrap(),
        LogicalKey::new("order-1").unwrap(),
    )
    .idempotency_key()
    .to_string();
    assert_eq!(request.headers["idempotency-key"], expected);
}

#[tokio::test]
async fn statuses_reach_the_runtime_as_failure_classes() {
    let client = reqwest::Client::new();
    let cases: Vec<(u16, Headers, FailureClass)> = vec![
        (400, vec![], FailureClass::Validation),
        (401, vec![], FailureClass::Authentication),
        (403, vec![], FailureClass::Authorization),
        (404, vec![], FailureClass::Permanent),
        (408, vec![], FailureClass::Transient),
        (409, vec![], FailureClass::Ambiguous),
        (422, vec![], FailureClass::Validation),
        (
            429,
            vec![("retry-after", "7".to_owned())],
            FailureClass::RateLimited {
                retry_after: Some(Duration::from_secs(7)),
            },
        ),
        (500, vec![], FailureClass::Ambiguous),
        (501, vec![], FailureClass::Permanent),
        (503, vec![], FailureClass::Transient),
        (504, vec![], FailureClass::Ambiguous),
    ];
    for (status, headers, class) in cases {
        let server = Server::start([Reply::Status(status, headers, "server says no".into())]).await;
        let store = MemoryStore::new();
        let outcome = runtime(&store, RetryPolicy::NONE)
            .effect("http.call", status)
            .run(HttpEffect::post(&client, server.url("/x")).send_json::<Payment>())
            .await
            .unwrap();
        let error = last_error(&store, &status.to_string()).await;
        assert_eq!(error.class, Some(class), "HTTP {status}");
        assert!(
            error.message.contains("server says no"),
            "HTTP {status}: {}",
            error.message
        );
        if class == FailureClass::Ambiguous {
            assert!(
                matches!(outcome, EffectOutcome::NeedsIntervention { .. }),
                "HTTP {status}"
            );
        } else {
            assert!(
                matches!(outcome, EffectOutcome::Failed(_)),
                "HTTP {status}: {outcome:?}"
            );
        }
    }
}

/// An address nothing listens on.
async fn unreachable(path: &str) -> String {
    let free = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}{path}", free.local_addr().unwrap());
    drop(free);
    url
}

fn client_without_redirects() -> reqwest::Client {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap()
}

#[tokio::test]
async fn a_refused_connection_was_never_sent_by_a_client_without_redirects() {
    let url = unreachable("/x").await;
    let store = MemoryStore::new();
    let outcome = runtime(&store, RetryPolicy::NONE)
        .effect("http.call", "refused")
        .run(
            HttpEffect::post(&client_without_redirects(), url)
                .client_follows_no_redirects()
                .send_json::<Payment>(),
        )
        .await
        .unwrap();
    assert!(matches!(outcome, EffectOutcome::Failed(_)), "{outcome:?}");
    assert_eq!(
        last_error(&store, "refused").await.class,
        Some(FailureClass::Transient)
    );
}

#[tokio::test]
async fn a_refused_connection_is_ambiguous_for_a_client_that_may_follow_redirects() {
    let url = unreachable("/x").await;
    let store = MemoryStore::new();
    let outcome = runtime(&store, NO_WAIT)
        .effect("http.call", "maybe-redirected")
        .kind(EffectKind::IrreversibleWrite)
        .run(HttpEffect::post(&reqwest::Client::new(), url).send_json::<Payment>())
        .await
        .unwrap();
    assert!(
        matches!(outcome, EffectOutcome::NeedsIntervention { .. }),
        "{outcome:?}"
    );
    assert_eq!(
        last_error(&store, "maybe-redirected").await.class,
        Some(FailureClass::Ambiguous)
    );
}

#[tokio::test]
async fn an_unreachable_redirect_after_an_applied_request_is_not_resent() {
    let target = unreachable("/receipt").await;
    let server = Server::start([
        Reply::ApplyThenRedirect(target.clone()),
        Reply::ApplyThenRedirect(target),
    ])
    .await;
    let outcome = runtime(&MemoryStore::new(), NO_WAIT)
        .effect("http.call", "redirected")
        .kind(EffectKind::IrreversibleWrite)
        .run(HttpEffect::post(&reqwest::Client::new(), server.url("/charges")).send())
        .await
        .unwrap();
    assert!(
        matches!(outcome, EffectOutcome::NeedsIntervention { .. }),
        "{outcome:?}"
    );
    assert_eq!(server.applied(), 1, "applied once");
}

#[tokio::test]
async fn an_unfollowed_redirect_is_ambiguous() {
    let server = Server::start([
        Reply::ApplyThenRedirect("/receipt".into()),
        Reply::ApplyThenRedirect("/receipt".into()),
    ])
    .await;
    let store = MemoryStore::new();
    let outcome = runtime(&store, NO_WAIT)
        .effect("http.call", "unfollowed")
        .kind(EffectKind::IrreversibleWrite)
        .run(
            HttpEffect::post(&client_without_redirects(), server.url("/charges"))
                .client_follows_no_redirects()
                .send(),
        )
        .await
        .unwrap();
    assert!(
        matches!(outcome, EffectOutcome::NeedsIntervention { .. }),
        "{outcome:?}"
    );
    assert_eq!(server.applied(), 1, "applied once");
    assert_eq!(
        last_error(&store, "unfollowed").await.class,
        Some(FailureClass::Ambiguous)
    );
}

#[tokio::test]
async fn a_timeout_after_sending_is_ambiguous() {
    let server = Server::start([Reply::Hang]).await;
    let store = MemoryStore::new();
    let call = runtime(&store, RetryPolicy::NONE)
        .effect("http.call", "slow")
        .run(
            HttpEffect::post(&reqwest::Client::new(), server.url("/x"))
                .timeout(Duration::from_millis(300))
                .send_json::<Payment>(),
        );
    let outcome = tokio::time::timeout(Duration::from_secs(10), call)
        .await
        .expect("the request timeout fired")
        .unwrap();
    assert!(
        matches!(outcome, EffectOutcome::NeedsIntervention { .. }),
        "{outcome:?}"
    );
    assert_eq!(
        last_error(&store, "slow").await.class,
        Some(FailureClass::Ambiguous)
    );
    assert_eq!(server.requests().len(), 1, "it was sent");
}

#[tokio::test]
async fn a_dropped_answer_is_resent_under_the_same_key_and_applied_once() {
    let server = Server::start([Reply::ApplyThenDrop]).await;
    let outcome = runtime(&MemoryStore::new(), NO_WAIT)
        .effect("http.call", "dropped")
        .remote_idempotency(true)
        .run(
            HttpEffect::post(&reqwest::Client::new(), server.url("/charges"))
                .idempotency_key_header()
                .send_json::<Payment>(),
        )
        .await
        .unwrap();
    assert_eq!(
        outcome,
        EffectOutcome::Committed(Payment { id: "res#1".into() })
    );
    let requests = server.requests();
    assert_eq!(requests.len(), 2, "re-sent");
    assert_eq!(
        requests[0].headers["idempotency-key"],
        requests[1].headers["idempotency-key"]
    );
    assert_eq!(server.applied(), 1, "applied once");
}

#[tokio::test]
async fn a_lookup_confirms_a_dropped_answer_without_resending() {
    let server = Server::start([Reply::ApplyThenDrop]).await;
    let client = reqwest::Client::new();
    let outcome = runtime(&MemoryStore::new(), NO_WAIT)
        .effect("http.call", "verified")
        .kind(EffectKind::IrreversibleWrite)
        .verify(HttpEffect::get(&client, server.url("/lookup")).verify_json::<Payment>())
        .run(HttpEffect::post(&client, server.url("/charges")).send_json::<Payment>())
        .await
        .unwrap();
    assert_eq!(
        outcome,
        EffectOutcome::Committed(Payment { id: "res#1".into() })
    );
    let posts = server
        .requests()
        .iter()
        .filter(|r| r.method == "POST")
        .count();
    assert_eq!((posts, server.applied()), (1, 1));
}

#[tokio::test]
async fn a_lookup_that_finds_nothing_lets_the_effect_run_again() {
    // The first request is lost before it applies; the lookup's 404 proves
    // that, so the runtime sends it again.
    let server = Server::start([Reply::Hang]).await;
    let client = reqwest::Client::new();
    let outcome = runtime(&MemoryStore::new(), NO_WAIT)
        .effect("http.call", "lost")
        .kind(EffectKind::IrreversibleWrite)
        .verify(HttpEffect::get(&client, server.url("/lookup")).verify_json::<Payment>())
        .run(
            HttpEffect::post(&client, server.url("/charges"))
                .timeout(Duration::from_millis(300))
                .send_json::<Payment>(),
        )
        .await
        .unwrap();
    assert_eq!(
        outcome,
        EffectOutcome::Committed(Payment { id: "res#1".into() })
    );
    let posts = server
        .requests()
        .iter()
        .filter(|r| r.method == "POST")
        .count();
    assert_eq!((posts, server.applied()), (2, 1));
}

#[tokio::test]
async fn a_broken_lookup_is_never_read_as_not_applied() {
    // The POST applied and its answer was lost; the lookup answers 405. The
    // runtime must not take that as "not applied" and charge again.
    let server = Server::start([Reply::ApplyThenDrop]).await;
    let client = reqwest::Client::new();
    let outcome = runtime(&MemoryStore::new(), NO_WAIT)
        .effect("http.call", "broken-lookup")
        .verify(HttpEffect::get(&client, server.url("/broken")).verify_json::<Payment>())
        .run(HttpEffect::post(&client, server.url("/charges")).send_json::<Payment>())
        .await
        .unwrap();
    assert!(
        matches!(outcome, EffectOutcome::Unknown { .. }),
        "{outcome:?}"
    );
    let posts = server
        .requests()
        .iter()
        .filter(|r| r.method == "POST")
        .count();
    assert_eq!((posts, server.applied()), (1, 1), "never re-sent");
}

#[tokio::test]
async fn an_unreadable_success_is_ambiguous() {
    let server = Server::start([Reply::Status(200, vec![], "not json".into())]).await;
    let store = MemoryStore::new();
    let outcome = runtime(&store, RetryPolicy::NONE)
        .effect("http.call", "garbled")
        .run(HttpEffect::post(&reqwest::Client::new(), server.url("/x")).send_json::<Payment>())
        .await
        .unwrap();
    assert!(
        matches!(outcome, EffectOutcome::NeedsIntervention { .. }),
        "{outcome:?}"
    );
    assert_eq!(
        last_error(&store, "garbled").await.class,
        Some(FailureClass::Ambiguous)
    );
    assert_eq!(server.applied(), 1, "it did apply");
}

#[tokio::test]
async fn an_invalid_request_is_a_validation_failure_and_never_sent() {
    let server = Server::start([]).await;
    let store = MemoryStore::new();
    let outcome = runtime(&store, NO_WAIT)
        .effect("http.call", "invalid")
        .run(
            HttpEffect::post(&reqwest::Client::new(), server.url("/x"))
                .header("bad header", "x")
                .send_json::<Payment>(),
        )
        .await
        .unwrap();
    assert!(matches!(outcome, EffectOutcome::Failed(_)), "{outcome:?}");
    assert_eq!(
        last_error(&store, "invalid").await.class,
        Some(FailureClass::Validation)
    );
    assert!(server.requests().is_empty());
}

#[tokio::test]
async fn a_failing_status_after_a_redirect_is_ambiguous() {
    // The POST applied, then the client followed the redirect to a target
    // that failed. That failure says nothing about the POST: a 503 must not
    // re-send it, a 405 must not report it failed.
    for (target, logical) in [
        ("/unavailable", "redirect-503"),
        ("/broken", "redirect-405"),
    ] {
        let server = Server::start([
            Reply::ApplyThenRedirect(target.into()),
            Reply::ApplyThenRedirect(target.into()),
            Reply::ApplyThenRedirect(target.into()),
        ])
        .await;
        let store = MemoryStore::new();
        let outcome = runtime(&store, NO_WAIT)
            .effect("http.call", logical)
            .kind(EffectKind::IrreversibleWrite)
            .run(HttpEffect::post(&reqwest::Client::new(), server.url("/charges")).send())
            .await
            .unwrap();
        assert!(
            matches!(outcome, EffectOutcome::NeedsIntervention { .. }),
            "{target}: {outcome:?}"
        );
        assert_eq!(server.applied(), 1, "{target}: applied once");
        let error = last_error(&store, logical).await;
        assert_eq!(error.class, Some(FailureClass::Ambiguous), "{target}");
        assert!(error.message.contains("redirect"), "{}", error.message);
    }
}

#[tokio::test]
async fn a_lookup_redirected_to_not_found_is_not_read_as_not_applied() {
    // The POST applied and its answer was lost; the lookup is redirected to
    // a 404. That 404 is the target's, so the POST must not be re-sent.
    let server = Server::start([Reply::ApplyThenDrop]).await;
    let client = reqwest::Client::new();
    let outcome = runtime(&MemoryStore::new(), NO_WAIT)
        .effect("http.call", "moved-lookup")
        .verify(HttpEffect::get(&client, server.url("/moved")).verify_json::<Payment>())
        .run(HttpEffect::post(&client, server.url("/charges")).send_json::<Payment>())
        .await
        .unwrap();
    assert!(
        matches!(outcome, EffectOutcome::Unknown { .. }),
        "{outcome:?}"
    );
    let posts = server
        .requests()
        .iter()
        .filter(|r| r.method == "POST")
        .count();
    assert_eq!((posts, server.applied()), (1, 1), "never re-sent");
}

#[tokio::test]
async fn an_endless_error_body_is_cut_short() {
    let server = Server::start([Reply::EndlessError]).await;
    let store = MemoryStore::new();
    let call = runtime(&store, RetryPolicy::NONE)
        .effect("http.call", "endless")
        .run(HttpEffect::post(&reqwest::Client::new(), server.url("/x")).send_json::<Payment>());
    let outcome = tokio::time::timeout(Duration::from_secs(10), call)
        .await
        .expect("reading the error body stops early")
        .unwrap();
    assert!(
        matches!(outcome, EffectOutcome::NeedsIntervention { .. }),
        "{outcome:?}"
    );
    let error = last_error(&store, "endless").await;
    assert_eq!(error.class, Some(FailureClass::Ambiguous));
    assert!(error.message.len() < 600, "{}", error.message.len());
    assert!(error.message.ends_with('…'));
}
