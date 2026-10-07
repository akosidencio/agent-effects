//! HTTP effects for [`agent-effects`](https://crates.io/crates/agent-effects).
//!
//! [`HttpEffect`] builds an effect's action from an HTTP request: it sends
//! the effect's stable idempotency key in a header, and classifies every
//! failure so the runtime knows whether the request may have applied.
//!
//! ```no_run
//! use agent_effects::{EffectKind, Runtime};
//! use agent_effects_http::HttpEffect;
//! use agent_effects_memory::MemoryStore;
//!
//! #[derive(serde::Serialize)]
//! struct Charge { amount: u64, currency: &'static str }
//! #[derive(serde::Serialize, serde::Deserialize)]
//! struct Payment { id: String }
//!
//! # async fn demo() -> Result<(), agent_effects::RuntimeError> {
//! let client = reqwest::Client::new();
//! let runtime = Runtime::new(MemoryStore::new());
//! let outcome = runtime
//!     .effect("payment.charge", "order-42")
//!     .kind(EffectKind::IrreversibleWrite)
//!     .remote_idempotency(true) // the provider honours Idempotency-Key
//!     .run(
//!         HttpEffect::post(&client, "https://api.example.com/v1/charges")
//!             .json(&Charge { amount: 4200, currency: "eur" })
//!             .idempotency_key_header()
//!             .send_json::<Payment>(),
//!     )
//!     .await?;
//! # Ok(())
//! # }
//! ```
//!
//! The crate enables no reqwest features: TLS and the rest come from your
//! own `reqwest` dependency and the [`reqwest::Client`] you pass in.
//!
//! # How failures are classified
//!
//! A failure is only definite when the request provably never reached the
//! server, or the server's answer says it did not apply. Everything in
//! between is ambiguous, and the runtime then verifies, re-runs only if
//! safe, or escalates.
//!
//! | Outcome | Class |
//! |---|---|
//! | connect, DNS or TLS failure | `Ambiguous`; `Transient` (never sent) only with [`HttpEffect::client_follows_no_redirects`] |
//! | invalid request (bad URL or header) | `Validation` (never sent) |
//! | timeout or connection lost after sending | `Ambiguous` |
//! | 2xx with an unreadable or unparseable body | `Ambiguous` (it applied, the answer was lost) |
//! | 3xx (seen only when the client does not follow redirects) | `Ambiguous` |
//! | 408, 425, 503 | `Transient` |
//! | 429, or 503 with `Retry-After` | `RateLimited`, honouring `Retry-After` |
//! | 400, 422 | `Validation` |
//! | 401 | `Authentication` |
//! | 403 | `Authorization` |
//! | 409, 500, 502, 504, other 5xx | `Ambiguous` |
//! | 501, other 4xx | `Permanent` |
//!
//! Override per request with [`HttpEffect::classify_status`].
//!
//! A client that follows redirects (reqwest's default) can fail to connect
//! to a redirect's target after the original request applied, and reqwest
//! reports that exactly like a failure to reach the original server. So a
//! connection failure only proves the request was never sent when the
//! client does not follow redirects: build the client with
//! `.redirect(reqwest::redirect::Policy::none())` and say so with
//! [`HttpEffect::client_follows_no_redirects`].

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use agent_effects::{EffectContext, EffectFailure, FailureClass, Verification};
use reqwest::header::{CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue, RETRY_AFTER};
use reqwest::{Client, Method, StatusCode};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

/// The conventional idempotency header.
pub const IDEMPOTENCY_KEY: &str = "Idempotency-Key";

/// At most this much of an error response's body is kept in the message.
const ERROR_BODY_LIMIT: usize = 512;

/// What a response status means.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StatusClass {
    /// The request succeeded; read the body.
    Success,
    /// The request failed with this class.
    Failure(FailureClass),
}

/// The default meaning of `status`; see the [crate docs](crate).
pub fn classify_status(status: StatusCode, headers: &HeaderMap) -> StatusClass {
    let failure = match status.as_u16() {
        200..=299 => return StatusClass::Success,
        // A redirect the client did not follow: whether the request
        // applied before it is unknown.
        300..=399 => FailureClass::Ambiguous,
        408 | 425 => FailureClass::Transient,
        429 => FailureClass::RateLimited {
            retry_after: retry_after(headers),
        },
        503 => match retry_after(headers) {
            Some(after) => FailureClass::RateLimited {
                retry_after: Some(after),
            },
            None => FailureClass::Transient,
        },
        400 | 422 => FailureClass::Validation,
        401 => FailureClass::Authentication,
        403 => FailureClass::Authorization,
        409 | 500 | 502 | 504 => FailureClass::Ambiguous,
        501 => FailureClass::Permanent,
        500..=599 => FailureClass::Ambiguous,
        _ => FailureClass::Permanent,
    };
    StatusClass::Failure(failure)
}

/// `Retry-After` as a delay: seconds, or an HTTP date.
fn retry_after(headers: &HeaderMap) -> Option<Duration> {
    let value = headers.get(RETRY_AFTER)?.to_str().ok()?.trim();
    if let Ok(seconds) = value.parse::<u64>() {
        return Some(Duration::from_secs(seconds));
    }
    let at = httpdate::parse_http_date(value).ok()?;
    Some(at.duration_since(SystemTime::now()).unwrap_or_default())
}

/// The class of a request that failed without a response, from a client
/// that may follow redirects: everything but an invalid request is
/// ambiguous, a connection failure included (see the [crate docs](crate)).
pub fn classify_error(error: &reqwest::Error) -> EffectFailure {
    let message = error.to_string();
    if error.is_builder() {
        EffectFailure::validation(message)
    } else {
        // Timed out or lost after sending, the response broke midway, or a
        // redirect's target was unreachable after the request applied.
        EffectFailure::ambiguous(message)
    }
}

/// Like [`classify_error`], for a client that does not follow redirects: a
/// connection failure (including a connect timeout) means nothing reached
/// the server, so the request was never sent.
pub fn classify_error_without_redirects(error: &reqwest::Error) -> EffectFailure {
    if error.is_connect() {
        EffectFailure::ambiguous(error.to_string()).request_sent(false)
    } else {
        classify_error(error)
    }
}

/// The future an action or verification built here returns.
type Call<T> = Pin<Box<dyn Future<Output = Result<T, EffectFailure>> + Send>>;

type StatusOverride = Arc<dyn Fn(StatusCode, &HeaderMap) -> Option<StatusClass> + Send + Sync>;

/// An HTTP request as an effect's action. Build it, then turn it into an
/// action with [`Self::send_json`] or [`Self::send`].
#[derive(Clone)]
#[must_use]
pub struct HttpEffect {
    client: Client,
    method: Method,
    url: String,
    headers: HeaderMap,
    body: Option<Vec<u8>>,
    idempotency_header: Option<HeaderName>,
    timeout: Option<Duration>,
    classify: Option<StatusOverride>,
    /// The caller declared that the client does not follow redirects.
    no_redirects: bool,
    /// A builder mistake, reported as a `Validation` failure when run.
    invalid: Option<String>,
}

impl HttpEffect {
    /// A request with `method` to `url`.
    pub fn new(client: &Client, method: Method, url: impl Into<String>) -> Self {
        Self {
            client: client.clone(),
            method,
            url: url.into(),
            headers: HeaderMap::new(),
            body: None,
            idempotency_header: None,
            timeout: None,
            classify: None,
            no_redirects: false,
            invalid: None,
        }
    }

    /// A `GET` request.
    pub fn get(client: &Client, url: impl Into<String>) -> Self {
        Self::new(client, Method::GET, url)
    }

    /// A `POST` request.
    pub fn post(client: &Client, url: impl Into<String>) -> Self {
        Self::new(client, Method::POST, url)
    }

    /// A `PUT` request.
    pub fn put(client: &Client, url: impl Into<String>) -> Self {
        Self::new(client, Method::PUT, url)
    }

    /// A `PATCH` request.
    pub fn patch(client: &Client, url: impl Into<String>) -> Self {
        Self::new(client, Method::PATCH, url)
    }

    /// A `DELETE` request.
    pub fn delete(client: &Client, url: impl Into<String>) -> Self {
        Self::new(client, Method::DELETE, url)
    }

    /// Adds a header. Credentials belong here or in the client, not in the
    /// effect's input.
    pub fn header(mut self, name: &str, value: &str) -> Self {
        match (HeaderName::try_from(name), HeaderValue::try_from(value)) {
            (Ok(name), Ok(value)) => {
                self.headers.append(name, value);
            }
            _ => self.invalid = Some(format!("invalid header `{name}`")),
        }
        self
    }

    /// Sends `body` as JSON.
    pub fn json<T: Serialize + ?Sized>(mut self, body: &T) -> Self {
        match serde_json::to_vec(body) {
            Ok(bytes) => {
                self.body = Some(bytes);
                self.headers
                    .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
            }
            Err(e) => self.invalid = Some(format!("body is not JSON-serializable: {e}")),
        }
        self
    }

    /// Sends `body` as is.
    pub fn body(mut self, body: impl Into<Vec<u8>>) -> Self {
        self.body = Some(body.into());
        self
    }

    /// Sends the effect's idempotency key in header `name` on every attempt.
    /// The key is the same across attempts, so a server that honours it
    /// applies the request once. Pair with
    /// [`EffectBuilder::remote_idempotency`](agent_effects::EffectBuilder::remote_idempotency).
    pub fn idempotency_header(mut self, name: &str) -> Self {
        match HeaderName::try_from(name) {
            Ok(name) => self.idempotency_header = Some(name),
            Err(_) => self.invalid = Some(format!("invalid header `{name}`")),
        }
        self
    }

    /// Sends the idempotency key as `Idempotency-Key`.
    pub fn idempotency_key_header(self) -> Self {
        self.idempotency_header(IDEMPOTENCY_KEY)
    }

    /// Gives up on one request after `timeout`: an ambiguous failure, since
    /// it may have been sent.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Declares that the client does not follow redirects: it was built
    /// with `.redirect(reqwest::redirect::Policy::none())`. Only then does a
    /// connection failure prove the request was never sent, so that it is
    /// retried as `Transient`, even for an irreversible write. Without it,
    /// a connection failure is ambiguous. This does not change the client:
    /// declaring it for a client that does follow redirects can duplicate
    /// effects.
    pub fn client_follows_no_redirects(mut self) -> Self {
        self.no_redirects = true;
        self
    }

    /// Overrides how statuses are classified: return `Some` to decide,
    /// `None` to fall back to [`classify_status`]. For APIs with their own
    /// conventions, e.g. a 409 that means "already done".
    pub fn classify_status(
        mut self,
        classify: impl Fn(StatusCode, &HeaderMap) -> Option<StatusClass> + Send + Sync + 'static,
    ) -> Self {
        self.classify = Some(Arc::new(classify));
        self
    }

    /// The action: sends the request and parses a successful response as
    /// JSON `T`. An empty body reads as JSON `null`, e.g. for `T = ()`.
    pub fn send_json<T: DeserializeOwned + Send + 'static>(
        self,
    ) -> impl Fn(EffectContext) -> Call<T> + Send + Sync + 'static {
        let request = Arc::new(self);
        move |ctx| {
            let request = Arc::clone(&request);
            Box::pin(async move {
                let body = request.execute(&ctx).await?;
                parse_json(&body).map_err(|e| {
                    EffectFailure::ambiguous(format!(
                        "the request succeeded but its response could not be read: {e}"
                    ))
                })
            })
        }
    }

    /// The action: sends the request and returns the successful response's
    /// status and body as text.
    pub fn send(self) -> impl Fn(EffectContext) -> Call<HttpResponse> + Send + Sync + 'static {
        let request = Arc::new(self);
        move |ctx| {
            let request = Arc::clone(&request);
            Box::pin(async move {
                let (status, body) = request.execute_with_status(&ctx).await?;
                Ok(HttpResponse {
                    status,
                    body: String::from_utf8_lossy(&body).into_owned(),
                })
            })
        }
    }

    /// A verification: a lookup whose 2xx response, parsed as JSON `T`,
    /// confirms the effect, and whose 404 or 410 (only those) says it did not
    /// apply. Any other status or error is a failed check, which the runtime
    /// treats as inconclusive: a broken lookup must never be read as "not
    /// applied". Pass it to `.verify(..)` if the lookup reads its own
    /// writes, else to `.verify_eventually(settle, ..)`.
    pub fn verify_json<T: DeserializeOwned + Send + 'static>(
        self,
    ) -> impl Fn(EffectContext) -> Call<Verification<T>> + Send + Sync + 'static {
        let request = Arc::new(self);
        move |ctx| {
            let request = Arc::clone(&request);
            Box::pin(async move {
                let response = request.send_once(&ctx).await?;
                let status = response.status();
                // Only "not found" proves the effect did not apply; every
                // other failure is a check that could not tell.
                if matches!(status.as_u16(), 404 | 410) {
                    return Ok(Verification::NotApplied);
                }
                let body = request.success_body(response).await?;
                parse_json(&body)
                    .map(Verification::Confirmed)
                    .map_err(|e| EffectFailure::ambiguous(format!("unreadable lookup: {e}")))
            })
        }
    }

    async fn execute(&self, ctx: &EffectContext) -> Result<Vec<u8>, EffectFailure> {
        self.execute_with_status(ctx).await.map(|(_, body)| body)
    }

    /// Sends one attempt; returns the status and body of a success.
    async fn execute_with_status(
        &self,
        ctx: &EffectContext,
    ) -> Result<(u16, Vec<u8>), EffectFailure> {
        let response = self.send_once(ctx).await?;
        let status = response.status().as_u16();
        Ok((status, self.success_body(response).await?))
    }

    /// Sends the request once. Fails only if no response arrived.
    async fn send_once(&self, ctx: &EffectContext) -> Result<reqwest::Response, EffectFailure> {
        if let Some(problem) = &self.invalid {
            return Err(EffectFailure::validation(problem.clone()));
        }
        let mut request = self
            .client
            .request(self.method.clone(), &self.url)
            .headers(self.headers.clone());
        if let Some(name) = &self.idempotency_header {
            let key = ctx.idempotency_key().to_string();
            let value = HeaderValue::try_from(key).map_err(EffectFailure::validation)?;
            request = request.header(name.clone(), value);
        }
        if let Some(body) = &self.body {
            request = request.body(body.clone());
        }
        if let Some(timeout) = self.timeout {
            request = request.timeout(timeout);
        }
        request.send().await.map_err(|e| {
            if self.no_redirects {
                classify_error_without_redirects(&e)
            } else {
                classify_error(&e)
            }
        })
    }

    /// The body of a successful response, or the classified failure.
    async fn success_body(&self, response: reqwest::Response) -> Result<Vec<u8>, EffectFailure> {
        let status = response.status();
        let verdict = self
            .classify
            .as_ref()
            .and_then(|classify| classify(status, response.headers()))
            .unwrap_or_else(|| classify_status(status, response.headers()));
        match verdict {
            StatusClass::Success => {
                // It applied; losing the body now makes the answer unknown.
                let body = response.bytes().await.map_err(|e| {
                    EffectFailure::ambiguous(format!(
                        "HTTP {status}, but reading the response failed: {e}"
                    ))
                })?;
                Ok(body.to_vec())
            }
            StatusClass::Failure(class) => {
                let body = response.bytes().await.unwrap_or_default();
                let mut snippet = String::from_utf8_lossy(&body).into_owned();
                if snippet.len() > ERROR_BODY_LIMIT {
                    let cut = (0..=ERROR_BODY_LIMIT)
                        .rev()
                        .find(|&i| snippet.is_char_boundary(i))
                        .unwrap_or(0);
                    snippet.truncate(cut);
                    snippet.push('…');
                }
                Err(EffectFailure::new(
                    class,
                    format!("HTTP {status}: {snippet}"),
                ))
            }
        }
    }
}

/// A successful response, as stored and replayed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HttpResponse {
    /// The status code.
    pub status: u16,
    /// The body, as text (invalid UTF-8 replaced).
    pub body: String,
}

fn parse_json<T: DeserializeOwned>(body: &[u8]) -> Result<T, serde_json::Error> {
    if body.iter().all(u8::is_ascii_whitespace) {
        serde_json::from_slice(b"null")
    } else {
        serde_json::from_slice(body)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn class(status: u16, headers: &[(&str, &str)]) -> StatusClass {
        let mut map = HeaderMap::new();
        for (name, value) in headers {
            map.insert(
                HeaderName::try_from(*name).unwrap(),
                HeaderValue::try_from(*value).unwrap(),
            );
        }
        classify_status(StatusCode::from_u16(status).unwrap(), &map)
    }

    #[test]
    fn statuses_map_to_failure_classes() {
        use FailureClass as F;
        use StatusClass::{Failure, Success};
        let table = [
            (200, Success),
            (201, Success),
            (204, Success),
            (301, Failure(F::Ambiguous)),
            (303, Failure(F::Ambiguous)),
            (308, Failure(F::Ambiguous)),
            (400, Failure(F::Validation)),
            (401, Failure(F::Authentication)),
            (403, Failure(F::Authorization)),
            (404, Failure(F::Permanent)),
            (408, Failure(F::Transient)),
            (409, Failure(F::Ambiguous)),
            (418, Failure(F::Permanent)),
            (422, Failure(F::Validation)),
            (425, Failure(F::Transient)),
            (429, Failure(F::RateLimited { retry_after: None })),
            (500, Failure(F::Ambiguous)),
            (501, Failure(F::Permanent)),
            (502, Failure(F::Ambiguous)),
            (503, Failure(F::Transient)),
            (504, Failure(F::Ambiguous)),
            (599, Failure(F::Ambiguous)),
        ];
        for (status, expected) in table {
            assert_eq!(class(status, &[]), expected, "HTTP {status}");
        }
    }

    #[test]
    fn retry_after_is_honoured_in_seconds_and_as_a_date() {
        let seven = Some(Duration::from_secs(7));
        assert_eq!(
            class(429, &[("retry-after", "7")]),
            StatusClass::Failure(FailureClass::RateLimited { retry_after: seven })
        );
        assert_eq!(
            class(503, &[("retry-after", "7")]),
            StatusClass::Failure(FailureClass::RateLimited { retry_after: seven })
        );
        let in_a_minute = httpdate::fmt_http_date(SystemTime::now() + Duration::from_secs(60));
        let StatusClass::Failure(FailureClass::RateLimited {
            retry_after: Some(after),
        }) = class(429, &[("retry-after", &in_a_minute)])
        else {
            panic!("expected a rate limit");
        };
        assert!(after > Duration::from_secs(55) && after <= Duration::from_secs(60));
    }

    #[test]
    fn an_empty_body_reads_as_null() {
        let unit: () = parse_json(b"").unwrap();
        assert_eq!(unit, ());
        let none: Option<u8> = parse_json(b"  \n").unwrap();
        assert_eq!(none, None);
    }
}
