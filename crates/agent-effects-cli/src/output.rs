//! What the CLI prints. Every view is built as JSON first; text output is
//! rendered from the same values, so both formats show the same facts.
//!
//! Stored inputs and outputs are omitted unless asked for: they are the
//! fields most likely to hold personal or secret data. Audit notes, errors
//! and keys are shown; the runtime redacts them before storing them when the
//! application configures a redactor.

use std::fmt::Write as _;
use std::time::{SystemTime, UNIX_EPOCH};

use agent_effects::store::{EffectEvent, LateObservation};
use agent_effects::{EffectRecord, EffectStatus};
use serde_json::{Value, json};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

pub(crate) fn timestamp(at: SystemTime) -> String {
    // Stored timestamps can be outside time's calendar range. Its From
    // conversion panics there, before a formatting fallback can run.
    let nanos = match at.duration_since(UNIX_EPOCH) {
        Ok(after) => i128::try_from(after.as_nanos()).ok(),
        Err(before) => i128::try_from(before.duration().as_nanos())
            .ok()
            .map(|nanos| -nanos),
    };
    nanos
        .and_then(|nanos| OffsetDateTime::from_unix_timestamp_nanos(nanos).ok())
        .and_then(|at| at.format(&Rfc3339).ok())
        .unwrap_or_else(|| format!("{at:?}"))
}

fn optional_time(at: Option<SystemTime>) -> Value {
    at.map_or(Value::Null, |at| Value::String(timestamp(at)))
}

/// One line of a listing.
pub(crate) fn summary(record: &EffectRecord, now: SystemTime) -> Value {
    json!({
        "id": record.id.to_string(),
        "name": record.key.name.as_str(),
        "key": record.key.key.as_str(),
        "status": record.status.as_str(),
        "attempts": record.attempt_count,
        "version": record.version,
        "updated_at": timestamp(record.updated_at),
        "next_attempt_at": optional_time(record.next_attempt_at),
        "retry_due": retry_due(record, now),
        "leased": record.live_lease_owner(now).is_some(),
    })
}

/// Whether a scheduled retry may start now; `null` when none is scheduled.
fn retry_due(record: &EffectRecord, now: SystemTime) -> Value {
    match (record.status, record.next_attempt_at) {
        (EffectStatus::Pending | EffectStatus::Compensating, Some(at)) => Value::Bool(at <= now),
        _ => Value::Null,
    }
}

/// Everything about one effect.
pub(crate) fn detail(
    record: &EffectRecord,
    late: &[LateObservation],
    now: SystemTime,
    show_payloads: bool,
) -> Value {
    let mut view = json!({
        "id": record.id.to_string(),
        "name": record.key.name.as_str(),
        "key": record.key.key.as_str(),
        "kind": record.kind.as_str(),
        "status": record.status.as_str(),
        "version": record.version,
        "attempts": record.attempt_count,
        "may_have_applied": record.may_have_applied,
        "approved": record.approved,
        "compensation_attempts": record.compensation_attempts,
        "retry": {
            "next_attempt_at": optional_time(record.next_attempt_at),
            "due": retry_due(record, now),
        },
        "lease": {
            "owner": record.lease_owner.as_ref().map(ToString::to_string),
            "epoch": record.lease_epoch,
            "expires_at": optional_time(record.lease_expires_at),
            "live": record.live_lease_owner(now).is_some(),
        },
        "created_by": record.created_by,
        "created_at": timestamp(record.created_at),
        "updated_at": timestamp(record.updated_at),
        "committed_at": optional_time(record.committed_at),
        "attempt_started_at": optional_time(record.attempt_started_at),
        "attempt_ended_at": optional_time(record.attempt_ended_at),
        "last_error": record.last_error.as_ref().map(|e| json!({
            "class": e.class,
            "message": e.message,
        })),
        "execution_contract": record.execution_contract,
        "input_schema_version": record.input_schema_version,
        "output_schema_version": record.output_schema_version,
        "input_fingerprint": record.input_fingerprint,
        "input_stored": record.input.is_some(),
        "output_stored": record.output.is_some(),
        "late_observations": late.iter().map(late_view).collect::<Vec<_>>(),
    });
    if show_payloads {
        view["input"] = record.input.clone().unwrap_or(Value::Null);
        view["output"] = record.output.clone().unwrap_or(Value::Null);
    }
    view
}

pub(crate) fn event_view(event: &EffectEvent) -> Value {
    json!({
        "sequence": event.sequence,
        "at": timestamp(event.at),
        "transition": event.transition.as_str(),
        "from": event.from.as_str(),
        "to": event.to.as_str(),
        "attempt": event.attempt,
        "actor": event.actor,
        "payload": event.payload,
    })
}

pub(crate) fn late_view(late: &LateObservation) -> Value {
    json!({
        "id": late.id.to_string(),
        "worker": late.worker.to_string(),
        "lease_epoch": late.lease_epoch,
        "attempt": late.attempt,
        "attempt_started_at": optional_time(late.attempt_started_at),
        "observed_at": timestamp(late.observed_at),
        "outcome": format!("{:?}", late.outcome).to_lowercase(),
    })
}

/// Text rendering: a table for a list of flat objects, `key: value` lines
/// for one object.
pub(crate) fn table(rows: &[Value], columns: &[&str]) -> String {
    let cells: Vec<Vec<String>> = rows
        .iter()
        .map(|row| columns.iter().map(|c| cell(&row[*c])).collect())
        .collect();
    let widths: Vec<usize> = columns
        .iter()
        .enumerate()
        .map(|(i, c)| {
            cells
                .iter()
                .map(|r| r[i].chars().count())
                .chain([c.len()])
                .max()
                .unwrap_or(0)
        })
        .collect();
    let mut out = String::new();
    let line = |out: &mut String, values: Vec<String>| {
        let padded: Vec<String> = values
            .iter()
            .zip(&widths)
            .map(|(v, w)| format!("{v:<w$}"))
            .collect();
        let _ = writeln!(out, "{}", padded.join("  ").trim_end());
    };
    line(&mut out, columns.iter().map(|c| c.to_uppercase()).collect());
    for row in cells {
        line(&mut out, row);
    }
    out
}

pub(crate) fn fields(value: &Value, indent: usize) -> String {
    let mut out = String::new();
    if let Value::Object(map) = value {
        for (key, v) in map {
            match v {
                Value::Object(inner) if !inner.is_empty() && key != "execution_contract" => {
                    let _ = writeln!(out, "{:indent$}{key}:", "");
                    out.push_str(&fields(v, indent + 2));
                }
                Value::Array(items) if items.iter().all(Value::is_object) && !items.is_empty() => {
                    let _ = writeln!(out, "{:indent$}{key}:", "");
                    for item in items {
                        let _ = writeln!(out, "{:w$}-", "", w = indent + 2);
                        out.push_str(&fields(item, indent + 4));
                    }
                }
                _ => {
                    let _ = writeln!(out, "{:indent$}{key}: {}", "", cell(v));
                }
            }
        }
    }
    out
}

fn cell(value: &Value) -> String {
    match value {
        Value::Null => "-".into(),
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod timestamp_tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn timestamps_outside_the_calendar_range_have_a_safe_fallback() {
        assert_eq!(timestamp(UNIX_EPOCH), "1970-01-01T00:00:00Z");
        let delta = Duration::from_millis(i64::MAX.unsigned_abs());
        for at in [UNIX_EPOCH.checked_add(delta), UNIX_EPOCH.checked_sub(delta)]
            .into_iter()
            .flatten()
        {
            assert_eq!(timestamp(at), format!("{at:?}"));
        }
    }
}
