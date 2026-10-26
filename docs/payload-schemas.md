# Payload schema compatibility

Legacy payloads and unchanged applications use schema version 1. Version
numbers describe your data format, independently of the execution contract
and crate release. Increment a schema when its stored JSON changes meaning
or shape; keep `contract_version` only if external execution semantics remain
compatible.

Handlers override `input_schema()` and `output_schema()` to return
`PayloadSchema::new(version)`. Add `.migrate(|stored_version, json| ...)` for
supported historical versions. Return `PayloadError::UnsupportedVersion` for
unrecognized versions, or `PayloadError::Migration` when validation fails.
The runtime deserializes migrated JSON into the handler's current Rust type.
Migrations must be deterministic, avoid side effects and credentials, and
return errors without sensitive values.

For example, migrating a previously stored numeric output into an object:

```rust
use agent_effects::{PayloadError, PayloadSchema};
use serde_json::json;

let schema = PayloadSchema::new(2).migrate(|version, value| {
    if version != 1 {
        return Err(PayloadError::UnsupportedVersion { stored: version, current: 2 });
    }
    let amount = value.as_u64()
        .ok_or_else(|| PayloadError::Migration("expected an amount".into()))?;
    Ok(json!({ "amount": amount }))
});
```

Closure effects accept `.input_schema_version(version)` and
`.output_schema(schema)`. Input migration is available through durable
handlers, because closures do not recover stored inputs. Existing keys retain
the original stored input, schema, and fingerprint. Recovery migrates a copy;
resubmitting a current input under an older key returns `InputSchemaMismatch`.
Use recovery to finish older effects rather than inventing a new logical key.

New output is stamped with the current output schema in the same durable
transition. Replay migrations operate on copies; history is unchanged.
`Runtime::wait_with_output_schema` reads versioned results without a handler;
`wait` expects version 1. `resolve_with_output_schema` records the version of
operator-supplied applied output; `resolve` uses version 1. Applications must
authorize operators and validate their supplied output.

Durable compensation uses the handler's input/output migrations. Closure
compensation uses `.output_schema(schema)` when its stored result is not
version 1. Unsupported or malformed payloads never invoke the compensation.

A remote success remains committed even when output serialization fails.
The live caller receives `Committed(value)` and an audit note records that
output was not stored. Later reads return `OutputUnavailable`; JSON null is
distinct and can still replay as unit. Unsupported output schemas return
`OutputPayload`; typed deserialization errors return `Output`. These errors
state that the effect committed and must never be converted to a new attempt.
They require a compatible migration or operator investigation, not a new key.

Schema upgrades require SQL migration 0003; no legacy JSON is rewritten.
Unsupported/malformed input appears in `RecoveryReport::resume_errors` with
no external execution. Test migrations against actual stored fixtures before
upgrading. Redacted fields cannot be reconstructed by schema migration; keep
credentials in the handler or an application credential provider.
