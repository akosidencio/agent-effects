//! Stable fingerprints of effect inputs.
//!
//! A fingerprint is SHA-256 over a canonical JSON encoding: object keys
//! sorted at every level, no whitespace. Canonicalizing explicitly, rather
//! than relying on `serde_json`'s map ordering, keeps fingerprints stable even
//! if another crate in the build enables `serde_json/preserve_order`, and for
//! inputs containing `HashMap`s.
//!
//! The format (`sha256:<hex>`) is stored in effect records and is part of the
//! stability contract.

use std::fmt::Write as _;

use serde_json::Value;
use sha2::{Digest, Sha256};

/// The fingerprint of `value`.
pub(crate) fn fingerprint(value: &Value) -> String {
    let mut canonical = String::new();
    write_canonical(value, &mut canonical);
    let digest = Sha256::digest(canonical.as_bytes());
    let mut out = String::with_capacity(7 + 64);
    out.push_str("sha256:");
    for byte in &digest {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

fn write_canonical(value: &Value, out: &mut String) {
    match value {
        Value::Object(map) => {
            let mut entries: Vec<_> = map.iter().collect();
            entries.sort_unstable_by(|a, b| a.0.cmp(b.0));
            out.push('{');
            for (i, (key, value)) in entries.into_iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str(&Value::String(key.clone()).to_string());
                out.push(':');
                write_canonical(value, out);
            }
            out.push('}');
        }
        Value::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_canonical(item, out);
            }
            out.push(']');
        }
        scalar => out.push_str(&scalar.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use serde_json::json;

    use super::*;

    #[test]
    fn key_order_does_not_matter() {
        let a = json!({ "b": 1, "a": { "y": [1, 2], "x": null } });
        let b = json!({ "a": { "x": null, "y": [1, 2] }, "b": 1 });
        assert_eq!(fingerprint(&a), fingerprint(&b));
    }

    #[test]
    fn hash_maps_fingerprint_stably() {
        let map: HashMap<String, u32> = (0..64).map(|i| (format!("k{i}"), i)).collect();
        let first = fingerprint(&serde_json::to_value(&map).unwrap());
        for _ in 0..8 {
            let copy: HashMap<String, u32> = map.clone().into_iter().collect();
            assert_eq!(fingerprint(&serde_json::to_value(&copy).unwrap()), first);
        }
    }

    #[test]
    fn values_and_structure_matter() {
        assert_ne!(
            fingerprint(&json!({ "a": 1 })),
            fingerprint(&json!({ "a": 2 }))
        );
        assert_ne!(fingerprint(&json!([1, 2])), fingerprint(&json!([2, 1])));
        assert_ne!(fingerprint(&json!("1")), fingerprint(&json!(1)));
    }

    #[test]
    fn format_is_pinned() {
        // Pinned: changing the encoding breaks matching against stored records.
        assert_eq!(
            fingerprint(&json!({ "b": [true, null], "a": "x\"y" })),
            "sha256:f30e11011c10c5d0aa8bc6c5d5d3524c5674821d8e100c82b9c1de63bda884e8"
        );
    }
}
