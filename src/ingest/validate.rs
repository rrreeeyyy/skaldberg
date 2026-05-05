//! Pure validation. No I/O, no globals — `now_ms` is passed in so tests
//! can pin the clock.
//!
//! Rejection rules (matches the §1 / §6 / §9 decisions in `phase-3-ingester.md`):
//!   - metric and label names match `[a-zA-Z_][a-zA-Z0-9_]*`
//!   - label names starting with `__` are reserved
//!   - value must be finite (NaN/Inf rejected — they break rate macros)
//!   - timestamp must be within `[now - GRACE_PAST, now + GRACE_FUTURE]`
//!
//! Successful validation produces a `ValidatedSample` whose `series_id` is
//! derived deterministically from `(metric, sorted_labels)`. The shift-by-1
//! keeps the id in non-negative i64 range so downstream code can store it
//! as BIGINT (Phase 2 convention).

use std::collections::BTreeMap;

use thiserror::Error;
use xxhash_rust::xxh3::xxh3_64;

use crate::ingest::types::{RawSample, ValidatedSample};

/// 1 hour. Beyond this, we reject as out-of-order. Matches the design's
/// "OOO grace window" knob.
pub const GRACE_PAST_MS: i64 = 60 * 60 * 1_000;

/// 5 minutes. Allow modest client clock skew without rejecting writes.
pub const GRACE_FUTURE_MS: i64 = 5 * 60 * 1_000;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ValidationError {
    #[error("metric name is empty")]
    EmptyMetricName,
    #[error("metric name '{0}' contains invalid characters (must match [a-zA-Z_][a-zA-Z0-9_]*)")]
    InvalidMetricName(String),
    #[error("label name '{0}' is invalid (must match [a-zA-Z_][a-zA-Z0-9_]*)")]
    InvalidLabelName(String),
    #[error("label name '{0}' is reserved (starts with '__')")]
    ReservedLabelName(String),
    #[error("value is not finite: {0}")]
    NonFiniteValue(String),
    #[error("timestamp {ts} ms is too old: now={now} ms, grace={grace_ms} ms")]
    TimestampTooOld { ts: i64, now: i64, grace_ms: i64 },
    #[error("timestamp {ts} ms is in the future: now={now} ms, grace={grace_ms} ms")]
    TimestampTooNew { ts: i64, now: i64, grace_ms: i64 },
}

pub fn validate(raw: RawSample, now_ms: i64) -> Result<ValidatedSample, ValidationError> {
    if raw.metric.is_empty() {
        return Err(ValidationError::EmptyMetricName);
    }
    if !is_valid_name(&raw.metric) {
        return Err(ValidationError::InvalidMetricName(raw.metric));
    }

    let mut sorted: BTreeMap<String, String> = BTreeMap::new();
    for (k, v) in raw.labels.into_iter() {
        if k.starts_with("__") {
            return Err(ValidationError::ReservedLabelName(k));
        }
        if !is_valid_name(&k) {
            return Err(ValidationError::InvalidLabelName(k));
        }
        sorted.insert(k, v);
    }

    if !raw.value.is_finite() {
        let s = if raw.value.is_nan() {
            "NaN".to_string()
        } else if raw.value > 0.0 {
            "Infinity".to_string()
        } else {
            "-Infinity".to_string()
        };
        return Err(ValidationError::NonFiniteValue(s));
    }

    let lo = now_ms - GRACE_PAST_MS;
    let hi = now_ms + GRACE_FUTURE_MS;
    if raw.ts < lo {
        return Err(ValidationError::TimestampTooOld {
            ts: raw.ts,
            now: now_ms,
            grace_ms: GRACE_PAST_MS,
        });
    }
    if raw.ts > hi {
        return Err(ValidationError::TimestampTooNew {
            ts: raw.ts,
            now: now_ms,
            grace_ms: GRACE_FUTURE_MS,
        });
    }

    let series_id = derive_series_id(&raw.metric, &sorted);
    let ts_us = raw.ts.saturating_mul(1_000);

    Ok(ValidatedSample {
        series_id,
        metric: raw.metric,
        labels: sorted,
        ts_us,
        value: raw.value,
    })
}

fn is_valid_name(name: &str) -> bool {
    let mut chars = name.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// `xxh3_64(metric + 0x00 + (k=v\0)*) >> 1`, cast to i64.
///
/// Right-shift keeps the id non-negative; cast is lossless for the result
/// of the shift. Order of `sorted_labels` matters for hash stability — we
/// require the caller to pass a `BTreeMap` (which iterates in key order)
/// rather than a `HashMap`.
pub fn derive_series_id(metric: &str, sorted_labels: &BTreeMap<String, String>) -> i64 {
    let mut buf: Vec<u8> = Vec::with_capacity(64 + metric.len());
    buf.extend_from_slice(metric.as_bytes());
    buf.push(0);
    for (k, v) in sorted_labels {
        buf.extend_from_slice(k.as_bytes());
        buf.push(b'=');
        buf.extend_from_slice(v.as_bytes());
        buf.push(0);
    }
    let h = xxh3_64(&buf);
    (h >> 1) as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    const NOW: i64 = 1_714_800_000_000; // arbitrary fixed instant

    fn raw(metric: &str, labels: &[(&str, &str)], ts: i64, value: f64) -> RawSample {
        let mut hm = HashMap::new();
        for (k, v) in labels {
            hm.insert((*k).to_string(), (*v).to_string());
        }
        RawSample {
            metric: metric.into(),
            labels: hm,
            ts,
            value,
        }
    }

    #[test]
    fn ok_basic() {
        let r = raw("http_requests_total", &[("job", "api"), ("status", "200")], NOW, 42.0);
        let v = validate(r, NOW).unwrap();
        assert_eq!(v.metric, "http_requests_total");
        assert_eq!(v.labels.len(), 2);
        assert_eq!(v.ts_us, NOW * 1_000);
        assert_eq!(v.value, 42.0);
        assert!(v.series_id >= 0);
    }

    #[test]
    fn series_id_is_label_order_invariant() {
        // HashMap iteration order is non-deterministic, but BTreeMap (used
        // internally) sorts. Different request orderings of the same labels
        // must produce the same series_id.
        let r1 = raw("m", &[("a", "1"), ("b", "2"), ("c", "3")], NOW, 1.0);
        let r2 = raw("m", &[("c", "3"), ("a", "1"), ("b", "2")], NOW, 1.0);
        assert_eq!(validate(r1, NOW).unwrap().series_id, validate(r2, NOW).unwrap().series_id);
    }

    #[test]
    fn series_id_distinguishes_metric_name() {
        let v1 = validate(raw("m1", &[("a", "1")], NOW, 1.0), NOW).unwrap();
        let v2 = validate(raw("m2", &[("a", "1")], NOW, 1.0), NOW).unwrap();
        assert_ne!(v1.series_id, v2.series_id);
    }

    #[test]
    fn series_id_distinguishes_label_value() {
        let v1 = validate(raw("m", &[("a", "1")], NOW, 1.0), NOW).unwrap();
        let v2 = validate(raw("m", &[("a", "2")], NOW, 1.0), NOW).unwrap();
        assert_ne!(v1.series_id, v2.series_id);
    }

    #[test]
    fn series_id_distinguishes_label_name() {
        let v1 = validate(raw("m", &[("a", "1")], NOW, 1.0), NOW).unwrap();
        let v2 = validate(raw("m", &[("b", "1")], NOW, 1.0), NOW).unwrap();
        assert_ne!(v1.series_id, v2.series_id);
    }

    #[test]
    fn series_id_is_non_negative_for_random_inputs() {
        for i in 0..2000 {
            let m = format!("m{}", i);
            let v = validate(raw(&m, &[], NOW, 1.0), NOW).unwrap();
            assert!(v.series_id >= 0, "negative id for {}: {}", m, v.series_id);
        }
    }

    #[test]
    fn rejects_empty_metric() {
        let r = raw("", &[], NOW, 1.0);
        assert_eq!(validate(r, NOW), Err(ValidationError::EmptyMetricName));
    }

    #[test]
    fn rejects_invalid_metric_chars() {
        for bad in ["foo bar", "1foo", "foo-bar", "foo.bar", "foo:bar", "日本語"] {
            let r = raw(bad, &[], NOW, 1.0);
            match validate(r, NOW) {
                Err(ValidationError::InvalidMetricName(_)) => {}
                other => panic!("expected InvalidMetricName for {:?}, got {:?}", bad, other),
            }
        }
    }

    #[test]
    fn rejects_invalid_label_name() {
        let r = raw("m", &[("bad name", "v")], NOW, 1.0);
        assert!(matches!(
            validate(r, NOW),
            Err(ValidationError::InvalidLabelName(_))
        ));
    }

    #[test]
    fn rejects_reserved_label_name() {
        let r = raw("m", &[("__name__", "v")], NOW, 1.0);
        assert!(matches!(
            validate(r, NOW),
            Err(ValidationError::ReservedLabelName(_))
        ));
    }

    #[test]
    fn rejects_nan() {
        let r = raw("m", &[], NOW, f64::NAN);
        assert_eq!(
            validate(r, NOW),
            Err(ValidationError::NonFiniteValue("NaN".into()))
        );
    }

    #[test]
    fn rejects_pos_inf() {
        let r = raw("m", &[], NOW, f64::INFINITY);
        assert_eq!(
            validate(r, NOW),
            Err(ValidationError::NonFiniteValue("Infinity".into()))
        );
    }

    #[test]
    fn rejects_neg_inf() {
        let r = raw("m", &[], NOW, f64::NEG_INFINITY);
        assert_eq!(
            validate(r, NOW),
            Err(ValidationError::NonFiniteValue("-Infinity".into()))
        );
    }

    #[test]
    fn rejects_too_old_timestamp() {
        let r = raw("m", &[], NOW - GRACE_PAST_MS - 1, 1.0);
        assert!(matches!(
            validate(r, NOW),
            Err(ValidationError::TimestampTooOld { .. })
        ));
    }

    #[test]
    fn rejects_too_new_timestamp() {
        let r = raw("m", &[], NOW + GRACE_FUTURE_MS + 1, 1.0);
        assert!(matches!(
            validate(r, NOW),
            Err(ValidationError::TimestampTooNew { .. })
        ));
    }

    #[test]
    fn accepts_at_grace_boundaries() {
        let r = raw("m", &[], NOW - GRACE_PAST_MS, 1.0);
        assert!(validate(r, NOW).is_ok());
        let r = raw("m", &[], NOW + GRACE_FUTURE_MS, 1.0);
        assert!(validate(r, NOW).is_ok());
    }

    #[test]
    fn accepts_underscore_first() {
        let r = raw("_foo", &[("_bar", "v")], NOW, 1.0);
        assert!(validate(r, NOW).is_ok());
    }

    #[test]
    fn accepts_no_labels() {
        let r = raw("m", &[], NOW, 1.0);
        let v = validate(r, NOW).unwrap();
        assert_eq!(v.labels.len(), 0);
        assert!(v.series_id >= 0);
    }

    #[test]
    fn ts_ms_to_us_conversion() {
        let r = raw("m", &[], 1_000, 1.0);
        // Need a `now_ms` close enough to 1_000 to pass grace; just override.
        let v = validate(r, 1_000).unwrap();
        assert_eq!(v.ts_us, 1_000_000);
    }
}
