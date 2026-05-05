//! Prometheus Remote-Write 1.0 receiver.
//!
//! Spec: <https://prometheus.io/docs/specs/prw/remote_write_spec/>
//!
//! Wire format:
//! - HTTP POST
//! - `Content-Type: application/x-protobuf`
//! - `Content-Encoding: snappy` (raw snappy, NOT the framing format)
//! - `X-Prometheus-Remote-Write-Version: 0.1.0`
//! - Body: snappy-compressed protobuf `WriteRequest`
//! - Response body MUST be empty; status code conveys outcome
//!     - 200 / 204 = accepted
//!     - 4xx = non-retryable (sender drops the batch)
//!     - 5xx / 429 = retryable
//!
//! Schema:
//! ```proto
//! message WriteRequest { repeated TimeSeries timeseries = 1; }
//! message TimeSeries   { repeated Label labels = 1;
//!                        repeated Sample samples = 2;
//!                        repeated Exemplar exemplars = 3;
//!                        repeated Histogram histograms = 4; }
//! message Label        { string name = 1; string value = 2; }
//! message Sample       { double value = 1; int64 timestamp = 2; }
//! ```
//!
//! We accept v1 (`prompb.WriteRequest`). Histograms and Exemplars are
//! ignored — `prost` silently drops fields it doesn't have type
//! information for, and we declare only `labels` + `samples` here.
//!
//! The Prometheus convention is that the metric name is carried in a
//! reserved label `__name__`. We extract that and route it as
//! `RawSample::metric`; all other labels are passed through as-is.
//! Labels starting with `__` (other than `__name__`) are reserved by
//! Prometheus for internal use and we drop them quietly rather than
//! reject the sample (validate() would also reject them, but tooling
//! would lose the rest of the batch unnecessarily).

use std::collections::HashMap;

use prost::Message;
use thiserror::Error;

use crate::ingest::types::RawSample;

/// Maximum decompressed body we'll accept. Snappy can expand to ~32x of
/// input in pathological cases; cap so a malicious sender can't OOM us.
pub const MAX_DECOMPRESSED_BYTES: usize = 32 * 1024 * 1024;

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct WriteRequest {
    #[prost(message, repeated, tag = "1")]
    pub timeseries: Vec<TimeSeries>,
    // tag = 2 is reserved (Cortex source). prost ignores unknown tags.
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TimeSeries {
    #[prost(message, repeated, tag = "1")]
    pub labels: Vec<Label>,
    #[prost(message, repeated, tag = "2")]
    pub samples: Vec<Sample>,
    // tag = 3 (exemplars) and tag = 4 (histograms) are silently dropped.
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Label {
    #[prost(string, tag = "1")]
    pub name: String,
    #[prost(string, tag = "2")]
    pub value: String,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Sample {
    #[prost(double, tag = "1")]
    pub value: f64,
    #[prost(int64, tag = "2")]
    pub timestamp: i64,
}

#[derive(Debug, Error)]
pub enum DecodeError {
    #[error("snappy decompress: {0}")]
    Snappy(#[from] snap::Error),
    #[error("decompressed body is {actual} bytes, max is {max}")]
    TooLarge { actual: usize, max: usize },
    #[error("protobuf decode: {0}")]
    Protobuf(#[from] prost::DecodeError),
}

/// Snappy-decompress and protobuf-decode a Remote-Write 1.0 body.
pub fn decode_write_request(compressed: &[u8]) -> Result<WriteRequest, DecodeError> {
    // Pre-flight the decompressed size if the snappy header advertises it.
    // `decompress_len` reads the varint length prefix without allocating.
    if let Ok(declared) = snap::raw::decompress_len(compressed) {
        if declared > MAX_DECOMPRESSED_BYTES {
            return Err(DecodeError::TooLarge {
                actual: declared,
                max: MAX_DECOMPRESSED_BYTES,
            });
        }
    }
    let mut decoder = snap::raw::Decoder::new();
    let body = decoder.decompress_vec(compressed)?;
    if body.len() > MAX_DECOMPRESSED_BYTES {
        return Err(DecodeError::TooLarge {
            actual: body.len(),
            max: MAX_DECOMPRESSED_BYTES,
        });
    }
    let req = WriteRequest::decode(body.as_slice())?;
    Ok(req)
}

/// Outcome of converting one `TimeSeries`.
#[derive(Debug)]
pub struct ConversionStats {
    pub series_total: usize,
    pub series_dropped_no_name: usize,
    pub samples_emitted: usize,
}

/// Flatten a `WriteRequest` into the same `RawSample` shape used by the
/// JSON ingest path, so it goes through one validate + WAL pipeline.
///
/// Drops (silently, with a count) any TimeSeries that has no `__name__`
/// label. Drops (silently) any other label whose name starts with `__`,
/// because Prometheus reserves those for internal use and our validator
/// would reject them as a hard error.
pub fn flatten_write_request(req: WriteRequest) -> (Vec<RawSample>, ConversionStats) {
    let mut out: Vec<RawSample> = Vec::new();
    let mut stats = ConversionStats {
        series_total: req.timeseries.len(),
        series_dropped_no_name: 0,
        samples_emitted: 0,
    };

    for ts in req.timeseries {
        let mut metric: Option<String> = None;
        let mut labels: HashMap<String, String> = HashMap::with_capacity(ts.labels.len());
        for l in ts.labels {
            if l.name == "__name__" {
                metric = Some(l.value);
                continue;
            }
            if l.name.starts_with("__") {
                continue; // drop other reserved labels silently
            }
            labels.insert(l.name, l.value);
        }
        let metric = match metric {
            Some(m) if !m.is_empty() => m,
            _ => {
                stats.series_dropped_no_name += 1;
                continue;
            }
        };
        for s in ts.samples {
            out.push(RawSample {
                metric: metric.clone(),
                labels: labels.clone(),
                ts: s.timestamp,
                value: s.value,
            });
            stats.samples_emitted += 1;
        }
    }
    (out, stats)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_request_with(name: &str, labels: &[(&str, &str)], samples: &[(i64, f64)]) -> WriteRequest {
        let mut all_labels = vec![Label { name: "__name__".into(), value: name.into() }];
        for (k, v) in labels {
            all_labels.push(Label { name: (*k).into(), value: (*v).into() });
        }
        WriteRequest {
            timeseries: vec![TimeSeries {
                labels: all_labels,
                samples: samples
                    .iter()
                    .map(|(t, v)| Sample { timestamp: *t, value: *v })
                    .collect(),
            }],
        }
    }

    fn encode_compressed(req: &WriteRequest) -> Vec<u8> {
        let mut buf = Vec::new();
        req.encode(&mut buf).unwrap();
        let mut enc = snap::raw::Encoder::new();
        enc.compress_vec(&buf).unwrap()
    }

    #[test]
    fn decode_roundtrip_minimal() {
        let req = write_request_with("foo", &[("a", "1")], &[(1714800000000, 42.0)]);
        let body = encode_compressed(&req);
        let decoded = decode_write_request(&body).unwrap();
        assert_eq!(decoded, req);
    }

    #[test]
    fn decode_empty_request() {
        let req = WriteRequest { timeseries: vec![] };
        let body = encode_compressed(&req);
        let d = decode_write_request(&body).unwrap();
        assert!(d.timeseries.is_empty());
    }

    #[test]
    fn decode_rejects_garbage_snappy() {
        let r = decode_write_request(&[0xFF, 0xFE, 0xFD]);
        assert!(r.is_err());
    }

    #[test]
    fn decode_rejects_garbage_protobuf() {
        // Valid snappy of garbage bytes that will fail protobuf decoding.
        let mut enc = snap::raw::Encoder::new();
        let garbage = enc.compress_vec(&[0xFF; 64]).unwrap();
        let r = decode_write_request(&garbage);
        assert!(matches!(r, Err(DecodeError::Protobuf(_))));
    }

    #[test]
    fn flatten_extracts_name_label() {
        let req = write_request_with("http_requests_total",
            &[("job", "api"), ("status", "200")],
            &[(1, 1.0), (2, 2.0)]);
        let (raws, stats) = flatten_write_request(req);
        assert_eq!(stats.series_total, 1);
        assert_eq!(stats.series_dropped_no_name, 0);
        assert_eq!(stats.samples_emitted, 2);
        assert_eq!(raws.len(), 2);
        assert_eq!(raws[0].metric, "http_requests_total");
        assert!(!raws[0].labels.contains_key("__name__"));
        assert_eq!(raws[0].labels.get("job"), Some(&"api".to_string()));
        assert_eq!(raws[0].ts, 1);
        assert_eq!(raws[0].value, 1.0);
        assert_eq!(raws[1].ts, 2);
    }

    #[test]
    fn flatten_drops_series_with_no_name() {
        let req = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![Label { name: "job".into(), value: "x".into() }],
                samples: vec![Sample { timestamp: 1, value: 1.0 }],
            }],
        };
        let (raws, stats) = flatten_write_request(req);
        assert!(raws.is_empty());
        assert_eq!(stats.series_dropped_no_name, 1);
        assert_eq!(stats.samples_emitted, 0);
    }

    #[test]
    fn flatten_drops_empty_name() {
        let req = write_request_with("", &[], &[(1, 1.0)]);
        let (raws, stats) = flatten_write_request(req);
        assert!(raws.is_empty());
        assert_eq!(stats.series_dropped_no_name, 1);
    }

    #[test]
    fn flatten_drops_reserved_labels_silently() {
        // `__replica__` is something senders sometimes add. We strip it
        // rather than fail the whole sample.
        let req = write_request_with("foo",
            &[("__replica__", "ignored"), ("job", "x")],
            &[(1, 1.0)]);
        let (raws, _) = flatten_write_request(req);
        assert_eq!(raws.len(), 1);
        assert_eq!(raws[0].labels.get("job"), Some(&"x".to_string()));
        assert!(!raws[0].labels.contains_key("__replica__"));
    }

    #[test]
    fn flatten_multiple_series() {
        let req = WriteRequest {
            timeseries: vec![
                write_request_with("a", &[], &[(1, 1.0)]).timeseries[0].clone(),
                write_request_with("b", &[("k", "v")], &[(2, 2.0), (3, 3.0)]).timeseries[0].clone(),
            ],
        };
        let (raws, stats) = flatten_write_request(req);
        assert_eq!(stats.series_total, 2);
        assert_eq!(stats.samples_emitted, 3);
        assert_eq!(raws[0].metric, "a");
        assert_eq!(raws[1].metric, "b");
        assert_eq!(raws[2].metric, "b");
    }

    #[test]
    fn flatten_preserves_sample_values_including_special() {
        // NaN / Inf are decode-able through prost (they're just f64 bytes).
        // Our validate() will reject them later; here we just confirm we
        // don't drop them at decode time.
        let req = write_request_with("m", &[],
            &[(1, f64::NAN), (2, f64::INFINITY), (3, 0.0)]);
        let (raws, stats) = flatten_write_request(req);
        assert_eq!(stats.samples_emitted, 3);
        assert!(raws[0].value.is_nan());
        assert!(raws[1].value.is_infinite());
        assert_eq!(raws[2].value, 0.0);
    }

    #[test]
    fn decode_then_flatten_full_pipeline() {
        let req = write_request_with("http_requests_total",
            &[("job", "api"), ("status", "200")],
            &[(1714800000000, 42.0), (1714800001000, 43.0)]);
        let body = encode_compressed(&req);
        let decoded = decode_write_request(&body).unwrap();
        let (raws, stats) = flatten_write_request(decoded);
        assert_eq!(stats.samples_emitted, 2);
        assert_eq!(raws.len(), 2);
        assert_eq!(raws[0].metric, "http_requests_total");
        assert_eq!(raws[0].ts, 1714800000000);
    }
}
