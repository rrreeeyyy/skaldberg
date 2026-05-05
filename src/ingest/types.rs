//! Request / response shapes and the internal `ValidatedSample`.

use std::collections::{BTreeMap, HashMap};

use serde::{Deserialize, Serialize};

/// Body of `POST /api/v1/ingest`.
#[derive(Debug, Clone, Deserialize)]
pub struct IngestRequest {
    pub samples: Vec<RawSample>,
}

/// One sample as it appears on the wire. `ts` is **milliseconds** since
/// the Unix epoch.
#[derive(Debug, Clone, Deserialize)]
pub struct RawSample {
    pub metric: String,
    #[serde(default)]
    pub labels: HashMap<String, String>,
    pub ts: i64,
    pub value: f64,
}

/// A sample that has cleared validation. Note `ts_us` is **microseconds**
/// to match the on-disk Phase 2 schema (`Timestamp(Microsecond, None)`),
/// and `labels` is a `BTreeMap` so iteration is deterministic — important
/// because `series_id` is a hash over the labels in sorted order.
///
/// Serialize/Deserialize lets us roundtrip through the WAL as JSON.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ValidatedSample {
    pub series_id: i64,
    pub metric: String,
    pub labels: BTreeMap<String, String>,
    pub ts_us: i64,
    pub value: f64,
}

/// Successful (HTTP 200) response. Some samples may have been rejected
/// individually; that is *not* a request-level error.
#[derive(Debug, Clone, Serialize)]
pub struct IngestResponse {
    pub accepted: usize,
    pub rejected: Vec<RejectedSample>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RejectedSample {
    /// Index of the offending entry in the request's `samples` array.
    pub index: usize,
    pub reason: String,
}
