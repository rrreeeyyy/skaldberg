//! Grafana JSON Datasource adapter.
//!
//! Implements the SimPod JSON Datasource HTTP contract
//! (<https://github.com/simPod/GrafanaJsonDatasource>) so a stock
//! Grafana instance with the SimPod plugin can pull series out of
//! Skaldberg by POSTing to `/api/v1/grafana/{,search,query}`. We
//! deliberately stay inside the existing `/api/v1/*` umbrella so
//! the bearer-token auth layer applies — Grafana sets the token
//! once on the datasource and forwards it on every request.
//!
//! Series identity:
//!
//!   target = `metric_name`                 (when there are no labels)
//!   target = `metric_name{k1=v1,k2=v2}`    (labels sorted by key)
//!
//! That format is human-friendly in Grafana's series picker and
//! matches the Prometheus convention closely enough that pasting a
//! target into a Grafana legend formatter does the right thing.
//!
//! What's intentionally minimal here:
//!
//!   - no `/annotations` endpoint (no annotation source today)
//!   - no `/tag-keys` / `/tag-values` (ad-hoc filters defer to a
//!     follow-up PR)
//!   - no downsampling / step alignment — we return raw points and
//!     let Grafana cope. `maxDataPoints` is honored as a hard cap.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{Array, Float64Array, MapArray, StringArray, TimestampMicrosecondArray};
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue};

use crate::state::{AppState, DF_CATALOG_NAME};

#[derive(Debug)]
pub struct GrafanaError {
    status: StatusCode,
    message: String,
}

impl GrafanaError {
    fn internal(msg: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: msg.into(),
        }
    }
    fn bad_request(msg: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: msg.into(),
        }
    }
}

impl IntoResponse for GrafanaError {
    fn into_response(self) -> Response {
        (self.status, Json(json!({"error": self.message}))).into_response()
    }
}

/// `POST /` — connection test. SimPod expects 200 OK with any body.
pub async fn root() -> &'static str {
    "ok"
}

#[derive(Deserialize)]
pub struct SearchRequest {
    /// Optional substring filter. Empty / missing → return everything.
    #[serde(default)]
    pub target: String,
}

/// `POST /search` — list metric names. Body may be empty `{}`,
/// `{"target": ""}`, or `{"target": "substring"}`.
pub async fn search(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SearchRequest>,
) -> Result<Json<Vec<String>>, GrafanaError> {
    let sql = format!(
        "SELECT DISTINCT metric_name FROM {}.skaldberg.series ORDER BY metric_name",
        DF_CATALOG_NAME
    );
    let df = state
        .ctx
        .sql(&sql)
        .await
        .map_err(|e| GrafanaError::internal(format!("search sql: {e}")))?;
    let batches = df
        .collect()
        .await
        .map_err(|e| GrafanaError::internal(format!("search collect: {e}")))?;
    let mut names = Vec::new();
    let needle = req.target;
    for batch in batches {
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| GrafanaError::internal("metric_name column not Utf8"))?;
        for i in 0..col.len() {
            if col.is_null(i) {
                continue;
            }
            let v = col.value(i);
            if needle.is_empty() || v.contains(&needle) {
                names.push(v.to_string());
            }
        }
    }
    Ok(Json(names))
}

#[derive(Deserialize)]
pub struct QueryRequest {
    pub range: TimeRange,
    pub targets: Vec<Target>,
    #[serde(default, rename = "maxDataPoints")]
    pub max_data_points: Option<i64>,
}

#[derive(Deserialize)]
pub struct TimeRange {
    pub from: String,
    pub to: String,
}

#[derive(Deserialize)]
pub struct Target {
    pub target: String,
}

#[derive(Serialize)]
pub struct TimeseriesResp {
    pub target: String,
    pub datapoints: Vec<[JsonValue; 2]>,
}

/// `POST /query` — fetch time series for the given targets within
/// `range`. We expand each requested metric into one `target`
/// envelope per `(metric_name, labels)` series found in the data.
pub async fn query(
    State(state): State<Arc<AppState>>,
    Json(req): Json<QueryRequest>,
) -> Result<Json<Vec<TimeseriesResp>>, GrafanaError> {
    if req.targets.is_empty() {
        return Ok(Json(Vec::new()));
    }

    let from = parse_rfc3339_to_us(&req.range.from)
        .ok_or_else(|| GrafanaError::bad_request("range.from is not RFC-3339"))?;
    let to = parse_rfc3339_to_us(&req.range.to)
        .ok_or_else(|| GrafanaError::bad_request("range.to is not RFC-3339"))?;
    if to < from {
        return Err(GrafanaError::bad_request("range.to is before range.from"));
    }

    // Build the metric_name IN (...) clause. Targets are surfaced to
    // Grafana operators verbatim; quote-escaping keeps an apostrophe
    // in a metric name from breaking the query (we forbid such names
    // at validate time anyway, but this is cheap insurance).
    let in_list = req
        .targets
        .iter()
        .map(|t| format!("'{}'", t.target.replace('\'', "''")))
        .collect::<Vec<_>>()
        .join(", ");

    // DataFusion's TIMESTAMP literal accepts microsecond precision.
    let from_lit = us_to_timestamp_literal(from);
    let to_lit = us_to_timestamp_literal(to);

    let limit_clause = match req.max_data_points {
        Some(n) if n > 0 => format!(" LIMIT {}", n),
        _ => String::new(),
    };

    let sql = format!(
        "SELECT metric_name, labels, timestamp, value
         FROM sk_metric
         WHERE metric_name IN ({})
           AND timestamp BETWEEN TIMESTAMP '{}' AND TIMESTAMP '{}'
         ORDER BY metric_name, timestamp{}",
        in_list, from_lit, to_lit, limit_clause
    );

    let df = state
        .ctx
        .sql(&sql)
        .await
        .map_err(|e| GrafanaError::internal(format!("query sql: {e}")))?;
    let batches = df
        .collect()
        .await
        .map_err(|e| GrafanaError::internal(format!("query collect: {e}")))?;

    // Group rows by (metric_name, labels-rendered-as-string). BTreeMap
    // gives us a stable, name-sorted output which is what Grafana's
    // legend rendering looks nicer with.
    let mut groups: BTreeMap<String, Vec<[JsonValue; 2]>> = BTreeMap::new();
    for batch in &batches {
        let metric = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| GrafanaError::internal("metric_name col"))?;
        let labels = batch
            .column(1)
            .as_any()
            .downcast_ref::<MapArray>()
            .ok_or_else(|| GrafanaError::internal("labels col"))?;
        let ts = batch
            .column(2)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .ok_or_else(|| GrafanaError::internal("timestamp col"))?;
        let val = batch
            .column(3)
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| GrafanaError::internal("value col"))?;

        for i in 0..batch.num_rows() {
            let m = metric.value(i);
            let lbl_str = labels_to_target_suffix(labels, i)?;
            let target = if lbl_str.is_empty() {
                m.to_string()
            } else {
                format!("{m}{{{lbl_str}}}")
            };
            let ts_ms = ts.value(i) / 1000; // us → ms
            let v = val.value(i);
            // Grafana's datapoints format: [value, timestampMs].
            // Use serde_json's Number directly so we don't lose
            // f64 precision through a string round-trip.
            groups
                .entry(target)
                .or_default()
                .push([JsonValue::from(v), JsonValue::from(ts_ms)]);
        }
    }

    let out: Vec<TimeseriesResp> = groups
        .into_iter()
        .map(|(target, datapoints)| TimeseriesResp { target, datapoints })
        .collect();
    Ok(Json(out))
}

fn parse_rfc3339_to_us(s: &str) -> Option<i64> {
    DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|dt| dt.with_timezone(&Utc).timestamp_micros())
}

fn us_to_timestamp_literal(us: i64) -> String {
    let dt = DateTime::<Utc>::from_timestamp_micros(us).unwrap_or(DateTime::<Utc>::UNIX_EPOCH);
    dt.format("%Y-%m-%d %H:%M:%S%.6f").to_string()
}

/// Render the labels MAP at row `i` as `k1=v1,k2=v2,...` with keys
/// sorted alphabetically, so the rendered target string is stable
/// across calls (Grafana groups series by exact target text).
fn labels_to_target_suffix(labels: &MapArray, i: usize) -> Result<String, GrafanaError> {
    if labels.is_null(i) {
        return Ok(String::new());
    }
    // `MapArray::value(i)` returns a StructArray with two fields:
    // the key column and the value column. Their indexing follows
    // the same offsets as a ListArray.
    let entries = labels.value(i);
    let entries = entries
        .as_any()
        .downcast_ref::<arrow::array::StructArray>()
        .ok_or_else(|| GrafanaError::internal("labels entries not StructArray"))?;
    let keys = entries
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| GrafanaError::internal("labels.key not Utf8"))?;
    let values = entries
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| GrafanaError::internal("labels.value not Utf8"))?;

    let mut sorted: BTreeMap<&str, &str> = BTreeMap::new();
    for j in 0..entries.len() {
        if keys.is_null(j) || values.is_null(j) {
            continue;
        }
        sorted.insert(keys.value(j), values.value(j));
    }
    let parts: Vec<String> = sorted
        .into_iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect();
    Ok(parts.join(","))
}
