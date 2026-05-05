//! Prometheus HTTP API subset adapter.
//!
//! Implements the parts of `/api/v1/*` that Grafana's built-in
//! Prometheus datasource calls during normal operation:
//!
//!   GET/POST /api/v1/query
//!   GET/POST /api/v1/query_range
//!   GET     /api/v1/labels
//!   GET     /api/v1/label/{name}/values
//!   GET     /api/v1/series
//!
//! ## PromQL coverage in step 1
//!
//! Native execution:
//!
//!   - vector selectors: `metric{label="v", label!="v"}`
//!   - range vectors:    `metric[5m]`  (range duration is parsed
//!                                       and ignored — we return raw
//!                                       points, Grafana resamples)
//!   - offset modifier:  `metric offset 1h`  (parsed, ignored for now)
//!
//! Equality / inequality matchers (`=`, `!=`) are pushed down as
//! label-map predicates. Regex matchers (`=~`, `!~`) are dropped:
//! the resulting series set is a superset of what strict PromQL
//! would return, which is a deliberate trade-off — false positives
//! are easier to spot than crashes from unsupported syntax.
//!
//! Function calls and aggregations parse fine but execute as
//! "unwrap to inner vector selector": `rate(metric[5m])` returns
//! the raw `metric` samples rather than per-second rates. The
//! numbers are wrong, but the panel renders something. Step 2 will
//! replace the wrappers with real semantics.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow::array::{Array, Float64Array, MapArray, StringArray, TimestampMicrosecondArray};
use axum::extract::{Path, Query, RawQuery, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::{Form, Json};
use chrono::{DateTime, Utc};
use promql_parser::label::{MatchOp, Matcher};
use promql_parser::parser::{parse, Expr, VectorSelector};
use serde::Deserialize;
use serde_json::{json, Value as JsonValue};

use crate::state::{AppState, DF_CATALOG_NAME};

#[derive(Debug)]
pub struct PromError {
    status: StatusCode,
    error_type: &'static str,
    message: String,
}

impl PromError {
    fn bad_data(msg: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            error_type: "bad_data",
            message: msg.into(),
        }
    }
    fn internal(msg: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            error_type: "internal",
            message: msg.into(),
        }
    }
}

impl IntoResponse for PromError {
    fn into_response(self) -> Response {
        let body = json!({
            "status": "error",
            "errorType": self.error_type,
            "error": self.message,
        });
        (self.status, Json(body)).into_response()
    }
}

/// Prometheus default `lookback-delta`. An instant query at `t` looks
/// back this far for the latest sample of each series.
const LOOKBACK_US: i64 = 5 * 60 * 1_000_000;

// ---------- /api/v1/query (instant) ----------

#[derive(Deserialize, Default)]
pub struct QueryParams {
    pub query: String,
    pub time: Option<String>,
}

pub async fn query(
    State(state): State<Arc<AppState>>,
    Query(p): Query<QueryParams>,
) -> Result<Json<JsonValue>, PromError> {
    instant_query_inner(state, p).await
}

pub async fn query_post(
    State(state): State<Arc<AppState>>,
    Form(p): Form<QueryParams>,
) -> Result<Json<JsonValue>, PromError> {
    instant_query_inner(state, p).await
}

async fn instant_query_inner(
    state: Arc<AppState>,
    p: QueryParams,
) -> Result<Json<JsonValue>, PromError> {
    let now_us = Utc::now().timestamp_micros();
    let time_us = match p.time.as_deref() {
        Some(t) => parse_timestamp(t)?,
        None => now_us,
    };

    let expr =
        parse(&p.query).map_err(|e| PromError::bad_data(format!("PromQL parse: {e}")))?;
    let kind = match detect_query_kind(&expr) {
        Some(k) => k,
        None => return Ok(Json(success_vector(vec![]))),
    };

    let result = match kind {
        QueryKind::Selector(sel) => instant_selector(&state, sel, time_us).await?,
        QueryKind::Rate { sel, range_us } => {
            instant_rate(&state, sel, range_us, time_us).await?
        }
    };
    Ok(Json(success_vector(result)))
}

async fn instant_selector(
    state: &AppState,
    sel: &VectorSelector,
    time_us: i64,
) -> Result<Vec<JsonValue>, PromError> {
    let from_us = time_us - LOOKBACK_US;
    let rows = run_selector_query(state, sel, from_us, time_us).await?;
    // Latest sample per series within the lookback window.
    let mut latest: BTreeMap<String, SeriesRow> = BTreeMap::new();
    for r in rows {
        let key = series_key(&r.metric_name, &r.labels);
        match latest.get(&key) {
            Some(prev) if prev.ts_us >= r.ts_us => {}
            _ => {
                latest.insert(key, r);
            }
        }
    }
    Ok(latest
        .into_values()
        .map(|r| {
            json!({
                "metric": series_metric_obj(&r.metric_name, &r.labels),
                "value": [(r.ts_us as f64) / 1_000_000.0, r.value.to_string()],
            })
        })
        .collect())
}

async fn instant_rate(
    state: &AppState,
    sel: &VectorSelector,
    range_us: i64,
    time_us: i64,
) -> Result<Vec<JsonValue>, PromError> {
    let from_us = time_us - range_us;
    let rows = run_selector_query(state, sel, from_us, time_us).await?;
    let by_series = group_rows_by_series(rows);
    let mut out = Vec::new();
    for (_, (metric_name, labels, mut points)) in by_series {
        points.sort_by_key(|(t, _)| *t);
        if let Some(rate) = compute_rate(&points) {
            out.push(json!({
                "metric": series_metric_obj(&metric_name, &labels),
                "value": [(time_us as f64) / 1_000_000.0, rate.to_string()],
            }));
        }
    }
    Ok(out)
}

// ---------- /api/v1/query_range (range) ----------

#[derive(Deserialize, Default)]
pub struct QueryRangeParams {
    pub query: String,
    pub start: String,
    pub end: String,
    /// Resampling step. Parsed for validation but not enforced server-side
    /// (we return raw points; Grafana picks the resolution).
    pub step: String,
}

pub async fn query_range(
    State(state): State<Arc<AppState>>,
    Query(p): Query<QueryRangeParams>,
) -> Result<Json<JsonValue>, PromError> {
    range_query_inner(state, p).await
}

pub async fn query_range_post(
    State(state): State<Arc<AppState>>,
    Form(p): Form<QueryRangeParams>,
) -> Result<Json<JsonValue>, PromError> {
    range_query_inner(state, p).await
}

async fn range_query_inner(
    state: Arc<AppState>,
    p: QueryRangeParams,
) -> Result<Json<JsonValue>, PromError> {
    let start_us = parse_timestamp(&p.start)?;
    let end_us = parse_timestamp(&p.end)?;
    let step_us = parse_duration_us(&p.step)?;

    let expr =
        parse(&p.query).map_err(|e| PromError::bad_data(format!("PromQL parse: {e}")))?;
    let kind = match detect_query_kind(&expr) {
        Some(k) => k,
        None => return Ok(Json(success_matrix(vec![]))),
    };

    let result = match kind {
        QueryKind::Selector(sel) => range_selector(&state, sel, start_us, end_us).await?,
        QueryKind::Rate { sel, range_us } => {
            range_rate(&state, sel, range_us, start_us, end_us, step_us).await?
        }
    };
    Ok(Json(success_matrix(result)))
}

async fn range_selector(
    state: &AppState,
    sel: &VectorSelector,
    start_us: i64,
    end_us: i64,
) -> Result<Vec<JsonValue>, PromError> {
    let rows = run_selector_query(state, sel, start_us, end_us).await?;
    let mut groups: BTreeMap<String, (BTreeMap<String, String>, String, Vec<JsonValue>)> =
        BTreeMap::new();
    for r in rows {
        let key = series_key(&r.metric_name, &r.labels);
        let entry = groups
            .entry(key)
            .or_insert_with(|| (r.labels.clone(), r.metric_name.clone(), Vec::new()));
        entry
            .2
            .push(json!([(r.ts_us as f64) / 1_000_000.0, r.value.to_string()]));
    }
    Ok(groups
        .into_values()
        .map(|(labels, metric_name, values)| {
            json!({
                "metric": series_metric_obj(&metric_name, &labels),
                "values": values,
            })
        })
        .collect())
}

async fn range_rate(
    state: &AppState,
    sel: &VectorSelector,
    range_us: i64,
    start_us: i64,
    end_us: i64,
    step_us: i64,
) -> Result<Vec<JsonValue>, PromError> {
    if step_us <= 0 {
        return Err(PromError::bad_data("step must be positive"));
    }
    // Pull samples covering every step's lookback window in one go,
    // then bucket per-series and walk the steps in Rust.
    let fetch_from = start_us - range_us;
    let rows = run_selector_query(state, sel, fetch_from, end_us).await?;
    let by_series = group_rows_by_series(rows);
    let mut out = Vec::new();
    for (_, (metric_name, labels, mut points)) in by_series {
        points.sort_by_key(|(t, _)| *t);
        let mut values: Vec<JsonValue> = Vec::new();
        let mut t = start_us;
        while t <= end_us {
            // Window is `(t - range, t]` per Prometheus convention.
            // We bisect into the sorted points to avoid an O(N*M) scan
            // on long ranges.
            let lo_ts = t - range_us;
            let lo = points.partition_point(|(ts, _)| *ts < lo_ts);
            let hi = points.partition_point(|(ts, _)| *ts <= t);
            if hi.saturating_sub(lo) >= 2 {
                if let Some(rate) = compute_rate(&points[lo..hi]) {
                    values.push(json!([(t as f64) / 1_000_000.0, rate.to_string()]));
                }
            }
            t += step_us;
        }
        if !values.is_empty() {
            out.push(json!({
                "metric": series_metric_obj(&metric_name, &labels),
                "values": values,
            }));
        }
    }
    Ok(out)
}

/// Bucket selector-query rows into a per-series map. Key is the
/// stable `metric{label=...}` string so series stay sorted in output.
fn group_rows_by_series(
    rows: Vec<SeriesRow>,
) -> BTreeMap<String, (String, BTreeMap<String, String>, Vec<(i64, f64)>)> {
    let mut by_series: BTreeMap<String, (String, BTreeMap<String, String>, Vec<(i64, f64)>)> =
        BTreeMap::new();
    for r in rows {
        let key = series_key(&r.metric_name, &r.labels);
        let entry = by_series
            .entry(key)
            .or_insert_with(|| (r.metric_name.clone(), r.labels.clone(), Vec::new()));
        entry.2.push((r.ts_us, r.value));
    }
    by_series
}

/// PromQL-style `rate` over an ascending-sorted slice of (ts_us, value).
///
/// Returns `delta / seconds`, where `delta` accounts for counter
/// resets in the same way Prometheus's `delta`/`rate` do *internally*
/// — at every drop (`curr < prev`) we treat `curr` as a fresh value
/// counted from zero, instead of letting a reset register as a huge
/// negative spike. We do **not** apply Prometheus's extrapolation
/// to the range edges, so on short windows numbers will differ
/// slightly from a real Prometheus server. Panel rendering and
/// magnitude are correct.
fn compute_rate(points: &[(i64, f64)]) -> Option<f64> {
    if points.len() < 2 {
        return None;
    }
    let first_ts = points.first()?.0;
    let last_ts = points.last()?.0;
    let secs = (last_ts - first_ts) as f64 / 1_000_000.0;
    if secs <= 0.0 {
        return None;
    }
    let mut delta = 0.0_f64;
    for w in points.windows(2) {
        let prev = w[0].1;
        let curr = w[1].1;
        if curr >= prev {
            delta += curr - prev;
        } else {
            // Counter reset: assume the underlying counter went 0 →
            // curr in the gap. Equivalent to `curr - 0`.
            delta += curr;
        }
    }
    Some(delta / secs)
}

// ---------- /api/v1/labels ----------

pub async fn labels(
    State(state): State<Arc<AppState>>,
) -> Result<Json<JsonValue>, PromError> {
    let sql = format!(
        "SELECT labels FROM {}.skaldberg.series",
        DF_CATALOG_NAME
    );
    let df = state
        .ctx
        .sql(&sql)
        .await
        .map_err(|e| PromError::internal(format!("labels sql: {e}")))?;
    let batches = df
        .collect()
        .await
        .map_err(|e| PromError::internal(format!("labels collect: {e}")))?;
    let mut keys: BTreeSet<String> = BTreeSet::new();
    keys.insert("__name__".to_string());
    for batch in batches {
        let labels_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<MapArray>()
            .ok_or_else(|| PromError::internal("labels col not Map"))?;
        for i in 0..batch.num_rows() {
            collect_label_keys(labels_col, i, &mut keys)?;
        }
    }
    Ok(Json(json!({
        "status": "success",
        "data": keys.into_iter().collect::<Vec<_>>(),
    })))
}

// ---------- /api/v1/label/{name}/values ----------

pub async fn label_values(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Result<Json<JsonValue>, PromError> {
    let mut values: BTreeSet<String> = BTreeSet::new();
    if name == "__name__" {
        let sql = format!(
            "SELECT DISTINCT metric_name FROM {}.skaldberg.series",
            DF_CATALOG_NAME
        );
        let df = state
            .ctx
            .sql(&sql)
            .await
            .map_err(|e| PromError::internal(format!("label_values sql: {e}")))?;
        let batches = df
            .collect()
            .await
            .map_err(|e| PromError::internal(format!("collect: {e}")))?;
        for batch in batches {
            let col = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| PromError::internal("metric_name col not Utf8"))?;
            for i in 0..col.len() {
                if !col.is_null(i) {
                    values.insert(col.value(i).to_string());
                }
            }
        }
    } else {
        let sql = format!(
            "SELECT labels FROM {}.skaldberg.series",
            DF_CATALOG_NAME
        );
        let df = state
            .ctx
            .sql(&sql)
            .await
            .map_err(|e| PromError::internal(format!("label_values sql: {e}")))?;
        let batches = df
            .collect()
            .await
            .map_err(|e| PromError::internal(format!("collect: {e}")))?;
        for batch in batches {
            let labels_col = batch
                .column(0)
                .as_any()
                .downcast_ref::<MapArray>()
                .ok_or_else(|| PromError::internal("labels col not Map"))?;
            for i in 0..batch.num_rows() {
                if let Some(v) = label_value_at(labels_col, i, &name)? {
                    values.insert(v);
                }
            }
        }
    }
    Ok(Json(json!({
        "status": "success",
        "data": values.into_iter().collect::<Vec<_>>(),
    })))
}

// ---------- /api/v1/series ----------

/// Parse `match[]=...&match[]=...` from the raw query string. axum's
/// `Query` extractor uses serde_urlencoded which collapses repeated
/// keys to the last value rather than a Vec, so we walk the
/// percent-encoded string by hand.
fn parse_series_matches(raw: &str) -> Vec<String> {
    let mut out = Vec::new();
    for (k, v) in url::form_urlencoded::parse(raw.as_bytes()) {
        if k == "match[]" {
            out.push(v.into_owned());
        }
    }
    out
}

pub async fn series(
    State(state): State<Arc<AppState>>,
    RawQuery(qs): RawQuery,
) -> Result<Json<JsonValue>, PromError> {
    let matches = parse_series_matches(qs.as_deref().unwrap_or(""));
    if matches.is_empty() {
        return Ok(Json(json!({"status": "success", "data": []})));
    }
    let mut out: Vec<JsonValue> = Vec::new();
    let mut seen: BTreeSet<String> = BTreeSet::new();
    for q in matches {
        let expr =
            parse(&q).map_err(|e| PromError::bad_data(format!("PromQL parse: {e}")))?;
        let sel = match extract_selector(&expr) {
            Some(s) => s,
            None => continue,
        };
        let label_filters = label_filters_for_selector(sel);
        let mut conds = vec![];
        if let Some(name) = effective_metric_name(sel) {
            conds.push(format!("s.metric_name = '{}'", sql_escape(&name)));
        }
        let where_clause = if conds.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", conds.join(" AND "))
        };
        let sql = format!(
            "SELECT s.metric_name, s.labels FROM {}.skaldberg.series s{}",
            DF_CATALOG_NAME, where_clause
        );
        let df = state
            .ctx
            .sql(&sql)
            .await
            .map_err(|e| PromError::internal(format!("series sql: {e}")))?;
        let batches = df
            .collect()
            .await
            .map_err(|e| PromError::internal(format!("series collect: {e}")))?;
        for batch in batches {
            let metric_col = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| PromError::internal("metric_name col"))?;
            let labels_col = batch
                .column(1)
                .as_any()
                .downcast_ref::<MapArray>()
                .ok_or_else(|| PromError::internal("labels col"))?;
            for i in 0..batch.num_rows() {
                let m = metric_col.value(i).to_string();
                let lbls = labels_to_btree(labels_col, i)?;
                if !label_filters.iter().all(|f| match_label(&lbls, f)) {
                    continue;
                }
                let key = series_key(&m, &lbls);
                if seen.insert(key) {
                    out.push(series_metric_obj(&m, &lbls));
                }
            }
        }
    }
    Ok(Json(json!({"status": "success", "data": out})))
}

// ---------- helpers ----------

struct SeriesRow {
    metric_name: String,
    labels: BTreeMap<String, String>,
    ts_us: i64,
    value: f64,
}

/// What we plan to do with the parsed PromQL expression. Step 2 only
/// recognizes `rate(matrix)` natively; everything else falls through
/// to the selector-unwrap path so panels still render *something*.
enum QueryKind<'a> {
    /// Return raw points for the wrapped vector selector.
    Selector(&'a VectorSelector),
    /// Compute Prometheus-style rate over the inner matrix selector.
    /// `range_us` is the matrix's range duration.
    Rate {
        sel: &'a VectorSelector,
        range_us: i64,
    },
}

/// Pick a query plan for the AST. Step 2 only adds `rate`; new
/// recognizers (sum / histogram_quantile / topk / ...) drop into
/// this `match` over time.
fn detect_query_kind(expr: &Expr) -> Option<QueryKind<'_>> {
    if let Expr::Call(c) = expr {
        if c.func.name.eq_ignore_ascii_case("rate") {
            if let Some(arg) = c.args.args.first() {
                if let Expr::MatrixSelector(m) = arg.as_ref() {
                    return Some(QueryKind::Rate {
                        sel: &m.vs,
                        range_us: m.range.as_micros() as i64,
                    });
                }
            }
        }
    }
    extract_selector(expr).map(QueryKind::Selector)
}

/// Walk an Expr down to the first vector/matrix selector found.
/// Functions, aggregations, paren wrappers, etc are unwrapped — see
/// the module doc for why we accept "wrong numbers, panel renders"
/// over "400, panel breaks".
fn extract_selector(expr: &Expr) -> Option<&VectorSelector> {
    match expr {
        Expr::VectorSelector(v) => Some(v),
        Expr::MatrixSelector(m) => Some(&m.vs),
        Expr::Call(c) => c.args.args.iter().find_map(|e| extract_selector(e.as_ref())),
        Expr::Aggregate(a) => extract_selector(a.expr.as_ref()),
        Expr::Binary(b) => extract_selector(b.lhs.as_ref())
            .or_else(|| extract_selector(b.rhs.as_ref())),
        Expr::Paren(p) => extract_selector(p.expr.as_ref()),
        Expr::Unary(u) => extract_selector(u.expr.as_ref()),
        Expr::Subquery(s) => extract_selector(s.expr.as_ref()),
        _ => None,
    }
}

async fn run_selector_query(
    state: &AppState,
    sel: &VectorSelector,
    from_us: i64,
    to_us: i64,
) -> Result<Vec<SeriesRow>, PromError> {
    // SQL filters: metric_name (push-down friendly) and the time
    // window. Label matchers are evaluated in Rust below — DataFusion
    // 52 doesn't expose a clean Map<Utf8,Utf8> equality predicate, so
    // pushing them down would require fragile workarounds. The
    // (metric_name, timerange) prefilter alone is selective enough
    // for the data shapes we care about; if that ever stops being
    // true we'll push labels down properly.
    let mut conds = Vec::new();
    if let Some(name) = effective_metric_name(sel) {
        conds.push(format!("s.metric_name = '{}'", sql_escape(&name)));
    }
    conds.push(format!(
        "sa.timestamp BETWEEN TIMESTAMP '{}' AND TIMESTAMP '{}'",
        us_to_ts_lit(from_us),
        us_to_ts_lit(to_us),
    ));
    let where_clause = format!(" WHERE {}", conds.join(" AND "));
    let sql = format!(
        "SELECT s.metric_name, s.labels, sa.timestamp, sa.value
         FROM {cat}.skaldberg.samples sa
         JOIN {cat}.skaldberg.series s ON sa.series_id = s.series_id
         {where_clause}
         ORDER BY s.metric_name, sa.timestamp",
        cat = DF_CATALOG_NAME,
    );
    let df = state
        .ctx
        .sql(&sql)
        .await
        .map_err(|e| PromError::internal(format!("selector sql: {e}")))?;
    let batches = df
        .collect()
        .await
        .map_err(|e| PromError::internal(format!("selector collect: {e}")))?;

    let label_filters = label_filters_for_selector(sel);
    let mut rows = Vec::new();
    for batch in batches {
        let metric_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| PromError::internal("metric_name col"))?;
        let labels_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<MapArray>()
            .ok_or_else(|| PromError::internal("labels col"))?;
        let ts_col = batch
            .column(2)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .ok_or_else(|| PromError::internal("timestamp col"))?;
        let val_col = batch
            .column(3)
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| PromError::internal("value col"))?;
        for i in 0..batch.num_rows() {
            let labels = labels_to_btree(labels_col, i)?;
            if !label_filters.iter().all(|f| match_label(&labels, f)) {
                continue;
            }
            rows.push(SeriesRow {
                metric_name: metric_col.value(i).to_string(),
                labels,
                ts_us: ts_col.value(i),
                value: val_col.value(i),
            });
        }
    }
    Ok(rows)
}

/// Resolve the metric name from a vector selector.
///
/// Prefers `sel.name` when set (the `metric{...}` syntax). Falls
/// back to a `__name__="x"` matcher when the operator wrote it that
/// way, and returns `None` if neither was supplied (`{label="x"}`).
fn effective_metric_name(sel: &VectorSelector) -> Option<String> {
    if let Some(n) = &sel.name {
        return Some(n.clone());
    }
    sel.matchers
        .matchers
        .iter()
        .find(|m| m.name == "__name__" && matches!(m.op, MatchOp::Equal))
        .map(|m| m.value.clone())
}

/// Return label matchers that need post-filtering in Rust (everything
/// except the `__name__` matcher, which we push down via metric_name).
fn label_filters_for_selector(sel: &VectorSelector) -> Vec<&Matcher> {
    sel.matchers
        .matchers
        .iter()
        .filter(|m| m.name != "__name__")
        .collect()
}

/// Apply a single label matcher to a series' label map. Mirrors the
/// PromQL semantics: a missing label is treated as the empty string,
/// so e.g. `{job!=""}` matches series that *do* have a `job` label.
fn match_label(labels: &BTreeMap<String, String>, m: &Matcher) -> bool {
    let v = labels.get(&m.name).map(|s| s.as_str()).unwrap_or("");
    match &m.op {
        MatchOp::Equal => v == m.value,
        MatchOp::NotEqual => v != m.value,
        MatchOp::Re(re) => re.is_match(v),
        MatchOp::NotRe(re) => !re.is_match(v),
    }
}

fn sql_escape(s: &str) -> String {
    s.replace('\'', "''")
}

fn parse_timestamp(s: &str) -> Result<i64, PromError> {
    if let Ok(secs) = s.parse::<f64>() {
        return Ok((secs * 1_000_000.0) as i64);
    }
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Ok(dt.with_timezone(&Utc).timestamp_micros());
    }
    Err(PromError::bad_data(format!("invalid timestamp: {s}")))
}

fn parse_duration_us(s: &str) -> Result<i64, PromError> {
    if let Ok(secs) = s.parse::<f64>() {
        return Ok((secs * 1_000_000.0) as i64);
    }
    let s = s.trim();
    if s.is_empty() {
        return Err(PromError::bad_data("empty duration"));
    }
    let (num_str, unit) = s.split_at(s.len() - 1);
    let n: f64 = num_str
        .parse()
        .map_err(|_| PromError::bad_data(format!("invalid duration: {s}")))?;
    let scale_us = match unit {
        "s" => 1_000_000.0,
        "m" => 60_000_000.0,
        "h" => 3_600_000_000.0,
        "d" => 86_400_000_000.0,
        _ => return Err(PromError::bad_data(format!("invalid duration unit: {s}"))),
    };
    Ok((n * scale_us) as i64)
}

fn us_to_ts_lit(us: i64) -> String {
    let dt = DateTime::<Utc>::from_timestamp_micros(us).unwrap_or(DateTime::<Utc>::UNIX_EPOCH);
    dt.format("%Y-%m-%d %H:%M:%S%.6f").to_string()
}

fn labels_to_btree(
    labels: &MapArray,
    i: usize,
) -> Result<BTreeMap<String, String>, PromError> {
    let mut out = BTreeMap::new();
    if labels.is_null(i) {
        return Ok(out);
    }
    let entries = labels.value(i);
    let entries = entries
        .as_any()
        .downcast_ref::<arrow::array::StructArray>()
        .ok_or_else(|| PromError::internal("labels entries not Struct"))?;
    let keys = entries
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| PromError::internal("labels.key not Utf8"))?;
    let values = entries
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| PromError::internal("labels.value not Utf8"))?;
    for j in 0..entries.len() {
        if !keys.is_null(j) && !values.is_null(j) {
            out.insert(keys.value(j).to_string(), values.value(j).to_string());
        }
    }
    Ok(out)
}

fn collect_label_keys(
    labels: &MapArray,
    i: usize,
    set: &mut BTreeSet<String>,
) -> Result<(), PromError> {
    if labels.is_null(i) {
        return Ok(());
    }
    let entries = labels.value(i);
    let entries = entries
        .as_any()
        .downcast_ref::<arrow::array::StructArray>()
        .ok_or_else(|| PromError::internal("labels entries not Struct"))?;
    let keys = entries
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| PromError::internal("labels.key not Utf8"))?;
    for j in 0..entries.len() {
        if !keys.is_null(j) {
            set.insert(keys.value(j).to_string());
        }
    }
    Ok(())
}

fn label_value_at(
    labels: &MapArray,
    i: usize,
    key_name: &str,
) -> Result<Option<String>, PromError> {
    if labels.is_null(i) {
        return Ok(None);
    }
    let entries = labels.value(i);
    let entries = entries
        .as_any()
        .downcast_ref::<arrow::array::StructArray>()
        .ok_or_else(|| PromError::internal("labels entries not Struct"))?;
    let keys = entries
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| PromError::internal("labels.key not Utf8"))?;
    let values = entries
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| PromError::internal("labels.value not Utf8"))?;
    for j in 0..entries.len() {
        if !keys.is_null(j) && keys.value(j) == key_name && !values.is_null(j) {
            return Ok(Some(values.value(j).to_string()));
        }
    }
    Ok(None)
}

fn series_key(metric_name: &str, labels: &BTreeMap<String, String>) -> String {
    let lbls: Vec<String> = labels.iter().map(|(k, v)| format!("{k}={v}")).collect();
    format!("{metric_name}{{{}}}", lbls.join(","))
}

fn series_metric_obj(metric_name: &str, labels: &BTreeMap<String, String>) -> JsonValue {
    let mut m = serde_json::Map::new();
    m.insert(
        "__name__".to_string(),
        JsonValue::String(metric_name.to_string()),
    );
    for (k, v) in labels {
        m.insert(k.clone(), JsonValue::String(v.clone()));
    }
    JsonValue::Object(m)
}

fn success_vector(result: Vec<JsonValue>) -> JsonValue {
    json!({
        "status": "success",
        "data": {"resultType": "vector", "result": result},
    })
}

fn success_matrix(result: Vec<JsonValue>) -> JsonValue {
    json!({
        "status": "success",
        "data": {"resultType": "matrix", "result": result},
    })
}
