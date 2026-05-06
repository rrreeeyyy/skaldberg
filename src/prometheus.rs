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
use promql_parser::parser::token::{
    T_ADD, T_AVG, T_BOTTOMK, T_COUNT, T_DIV, T_EQLC, T_GTE, T_GTR, T_LTE, T_LSS, T_MAX, T_MIN,
    T_MOD, T_MUL, T_NEQ, T_POW, T_SUB, T_SUM, T_TOPK,
};
use promql_parser::parser::{parse, Expr, LabelModifier, VectorSelector};
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
    let points = evaluate_instant(&state, &expr, time_us).await?;
    Ok(Json(success_vector(instant_points_to_json(points))))
}

/// Result of an instant query evaluation — one entry per output series.
struct InstantPoint {
    metric_name: String,
    labels: BTreeMap<String, String>,
    ts_us: i64,
    value: f64,
}

/// Result of a range query evaluation — one entry per output series.
struct RangePoints {
    metric_name: String,
    labels: BTreeMap<String, String>,
    points: Vec<(i64, f64)>,
}

type InstantFut<'a> = std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<Vec<InstantPoint>, PromError>> + Send + 'a>,
>;
type RangeFut<'a> = std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<Vec<RangePoints>, PromError>> + Send + 'a>,
>;

/// Recursive evaluator for instant queries. `Box::pin` is what lets us
/// recurse through `Aggregate { inner: ... }` into the nested
/// expression (e.g. `sum(rate(metric[5m])) by (job)`).
fn evaluate_instant<'a>(state: &'a AppState, expr: &'a Expr, time_us: i64) -> InstantFut<'a> {
    Box::pin(async move {
        match detect_query_kind(expr) {
            Some(QueryKind::Selector(sel)) => instant_selector(state, sel, time_us).await,
            Some(QueryKind::RangeFn {
                sel,
                range_us,
                op,
            }) => instant_range_fn_via_sql(state, sel, range_us, op, time_us).await,
            Some(QueryKind::Aggregate {
                op,
                modifier,
                inner,
            }) => {
                // SQL pushdown is only sound for `by(...)` and "no
                // modifier"; `without(...)` would need the full label
                // set up front. The inner expression also has to be a
                // pure selector — anything richer (rate, binary,
                // nested aggregation) still goes through the Rust path.
                if aggregate_can_push_down(modifier) {
                    if let Some(sel) = pure_selector(inner) {
                        return instant_aggregate_via_sql(
                            state, sel, op, modifier, time_us,
                        )
                        .await;
                    }
                }
                let inner_pts = evaluate_instant(state, inner, time_us).await?;
                Ok(aggregate_instant_points(inner_pts, op, modifier, time_us))
            }
            Some(QueryKind::HistogramQuantile { quantile, inner }) => {
                let inner_pts = evaluate_instant(state, inner, time_us).await?;
                Ok(histogram_quantile_instant(inner_pts, quantile, time_us))
            }
            Some(QueryKind::TopK {
                n,
                top,
                modifier,
                inner,
            }) => {
                // Same pushdown criteria as `Aggregate`: pure selector
                // inner + None / `by(...)` modifier. `without(...)`
                // would need the full label set up front, and a
                // non-selector inner (e.g. `topk(3, rate(m[1m]))`)
                // still goes through the Rust path.
                if aggregate_can_push_down(modifier) {
                    if let Some(sel) = pure_selector(inner) {
                        return instant_topk_via_sql(
                            state, sel, n, top, modifier, time_us,
                        )
                        .await;
                    }
                }
                let inner_pts = evaluate_instant(state, inner, time_us).await?;
                Ok(topk_instant_points(inner_pts, n, top, modifier))
            }
            Some(QueryKind::Binary {
                op,
                lhs,
                rhs,
                return_bool,
            }) => binary_instant_eval(state, lhs, rhs, op, return_bool, time_us).await,
            None => Ok(vec![]),
        }
    })
}

async fn instant_selector(
    state: &AppState,
    sel: &VectorSelector,
    time_us: i64,
) -> Result<Vec<InstantPoint>, PromError> {
    let from_us = time_us - LOOKBACK_US;
    let rows = run_selector_query(state, sel, from_us, time_us).await?;
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
        .map(|r| InstantPoint {
            metric_name: r.metric_name,
            labels: r.labels,
            ts_us: r.ts_us,
            value: r.value,
        })
        .collect())
}

/// `rate(m[r])` / `irate(...)` / `increase(...)` / `delta(...)` at a
/// single instant timestamp, evaluated as one SQL plan.
///
/// Window: `[t - r, t]` (closed-closed; matches the existing Rust
/// path's `partition_point` behavior). Counter-reset adjustment for
/// rate/irate/increase replicates `delta_with_reset_and_secs`:
/// adjacent pairs sum positive deltas and treat each drop as the
/// raw current value (the underlying counter is assumed to have
/// gone `0 → curr` across the gap).
async fn instant_range_fn_via_sql(
    state: &AppState,
    sel: &VectorSelector,
    range_us: i64,
    op: RangeFnOp,
    time_us: i64,
) -> Result<Vec<InstantPoint>, PromError> {
    let from_us = time_us - range_us;
    let mut conds = selector_predicates(sel);
    conds.push(format!(
        "sa.timestamp BETWEEN TIMESTAMP '{}' AND TIMESTAMP '{}'",
        us_to_ts_lit(from_us),
        us_to_ts_lit(time_us),
    ));
    let where_clause = format!("WHERE {}", conds.join(" AND "));

    let sql = build_range_fn_sql(op, &where_clause);
    let df = state
        .ctx
        .sql(&sql)
        .await
        .map_err(|e| PromError::internal(format!("range_fn sql: {e}")))?;
    let batches = df
        .collect()
        .await
        .map_err(|e| PromError::internal(format!("range_fn collect: {e}")))?;

    let label_filters = label_filters_for_selector(sel);
    let mut out = Vec::new();
    for batch in batches {
        let metric_col = batch.column(0).as_any().downcast_ref::<StringArray>()
            .ok_or_else(|| PromError::internal("rate metric_name col"))?;
        let labels_col = batch.column(1).as_any().downcast_ref::<MapArray>()
            .ok_or_else(|| PromError::internal("rate labels col"))?;
        let val_col = batch.column(2).as_any().downcast_ref::<Float64Array>()
            .ok_or_else(|| PromError::internal("rate value col"))?;
        for i in 0..batch.num_rows() {
            let labels = labels_to_btree(labels_col, i)?;
            if !label_filters.iter().all(|f| match_label(&labels, f)) {
                continue;
            }
            // SUM may yield NULL on empty groups (filtered by HAVING /
            // WHERE upstream) and Float64 division could in principle
            // produce NaN; drop those rather than propagate.
            if val_col.is_null(i) {
                continue;
            }
            let v = val_col.value(i);
            if !v.is_finite() {
                continue;
            }
            out.push(InstantPoint {
                metric_name: metric_col.value(i).to_string(),
                labels,
                ts_us: time_us,
                value: v,
            });
        }
    }
    Ok(out)
}

/// Build the SQL plan for an instant range-function call.
///
/// Three shapes:
///   - rate / increase: `LAG()` per series + `SUM(reset-adjusted dv)`,
///     final value = `total_delta * 1e6 / (last_us - first_us)` for
///     rate, `total_delta` for increase.
///   - irate: take the latest two samples per series via
///     `ROW_NUMBER() ... DESC`, compute `(curr - prev)/secs` with
///     reset adjustment.
///   - delta: gauge difference, `last_v - first_v`. No reset
///     adjustment (use `increase` for counters).
///
/// All three require ≥2 samples in the window and a positive
/// timestamp span; they emit nothing for series that don't qualify.
fn build_range_fn_sql(op: RangeFnOp, where_clause: &str) -> String {
    let cat = DF_CATALOG_NAME;
    match op {
        RangeFnOp::Rate | RangeFnOp::Increase => {
            let value_expr = if matches!(op, RangeFnOp::Rate) {
                "total_delta * 1000000.0 / (last_ts_us - first_ts_us)"
            } else {
                "total_delta"
            };
            // `base` re-projects sa.value as `value + 0.0` and the
            // timestamp through `CAST(... AS BIGINT)` so the resulting
            // columns lose the Parquet `field_id` metadata. Without
            // this DataFusion 52's `LAG()` planner trips a logical-vs-
            // physical schema mismatch (apache/datafusion#…) when the
            // window input still carries the original Parquet metadata.
            format!(
                "WITH base AS ( \
                   SELECT sa.series_id, \
                          CAST(sa.timestamp AS BIGINT) AS ts_us, \
                          sa.value + 0.0 AS value \
                   FROM {cat}.skaldberg.samples sa \
                   JOIN {cat}.skaldberg.series s ON sa.series_id = s.series_id \
                   {where_clause} \
                 ), \
                 adjusted AS ( \
                   SELECT series_id, ts_us, value, \
                          LAG(value) OVER w AS prev_v \
                   FROM base \
                   WINDOW w AS (PARTITION BY series_id ORDER BY ts_us) \
                 ), \
                 agg AS ( \
                   SELECT series_id, \
                          SUM(CASE \
                                WHEN prev_v IS NULL THEN CAST(0.0 AS DOUBLE) \
                                WHEN value >= prev_v THEN value - prev_v \
                                ELSE value \
                              END) AS total_delta, \
                          MAX(ts_us) AS last_ts_us, \
                          MIN(ts_us) AS first_ts_us, \
                          COUNT(*) AS n \
                   FROM adjusted \
                   GROUP BY series_id \
                 ) \
                 SELECT s.metric_name, s.labels, ({value_expr}) AS v \
                 FROM agg a \
                 JOIN {cat}.skaldberg.series s ON a.series_id = s.series_id \
                 WHERE a.n >= 2 AND a.last_ts_us > a.first_ts_us"
            )
        }
        RangeFnOp::Irate => {
            // Pick rn_desc=1 (the latest sample) and pair it with
            // the immediately preceding sample via `LAG()` — that's
            // already the "prev" by construction since LAG is over
            // ASC ordering. total_n ≥ 2 guarantees prev exists.
            // `base` strips Parquet field metadata (see Rate path).
            format!(
                "WITH base AS ( \
                   SELECT sa.series_id, \
                          CAST(sa.timestamp AS BIGINT) AS ts_us, \
                          sa.value + 0.0 AS value \
                   FROM {cat}.skaldberg.samples sa \
                   JOIN {cat}.skaldberg.series s ON sa.series_id = s.series_id \
                   {where_clause} \
                 ), \
                 ranked AS ( \
                   SELECT series_id, ts_us, value, \
                          LAG(value) OVER w AS prev_v, \
                          LAG(ts_us) OVER w AS prev_ts_us, \
                          ROW_NUMBER() OVER (PARTITION BY series_id ORDER BY ts_us DESC) AS rn_desc, \
                          COUNT(*) OVER (PARTITION BY series_id) AS total_n \
                   FROM base \
                   WINDOW w AS (PARTITION BY series_id ORDER BY ts_us) \
                 ) \
                 SELECT s.metric_name, s.labels, \
                        ((CASE WHEN value >= prev_v THEN value - prev_v ELSE value END) * 1000000.0 / (ts_us - prev_ts_us)) AS v \
                 FROM ranked r \
                 JOIN {cat}.skaldberg.series s ON r.series_id = s.series_id \
                 WHERE r.rn_desc = 1 AND r.total_n >= 2 AND r.ts_us > r.prev_ts_us"
            )
        }
        RangeFnOp::Delta => {
            // Two ROW_NUMBER passes per partition: ASC for the first
            // sample, DESC for the last. Join the two sides on
            // series_id and subtract. `base` strips Parquet field
            // metadata so window functions plan cleanly.
            format!(
                "WITH base AS ( \
                   SELECT sa.series_id, \
                          CAST(sa.timestamp AS BIGINT) AS ts_us, \
                          sa.value + 0.0 AS value \
                   FROM {cat}.skaldberg.samples sa \
                   JOIN {cat}.skaldberg.series s ON sa.series_id = s.series_id \
                   {where_clause} \
                 ), \
                 ranked AS ( \
                   SELECT series_id, value, \
                          ROW_NUMBER() OVER (PARTITION BY series_id ORDER BY ts_us ASC) AS rn_asc, \
                          ROW_NUMBER() OVER (PARTITION BY series_id ORDER BY ts_us DESC) AS rn_desc, \
                          COUNT(*) OVER (PARTITION BY series_id) AS total_n \
                   FROM base \
                 ), \
                 endpoints AS ( \
                   SELECT series_id, value AS first_v, total_n FROM ranked WHERE rn_asc = 1 \
                 ), \
                 last_v AS ( \
                   SELECT series_id, value AS last_val FROM ranked WHERE rn_desc = 1 \
                 ) \
                 SELECT s.metric_name, s.labels, (l.last_val - e.first_v) AS v \
                 FROM endpoints e \
                 JOIN last_v l ON e.series_id = l.series_id \
                 JOIN {cat}.skaldberg.series s ON e.series_id = s.series_id \
                 WHERE e.total_n >= 2"
            )
        }
    }
}

/// Group instant points by retained labels, then collapse with `op`.
/// Aggregations strip `__name__` (Prometheus convention).
fn aggregate_instant_points(
    inner: Vec<InstantPoint>,
    op: AggOp,
    modifier: Option<&LabelModifier>,
    time_us: i64,
) -> Vec<InstantPoint> {
    let mut groups: BTreeMap<BTreeMap<String, String>, Vec<f64>> = BTreeMap::new();
    for p in inner {
        let key = retained_labels(&p.labels, modifier);
        groups.entry(key).or_default().push(p.value);
    }
    groups
        .into_iter()
        .map(|(labels, values)| InstantPoint {
            metric_name: String::new(),
            labels,
            ts_us: time_us,
            value: apply_agg(op, &values),
        })
        .collect()
}

fn instant_points_to_json(points: Vec<InstantPoint>) -> Vec<JsonValue> {
    points
        .into_iter()
        .map(|p| {
            let metric = if p.metric_name.is_empty() {
                metric_obj_no_name(&p.labels)
            } else {
                series_metric_obj(&p.metric_name, &p.labels)
            };
            json!({
                "metric": metric,
                "value": [(p.ts_us as f64) / 1_000_000.0, p.value.to_string()],
            })
        })
        .collect()
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
    if step_us <= 0 {
        return Err(PromError::bad_data("step must be positive"));
    }

    let expr =
        parse(&p.query).map_err(|e| PromError::bad_data(format!("PromQL parse: {e}")))?;
    let series = evaluate_range(&state, &expr, start_us, end_us, step_us).await?;
    Ok(Json(success_matrix(range_points_to_json(series))))
}

fn evaluate_range<'a>(
    state: &'a AppState,
    expr: &'a Expr,
    start_us: i64,
    end_us: i64,
    step_us: i64,
) -> RangeFut<'a> {
    Box::pin(async move {
        match detect_query_kind(expr) {
            Some(QueryKind::Selector(sel)) => {
                range_selector(state, sel, start_us, end_us).await
            }
            Some(QueryKind::RangeFn {
                sel,
                range_us,
                op,
            }) => range_range_fn(state, sel, range_us, op, start_us, end_us, step_us).await,
            Some(QueryKind::Aggregate {
                op,
                modifier,
                inner,
            }) => {
                if aggregate_can_push_down(modifier) {
                    if let Some(sel) = pure_selector(inner) {
                        return range_aggregate_via_sql(
                            state, sel, op, modifier, start_us, end_us,
                        )
                        .await;
                    }
                }
                let inner_series =
                    evaluate_range(state, inner, start_us, end_us, step_us).await?;
                Ok(aggregate_range_points(inner_series, op, modifier))
            }
            Some(QueryKind::HistogramQuantile { quantile, inner }) => {
                let inner_series =
                    evaluate_range(state, inner, start_us, end_us, step_us).await?;
                Ok(histogram_quantile_range(inner_series, quantile))
            }
            Some(QueryKind::TopK {
                n,
                top,
                modifier,
                inner,
            }) => {
                if aggregate_can_push_down(modifier) {
                    if let Some(sel) = pure_selector(inner) {
                        return range_topk_via_sql(
                            state, sel, n, top, modifier, start_us, end_us,
                        )
                        .await;
                    }
                }
                let inner_series =
                    evaluate_range(state, inner, start_us, end_us, step_us).await?;
                Ok(topk_range_points(inner_series, n, top, modifier))
            }
            Some(QueryKind::Binary {
                op,
                lhs,
                rhs,
                return_bool,
            }) => {
                binary_range_eval(state, lhs, rhs, op, return_bool, start_us, end_us, step_us)
                    .await
            }
            None => Ok(vec![]),
        }
    })
}

async fn range_selector(
    state: &AppState,
    sel: &VectorSelector,
    start_us: i64,
    end_us: i64,
) -> Result<Vec<RangePoints>, PromError> {
    let rows = run_selector_query(state, sel, start_us, end_us).await?;
    let by_series = group_rows_by_series(rows);
    Ok(by_series
        .into_values()
        .map(|(metric_name, labels, points)| RangePoints {
            metric_name,
            labels,
            points,
        })
        .collect())
}

async fn range_range_fn(
    state: &AppState,
    sel: &VectorSelector,
    range_us: i64,
    op: RangeFnOp,
    start_us: i64,
    end_us: i64,
    step_us: i64,
) -> Result<Vec<RangePoints>, PromError> {
    // Pull samples covering every step's lookback window in one go,
    // then bucket per-series and walk the steps in Rust.
    let fetch_from = start_us - range_us;
    let rows = run_selector_query(state, sel, fetch_from, end_us).await?;
    let by_series = group_rows_by_series(rows);
    let mut out = Vec::new();
    for (_, (metric_name, labels, mut points)) in by_series {
        points.sort_by_key(|(t, _)| *t);
        let mut series_points: Vec<(i64, f64)> = Vec::new();
        let mut t = start_us;
        while t <= end_us {
            // Window is `(t - range, t]` per Prometheus convention.
            // partition_point keeps this O((N+M) log N) on long ranges.
            let lo_ts = t - range_us;
            let lo = points.partition_point(|(ts, _)| *ts < lo_ts);
            let hi = points.partition_point(|(ts, _)| *ts <= t);
            if hi.saturating_sub(lo) >= 2 {
                if let Some(v) = compute_range_fn(&points[lo..hi], op) {
                    series_points.push((t, v));
                }
            }
            t += step_us;
        }
        if !series_points.is_empty() {
            out.push(RangePoints {
                metric_name,
                labels,
                points: series_points,
            });
        }
    }
    Ok(out)
}

/// Returns the inner `VectorSelector` if `expr` is a pure selector
/// (possibly wrapped in parens). Used to decide whether an
/// aggregation can be pushed straight down into a SQL `GROUP BY`.
/// Returns `None` for anything that needs in-Rust evaluation (rate,
/// binary, nested aggregations, etc).
fn pure_selector(expr: &Expr) -> Option<&VectorSelector> {
    match expr {
        Expr::VectorSelector(v) => Some(v),
        Expr::Paren(p) => pure_selector(p.expr.as_ref()),
        _ => None,
    }
}

/// `by(...)` and "no modifier" can be pushed to SQL — both reduce to
/// a fixed list of retained label keys (possibly empty). `without(...)`
/// would need to know every label name in the group at planning
/// time, which we only learn after reading the data, so it stays
/// in Rust.
fn aggregate_can_push_down(modifier: Option<&LabelModifier>) -> bool {
    matches!(modifier, None | Some(LabelModifier::Include(_)))
}

/// Retained label keys for a SQL-pushed aggregation. None or
/// `by(k1, k2)` are the only shapes that reach here.
fn aggregate_group_keys(modifier: Option<&LabelModifier>) -> Vec<String> {
    match modifier {
        Some(LabelModifier::Include(ls)) => ls.labels.clone(),
        _ => Vec::new(),
    }
}

fn agg_call_sql(op: AggOp, value_expr: &str) -> String {
    match op {
        AggOp::Sum => format!("SUM({value_expr})"),
        AggOp::Avg => format!("AVG({value_expr})"),
        AggOp::Min => format!("MIN({value_expr})"),
        AggOp::Max => format!("MAX({value_expr})"),
        // `COUNT(*)` returns Int64 in DataFusion; cast it to DOUBLE
        // so the result column is Float64 like the other aggregators
        // and our column downcast can stay uniform.
        AggOp::Count => "CAST(COUNT(*) AS DOUBLE)".to_string(),
    }
}

/// Push an `<agg>(<selector>) [by (...)]` instant query into a single
/// SQL statement that:
///   - finds the latest sample per series in the lookback window,
///   - joins to `series` to expose `s.labels`,
///   - groups by the retained labels (or globally) and aggregates.
async fn instant_aggregate_via_sql(
    state: &AppState,
    sel: &VectorSelector,
    op: AggOp,
    modifier: Option<&LabelModifier>,
    time_us: i64,
) -> Result<Vec<InstantPoint>, PromError> {
    let from_us = time_us - LOOKBACK_US;
    let group_keys = aggregate_group_keys(modifier);

    let mut window_conds = selector_predicates(sel);
    window_conds.push(format!(
        "sa.timestamp BETWEEN TIMESTAMP '{}' AND TIMESTAMP '{}'",
        us_to_ts_lit(from_us),
        us_to_ts_lit(time_us),
    ));
    let window_where = format!(" WHERE {}", window_conds.join(" AND "));

    let label_exprs: Vec<String> = group_keys
        .iter()
        .map(|k| format!("element_at(s.labels, '{}')[1]", sql_escape(k)))
        .collect();

    let select_label_part = if label_exprs.is_empty() {
        String::new()
    } else {
        let mut parts: Vec<String> = Vec::with_capacity(label_exprs.len());
        for (i, e) in label_exprs.iter().enumerate() {
            parts.push(format!("{e} AS lbl_{i}"));
        }
        format!("{}, ", parts.join(", "))
    };

    let group_by_clause = if label_exprs.is_empty() {
        String::new()
    } else {
        format!(" GROUP BY {}", label_exprs.join(", "))
    };

    let agg_call = agg_call_sql(op, "ws.value");

    // ROW_NUMBER over series_id picks the latest sample per series
    // within the lookback window. The selector predicates live in
    // the CTE so we don't compute row-numbers for series we'll just
    // throw away.
    let sql = format!(
        "WITH ws AS ( \
           SELECT sa.series_id, sa.value, sa.timestamp, \
                  ROW_NUMBER() OVER (PARTITION BY sa.series_id ORDER BY sa.timestamp DESC) AS rn \
           FROM {cat}.skaldberg.samples sa \
           JOIN {cat}.skaldberg.series s ON sa.series_id = s.series_id \
           {window_where} \
         ) \
         SELECT {select_label_part}{agg_call} AS agg_v \
         FROM ws \
         JOIN {cat}.skaldberg.series s ON ws.series_id = s.series_id \
         WHERE ws.rn = 1\
         {group_by_clause}",
        cat = DF_CATALOG_NAME,
    );

    let df = state
        .ctx
        .sql(&sql)
        .await
        .map_err(|e| PromError::internal(format!("aggregate sql: {e}")))?;
    let batches = df
        .collect()
        .await
        .map_err(|e| PromError::internal(format!("aggregate collect: {e}")))?;

    let mut out = Vec::new();
    for batch in batches {
        let label_count = group_keys.len();
        let agg_col = batch
            .column(label_count)
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| PromError::internal("aggregate value not Float64"))?;
        // Each label column is StringArray (NULL when the series
        // didn't carry that label — collapses into the "missing"
        // group, matching Prometheus's behavior).
        let mut label_arrays: Vec<&StringArray> = Vec::with_capacity(label_count);
        for li in 0..label_count {
            let arr = batch.column(li).as_any().downcast_ref::<StringArray>()
                .ok_or_else(|| PromError::internal("aggregate label col not Utf8"))?;
            label_arrays.push(arr);
        }
        for i in 0..batch.num_rows() {
            if agg_col.is_null(i) {
                continue;
            }
            let mut labels = BTreeMap::new();
            for (li, key) in group_keys.iter().enumerate() {
                if !label_arrays[li].is_null(i) {
                    labels.insert(key.clone(), label_arrays[li].value(i).to_string());
                }
            }
            out.push(InstantPoint {
                metric_name: String::new(),
                labels,
                ts_us: time_us,
                value: agg_col.value(i),
            });
        }
    }
    Ok(out)
}

/// Range counterpart: per-timestamp SQL `GROUP BY` over the
/// retained labels. No CTE / window function needed since we want
/// every sample's contribution per timestamp.
async fn range_aggregate_via_sql(
    state: &AppState,
    sel: &VectorSelector,
    op: AggOp,
    modifier: Option<&LabelModifier>,
    start_us: i64,
    end_us: i64,
) -> Result<Vec<RangePoints>, PromError> {
    let group_keys = aggregate_group_keys(modifier);

    let mut conds = selector_predicates(sel);
    conds.push(format!(
        "sa.timestamp BETWEEN TIMESTAMP '{}' AND TIMESTAMP '{}'",
        us_to_ts_lit(start_us),
        us_to_ts_lit(end_us),
    ));
    let where_clause = format!(" WHERE {}", conds.join(" AND "));

    let label_exprs: Vec<String> = group_keys
        .iter()
        .map(|k| format!("element_at(s.labels, '{}')[1]", sql_escape(k)))
        .collect();
    let select_label_part = if label_exprs.is_empty() {
        String::new()
    } else {
        let mut parts: Vec<String> = Vec::with_capacity(label_exprs.len());
        for (i, e) in label_exprs.iter().enumerate() {
            parts.push(format!("{e} AS lbl_{i}"));
        }
        format!("{}, ", parts.join(", "))
    };
    let mut group_cols = vec!["sa.timestamp".to_string()];
    group_cols.extend(label_exprs.iter().cloned());
    let group_by_clause = format!(" GROUP BY {}", group_cols.join(", "));
    let mut order_cols = label_exprs.clone();
    order_cols.push("sa.timestamp".to_string());
    let order_by_clause = format!(" ORDER BY {}", order_cols.join(", "));

    let agg_call = agg_call_sql(op, "sa.value");

    let sql = format!(
        "SELECT sa.timestamp, {select_label_part}{agg_call} AS agg_v \
         FROM {cat}.skaldberg.samples sa \
         JOIN {cat}.skaldberg.series s ON sa.series_id = s.series_id \
         {where_clause}\
         {group_by_clause}\
         {order_by_clause}",
        cat = DF_CATALOG_NAME,
    );

    let df = state
        .ctx
        .sql(&sql)
        .await
        .map_err(|e| PromError::internal(format!("range aggregate sql: {e}")))?;
    let batches = df
        .collect()
        .await
        .map_err(|e| PromError::internal(format!("range aggregate collect: {e}")))?;

    let mut by_group: BTreeMap<BTreeMap<String, String>, Vec<(i64, f64)>> = BTreeMap::new();
    for batch in batches {
        let label_count = group_keys.len();
        let ts_col = batch.column(0).as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .ok_or_else(|| PromError::internal("aggregate ts col not Timestamp(us)"))?;
        let agg_col = batch.column(1 + label_count).as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| PromError::internal("aggregate value col not Float64"))?;
        let mut label_arrays: Vec<&StringArray> = Vec::with_capacity(label_count);
        for li in 0..label_count {
            let arr = batch.column(1 + li).as_any().downcast_ref::<StringArray>()
                .ok_or_else(|| PromError::internal("aggregate label col not Utf8"))?;
            label_arrays.push(arr);
        }
        for i in 0..batch.num_rows() {
            if agg_col.is_null(i) {
                continue;
            }
            let mut labels = BTreeMap::new();
            for (li, key) in group_keys.iter().enumerate() {
                if !label_arrays[li].is_null(i) {
                    labels.insert(key.clone(), label_arrays[li].value(i).to_string());
                }
            }
            by_group
                .entry(labels)
                .or_default()
                .push((ts_col.value(i), agg_col.value(i)));
        }
    }
    Ok(by_group
        .into_iter()
        .filter(|(_, pts)| !pts.is_empty())
        .map(|(labels, points)| RangePoints {
            metric_name: String::new(),
            labels,
            points,
        })
        .collect())
}

/// `topk(n, <selector>) [by (...)]` pushed into a single SQL
/// statement. Two CTEs:
///
///   `latest` — pick the most recent sample per series within the
///              5-minute lookback window (same shape as the
///              aggregate-pushdown CTE).
///   `ranked` — `ROW_NUMBER()` partitioned by the retained labels,
///              ordered by value DESC (topk) or ASC (bottomk),
///              then filter `WHERE rnk <= n`.
///
/// Unlike reductive aggregations, topk is a *filter*: surviving
/// series keep their full `metric_name` and label map intact, so we
/// `SELECT s.metric_name, s.labels` after the rank cut and decode
/// the labels MAP exactly the way `run_selector_query` does.
async fn instant_topk_via_sql(
    state: &AppState,
    sel: &VectorSelector,
    n: usize,
    top: bool,
    modifier: Option<&LabelModifier>,
    time_us: i64,
) -> Result<Vec<InstantPoint>, PromError> {
    if n == 0 {
        return Ok(Vec::new());
    }
    let from_us = time_us - LOOKBACK_US;
    let group_keys = aggregate_group_keys(modifier);

    let mut window_conds = selector_predicates(sel);
    window_conds.push(format!(
        "sa.timestamp BETWEEN TIMESTAMP '{}' AND TIMESTAMP '{}'",
        us_to_ts_lit(from_us),
        us_to_ts_lit(time_us),
    ));
    let window_where = format!(" WHERE {}", window_conds.join(" AND "));

    let label_exprs: Vec<String> = group_keys
        .iter()
        .map(|k| format!("element_at(s.labels, '{}')[1]", sql_escape(k)))
        .collect();
    // Empty PARTITION BY (no `by`) ranks globally — DataFusion accepts
    // `ROW_NUMBER() OVER (ORDER BY ...)` without a PARTITION clause.
    let partition_clause = if label_exprs.is_empty() {
        String::new()
    } else {
        format!("PARTITION BY {} ", label_exprs.join(", "))
    };
    let order_dir = if top { "DESC" } else { "ASC" };

    let sql = format!(
        "WITH latest AS ( \
           SELECT sa.series_id, sa.value, \
                  ROW_NUMBER() OVER (PARTITION BY sa.series_id ORDER BY sa.timestamp DESC) AS rn \
           FROM {cat}.skaldberg.samples sa \
           JOIN {cat}.skaldberg.series s ON sa.series_id = s.series_id \
           {window_where} \
         ), \
         ranked AS ( \
           SELECT l.series_id, l.value, \
                  ROW_NUMBER() OVER ({partition_clause}ORDER BY l.value {order_dir}) AS rnk \
           FROM latest l \
           JOIN {cat}.skaldberg.series s ON l.series_id = s.series_id \
           WHERE l.rn = 1 \
         ) \
         SELECT s.metric_name, s.labels, rk.value \
         FROM ranked rk \
         JOIN {cat}.skaldberg.series s ON rk.series_id = s.series_id \
         WHERE rk.rnk <= {n}",
        cat = DF_CATALOG_NAME,
    );

    let df = state
        .ctx
        .sql(&sql)
        .await
        .map_err(|e| PromError::internal(format!("topk sql: {e}")))?;
    let batches = df
        .collect()
        .await
        .map_err(|e| PromError::internal(format!("topk collect: {e}")))?;

    let label_filters = label_filters_for_selector(sel);
    let mut out = Vec::new();
    for batch in batches {
        let metric_col = batch.column(0).as_any().downcast_ref::<StringArray>()
            .ok_or_else(|| PromError::internal("topk metric_name col"))?;
        let labels_col = batch.column(1).as_any().downcast_ref::<MapArray>()
            .ok_or_else(|| PromError::internal("topk labels col"))?;
        let val_col = batch.column(2).as_any().downcast_ref::<Float64Array>()
            .ok_or_else(|| PromError::internal("topk value col"))?;
        for i in 0..batch.num_rows() {
            let labels = labels_to_btree(labels_col, i)?;
            // Apply regex matchers as a Rust post-filter — same
            // safety net as `run_selector_query`.
            if !label_filters.iter().all(|f| match_label(&labels, f)) {
                continue;
            }
            out.push(InstantPoint {
                metric_name: metric_col.value(i).to_string(),
                labels,
                ts_us: time_us,
                value: val_col.value(i),
            });
        }
    }
    Ok(out)
}

/// Range counterpart: rank within `(timestamp, retained_labels)`
/// instead of just retained_labels, so each evaluation timestamp
/// gets its own top-n cut. Output rows are regrouped on the Rust
/// side by `series_key(metric_name, labels)` since a series may
/// surface in some timestamps and not others.
async fn range_topk_via_sql(
    state: &AppState,
    sel: &VectorSelector,
    n: usize,
    top: bool,
    modifier: Option<&LabelModifier>,
    start_us: i64,
    end_us: i64,
) -> Result<Vec<RangePoints>, PromError> {
    if n == 0 {
        return Ok(Vec::new());
    }
    let group_keys = aggregate_group_keys(modifier);

    let mut conds = selector_predicates(sel);
    conds.push(format!(
        "sa.timestamp BETWEEN TIMESTAMP '{}' AND TIMESTAMP '{}'",
        us_to_ts_lit(start_us),
        us_to_ts_lit(end_us),
    ));
    let where_clause = format!(" WHERE {}", conds.join(" AND "));

    let label_exprs: Vec<String> = group_keys
        .iter()
        .map(|k| format!("element_at(s.labels, '{}')[1]", sql_escape(k)))
        .collect();
    let mut partition_cols = vec!["sa.timestamp".to_string()];
    partition_cols.extend(label_exprs.iter().cloned());
    let partition_clause = format!("PARTITION BY {} ", partition_cols.join(", "));
    let order_dir = if top { "DESC" } else { "ASC" };

    let sql = format!(
        "WITH ranked AS ( \
           SELECT sa.series_id, sa.timestamp, sa.value, \
                  ROW_NUMBER() OVER ({partition_clause}ORDER BY sa.value {order_dir}) AS rnk \
           FROM {cat}.skaldberg.samples sa \
           JOIN {cat}.skaldberg.series s ON sa.series_id = s.series_id \
           {where_clause} \
         ) \
         SELECT s.metric_name, s.labels, rk.timestamp, rk.value \
         FROM ranked rk \
         JOIN {cat}.skaldberg.series s ON rk.series_id = s.series_id \
         WHERE rk.rnk <= {n} \
         ORDER BY s.metric_name, rk.timestamp",
        cat = DF_CATALOG_NAME,
    );

    let df = state
        .ctx
        .sql(&sql)
        .await
        .map_err(|e| PromError::internal(format!("range topk sql: {e}")))?;
    let batches = df
        .collect()
        .await
        .map_err(|e| PromError::internal(format!("range topk collect: {e}")))?;

    let label_filters = label_filters_for_selector(sel);
    let mut by_series: BTreeMap<String, RangePoints> = BTreeMap::new();
    for batch in batches {
        let metric_col = batch.column(0).as_any().downcast_ref::<StringArray>()
            .ok_or_else(|| PromError::internal("range topk metric_name col"))?;
        let labels_col = batch.column(1).as_any().downcast_ref::<MapArray>()
            .ok_or_else(|| PromError::internal("range topk labels col"))?;
        let ts_col = batch.column(2).as_any().downcast_ref::<TimestampMicrosecondArray>()
            .ok_or_else(|| PromError::internal("range topk ts col"))?;
        let val_col = batch.column(3).as_any().downcast_ref::<Float64Array>()
            .ok_or_else(|| PromError::internal("range topk value col"))?;
        for i in 0..batch.num_rows() {
            let labels = labels_to_btree(labels_col, i)?;
            if !label_filters.iter().all(|f| match_label(&labels, f)) {
                continue;
            }
            let metric = metric_col.value(i).to_string();
            let key = series_key(&metric, &labels);
            let entry = by_series.entry(key).or_insert_with(|| RangePoints {
                metric_name: metric,
                labels,
                points: Vec::new(),
            });
            entry.points.push((ts_col.value(i), val_col.value(i)));
        }
    }
    Ok(by_series.into_values().collect())
}

/// Group input series by retained labels, align by timestamp, then
/// collapse same-ts values with `op`. Aggregations strip `__name__`.
fn aggregate_range_points(
    inner: Vec<RangePoints>,
    op: AggOp,
    modifier: Option<&LabelModifier>,
) -> Vec<RangePoints> {
    let mut groups: BTreeMap<BTreeMap<String, String>, Vec<RangePoints>> = BTreeMap::new();
    for p in inner {
        let key = retained_labels(&p.labels, modifier);
        groups.entry(key).or_default().push(p);
    }
    let mut out = Vec::new();
    for (group_labels, members) in groups {
        // Collect every timestamp seen across the group's series, then
        // aggregate values that share the same timestamp.
        let mut by_ts: BTreeMap<i64, Vec<f64>> = BTreeMap::new();
        for m in &members {
            for (ts, v) in &m.points {
                by_ts.entry(*ts).or_default().push(*v);
            }
        }
        let series_points: Vec<(i64, f64)> = by_ts
            .into_iter()
            .map(|(ts, values)| (ts, apply_agg(op, &values)))
            .collect();
        if !series_points.is_empty() {
            out.push(RangePoints {
                metric_name: String::new(),
                labels: group_labels,
                points: series_points,
            });
        }
    }
    out
}

/// `topk` / `bottomk` over an instant vector. Unlike the reductive
/// aggregations these are filters: the surviving series keep their
/// `__name__` and full label set. Within each `(by/without)` group
/// we sort by value and take the n largest (`top=true`) or smallest.
fn topk_instant_points(
    inner: Vec<InstantPoint>,
    n: usize,
    top: bool,
    modifier: Option<&LabelModifier>,
) -> Vec<InstantPoint> {
    if n == 0 {
        return Vec::new();
    }
    let mut groups: BTreeMap<BTreeMap<String, String>, Vec<InstantPoint>> = BTreeMap::new();
    for p in inner {
        let key = retained_labels(&p.labels, modifier);
        groups.entry(key).or_default().push(p);
    }
    let mut out = Vec::new();
    for (_, mut members) in groups {
        members.sort_by(|a, b| {
            let ord = a
                .value
                .partial_cmp(&b.value)
                .unwrap_or(std::cmp::Ordering::Equal);
            if top {
                ord.reverse()
            } else {
                ord
            }
        });
        out.extend(members.into_iter().take(n));
    }
    out
}

/// Range-query analogue: at every timestamp seen across each group,
/// pick the n largest / smallest series and reattach the value to
/// the original (full-label) series. Output is one entry per surviving
/// (series, ts) pair, regrouped by series identity.
fn topk_range_points(
    inner: Vec<RangePoints>,
    n: usize,
    top: bool,
    modifier: Option<&LabelModifier>,
) -> Vec<RangePoints> {
    if n == 0 {
        return Vec::new();
    }
    // Group inner series by retained labels.
    let mut groups: BTreeMap<BTreeMap<String, String>, Vec<RangePoints>> = BTreeMap::new();
    for series in inner {
        let key = retained_labels(&series.labels, modifier);
        groups.entry(key).or_default().push(series);
    }

    // For O(log) lookups per (series, ts), turn each member's points
    // into a BTreeMap. Output keyed by series identity so we can
    // accumulate points back per surviving series across timestamps.
    let mut out_map: BTreeMap<String, RangePoints> = BTreeMap::new();
    for (_group_labels, members) in groups {
        let by_ts: Vec<BTreeMap<i64, f64>> = members
            .iter()
            .map(|m| m.points.iter().copied().collect::<BTreeMap<_, _>>())
            .collect();
        let mut all_ts: BTreeSet<i64> = BTreeSet::new();
        for m in &members {
            for (ts, _) in &m.points {
                all_ts.insert(*ts);
            }
        }
        for ts in all_ts {
            let mut at_ts: Vec<(usize, f64)> = Vec::new();
            for (idx, pts) in by_ts.iter().enumerate() {
                if let Some(&v) = pts.get(&ts) {
                    at_ts.push((idx, v));
                }
            }
            at_ts.sort_by(|a, b| {
                let ord = a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal);
                if top {
                    ord.reverse()
                } else {
                    ord
                }
            });
            for (idx, v) in at_ts.into_iter().take(n) {
                let m = &members[idx];
                let key = series_key(&m.metric_name, &m.labels);
                let entry = out_map.entry(key).or_insert_with(|| RangePoints {
                    metric_name: m.metric_name.clone(),
                    labels: m.labels.clone(),
                    points: Vec::new(),
                });
                entry.points.push((ts, v));
            }
        }
    }
    out_map.into_values().collect()
}

/// Evaluate a binary expression at a single timestamp. Each side is
/// either a NumberLiteral (treated as a scalar that applies to every
/// matching series) or a vector. We don't handle on/ignoring
/// modifiers: vector × vector matching uses the full label set.
async fn binary_instant_eval(
    state: &AppState,
    lhs: &Expr,
    rhs: &Expr,
    op: BinOp,
    return_bool: bool,
    time_us: i64,
) -> Result<Vec<InstantPoint>, PromError> {
    // Scalar × pure-selector: push the row-level transform / filter
    // straight into SQL instead of pulling every sample into Rust.
    if let Some((scalar, sel, scalar_on_left)) = scalar_vector_pair(lhs, rhs) {
        return scalar_vector_instant_via_sql(
            state, scalar, sel, op, return_bool, scalar_on_left, time_us,
        )
        .await;
    }

    let left = eval_side_instant(state, lhs, time_us).await?;
    let right = eval_side_instant(state, rhs, time_us).await?;
    Ok(match (left, right) {
        (Side::Scalar(s), Side::Vector(pts)) => {
            apply_scalar_vector_instant(s, pts, op, return_bool, /* scalar_on_left */ true)
        }
        (Side::Vector(pts), Side::Scalar(s)) => {
            apply_scalar_vector_instant(s, pts, op, return_bool, /* scalar_on_left */ false)
        }
        (Side::Vector(lpts), Side::Vector(rpts)) => {
            apply_vector_vector_instant(lpts, rpts, op, return_bool)
        }
        (Side::Scalar(_), Side::Scalar(_)) => {
            // Two scalars in a binary op is a real Prometheus case
            // (`scalar(...)` etc) but doesn't appear in panel queries.
            // Skip for now — no series to emit.
            Vec::new()
        }
    })
}

async fn binary_range_eval(
    state: &AppState,
    lhs: &Expr,
    rhs: &Expr,
    op: BinOp,
    return_bool: bool,
    start_us: i64,
    end_us: i64,
    step_us: i64,
) -> Result<Vec<RangePoints>, PromError> {
    if let Some((scalar, sel, scalar_on_left)) = scalar_vector_pair(lhs, rhs) {
        return scalar_vector_range_via_sql(
            state, scalar, sel, op, return_bool, scalar_on_left, start_us, end_us,
        )
        .await;
    }

    let left = eval_side_range(state, lhs, start_us, end_us, step_us).await?;
    let right = eval_side_range(state, rhs, start_us, end_us, step_us).await?;
    Ok(match (left, right) {
        (SideR::Scalar(s), SideR::Vector(series)) => {
            apply_scalar_vector_range(s, series, op, return_bool, true)
        }
        (SideR::Vector(series), SideR::Scalar(s)) => {
            apply_scalar_vector_range(s, series, op, return_bool, false)
        }
        (SideR::Vector(l), SideR::Vector(r)) => {
            apply_vector_vector_range(l, r, op, return_bool)
        }
        (SideR::Scalar(_), SideR::Scalar(_)) => Vec::new(),
    })
}

enum Side {
    Scalar(f64),
    Vector(Vec<InstantPoint>),
}

enum SideR {
    Scalar(f64),
    Vector(Vec<RangePoints>),
}

async fn eval_side_instant(
    state: &AppState,
    expr: &Expr,
    time_us: i64,
) -> Result<Side, PromError> {
    if let Expr::NumberLiteral(num) = expr {
        return Ok(Side::Scalar(num.val));
    }
    Ok(Side::Vector(evaluate_instant(state, expr, time_us).await?))
}

async fn eval_side_range(
    state: &AppState,
    expr: &Expr,
    start_us: i64,
    end_us: i64,
    step_us: i64,
) -> Result<SideR, PromError> {
    if let Expr::NumberLiteral(num) = expr {
        return Ok(SideR::Scalar(num.val));
    }
    Ok(SideR::Vector(
        evaluate_range(state, expr, start_us, end_us, step_us).await?,
    ))
}

fn apply_scalar_vector_instant(
    scalar: f64,
    pts: Vec<InstantPoint>,
    op: BinOp,
    return_bool: bool,
    scalar_on_left: bool,
) -> Vec<InstantPoint> {
    pts.into_iter()
        .filter_map(|mut p| {
            let (a, b) = if scalar_on_left {
                (scalar, p.value)
            } else {
                (p.value, scalar)
            };
            if op.is_comparison() && !return_bool {
                if comparison_passes(a, b, op) {
                    Some(p)
                } else {
                    None
                }
            } else {
                p.value = apply_bin_value(a, b, op);
                // Arithmetic / `bool` comparison strips __name__
                // (Prometheus convention).
                p.metric_name = String::new();
                Some(p)
            }
        })
        .collect()
}

/// Returns `(scalar, selector, scalar_on_left)` if exactly one side
/// is a finite `NumberLiteral` and the other is a pure
/// `VectorSelector`. NaN / Inf scalars fall through to the Rust path
/// because DataFusion's literal coercion handles them awkwardly.
fn scalar_vector_pair<'a>(
    lhs: &'a Expr,
    rhs: &'a Expr,
) -> Option<(f64, &'a VectorSelector, bool)> {
    fn pick_scalar(e: &Expr) -> Option<f64> {
        if let Expr::NumberLiteral(n) = e {
            if n.val.is_finite() {
                return Some(n.val);
            }
        }
        None
    }
    if let Some(s) = pick_scalar(lhs) {
        if let Some(sel) = pure_selector(rhs) {
            return Some((s, sel, true));
        }
    }
    if let Some(s) = pick_scalar(rhs) {
        if let Some(sel) = pure_selector(lhs) {
            return Some((s, sel, false));
        }
    }
    None
}

/// Float64 SQL literal. `{:?}` always emits a decimal point for
/// f64, so DataFusion sees `300.0` not `300` (which would parse as
/// Int and require coercion). We also wrap in `CAST(... AS DOUBLE)`
/// just to be explicit.
fn f64_sql_lit(v: f64) -> String {
    format!("CAST({:?} AS DOUBLE)", v)
}

fn cmp_op_sql(op: BinOp) -> &'static str {
    match op {
        BinOp::Eq => "=",
        BinOp::Ne => "<>",
        BinOp::Lt => "<",
        BinOp::Le => "<=",
        BinOp::Gt => ">",
        BinOp::Ge => ">=",
        // unreachable for arithmetic ops
        _ => "=",
    }
}

/// SQL expression for the *value* result of a binary op. Used for
/// arithmetic and comparison-with-`bool`. Comparison-without-bool
/// is the filter form; the caller emits a `WHERE` clause instead.
fn binop_value_sql(op: BinOp, lhs_sql: &str, rhs_sql: &str, return_bool: bool) -> String {
    if op.is_comparison() && return_bool {
        return format!(
            "CAST(CASE WHEN {lhs_sql} {} {rhs_sql} THEN 1.0 ELSE 0.0 END AS DOUBLE)",
            cmp_op_sql(op)
        );
    }
    match op {
        BinOp::Add => format!("({lhs_sql}) + ({rhs_sql})"),
        BinOp::Sub => format!("({lhs_sql}) - ({rhs_sql})"),
        BinOp::Mul => format!("({lhs_sql}) * ({rhs_sql})"),
        BinOp::Div => format!("({lhs_sql}) / ({rhs_sql})"),
        BinOp::Mod => format!("({lhs_sql}) % ({rhs_sql})"),
        BinOp::Pow => format!("power(({lhs_sql}), ({rhs_sql}))"),
        // Comparisons in non-bool mode are unreachable here — they
        // go down the filter path. Emit `0.0` defensively.
        _ => "CAST(0.0 AS DOUBLE)".to_string(),
    }
}

/// `<scalar> <op> <selector>` (or vice versa) at one timestamp.
/// Mirrors `apply_scalar_vector_instant`:
///   - arithmetic / `bool` comparison → recompute value, strip __name__
///   - filter comparison → keep value, keep __name__, drop rows that
///     don't match.
async fn scalar_vector_instant_via_sql(
    state: &AppState,
    scalar: f64,
    sel: &VectorSelector,
    op: BinOp,
    return_bool: bool,
    scalar_on_left: bool,
    time_us: i64,
) -> Result<Vec<InstantPoint>, PromError> {
    let from_us = time_us - LOOKBACK_US;
    let mut window_conds = selector_predicates(sel);
    window_conds.push(format!(
        "sa.timestamp BETWEEN TIMESTAMP '{}' AND TIMESTAMP '{}'",
        us_to_ts_lit(from_us),
        us_to_ts_lit(time_us),
    ));
    let window_where = format!(" WHERE {}", window_conds.join(" AND "));

    let scalar_lit = f64_sql_lit(scalar);
    let (lhs_sql, rhs_sql) = if scalar_on_left {
        (scalar_lit.as_str(), "l.value")
    } else {
        ("l.value", scalar_lit.as_str())
    };

    let is_filter = op.is_comparison() && !return_bool;
    let (metric_sel, value_sel, extra_where) = if is_filter {
        (
            "s.metric_name".to_string(),
            "l.value".to_string(),
            format!(" AND ({lhs_sql} {} {rhs_sql})", cmp_op_sql(op)),
        )
    } else {
        (
            "''".to_string(),
            binop_value_sql(op, lhs_sql, rhs_sql, return_bool),
            String::new(),
        )
    };

    let sql = format!(
        "WITH latest AS ( \
           SELECT sa.series_id, sa.value, \
                  ROW_NUMBER() OVER (PARTITION BY sa.series_id ORDER BY sa.timestamp DESC) AS rn \
           FROM {cat}.skaldberg.samples sa \
           JOIN {cat}.skaldberg.series s ON sa.series_id = s.series_id \
           {window_where} \
         ) \
         SELECT {metric_sel} AS metric_name, s.labels, ({value_sel}) AS value \
         FROM latest l \
         JOIN {cat}.skaldberg.series s ON l.series_id = s.series_id \
         WHERE l.rn = 1{extra_where}",
        cat = DF_CATALOG_NAME,
    );

    let df = state
        .ctx
        .sql(&sql)
        .await
        .map_err(|e| PromError::internal(format!("scalar-vector sql: {e}")))?;
    let batches = df
        .collect()
        .await
        .map_err(|e| PromError::internal(format!("scalar-vector collect: {e}")))?;

    let label_filters = label_filters_for_selector(sel);
    let mut out = Vec::new();
    for batch in batches {
        let metric_col = batch.column(0).as_any().downcast_ref::<StringArray>()
            .ok_or_else(|| PromError::internal("sv metric_name col"))?;
        let labels_col = batch.column(1).as_any().downcast_ref::<MapArray>()
            .ok_or_else(|| PromError::internal("sv labels col"))?;
        let val_col = batch.column(2).as_any().downcast_ref::<Float64Array>()
            .ok_or_else(|| PromError::internal("sv value col"))?;
        for i in 0..batch.num_rows() {
            let labels = labels_to_btree(labels_col, i)?;
            if !label_filters.iter().all(|f| match_label(&labels, f)) {
                continue;
            }
            out.push(InstantPoint {
                metric_name: metric_col.value(i).to_string(),
                labels,
                ts_us: time_us,
                value: val_col.value(i),
            });
        }
    }
    Ok(out)
}

/// Range counterpart: every sample in the time window goes through
/// the same row-level transform / filter, then results are regrouped
/// by `series_key(metric_name, labels)` so each series's points
/// stick together.
async fn scalar_vector_range_via_sql(
    state: &AppState,
    scalar: f64,
    sel: &VectorSelector,
    op: BinOp,
    return_bool: bool,
    scalar_on_left: bool,
    start_us: i64,
    end_us: i64,
) -> Result<Vec<RangePoints>, PromError> {
    let mut conds = selector_predicates(sel);
    conds.push(format!(
        "sa.timestamp BETWEEN TIMESTAMP '{}' AND TIMESTAMP '{}'",
        us_to_ts_lit(start_us),
        us_to_ts_lit(end_us),
    ));
    let where_clause = format!(" WHERE {}", conds.join(" AND "));

    let scalar_lit = f64_sql_lit(scalar);
    let (lhs_sql, rhs_sql) = if scalar_on_left {
        (scalar_lit.as_str(), "sa.value")
    } else {
        ("sa.value", scalar_lit.as_str())
    };

    let is_filter = op.is_comparison() && !return_bool;
    let (metric_sel, value_sel, extra_where) = if is_filter {
        (
            "s.metric_name".to_string(),
            "sa.value".to_string(),
            format!(" AND ({lhs_sql} {} {rhs_sql})", cmp_op_sql(op)),
        )
    } else {
        (
            "''".to_string(),
            binop_value_sql(op, lhs_sql, rhs_sql, return_bool),
            String::new(),
        )
    };

    // Output alias is `m_name` (not `metric_name`) so ORDER BY can
    // reference the qualified `s.metric_name` without colliding
    // with the projection — the literal-`''` arithmetic case would
    // otherwise create an unqualified `metric_name` column that
    // shadows `s.metric_name`.
    let sql = format!(
        "SELECT {metric_sel} AS m_name, s.labels, sa.timestamp, ({value_sel}) AS value \
         FROM {cat}.skaldberg.samples sa \
         JOIN {cat}.skaldberg.series s ON sa.series_id = s.series_id \
         {where_clause}{extra_where} \
         ORDER BY s.metric_name, sa.timestamp",
        cat = DF_CATALOG_NAME,
    );

    let df = state
        .ctx
        .sql(&sql)
        .await
        .map_err(|e| PromError::internal(format!("scalar-vector range sql: {e}")))?;
    let batches = df
        .collect()
        .await
        .map_err(|e| PromError::internal(format!("scalar-vector range collect: {e}")))?;

    let label_filters = label_filters_for_selector(sel);
    let mut by_series: BTreeMap<String, RangePoints> = BTreeMap::new();
    for batch in batches {
        let metric_col = batch.column(0).as_any().downcast_ref::<StringArray>()
            .ok_or_else(|| PromError::internal("sv range metric_name col"))?;
        let labels_col = batch.column(1).as_any().downcast_ref::<MapArray>()
            .ok_or_else(|| PromError::internal("sv range labels col"))?;
        let ts_col = batch.column(2).as_any().downcast_ref::<TimestampMicrosecondArray>()
            .ok_or_else(|| PromError::internal("sv range ts col"))?;
        let val_col = batch.column(3).as_any().downcast_ref::<Float64Array>()
            .ok_or_else(|| PromError::internal("sv range value col"))?;
        for i in 0..batch.num_rows() {
            let labels = labels_to_btree(labels_col, i)?;
            if !label_filters.iter().all(|f| match_label(&labels, f)) {
                continue;
            }
            let metric = metric_col.value(i).to_string();
            let key = series_key(&metric, &labels);
            let entry = by_series.entry(key).or_insert_with(|| RangePoints {
                metric_name: metric,
                labels,
                points: Vec::new(),
            });
            entry.points.push((ts_col.value(i), val_col.value(i)));
        }
    }
    Ok(by_series.into_values().collect())
}

fn apply_scalar_vector_range(
    scalar: f64,
    series: Vec<RangePoints>,
    op: BinOp,
    return_bool: bool,
    scalar_on_left: bool,
) -> Vec<RangePoints> {
    series
        .into_iter()
        .map(|mut s| {
            s.points = s
                .points
                .into_iter()
                .filter_map(|(ts, v)| {
                    let (a, b) = if scalar_on_left { (scalar, v) } else { (v, scalar) };
                    if op.is_comparison() && !return_bool {
                        if comparison_passes(a, b, op) {
                            Some((ts, v))
                        } else {
                            None
                        }
                    } else {
                        Some((ts, apply_bin_value(a, b, op)))
                    }
                })
                .collect();
            if !(op.is_comparison() && !return_bool) {
                s.metric_name = String::new();
            }
            s
        })
        .filter(|s| !s.points.is_empty())
        .collect()
}

fn apply_vector_vector_instant(
    lhs: Vec<InstantPoint>,
    rhs: Vec<InstantPoint>,
    op: BinOp,
    return_bool: bool,
) -> Vec<InstantPoint> {
    let mut rhs_index: BTreeMap<BTreeMap<String, String>, &InstantPoint> = BTreeMap::new();
    for r in &rhs {
        rhs_index.insert(r.labels.clone(), r);
    }
    let mut out = Vec::new();
    for l in &lhs {
        if let Some(r) = rhs_index.get(&l.labels) {
            if op.is_comparison() && !return_bool {
                if comparison_passes(l.value, r.value, op) {
                    out.push(InstantPoint {
                        metric_name: l.metric_name.clone(),
                        labels: l.labels.clone(),
                        ts_us: l.ts_us,
                        value: l.value,
                    });
                }
            } else {
                out.push(InstantPoint {
                    metric_name: String::new(),
                    labels: l.labels.clone(),
                    ts_us: l.ts_us,
                    value: apply_bin_value(l.value, r.value, op),
                });
            }
        }
    }
    out
}

fn apply_vector_vector_range(
    lhs: Vec<RangePoints>,
    rhs: Vec<RangePoints>,
    op: BinOp,
    return_bool: bool,
) -> Vec<RangePoints> {
    // Index rhs by full label set for O(log) lookup; per-series points
    // are also kept as BTreeMap<ts, v> so we can align by timestamp.
    let mut rhs_index: BTreeMap<BTreeMap<String, String>, BTreeMap<i64, f64>> =
        BTreeMap::new();
    for r in rhs {
        rhs_index.insert(r.labels, r.points.into_iter().collect());
    }
    let mut out = Vec::new();
    for l in lhs {
        let r_pts = match rhs_index.get(&l.labels) {
            Some(m) => m,
            None => continue,
        };
        let mut new_points: Vec<(i64, f64)> = Vec::new();
        for (ts, lv) in l.points {
            if let Some(&rv) = r_pts.get(&ts) {
                if op.is_comparison() && !return_bool {
                    if comparison_passes(lv, rv, op) {
                        new_points.push((ts, lv));
                    }
                } else {
                    new_points.push((ts, apply_bin_value(lv, rv, op)));
                }
            }
        }
        if !new_points.is_empty() {
            out.push(RangePoints {
                metric_name: if op.is_comparison() && !return_bool {
                    l.metric_name
                } else {
                    String::new()
                },
                labels: l.labels,
                points: new_points,
            });
        }
    }
    out
}

/// Reduce a vector of histogram-bucket points (each carrying an
/// `le="..."` label) to a single quantile value per `(label-set
/// excluding le)` group. Mirrors Prometheus's `histogram_quantile`
/// algorithm without the recent native histogram extensions.
fn histogram_quantile_instant(
    inner: Vec<InstantPoint>,
    quantile: f64,
    time_us: i64,
) -> Vec<InstantPoint> {
    // Group by every label except `le`. Inside each group we sort
    // bucket boundaries and walk the cumulative counts.
    let mut groups: BTreeMap<BTreeMap<String, String>, Vec<(f64, f64)>> = BTreeMap::new();
    for p in inner {
        let mut labels = p.labels;
        let le = match labels.remove("le").as_deref().and_then(parse_le) {
            Some(v) => v,
            None => continue, // not a histogram bucket — drop
        };
        groups.entry(labels).or_default().push((le, p.value));
    }
    let mut out = Vec::new();
    for (labels, mut buckets) in groups {
        buckets.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        if let Some(v) = quantile_from_buckets(&buckets, quantile) {
            out.push(InstantPoint {
                metric_name: String::new(),
                labels,
                ts_us: time_us,
                value: v,
            });
        }
    }
    out
}

fn histogram_quantile_range(inner: Vec<RangePoints>, quantile: f64) -> Vec<RangePoints> {
    // ts → bucket vec, scoped per `(labels minus le)` group. We need
    // the timestamps aligned across `le` siblings to compute a
    // quantile per timestep, so build a `group → ts → buckets` map
    // first, then compute per-ts quantile per group.
    let mut groups: BTreeMap<BTreeMap<String, String>, BTreeMap<i64, Vec<(f64, f64)>>> =
        BTreeMap::new();
    for series in inner {
        let mut labels = series.labels;
        let le = match labels.remove("le").as_deref().and_then(parse_le) {
            Some(v) => v,
            None => continue,
        };
        let group_buckets = groups.entry(labels).or_default();
        for (ts, v) in series.points {
            group_buckets.entry(ts).or_default().push((le, v));
        }
    }
    let mut out = Vec::new();
    for (labels, ts_buckets) in groups {
        let mut points = Vec::new();
        for (ts, mut buckets) in ts_buckets {
            buckets.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
            if let Some(v) = quantile_from_buckets(&buckets, quantile) {
                points.push((ts, v));
            }
        }
        if !points.is_empty() {
            out.push(RangePoints {
                metric_name: String::new(),
                labels,
                points,
            });
        }
    }
    out
}

fn parse_le(s: &str) -> Option<f64> {
    if s.eq_ignore_ascii_case("+inf") || s.eq_ignore_ascii_case("inf") {
        Some(f64::INFINITY)
    } else if s.eq_ignore_ascii_case("-inf") {
        Some(f64::NEG_INFINITY)
    } else {
        s.parse::<f64>().ok()
    }
}

/// Linear interpolation across a Prometheus cumulative histogram.
///
/// `buckets` is `[(le, cumulative_count)]` sorted by `le` ascending.
/// The last bucket is expected to be `le=+Inf` and carries the total
/// count. `q` is the requested quantile in `[0, 1]`.
///
/// For `q ≤ 0` we return the lowest non-empty bucket boundary, for
/// `q ≥ 1` the largest finite boundary observed. Otherwise we find
/// the bucket whose cumulative count first crosses `q * total` and
/// linearly interpolate between its lower and upper `le` bounds. If
/// the crossing bucket has unbounded upper edge (`+Inf`) we return
/// the previous boundary instead of trying to interpolate to
/// infinity.
fn quantile_from_buckets(buckets: &[(f64, f64)], q: f64) -> Option<f64> {
    if buckets.len() < 2 {
        return None;
    }
    let total = buckets.last()?.1;
    if total <= 0.0 || q.is_nan() {
        return None;
    }
    if q <= 0.0 {
        // Smallest le boundary that has any count.
        for &(le, count) in buckets {
            if count > 0.0 {
                return Some(le);
            }
        }
        return None;
    }
    if q >= 1.0 {
        // Largest finite le boundary observed.
        for &(le, _) in buckets.iter().rev() {
            if le.is_finite() {
                return Some(le);
            }
        }
        return None;
    }

    let target = q * total;
    let mut lower_le = 0.0_f64;
    let mut lower_count = 0.0_f64;
    for &(le, count) in buckets {
        if count >= target {
            if le.is_infinite() {
                return Some(lower_le);
            }
            let bucket_count = count - lower_count;
            if bucket_count <= 0.0 {
                return Some(le);
            }
            let frac = (target - lower_count) / bucket_count;
            return Some(lower_le + frac * (le - lower_le));
        }
        lower_le = le;
        lower_count = count;
    }
    None
}

fn range_points_to_json(series: Vec<RangePoints>) -> Vec<JsonValue> {
    series
        .into_iter()
        .map(|s| {
            let metric = if s.metric_name.is_empty() {
                metric_obj_no_name(&s.labels)
            } else {
                series_metric_obj(&s.metric_name, &s.labels)
            };
            let values: Vec<JsonValue> = s
                .points
                .into_iter()
                .map(|(ts, v)| json!([(ts as f64) / 1_000_000.0, v.to_string()]))
                .collect();
            json!({
                "metric": metric,
                "values": values,
            })
        })
        .collect()
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

/// Apply a range-vector function to a sorted slice of (ts_us, value).
///
/// We do **not** apply Prometheus's extrapolation to the range edges
/// for `rate / increase`, so on short windows numbers will differ
/// slightly from a real Prometheus server. Magnitudes and shapes are
/// correct, which is what panels need.
fn compute_range_fn(points: &[(i64, f64)], op: RangeFnOp) -> Option<f64> {
    match op {
        RangeFnOp::Rate => {
            let (delta, secs) = delta_with_reset_and_secs(points)?;
            Some(delta / secs)
        }
        RangeFnOp::Increase => {
            // Same `delta` as rate; just don't divide by time.
            // Equivalent to `rate * range_secs` — Prometheus
            // documents it that way.
            let (delta, _) = delta_with_reset_and_secs(points)?;
            Some(delta)
        }
        RangeFnOp::Irate => {
            // Per-second rate from only the last two samples in the
            // window. Used for "show me the most recent slope" panels.
            let n = points.len();
            if n < 2 {
                return None;
            }
            let (prev_ts, prev_v) = points[n - 2];
            let (curr_ts, curr_v) = points[n - 1];
            let secs = (curr_ts - prev_ts) as f64 / 1_000_000.0;
            if secs <= 0.0 {
                return None;
            }
            let delta = if curr_v >= prev_v {
                curr_v - prev_v
            } else {
                // Counter-reset adjustment, same as in rate.
                curr_v
            };
            Some(delta / secs)
        }
        RangeFnOp::Delta => {
            // Gauge delta: trust the values, don't reset-adjust.
            // For counters use `increase` instead.
            if points.len() < 2 {
                return None;
            }
            let first = points.first()?.1;
            let last = points.last()?.1;
            Some(last - first)
        }
    }
}

/// Walk an ascending-sorted slice pairwise, summing positive deltas
/// and treating each drop as `curr` (counter-reset adjustment).
/// Returns `(delta, seconds_between_first_and_last)`.
fn delta_with_reset_and_secs(points: &[(i64, f64)]) -> Option<(f64, f64)> {
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
            // Counter reset — assume the underlying counter went
            // 0 → curr in the gap.
            delta += curr;
        }
    }
    Some((delta, secs))
}

// ---------- /api/v1/labels ----------

pub async fn labels(
    State(state): State<Arc<AppState>>,
) -> Result<Json<JsonValue>, PromError> {
    // Distinct label-name list, computed in DataFusion: explode the
    // labels MAP into one row per (series, key), then DISTINCT.
    // `__name__` isn't a real key in `labels`; we splice it in
    // unconditionally on the Rust side.
    let sql = format!(
        "SELECT DISTINCT k FROM \
         (SELECT unnest(map_keys(s.labels)) AS k FROM {}.skaldberg.series s) \
         ORDER BY k",
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
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| PromError::internal("labels col not Utf8"))?;
        for i in 0..col.len() {
            if !col.is_null(i) {
                keys.insert(col.value(i).to_string());
            }
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
    // Both branches do `SELECT DISTINCT ... ORDER BY ...` in
    // DataFusion. The `__name__` shortcut hits the dedicated
    // `metric_name` column on the `series` table; everything else
    // goes through `element_at(labels, key)[1]` (the same trick
    // the matcher pushdown uses) with a NULL filter so series
    // missing the label simply don't contribute.
    let sql = if name == "__name__" {
        format!(
            "SELECT DISTINCT metric_name AS v FROM {}.skaldberg.series ORDER BY v",
            DF_CATALOG_NAME
        )
    } else {
        let escaped = sql_escape(&name);
        format!(
            "SELECT DISTINCT element_at(labels, '{escaped}')[1] AS v \
             FROM {}.skaldberg.series \
             WHERE element_at(labels, '{escaped}')[1] IS NOT NULL \
             ORDER BY v",
            DF_CATALOG_NAME
        )
    };

    let df = state
        .ctx
        .sql(&sql)
        .await
        .map_err(|e| PromError::internal(format!("label_values sql: {e}")))?;
    let batches = df
        .collect()
        .await
        .map_err(|e| PromError::internal(format!("collect: {e}")))?;
    let mut values: Vec<String> = Vec::new();
    for batch in batches {
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| PromError::internal("label value col not Utf8"))?;
        for i in 0..col.len() {
            if !col.is_null(i) {
                values.push(col.value(i).to_string());
            }
        }
    }
    Ok(Json(json!({
        "status": "success",
        "data": values,
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

/// What we plan to do with the parsed PromQL expression. Each
/// step adds variants here; everything we don't recognize natively
/// falls through to the selector-unwrap path so panels still render
/// *something*.
enum QueryKind<'a> {
    /// Return raw points for the wrapped vector selector.
    Selector(&'a VectorSelector),
    /// Apply a per-second / per-window function (rate / irate /
    /// increase / delta) to samples in the matrix selector's range.
    RangeFn {
        sel: &'a VectorSelector,
        range_us: i64,
        op: RangeFnOp,
    },
    /// Aggregate the inner expression's results, optionally grouped
    /// by/without a label set.
    Aggregate {
        op: AggOp,
        modifier: Option<&'a LabelModifier>,
        inner: &'a Expr,
    },
    /// `histogram_quantile(q, vector)` — interpolate a quantile out
    /// of a Prometheus histogram (cumulative `_bucket` series with
    /// `le="..."` labels).
    HistogramQuantile { quantile: f64, inner: &'a Expr },
    /// `topk(n, vector)` / `bottomk(n, vector)` — keep the n largest
    /// or smallest series within each `(by/without)` group. Unlike
    /// the reductive aggregators, this is a filter: the surviving
    /// series keep their full label set (and `__name__`) intact.
    TopK {
        n: usize,
        top: bool,
        modifier: Option<&'a LabelModifier>,
        inner: &'a Expr,
    },
    /// Binary operator between two operands. Each side is either a
    /// scalar (NumberLiteral) or a vector. Only complete-label-set
    /// 1:1 matching is supported — `on(...)` / `ignoring(...)` /
    /// `group_left` / `group_right` are deferred.
    Binary {
        op: BinOp,
        lhs: &'a Expr,
        rhs: &'a Expr,
        return_bool: bool,
    },
}

#[derive(Clone, Copy)]
enum BinOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Pow,
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

impl BinOp {
    fn is_comparison(self) -> bool {
        matches!(
            self,
            BinOp::Eq | BinOp::Ne | BinOp::Lt | BinOp::Le | BinOp::Gt | BinOp::Ge
        )
    }
}

#[derive(Clone, Copy)]
enum RangeFnOp {
    /// `rate(matrix)` — Prometheus per-second rate, counter-reset
    /// adjusted, no extrapolation.
    Rate,
    /// `irate(matrix)` — instantaneous rate from the last two points
    /// in the window.
    Irate,
    /// `increase(matrix)` — counter-reset-adjusted total delta over
    /// the window. Equivalent to `rate * range_secs` in our impl.
    Increase,
    /// `delta(matrix)` — gauge delta (`last - first`). No counter
    /// reset adjustment.
    Delta,
}

#[derive(Clone, Copy)]
enum AggOp {
    Sum,
    Avg,
    Min,
    Max,
    Count,
}

/// Pick a query plan for the AST. New recognizers drop into this
/// `match` as we add native semantics for more PromQL features.
fn detect_query_kind(expr: &Expr) -> Option<QueryKind<'_>> {
    if let Expr::Aggregate(a) = expr {
        let op_id = a.op.id();
        if op_id == T_TOPK || op_id == T_BOTTOMK {
            // topk / bottomk carry the count `n` in `param`. Skip
            // the recognizer if it's not a NumberLiteral — that
            // shouldn't happen for valid PromQL (parser enforces it)
            // but the fall-through to selector unwrap is harmless.
            if let Some(param) = a.param.as_ref() {
                if let Expr::NumberLiteral(num) = param.as_ref() {
                    let n = if num.val.is_finite() && num.val > 0.0 {
                        num.val as usize
                    } else {
                        0
                    };
                    return Some(QueryKind::TopK {
                        n,
                        top: op_id == T_TOPK,
                        modifier: a.modifier.as_ref(),
                        inner: a.expr.as_ref(),
                    });
                }
            }
        }
        if let Some(op) = parse_agg_op(op_id) {
            return Some(QueryKind::Aggregate {
                op,
                modifier: a.modifier.as_ref(),
                inner: a.expr.as_ref(),
            });
        }
        // Unknown aggregation op (quantile / stddev / count_values / ...)
        // — fall through to selector unwrap so the panel still draws
        // something instead of erroring.
    }
    if let Expr::Binary(b) = expr {
        if let Some(op) = parse_bin_op(b.op.id()) {
            return Some(QueryKind::Binary {
                op,
                lhs: b.lhs.as_ref(),
                rhs: b.rhs.as_ref(),
                return_bool: b
                    .modifier
                    .as_ref()
                    .map(|m| m.return_bool)
                    .unwrap_or(false),
            });
        }
    }
    if let Expr::Call(c) = expr {
        if c.func.name.eq_ignore_ascii_case("histogram_quantile") && c.args.args.len() == 2 {
            if let Expr::NumberLiteral(num) = c.args.args[0].as_ref() {
                return Some(QueryKind::HistogramQuantile {
                    quantile: num.val,
                    inner: c.args.args[1].as_ref(),
                });
            }
        }
        if let Some(op) = parse_range_fn_op(&c.func.name) {
            if let Some(arg) = c.args.args.first() {
                if let Expr::MatrixSelector(m) = arg.as_ref() {
                    return Some(QueryKind::RangeFn {
                        sel: &m.vs,
                        range_us: m.range.as_micros() as i64,
                        op,
                    });
                }
            }
        }
    }
    extract_selector(expr).map(QueryKind::Selector)
}

fn parse_range_fn_op(name: &str) -> Option<RangeFnOp> {
    match name.to_ascii_lowercase().as_str() {
        "rate" => Some(RangeFnOp::Rate),
        "irate" => Some(RangeFnOp::Irate),
        "increase" => Some(RangeFnOp::Increase),
        "delta" => Some(RangeFnOp::Delta),
        _ => None,
    }
}

fn parse_bin_op(id: u8) -> Option<BinOp> {
    match id {
        x if x == T_ADD => Some(BinOp::Add),
        x if x == T_SUB => Some(BinOp::Sub),
        x if x == T_MUL => Some(BinOp::Mul),
        x if x == T_DIV => Some(BinOp::Div),
        x if x == T_MOD => Some(BinOp::Mod),
        x if x == T_POW => Some(BinOp::Pow),
        x if x == T_EQLC => Some(BinOp::Eq),
        x if x == T_NEQ => Some(BinOp::Ne),
        x if x == T_LSS => Some(BinOp::Lt),
        x if x == T_LTE => Some(BinOp::Le),
        x if x == T_GTR => Some(BinOp::Gt),
        x if x == T_GTE => Some(BinOp::Ge),
        // T_LAND / T_LOR / T_LUNLESS are deferred (logical ops).
        _ => None,
    }
}

fn apply_bin_value(a: f64, b: f64, op: BinOp) -> f64 {
    match op {
        BinOp::Add => a + b,
        BinOp::Sub => a - b,
        BinOp::Mul => a * b,
        BinOp::Div => a / b,
        BinOp::Mod => a % b,
        BinOp::Pow => a.powf(b),
        // For comparisons the "value" form returns 0/1; the filter
        // form (default in Prometheus) is decided by the caller and
        // uses the original lhs value instead.
        BinOp::Eq => f64::from(u8::from(a == b)),
        BinOp::Ne => f64::from(u8::from(a != b)),
        BinOp::Lt => f64::from(u8::from(a < b)),
        BinOp::Le => f64::from(u8::from(a <= b)),
        BinOp::Gt => f64::from(u8::from(a > b)),
        BinOp::Ge => f64::from(u8::from(a >= b)),
    }
}

fn comparison_passes(a: f64, b: f64, op: BinOp) -> bool {
    match op {
        BinOp::Eq => a == b,
        BinOp::Ne => a != b,
        BinOp::Lt => a < b,
        BinOp::Le => a <= b,
        BinOp::Gt => a > b,
        BinOp::Ge => a >= b,
        _ => true,
    }
}

fn parse_agg_op(id: u8) -> Option<AggOp> {
    match id {
        x if x == T_SUM => Some(AggOp::Sum),
        x if x == T_AVG => Some(AggOp::Avg),
        x if x == T_MIN => Some(AggOp::Min),
        x if x == T_MAX => Some(AggOp::Max),
        x if x == T_COUNT => Some(AggOp::Count),
        _ => None,
    }
}

fn apply_agg(op: AggOp, values: &[f64]) -> f64 {
    match op {
        AggOp::Sum => values.iter().copied().sum(),
        AggOp::Avg => {
            if values.is_empty() {
                0.0
            } else {
                values.iter().copied().sum::<f64>() / values.len() as f64
            }
        }
        AggOp::Min => values.iter().copied().fold(f64::INFINITY, f64::min),
        AggOp::Max => values.iter().copied().fold(f64::NEG_INFINITY, f64::max),
        AggOp::Count => values.len() as f64,
    }
}

/// Reduce a series's labels to the group key implied by a `by(...)` /
/// `without(...)` modifier. No modifier means everything aggregates
/// into a single, label-less group (Prometheus default).
fn retained_labels(
    labels: &BTreeMap<String, String>,
    modifier: Option<&LabelModifier>,
) -> BTreeMap<String, String> {
    match modifier {
        Some(LabelModifier::Include(ls)) => {
            let keep: BTreeSet<&str> = ls.labels.iter().map(String::as_str).collect();
            labels
                .iter()
                .filter(|(k, _)| keep.contains(k.as_str()))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect()
        }
        Some(LabelModifier::Exclude(ls)) => {
            let drop: BTreeSet<&str> = ls.labels.iter().map(String::as_str).collect();
            labels
                .iter()
                .filter(|(k, _)| !drop.contains(k.as_str()))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect()
        }
        None => BTreeMap::new(),
    }
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
    let mut conds = selector_predicates(sel);
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

/// SQL `WHERE` predicates derived from a vector selector. Returns
/// conditions assuming the `series` table is aliased as `s` and the
/// `samples` table as `sa`. Doesn't include the time-range clause —
/// callers add that on top so they can decide which timestamp
/// column to gate on.
///
/// `=` / `!=` matchers go through `element_at(s.labels, '<k>')[1]`
/// (List<Utf8> indexing collapses to Utf8). Regex matchers are
/// dropped here and applied as a Rust post-filter — DataFusion 52
/// doesn't expose a clean regexp pushdown for label maps.
fn selector_predicates(sel: &VectorSelector) -> Vec<String> {
    let mut conds = Vec::new();
    if let Some(name) = effective_metric_name(sel) {
        conds.push(format!("s.metric_name = '{}'", sql_escape(&name)));
    }
    for m in &sel.matchers.matchers {
        if m.name == "__name__" {
            continue;
        }
        let key = sql_escape(&m.name);
        let val = sql_escape(&m.value);
        match &m.op {
            MatchOp::Equal => {
                conds.push(format!("element_at(s.labels, '{key}')[1] = '{val}'"));
            }
            MatchOp::NotEqual => {
                conds.push(format!(
                    "COALESCE(element_at(s.labels, '{key}')[1], '') != '{val}'"
                ));
            }
            MatchOp::Re(_) | MatchOp::NotRe(_) => {
                // Pushed down to Rust post-filter.
            }
        }
    }
    conds
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

/// Return label matchers that need post-filtering in Rust. `=` /
/// `!=` are pushed to SQL via `map_get_string`; only regex
/// matchers (`=~` / `!~`) and any future shapes the SQL path
/// can't express stay here.
fn label_filters_for_selector(sel: &VectorSelector) -> Vec<&Matcher> {
    sel.matchers
        .matchers
        .iter()
        .filter(|m| m.name != "__name__")
        .filter(|m| matches!(m.op, MatchOp::Re(_) | MatchOp::NotRe(_)))
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

/// Same as `series_metric_obj` but without `__name__` — used for
/// aggregation results, which Prometheus convention strips of name.
fn metric_obj_no_name(labels: &BTreeMap<String, String>) -> JsonValue {
    let mut m = serde_json::Map::new();
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
