//! HTTP handlers: `/healthz`, `/api/v1/sql`, `/api/v1/ingest`, `/api/v1/write`.
//!
//! `run_sql` uses DataFusion's `SessionContext::sql()` and walks the
//! resulting `RecordBatch`es through `crate::convert` to produce JSON.
//! `run_ingest` and `run_remote_write` go through the same `IngestState`
//! pipeline (validate → WAL → buffer); the only difference is the input
//! format.

use std::sync::Arc;
use std::time::Instant;

use arrow::record_batch::RecordBatch;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value as JsonValue};

use crate::convert::record_batches_to_json_rows;
use crate::ingest::{
    decode_write_request, flatten_write_request, validate, IngestRequest, IngestResponse,
    RejectedSample, ValidatedSample,
};
use crate::state::AppState;

#[derive(Deserialize)]
pub struct SqlRequest {
    pub sql: String,
    #[serde(default = "default_max_rows")]
    pub max_rows: usize,
}

fn default_max_rows() -> usize {
    100_000
}

#[derive(Serialize)]
pub struct SqlResponse {
    pub columns: Vec<ColumnMeta>,
    pub rows: Vec<Map<String, JsonValue>>,
    pub meta: Meta,
}

#[derive(Serialize)]
pub struct ColumnMeta {
    pub name: String,
    pub r#type: String,
}

#[derive(Serialize)]
pub struct Meta {
    pub row_count: usize,
    pub elapsed_ms: u128,
    pub truncated: bool,
}

pub async fn healthz() -> &'static str {
    "ok"
}

/// `POST /api/v1/sql`. Run an arbitrary DataFusion SQL statement and
/// return the result as JSON. Result is capped at `max_rows` (default
/// 100k); excess is reported via `meta.truncated`.
pub async fn run_sql(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SqlRequest>,
) -> Result<Json<SqlResponse>, ApiError> {
    let started = Instant::now();
    let max_rows = req.max_rows;

    let df = state
        .ctx
        .sql(&req.sql)
        .await
        .map_err(|e| ApiError::bad_request(format!("sql parse/plan: {e}")))?;

    // Schema is known up-front from the LogicalPlan, even before execution.
    let schema = df.schema().clone();
    let columns: Vec<ColumnMeta> = schema
        .fields()
        .iter()
        .map(|f| ColumnMeta {
            name: f.name().clone(),
            r#type: format!("{}", f.data_type()),
        })
        .collect();

    // Execute. `collect()` materializes all RecordBatches; we then trim
    // to max_rows. For now we don't push the limit into the plan — keep
    // the implementation simple, revisit if a query produces enough rows
    // to make naive collection painful.
    let batches: Vec<RecordBatch> = df
        .collect()
        .await
        .map_err(|e| ApiError::bad_request(format!("sql execute: {e}")))?;

    let mut row_count = 0usize;
    let mut truncated = false;
    let mut trimmed: Vec<RecordBatch> = Vec::with_capacity(batches.len());
    for batch in batches {
        let need = max_rows.saturating_sub(row_count);
        if need == 0 {
            truncated = true;
            break;
        }
        if batch.num_rows() > need {
            trimmed.push(batch.slice(0, need));
            row_count += need;
            truncated = true;
            break;
        }
        row_count += batch.num_rows();
        trimmed.push(batch);
    }

    let refs: Vec<&RecordBatch> = trimmed.iter().collect();
    let rows = record_batches_to_json_rows(&refs);

    Ok(Json(SqlResponse {
        columns,
        rows,
        meta: Meta {
            row_count,
            elapsed_ms: started.elapsed().as_millis(),
            truncated,
        },
    }))
}

// ---------- ingest ----------

/// `POST /api/v1/ingest`. Validates, writes to WAL+buffer, returns a
/// per-sample accept/reject report. Returns 200 even if some samples were
/// rejected; only request-level failures (backpressure, WAL fsync) are non-2xx.
pub async fn run_ingest(
    State(state): State<Arc<AppState>>,
    Json(req): Json<IngestRequest>,
) -> Result<Json<IngestResponse>, ApiError> {
    if let Err(reason) = state.ingest.check_backpressure() {
        return Err(ApiError {
            status: StatusCode::SERVICE_UNAVAILABLE,
            message: reason,
        });
    }

    let now_ms: i64 = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);

    let mut accepted: Vec<ValidatedSample> = Vec::with_capacity(req.samples.len());
    let mut rejected: Vec<RejectedSample> = Vec::new();
    for (i, raw) in req.samples.into_iter().enumerate() {
        match validate(raw, now_ms) {
            Ok(v) => accepted.push(v),
            Err(e) => rejected.push(RejectedSample {
                index: i,
                reason: e.to_string(),
            }),
        }
    }
    let n_accepted = accepted.len();

    if !accepted.is_empty() {
        let st = state.clone();
        tokio::task::spawn_blocking(move || st.ingest.append_validated(accepted))
            .await
            .map_err(|e| ApiError::internal(format!("ingest task join: {e}")))?
            .map_err(|e| ApiError::internal(format!("ingest: {:#}", e)))?;
    }

    Ok(Json(IngestResponse {
        accepted: n_accepted,
        rejected,
    }))
}

// ---------- remote write (Prometheus 1.0) ----------

/// `POST /api/v1/write`. Snappy-compressed protobuf body per Prometheus
/// Remote-Write 1.0. Empty response body; status code conveys outcome.
pub async fn run_remote_write(
    State(state): State<Arc<AppState>>,
    body: Bytes,
) -> Result<StatusCode, ApiError> {
    if let Err(reason) = state.ingest.check_backpressure() {
        return Err(ApiError {
            status: StatusCode::SERVICE_UNAVAILABLE,
            message: reason,
        });
    }

    let body_vec = body.to_vec();
    let (raws, conv_stats) = tokio::task::spawn_blocking(move || {
        let req = decode_write_request(&body_vec).map_err(|e| ApiError {
            status: StatusCode::BAD_REQUEST,
            message: format!("remote_write decode: {e}"),
        })?;
        Ok::<_, ApiError>(flatten_write_request(req))
    })
    .await
    .map_err(|e| ApiError::internal(format!("remote_write task join: {e}")))??;

    if conv_stats.series_dropped_no_name > 0 {
        tracing::warn!(
            dropped = conv_stats.series_dropped_no_name,
            total = conv_stats.series_total,
            "remote_write: dropped series with no __name__ label"
        );
    }

    let now_ms: i64 = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    let mut accepted: Vec<ValidatedSample> = Vec::with_capacity(raws.len());
    let mut rejected_count = 0usize;
    let mut sample_first_reject: Option<String> = None;
    for raw in raws {
        match validate(raw, now_ms) {
            Ok(v) => accepted.push(v),
            Err(e) => {
                rejected_count += 1;
                if sample_first_reject.is_none() {
                    sample_first_reject = Some(e.to_string());
                }
            }
        }
    }
    if rejected_count > 0 {
        tracing::warn!(
            rejected = rejected_count,
            first_reason = sample_first_reject.as_deref().unwrap_or(""),
            "remote_write: rejected samples"
        );
    }

    if !accepted.is_empty() {
        let st = state.clone();
        tokio::task::spawn_blocking(move || st.ingest.append_validated(accepted))
            .await
            .map_err(|e| ApiError::internal(format!("remote_write ingest task join: {e}")))?
            .map_err(|e| ApiError {
                status: StatusCode::SERVICE_UNAVAILABLE,
                message: format!("remote_write ingest: {:#}", e),
            })?;
    }

    Ok(StatusCode::NO_CONTENT)
}

// ---------- error handling ----------

pub struct ApiError {
    pub status: StatusCode,
    pub message: String,
}

impl ApiError {
    pub fn internal(msg: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: msg.into(),
        }
    }
    pub fn bad_request(msg: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: msg.into(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = json!({ "error": self.message });
        (self.status, Json(body)).into_response()
    }
}
