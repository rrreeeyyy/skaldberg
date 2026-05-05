//! Static bearer-token auth for /api/v1/*.
//!
//! When `--api-token <T>` (or `SKALDBERG_API_TOKEN=T`) is set, every
//! request below the layer must carry `Authorization: Bearer <T>` or
//! it gets a 401. When the token is unset (default), the layer is a
//! no-op — useful for local dev / unit tests / single-tenant private
//! deployments where the network boundary is the trust boundary.
//!
//! Intentionally simple: a single shared token, equality-checked.
//! Not OAuth2, not per-user. We expect callers behind this layer to
//! be programs (Prometheus remote_write clients, ingest jobs, the
//! Skaldberg CLI), not humans, and we expect the operator to rotate
//! the token by restarting the process. If multi-tenant or scoped
//! auth becomes a requirement later, this module is the place to
//! evolve.

use std::sync::Arc;

use axum::extract::{Request, State};
use axum::http::{header, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

/// Cloneable middleware state. `None` => auth disabled (pass-through).
pub type ApiTokenState = Arc<Option<String>>;

pub async fn require_bearer_token(
    State(expected): State<ApiTokenState>,
    req: Request,
    next: Next,
) -> Response {
    let Some(expected_token) = expected.as_deref() else {
        return next.run(req).await;
    };
    let provided = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|h| h.strip_prefix("Bearer "));
    match provided {
        Some(token) if token == expected_token => next.run(req).await,
        _ => StatusCode::UNAUTHORIZED.into_response(),
    }
}
