//! Skaldberg server.
//!
//! Cloud-native Iceberg-backed time-series database. The catalog is
//! selected at startup via `--catalog memory|s3tables`. `memory` is for
//! local dev / tests; `s3tables` talks to AWS S3 Tables.

mod auth;
mod aws_s3_storage;
mod convert;
mod handlers;
mod iceberg_table;
mod ingest;
mod prometheus;
mod state;

use std::env;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use axum::routing::{get, post};
use axum::{middleware, Router};
use metrics_exporter_prometheus::PrometheusBuilder;
use tower::limit::ConcurrencyLimitLayer;
use tower_http::timeout::TimeoutLayer;
use tower_http::trace::TraceLayer;
use tracing::info;
use tracing_subscriber::EnvFilter;

use crate::auth::ApiTokenState;
use crate::iceberg_table::CatalogConfig;
use crate::ingest::spawn_flusher;
use crate::state::AppState;

/// CLI / env-supplied auth token. Wrapped to keep the token out of
/// `Args`'s Debug output (Args is logged at startup).
#[derive(Clone)]
struct ApiToken(String);

impl std::fmt::Debug for ApiToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("<redacted>")
    }
}

#[derive(Debug)]
struct Args {
    wal_dir: PathBuf,
    bind: SocketAddr,
    flush_interval: Duration,
    /// Hard deadline for draining in-flight HTTP requests after a
    /// shutdown signal. When this elapses we stop waiting and run
    /// the final flush anyway — losing the in-flight sample beats
    /// hanging past the orchestrator's SIGKILL window.
    shutdown_timeout: Duration,
    catalog: CatalogConfig,
    /// If set, exported as `AWS_REGION` before the SDK initializes.
    /// `AWS_REGION` / `AWS_DEFAULT_REGION` already in the environment take
    /// precedence over CLI input only when this is `None`.
    aws_region: Option<String>,
    /// When `Some`, /api/v1/* requires `Authorization: Bearer <T>`.
    /// When `None`, auth is disabled.
    api_token: Option<ApiToken>,
    /// Per-request deadline for the /api/v1/* layer. After this
    /// elapses the response is `408 Request Timeout`.
    query_timeout: Duration,
    /// Hard cap on simultaneously-running /api/v1/* requests. New
    /// requests beyond the cap wait their turn (axum's tower
    /// integration backpressures on `poll_ready`).
    max_concurrent_queries: usize,
    /// Memory pool ceiling handed to DataFusion. A query whose
    /// in-flight allocations would exceed this fails with
    /// `ResourcesExhausted` instead of OOM-killing the process.
    query_memory_limit_bytes: usize,
}

impl Args {
    fn parse() -> Result<Self> {
        let mut wal_dir = PathBuf::from("data/wal");
        let mut bind: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let mut flush_interval = Duration::from_secs(300);
        let mut shutdown_timeout = Duration::from_secs(30);
        let mut query_timeout = Duration::from_secs(30);
        let mut max_concurrent_queries: usize = 64;
        let mut query_memory_limit_mb: u64 = 1024;
        let mut catalog_kind = String::from("memory");
        let mut warehouse_uri = String::from("memory:///warehouse");
        let mut table_bucket_arn: Option<String> = None;
        let mut s3tables_endpoint: Option<String> = None;
        let mut aws_region: Option<String> = None;
        let mut api_token_arg: Option<String> = None;

        let mut it = env::args().skip(1);
        while let Some(k) = it.next() {
            let v = it.next().unwrap_or_default();
            match k.as_str() {
                "--wal-dir" => wal_dir = PathBuf::from(v),
                "--bind" => bind = v.parse().expect("--bind addr:port"),
                "--flush-interval-secs" => {
                    let s: u64 = v.parse().expect("--flush-interval-secs <seconds>");
                    flush_interval = Duration::from_secs(s);
                }
                "--shutdown-timeout-secs" => {
                    let s: u64 = v.parse().expect("--shutdown-timeout-secs <seconds>");
                    shutdown_timeout = Duration::from_secs(s);
                }
                "--query-timeout-secs" => {
                    let s: u64 = v.parse().expect("--query-timeout-secs <seconds>");
                    query_timeout = Duration::from_secs(s);
                }
                "--max-concurrent-queries" => {
                    max_concurrent_queries =
                        v.parse().expect("--max-concurrent-queries <count>");
                }
                "--query-memory-limit-mb" => {
                    query_memory_limit_mb =
                        v.parse().expect("--query-memory-limit-mb <megabytes>");
                }
                "--catalog" => catalog_kind = v,
                "--warehouse-uri" => warehouse_uri = v,
                "--table-bucket-arn" => table_bucket_arn = Some(v),
                "--s3tables-endpoint" => s3tables_endpoint = Some(v),
                "--aws-region" => aws_region = Some(v),
                "--api-token" => api_token_arg = Some(v),
                other => eprintln!("warn: unknown arg {}", other),
            }
        }

        let catalog = match catalog_kind.as_str() {
            "memory" => CatalogConfig::Memory { warehouse_uri },
            "s3tables" => {
                let arn = table_bucket_arn.ok_or_else(|| {
                    anyhow!("--catalog s3tables requires --table-bucket-arn")
                })?;
                CatalogConfig::S3Tables {
                    table_bucket_arn: arn,
                    endpoint_url: s3tables_endpoint,
                }
            }
            other => {
                return Err(anyhow!(
                    "--catalog must be 'memory' or 's3tables' (got {other})"
                ));
            }
        };

        // CLI flag wins; otherwise fall back to the env var. Empty
        // strings are treated as unset to avoid confusing
        // `SKALDBERG_API_TOKEN=` (export with no value) with auth on.
        let api_token = api_token_arg
            .or_else(|| env::var("SKALDBERG_API_TOKEN").ok())
            .filter(|s| !s.is_empty())
            .map(ApiToken);

        let query_memory_limit_bytes = (query_memory_limit_mb as usize)
            .checked_mul(1024 * 1024)
            .expect("--query-memory-limit-mb overflow");

        if max_concurrent_queries == 0 {
            return Err(anyhow!("--max-concurrent-queries must be > 0"));
        }
        if query_memory_limit_bytes == 0 {
            return Err(anyhow!("--query-memory-limit-mb must be > 0"));
        }

        Ok(Self {
            wal_dir,
            bind,
            flush_interval,
            shutdown_timeout,
            catalog,
            aws_region,
            api_token,
            query_timeout,
            max_concurrent_queries,
            query_memory_limit_bytes,
        })
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    let args = Args::parse()?;
    if let Some(region) = &args.aws_region {
        // Inject before any AWS SDK initialization. The default credential
        // chain reads AWS_REGION / AWS_DEFAULT_REGION from the environment;
        // pre-existing values take precedence because we only set when the
        // caller explicitly passed --aws-region.
        env::set_var("AWS_REGION", region);
    }
    info!(?args, "starting skaldberg-server");

    std::fs::create_dir_all(&args.wal_dir)?;

    // Install the global Prometheus recorder before anything that
    // emits metrics runs (the flusher and the request handlers both
    // call `metrics::counter!` / `histogram!`). Re-installing would
    // panic, so we do this exactly once here.
    let prom_handle = PrometheusBuilder::new()
        .install_recorder()
        .map_err(|e| anyhow!("install prometheus recorder: {e}"))?;

    let state = Arc::new(
        AppState::open(&args.wal_dir, &args.catalog, args.query_memory_limit_bytes).await?,
    );
    spawn_flusher(state.ingest.clone(), args.flush_interval);

    // /api/v1/* is bearer-auth gated when --api-token / SKALDBERG_API_TOKEN
    // is set. /healthz stays public so external probes don't need the
    // token.
    let api_token_state: ApiTokenState =
        Arc::new(args.api_token.as_ref().map(|t| t.0.clone()));
    if api_token_state.is_some() {
        info!("API auth enabled (bearer token required for /api/v1/*)");
    } else {
        info!("API auth disabled (no token configured)");
    }
    let api_v1 = Router::new()
        .route("/api/v1/sql", post(handlers::run_sql))
        .route("/api/v1/ingest", post(handlers::run_ingest))
        .route("/api/v1/write", post(handlers::run_remote_write))
        // Prometheus HTTP API subset for Grafana's built-in
        // Prometheus datasource. Step 1 covers selectors + the
        // discovery endpoints; functions / aggregations are parsed
        // but unwrapped to the inner selector.
        .route(
            "/api/v1/query",
            get(prometheus::query).post(prometheus::query_post),
        )
        .route(
            "/api/v1/query_range",
            get(prometheus::query_range).post(prometheus::query_range_post),
        )
        .route("/api/v1/labels", get(prometheus::labels))
        .route("/api/v1/label/:name/values", get(prometheus::label_values))
        .route("/api/v1/series", get(prometheus::series))
        .layer(middleware::from_fn_with_state(
            api_token_state,
            auth::require_bearer_token,
        ))
        // Guardrails for the query path. Both layers wrap *only*
        // /api/v1/* — /healthz and /metrics need to stay snappy
        // for orchestrators / Prometheus scrape regardless of API
        // load. Timeout layer goes outermost so that even waiting
        // for a concurrency slot counts toward the deadline.
        .layer(ConcurrencyLimitLayer::new(args.max_concurrent_queries))
        .layer(TimeoutLayer::new(args.query_timeout));
    // /metrics stays outside the auth layer — typical Prometheus
    // scrape comes from a sidecar / kubelet that wouldn't carry the
    // operator's bearer token. Operators who need to keep /metrics
    // private should restrict access at the network layer.
    let metrics_handler = {
        let h = prom_handle.clone();
        move || {
            let h = h.clone();
            async move { h.render() }
        }
    };
    let app = Router::new()
        .route("/healthz", get(handlers::healthz))
        .route("/metrics", get(metrics_handler))
        .merge(api_v1)
        .layer(TraceLayer::new_for_http())
        .with_state(state.clone());

    info!(bind = %args.bind, "listening");
    let listener = tokio::net::TcpListener::bind(args.bind).await?;

    // Graceful shutdown:
    //
    //   1. Wait for SIGTERM / Ctrl-C.
    //   2. Tell axum to stop accepting new connections and let the
    //      in-flight requests drain (bounded by `shutdown_timeout`).
    //      We deliberately do NOT flush here — flushing while ingest
    //      requests are still landing would race with the buffer.
    //   3. Once the listener is fully drained (or we time out
    //      waiting), run a final flush so the WAL replay on the next
    //      start has the smallest possible work to do.
    //
    // If the in-flight drain doesn't finish inside `shutdown_timeout`
    // we move on anyway — the orchestrator's SIGKILL window is the
    // real wall, and a partial-but-flushed buffer is better than an
    // intact-but-never-committed one.
    let server_fut = axum::serve(listener, app).with_graceful_shutdown(shutdown_signal());

    match tokio::time::timeout(args.shutdown_timeout, server_fut).await {
        Ok(Ok(())) => info!("listener drained"),
        Ok(Err(e)) => tracing::error!(error = %e, "server returned error during shutdown"),
        Err(_) => tracing::warn!(
            timeout_secs = args.shutdown_timeout.as_secs(),
            "listener did not drain in time, proceeding to final flush",
        ),
    }

    info!("running final flush before exit");
    if let Err(e) = state.ingest.flush_once().await {
        tracing::error!(error = format!("{:#}", e), "final flush failed");
    }
    info!("server shut down cleanly");
    Ok(())
}

/// Resolves on the first SIGTERM (Unix) or Ctrl-C; never resolves on
/// platforms without SIGTERM (the Ctrl-C arm still works).
async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c().await.ok();
    };
    #[cfg(unix)]
    let term = async {
        use tokio::signal::unix::{signal, SignalKind};
        if let Ok(mut s) = signal(SignalKind::terminate()) {
            s.recv().await;
        }
    };
    #[cfg(not(unix))]
    let term = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => info!("ctrl-c received"),
        _ = term => info!("SIGTERM received"),
    }
}
