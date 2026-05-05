//! Skaldberg server (Phase 4).
//!
//! Cloud-native Iceberg-backed time-series database. Phase 4 runs against
//! an in-process MemoryCatalog; Phase 5 swaps in `S3TablesCatalog`.

mod convert;
mod handlers;
mod iceberg_table;
mod ingest;
mod state;

use std::env;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use axum::routing::{get, post};
use axum::Router;
use tower_http::trace::TraceLayer;
use tracing::info;
use tracing_subscriber::EnvFilter;

use crate::ingest::spawn_flusher;
use crate::state::AppState;

#[derive(Debug)]
struct Args {
    /// Filesystem path holding the WAL only. Iceberg data lives in the
    /// in-memory warehouse for Phase 4; Phase 5 will replace this with an
    /// S3 URI for the S3TablesCatalog warehouse.
    wal_dir: PathBuf,
    warehouse_uri: String,
    bind: SocketAddr,
    flush_interval: Duration,
}

impl Args {
    fn parse() -> Self {
        let mut a = Args {
            wal_dir: PathBuf::from("data/wal"),
            warehouse_uri: "memory:///warehouse".to_string(),
            bind: "127.0.0.1:8080".parse().unwrap(),
            flush_interval: Duration::from_secs(300),
        };
        let mut it = env::args().skip(1);
        while let Some(k) = it.next() {
            let v = it.next().unwrap_or_default();
            match k.as_str() {
                "--wal-dir" => a.wal_dir = PathBuf::from(v),
                "--warehouse-uri" => a.warehouse_uri = v,
                "--bind" => a.bind = v.parse().expect("--bind addr:port"),
                "--flush-interval-secs" => {
                    let s: u64 = v.parse().expect("--flush-interval-secs <seconds>");
                    a.flush_interval = Duration::from_secs(s);
                }
                other => eprintln!("warn: unknown arg {}", other),
            }
        }
        a
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    let args = Args::parse();
    info!(?args, "starting skaldberg-server");

    std::fs::create_dir_all(&args.wal_dir)?;

    let state = Arc::new(AppState::open(&args.wal_dir, &args.warehouse_uri).await?);
    spawn_flusher(state.ingest.clone(), args.flush_interval);

    let app = Router::new()
        .route("/healthz", get(handlers::healthz))
        .route("/api/v1/sql", post(handlers::run_sql))
        .route("/api/v1/ingest", post(handlers::run_ingest))
        .route("/api/v1/write", post(handlers::run_remote_write))
        .layer(TraceLayer::new_for_http())
        .with_state(state.clone());

    info!(bind = %args.bind, "listening");
    let listener = tokio::net::TcpListener::bind(args.bind).await?;

    // Graceful shutdown: on SIGTERM / Ctrl-C, stop accepting new requests
    // and run one final flush so the WAL replay on the next start has the
    // smallest possible work to do.
    let shutdown_state = state.clone();
    let server_fut = axum::serve(listener, app).with_graceful_shutdown(async move {
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
        info!("running final flush before shutdown");
        if let Err(e) = shutdown_state.ingest.flush_once().await {
            tracing::error!(error = format!("{:#}", e), "final flush failed");
        }
    });

    server_fut.await?;
    info!("server shut down cleanly");
    Ok(())
}
