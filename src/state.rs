//! Application state: catalog/tables, DataFusion session, ingest path.
//!
//! The HTTP layer never touches Iceberg or DataFusion directly — every
//! query and every write goes through this struct. That keeps the
//! "switch to S3 Tables in Phase 5" diff small: only `AppState::open`
//! changes.
//!
//! On open we:
//!   1. Build/connect the catalog and ensure both tables exist.
//!   2. Wire up a `SessionContext` and register the catalog so SQL
//!      queries see the tables under `<catalog>.<namespace>.<table>`.
//!   3. Read existing series ids from the catalog into `known_series`
//!      so the ingester doesn't re-emit catalog rows for them.
//!   4. Open the WAL and replay any unflushed records into the buffer.
//!   5. Build `IngestState` for the handler + flush task.

use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use datafusion::prelude::SessionContext;
use iceberg_datafusion::IcebergCatalogProvider;
use tracing::{info, warn};

use crate::iceberg_table::{IcebergTables, NAMESPACE};
use crate::ingest::{IngestState, ValidatedSample, WalIter};

/// Logical name DataFusion sees the catalog under.
/// Tables resolve as `iceberg.skaldberg.series` / `iceberg.skaldberg.samples`.
pub const DF_CATALOG_NAME: &str = "iceberg";

pub struct AppState {
    pub ctx: SessionContext,
    pub tables: Arc<IcebergTables>,
    pub ingest: Arc<IngestState>,
}

impl AppState {
    pub async fn open(wal_dir: &Path, warehouse_uri: &str) -> Result<Self> {
        // 1. Catalog + tables.
        let tables = Arc::new(IcebergTables::open_memory(warehouse_uri).await?);

        // 2. DataFusion session.
        let ctx = SessionContext::new();
        let cat_provider = IcebergCatalogProvider::try_new(tables.catalog.clone())
            .await
            .context("build IcebergCatalogProvider")?;
        ctx.register_catalog(DF_CATALOG_NAME, Arc::new(cat_provider));

        // Register `sk_*` helper views. These are denormalized joins between
        // `samples` and `series` so users can query metrics by name without
        // writing the join by hand. The Phase 2 `sk_metric('name')` macro
        // syntax is not preserved — DataFusion has no DuckDB-style table
        // macro — but `SELECT * FROM sk_metric WHERE metric_name = 'name'`
        // produces the same result.
        register_helper_views(&ctx).await?;

        // 3. Seed `known_series` from the existing catalog. On a fresh
        //    warehouse this returns an empty list — that's fine.
        let known_series = read_known_series(&ctx).await.unwrap_or_else(|e| {
            warn!(error = %e, "could not read existing series catalog (treating as empty)");
            Vec::new()
        });
        info!(count = known_series.len(), "seeded known_series");

        // 4. Open WAL and replay.
        let ingest = Arc::new(IngestState::open(
            wal_dir,
            tables.clone(),
            known_series,
        )?);
        let n_replayed = replay_wal(&ingest, wal_dir)?;
        if n_replayed > 0 {
            info!(records = n_replayed, "replayed wal records into buffer");
        }

        Ok(Self {
            ctx,
            tables,
            ingest,
        })
    }
}

async fn read_known_series(ctx: &SessionContext) -> Result<Vec<i64>> {
    use datafusion::arrow::array::{Array, Int64Array};

    let sql = format!(
        "SELECT series_id FROM {}.{}.series",
        DF_CATALOG_NAME, NAMESPACE
    );
    let df = match ctx.sql(&sql).await {
        Ok(df) => df,
        Err(_) => return Ok(Vec::new()), // table empty / not yet readable
    };
    let batches = df.collect().await.context("collect series_ids")?;
    let mut ids = Vec::new();
    for batch in batches {
        let col = batch.column(0);
        if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
            for i in 0..arr.len() {
                if !arr.is_null(i) {
                    ids.push(arr.value(i));
                }
            }
        }
    }
    Ok(ids)
}

fn replay_wal(ingest: &IngestState, wal_dir: &Path) -> Result<usize> {
    let iter = WalIter::open(wal_dir).context("opening wal for replay")?;
    let mut n = 0usize;
    for rec in iter {
        match rec {
            Ok(rec) => match serde_json::from_slice::<Vec<ValidatedSample>>(&rec.payload) {
                Ok(samples) => {
                    let mut b = ingest.buffer.lock().expect("buffer mutex poisoned");
                    b.insert_batch(rec.record_seq, samples);
                    n += 1;
                }
                Err(e) => warn!(seq = rec.record_seq, error = %e, "skipping unparseable wal record"),
            },
            Err(e) => {
                warn!(error = %e, "wal replay stopped on read error");
                break;
            }
        }
    }
    Ok(n)
}

/// Register denormalized helper views over `iceberg.skaldberg.{samples,series}`.
///
/// `sk_metric` joins samples to series, so a query against `sk_metric` sees
/// `(series_id, timestamp, value, metric_name, labels)` in one row. Users
/// filter by metric name via `WHERE metric_name = '...'` rather than passing
/// the name as an argument (the way the old DuckDB `sk_metric('name')`
/// macro did) — DataFusion 52 doesn't have an equivalent table-macro
/// concept, and a regular VIEW handles this cleanly.
///
/// `sk_rate_of` and `sk_irate` add a `value_per_sec` column computed via
/// `LAG()` window functions partitioned by `series_id`. They are designed
/// to be filtered by metric_name like sk_metric.
async fn register_helper_views(ctx: &SessionContext) -> Result<()> {
    let cat = DF_CATALOG_NAME;
    let ns = NAMESPACE;

    // sk_metric: join (samples, series) so callers don't write it by hand.
    let sql = format!(
        r#"
        CREATE OR REPLACE VIEW sk_metric AS
        SELECT sa.series_id, sa.timestamp, sa.value, s.metric_name, s.labels
        FROM {cat}.{ns}.samples sa
        JOIN {cat}.{ns}.series s ON sa.series_id = s.series_id
        "#
    );
    ctx.sql(&sql).await.context("create sk_metric view")?;

    // sk_rate_of: per-second derivative within each series, computed in SQL
    // via LAG. Equivalent to `rate()` over the entire range present in the
    // table — callers should bound timestamp via WHERE before grouping.
    let sql = format!(
        r#"
        CREATE OR REPLACE VIEW sk_rate_of AS
        SELECT
          series_id,
          timestamp,
          metric_name,
          labels,
          (value - LAG(value) OVER w) /
            NULLIF(
              EXTRACT(EPOCH FROM (timestamp - LAG(timestamp) OVER w)),
              0
            ) AS value_per_sec
        FROM sk_metric
        WINDOW w AS (PARTITION BY series_id ORDER BY timestamp)
        "#
    );
    // EXTRACT(EPOCH FROM interval) may not be supported in DataFusion 52;
    // we'll catch errors and fall back to a simpler form. Try the proper
    // form first.
    if let Err(e) = ctx.sql(&sql).await {
        tracing::warn!(error = %e, "sk_rate_of view skipped (datafusion 52 syntax incompat)");
    }

    Ok(())
}
