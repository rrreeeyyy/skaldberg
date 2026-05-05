//! Service layer for the write path.
//!
//! `IngestState` is shared between the HTTP handler and the background
//! flush task. It owns:
//!   - a `WalWriter` (mutex'd, single appender)
//!   - a `Buffer` (mutex'd, single accumulator)
//!   - the `IcebergTables` handle so the flusher can write
//!   - a `Notify` so the handler can wake the flusher early on size threshold
//!
//! Handler holds each lock briefly:
//!   1. `check_backpressure` — read buffer.bytes_estimate.
//!   2. WAL lock → append → fdatasync → release.
//!   3. Buffer lock → insert_batch → release.
//!   4. Maybe `notify_one`.
//!
//! Flusher:
//!   1. Wait on timer or `Notify`.
//!   2. Buffer lock → `take()` snapshot → release.
//!   3. `flush()` against Iceberg (no locks).
//!   4. WAL lock → `truncate_through(max_record_seq)` → release.

use std::path::Path;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use metrics::{counter, histogram};
use tokio::sync::Notify;
use tracing::{debug, error, info, warn};

use crate::iceberg_table::IcebergTables;
use crate::ingest::buffer::Buffer;
use crate::ingest::flush::flush as do_flush;
use crate::ingest::types::ValidatedSample;
use crate::ingest::wal::WalWriter;

/// Buffer-size threshold above which the handler signals the flusher
/// to wake up early.
pub const FLUSH_SIZE_BYTES: usize = 64 * 1024 * 1024;

/// Hard cap. At/above this, return 503 to keep memory bounded.
pub const BACKPRESSURE_BYTES: usize = 256 * 1024 * 1024;

pub struct IngestState {
    pub tables: Arc<IcebergTables>,
    pub wal: Mutex<WalWriter>,
    pub buffer: Mutex<Buffer>,
    pub notify: Notify,
}

impl IngestState {
    pub fn open(
        wal_dir: &Path,
        tables: Arc<IcebergTables>,
        known_series: Vec<i64>,
    ) -> Result<Self> {
        let wal = WalWriter::open(wal_dir).context("opening wal")?;
        let buffer = Buffer::with_known_series(known_series);

        Ok(Self {
            tables,
            wal: Mutex::new(wal),
            buffer: Mutex::new(buffer),
            notify: Notify::new(),
        })
    }

    /// Append a validated batch: WAL persist → buffer insert → maybe notify.
    /// Returns the assigned WAL record_seq.
    pub fn append_validated(&self, samples: Vec<ValidatedSample>) -> Result<u64> {
        let payload = serde_json::to_vec(&samples).context("serializing wal payload")?;

        let record_seq = {
            let mut w = self.wal.lock().expect("wal mutex poisoned");
            w.append(&payload).context("wal append")?
        };

        {
            let mut b = self.buffer.lock().expect("buffer mutex poisoned");
            b.insert_batch(record_seq, samples);
            if b.bytes_estimate() >= FLUSH_SIZE_BYTES {
                self.notify.notify_one();
            }
        }
        Ok(record_seq)
    }

    pub fn check_backpressure(&self) -> Result<(), String> {
        let n = self
            .buffer
            .lock()
            .expect("buffer mutex poisoned")
            .bytes_estimate();
        if n >= BACKPRESSURE_BYTES {
            Err(format!(
                "buffer at {} bytes (cap {}); flusher behind, retry shortly",
                n, BACKPRESSURE_BYTES
            ))
        } else {
            Ok(())
        }
    }

    /// Flush snapshot to Iceberg. Returns `true` if data was written.
    pub async fn flush_once(&self) -> Result<bool> {
        let snap = {
            let mut b = self.buffer.lock().expect("buffer mutex poisoned");
            if b.is_empty() {
                return Ok(false);
            }
            b.take()
        };

        let max_seq = snap.max_record_seq;
        let started = Instant::now();
        let res = do_flush(&self.tables, snap).await;
        let elapsed = started.elapsed();
        histogram!("skaldberg_flush_duration_seconds").record(elapsed.as_secs_f64());

        let res = match res {
            Ok(r) => {
                counter!("skaldberg_flush_total", "result" => "ok").increment(1);
                r
            }
            Err(e) => {
                counter!("skaldberg_flush_total", "result" => "err").increment(1);
                return Err(e).context("flush");
            }
        };
        info!(
            samples_files = res.samples_files,
            samples_rows = res.samples_rows,
            series_rows = res.series_rows,
            max_record_seq = res.max_record_seq,
            "flush complete"
        );

        if max_seq > 0 {
            let mut w = self.wal.lock().expect("wal mutex poisoned");
            if let Err(e) = w.truncate_through(max_seq) {
                warn!(error = %e, "wal truncate_through failed (non-fatal)");
            }
        }
        Ok(true)
    }
}

/// Spawn the background flush task. Wakes on either timer or `notify`.
pub fn spawn_flusher(state: Arc<IngestState>, interval: Duration) {
    tokio::spawn(async move {
        info!(interval_secs = interval.as_secs(), "flush task started");
        loop {
            tokio::select! {
                _ = tokio::time::sleep(interval) => {
                    debug!("flush tick");
                }
                _ = state.notify.notified() => {
                    debug!("flush wake on notify");
                }
            }
            // flush_once is async (Iceberg I/O); just await it directly.
            match state.flush_once().await {
                Ok(_) => {}
                Err(e) => error!(error = format!("{:#}", e), "flush failed"),
            }
        }
    });
}
