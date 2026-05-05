//! In-memory accumulator between WAL append and Parquet flush.
//!
//! Shape: `BTreeMap<NaiveDate, BTreeMap<series_id, Vec<(ts_us, value)>>>`.
//! - Outer key by day so a flush emits one Parquet file per day partition.
//! - Inner key by `series_id` so series stay grouped (helpful for row-group
//!   pruning even before a sort step).
//! - Inner Vec is *not* sorted on insert; the flusher sorts before writing.
//!
//! New-series tracking: the buffer keeps a `HashSet<i64>` of series ids it
//! has been told about (either seeded at startup from the on-disk catalog,
//! or added when a previous take() snapshot was committed). The first time
//! a never-seen `series_id` arrives, the buffer remembers `(metric, labels)`
//! in `new_series` so the flusher can emit a catalog row for it.
//!
//! `take()` is the swap-out: ownership of the day map and new-series map
//! transfers to the caller, the buffer is reset, and any ids that were in
//! `new_series` move into `known_series` so future inserts won't re-emit
//! them.

use std::collections::{BTreeMap, HashSet};

use chrono::{DateTime, NaiveDate};

use crate::ingest::types::ValidatedSample;

/// Per-sample memory cost estimate. Three i64-sized fields plus margin
/// for BTree node overhead. Used only for the size-based flush trigger;
/// not a tight upper bound.
const BYTES_PER_SAMPLE: usize = 32;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewSeriesEntry {
    pub series_id: i64,
    pub metric: String,
    pub labels: BTreeMap<String, String>,
}

#[derive(Debug)]
pub struct Buffer {
    by_day: BTreeMap<NaiveDate, BTreeMap<i64, Vec<(i64, f64)>>>,
    new_series: BTreeMap<i64, NewSeriesEntry>,
    known_series: HashSet<i64>,
    bytes_estimate: usize,
    sample_count: usize,
    max_record_seq: u64,
}

/// Result of `Buffer::take()`. Owned data, safe to hand to a flusher
/// running on a separate task without holding any buffer locks.
#[derive(Debug)]
pub struct Snapshot {
    pub by_day: BTreeMap<NaiveDate, BTreeMap<i64, Vec<(i64, f64)>>>,
    pub new_series: BTreeMap<i64, NewSeriesEntry>,
    pub bytes_estimate: usize,
    pub sample_count: usize,
    /// Largest WAL `record_seq` represented in this snapshot. After the
    /// flusher persists this snapshot, it can call
    /// `WalWriter::truncate_through(max_record_seq)` to reclaim disk.
    pub max_record_seq: u64,
}

impl Default for Buffer {
    fn default() -> Self {
        Self::new()
    }
}

impl Buffer {
    pub fn new() -> Self {
        Self::with_known_series(std::iter::empty())
    }

    /// Construct with a pre-populated `known_series` set. Used at startup
    /// after reading the existing on-disk series catalog.
    pub fn with_known_series<I: IntoIterator<Item = i64>>(seed: I) -> Self {
        Self {
            by_day: BTreeMap::new(),
            new_series: BTreeMap::new(),
            known_series: seed.into_iter().collect(),
            bytes_estimate: 0,
            sample_count: 0,
            max_record_seq: 0,
        }
    }

    /// Insert a batch of samples that originated from a single WAL record.
    /// `record_seq` becomes a candidate for `max_record_seq`.
    pub fn insert_batch(&mut self, record_seq: u64, samples: Vec<ValidatedSample>) {
        if record_seq > self.max_record_seq {
            self.max_record_seq = record_seq;
        }
        for s in samples {
            self.insert_one(s);
        }
    }

    fn insert_one(&mut self, s: ValidatedSample) {
        let day = day_of_us(s.ts_us);
        let series_id = s.series_id;
        let ts_us = s.ts_us;
        let value = s.value;

        if !self.known_series.contains(&series_id)
            && !self.new_series.contains_key(&series_id)
        {
            self.new_series.insert(
                series_id,
                NewSeriesEntry {
                    series_id,
                    metric: s.metric,
                    labels: s.labels,
                },
            );
        }
        // (If the series was already known, s.metric and s.labels are
        // unused and dropped at end of scope.)

        let series_map = self.by_day.entry(day).or_default();
        series_map.entry(series_id).or_default().push((ts_us, value));
        self.bytes_estimate += BYTES_PER_SAMPLE;
        self.sample_count += 1;
    }

    pub fn bytes_estimate(&self) -> usize {
        self.bytes_estimate
    }

    pub fn sample_count(&self) -> usize {
        self.sample_count
    }

    pub fn is_empty(&self) -> bool {
        self.sample_count == 0
    }

    pub fn max_record_seq(&self) -> u64 {
        self.max_record_seq
    }

    /// Atomically take ownership of the buffer's contents and reset.
    /// Series ids in the snapshot's `new_series` are promoted into
    /// `known_series` so they aren't re-emitted on subsequent inserts
    /// (the flusher is responsible for actually persisting them).
    pub fn take(&mut self) -> Snapshot {
        let by_day = std::mem::take(&mut self.by_day);
        let new_series = std::mem::take(&mut self.new_series);
        let bytes_estimate = self.bytes_estimate;
        let sample_count = self.sample_count;
        let max_record_seq = self.max_record_seq;

        for &id in new_series.keys() {
            self.known_series.insert(id);
        }

        self.bytes_estimate = 0;
        self.sample_count = 0;
        self.max_record_seq = 0;

        Snapshot {
            by_day,
            new_series,
            bytes_estimate,
            sample_count,
            max_record_seq,
        }
    }
}

fn day_of_us(ts_us: i64) -> NaiveDate {
    let s = ts_us.div_euclid(1_000_000);
    let ns = (ts_us.rem_euclid(1_000_000) * 1_000) as u32;
    DateTime::from_timestamp(s, ns)
        .map(|dt| dt.date_naive())
        .unwrap_or_else(|| NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(series_id: i64, metric: &str, labels: &[(&str, &str)], ts_us: i64, v: f64) -> ValidatedSample {
        let mut bm = BTreeMap::new();
        for (k, v) in labels {
            bm.insert((*k).to_string(), (*v).to_string());
        }
        ValidatedSample {
            series_id,
            metric: metric.to_string(),
            labels: bm,
            ts_us,
            value: v,
        }
    }

    fn ts(date: &str, time: &str) -> i64 {
        // Helper: produce a microsecond timestamp from "YYYY-MM-DD" + "HH:MM:SS".
        let s = format!("{}T{}Z", date, time);
        let dt = chrono::DateTime::parse_from_rfc3339(&s).unwrap();
        dt.timestamp() * 1_000_000
    }

    #[test]
    fn new_buffer_is_empty() {
        let b = Buffer::new();
        assert!(b.is_empty());
        assert_eq!(b.sample_count(), 0);
        assert_eq!(b.bytes_estimate(), 0);
        assert_eq!(b.max_record_seq(), 0);
    }

    #[test]
    fn insert_one_sample() {
        let mut b = Buffer::new();
        let s = sample(1, "m", &[], ts("2026-05-04", "12:00:00"), 1.0);
        b.insert_batch(7, vec![s]);
        assert_eq!(b.sample_count(), 1);
        assert_eq!(b.max_record_seq(), 7);
        assert_eq!(b.bytes_estimate(), BYTES_PER_SAMPLE);
        assert!(!b.is_empty());
    }

    #[test]
    fn samples_partition_by_day() {
        let mut b = Buffer::new();
        b.insert_batch(1, vec![
            sample(1, "m", &[], ts("2026-05-03", "23:30:00"), 1.0),
            sample(1, "m", &[], ts("2026-05-04", "00:30:00"), 2.0),
            sample(1, "m", &[], ts("2026-05-05", "12:00:00"), 3.0),
        ]);
        let snap = b.take();
        assert_eq!(snap.by_day.len(), 3);
        let dates: Vec<_> = snap.by_day.keys().map(|d| d.to_string()).collect();
        assert_eq!(dates, vec!["2026-05-03", "2026-05-04", "2026-05-05"]);
    }

    #[test]
    fn samples_for_same_series_grouped() {
        let mut b = Buffer::new();
        b.insert_batch(1, vec![
            sample(42, "m", &[], ts("2026-05-04", "12:00:00"), 1.0),
            sample(42, "m", &[], ts("2026-05-04", "12:00:30"), 2.0),
            sample(99, "m", &[], ts("2026-05-04", "12:00:00"), 9.0),
        ]);
        let snap = b.take();
        let day_map = snap.by_day.values().next().unwrap();
        assert_eq!(day_map.len(), 2);   // two series ids
        assert_eq!(day_map[&42].len(), 2);
        assert_eq!(day_map[&99].len(), 1);
    }

    #[test]
    fn first_seen_series_goes_into_new_series() {
        let mut b = Buffer::new();
        b.insert_batch(1, vec![
            sample(42, "metric_a", &[("job", "api")], ts("2026-05-04", "12:00:00"), 1.0),
        ]);
        let snap = b.take();
        assert_eq!(snap.new_series.len(), 1);
        let entry = &snap.new_series[&42];
        assert_eq!(entry.series_id, 42);
        assert_eq!(entry.metric, "metric_a");
        assert_eq!(entry.labels.get("job").map(String::as_str), Some("api"));
    }

    #[test]
    fn known_series_skip_new_series() {
        // Seed buffer with id 42 already on disk.
        let mut b = Buffer::with_known_series([42]);
        b.insert_batch(1, vec![
            sample(42, "metric_a", &[("job", "api")], ts("2026-05-04", "12:00:00"), 1.0),
            sample(99, "metric_b", &[], ts("2026-05-04", "12:00:00"), 1.0),
        ]);
        let snap = b.take();
        // 42 was seeded → not in new_series. 99 is fresh → present.
        assert!(!snap.new_series.contains_key(&42));
        assert!(snap.new_series.contains_key(&99));
        assert_eq!(snap.new_series.len(), 1);
    }

    #[test]
    fn duplicate_first_sight_within_batch_emits_once() {
        let mut b = Buffer::new();
        b.insert_batch(1, vec![
            sample(7, "m", &[("k", "v")], ts("2026-05-04", "12:00:00"), 1.0),
            sample(7, "m", &[("k", "v")], ts("2026-05-04", "12:00:30"), 2.0),
            sample(7, "m", &[("k", "v")], ts("2026-05-04", "12:01:00"), 3.0),
        ]);
        let snap = b.take();
        assert_eq!(snap.new_series.len(), 1);
        assert_eq!(snap.by_day.values().next().unwrap()[&7].len(), 3);
    }

    #[test]
    fn after_take_known_series_includes_previously_new() {
        let mut b = Buffer::new();
        b.insert_batch(1, vec![
            sample(7, "m", &[], ts("2026-05-04", "12:00:00"), 1.0),
        ]);
        let snap1 = b.take();
        assert!(snap1.new_series.contains_key(&7));

        // Insert id 7 again — should NOT be in new_series this time.
        b.insert_batch(2, vec![
            sample(7, "m", &[], ts("2026-05-04", "12:00:30"), 2.0),
        ]);
        let snap2 = b.take();
        assert!(snap2.new_series.is_empty());
        assert_eq!(snap2.by_day.values().next().unwrap()[&7].len(), 1);
    }

    #[test]
    fn take_resets_counters() {
        let mut b = Buffer::new();
        b.insert_batch(5, vec![
            sample(1, "m", &[], ts("2026-05-04", "12:00:00"), 1.0),
        ]);
        let _ = b.take();
        assert!(b.is_empty());
        assert_eq!(b.sample_count(), 0);
        assert_eq!(b.bytes_estimate(), 0);
        assert_eq!(b.max_record_seq(), 0);
    }

    #[test]
    fn max_record_seq_tracks_highest_in_window() {
        let mut b = Buffer::new();
        b.insert_batch(3, vec![sample(1, "m", &[], ts("2026-05-04", "12:00:00"), 1.0)]);
        b.insert_batch(7, vec![sample(1, "m", &[], ts("2026-05-04", "12:00:30"), 2.0)]);
        b.insert_batch(5, vec![sample(1, "m", &[], ts("2026-05-04", "12:01:00"), 3.0)]);  // out-of-order seq
        assert_eq!(b.max_record_seq(), 7);
    }

    #[test]
    fn bytes_estimate_scales_with_count() {
        let mut b = Buffer::new();
        for i in 0..100 {
            b.insert_batch(1, vec![sample(1, "m", &[], ts("2026-05-04", "12:00:00") + i, 1.0)]);
        }
        assert_eq!(b.sample_count(), 100);
        assert_eq!(b.bytes_estimate(), 100 * BYTES_PER_SAMPLE);
    }

    #[test]
    fn empty_take_is_safe() {
        let mut b = Buffer::new();
        let snap = b.take();
        assert!(snap.by_day.is_empty());
        assert!(snap.new_series.is_empty());
        assert_eq!(snap.sample_count, 0);
        assert_eq!(snap.max_record_seq, 0);
    }

    #[test]
    fn day_boundary_at_utc_midnight() {
        let mut b = Buffer::new();
        // 23:59:59 May 3 → day "2026-05-03"
        // 00:00:00 May 4 → day "2026-05-04"
        b.insert_batch(1, vec![
            sample(1, "m", &[], ts("2026-05-03", "23:59:59"), 1.0),
            sample(1, "m", &[], ts("2026-05-04", "00:00:00"), 2.0),
        ]);
        let snap = b.take();
        let dates: Vec<_> = snap.by_day.keys().map(|d| d.to_string()).collect();
        assert_eq!(dates, vec!["2026-05-03", "2026-05-04"]);
    }
}
