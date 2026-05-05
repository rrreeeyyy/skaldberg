//! Persist a `Snapshot` from the in-memory `Buffer` to Iceberg tables.
//!
//! Two writes per flush, each its own commit:
//!
//! 1. **samples** — partitioned by `days(timestamp)`. Uses `FanoutWriter`
//!    so a snapshot containing multiple days writes one Parquet file per
//!    day in a single pass.
//!
//! 2. **series** — only if there are new series to register. Unpartitioned;
//!    a single `DataFileWriter` is enough.
//!
//! Both commits are independent: if `samples` commit succeeds and `series`
//! fails, queries see samples whose `series_id` doesn't yet appear in the
//! catalog. The snapshot is taken atomically from the buffer before either
//! write, so re-running the failed half from a retry doesn't double-write
//! anything that already landed.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::{
    ArrayRef, Float64Array, Int64Array, MapArray, MapBuilder, MapFieldNames, StringArray,
    StringBuilder, StructArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::NaiveDate;
use iceberg::spec::{DataFileFormat, Literal, Struct};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::partitioning::fanout_writer::FanoutWriter;
use iceberg::writer::partitioning::PartitioningWriter;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::Catalog;
use iceberg::spec::PartitionKey;
use parquet::file::properties::WriterProperties;
use tracing::info;

use crate::iceberg_table::{
    field_ids, samples_iceberg_schema, series_iceberg_schema, IcebergTables,
};
use crate::ingest::buffer::Snapshot;

/// Outcome of a single flush call.
#[derive(Debug, Clone, Default)]
pub struct FlushResult {
    pub samples_files: usize,
    pub samples_rows: usize,
    pub series_rows: usize,
    pub max_record_seq: u64,
}

/// Flush a snapshot to both tables. Returns once both commits land or
/// errors out without attempting `series` if `samples` fails.
pub async fn flush(tables: &IcebergTables, snap: Snapshot) -> Result<FlushResult> {
    let max_record_seq = snap.max_record_seq;
    let samples_rows = snap.sample_count;
    let series_rows = snap.new_series.len();

    let mut samples_files = 0usize;
    if !snap.by_day.is_empty() {
        samples_files = flush_samples(tables, &snap).await.context("flush samples")?;
    }
    if !snap.new_series.is_empty() {
        flush_series(tables, &snap).await.context("flush series")?;
    }

    info!(
        samples_files,
        samples_rows, series_rows, max_record_seq, "flush complete"
    );

    Ok(FlushResult {
        samples_files,
        samples_rows,
        series_rows,
        max_record_seq,
    })
}

async fn flush_samples(tables: &IcebergTables, snap: &Snapshot) -> Result<usize> {
    let table = tables
        .catalog
        .load_table(&tables.samples)
        .await
        .context("reload samples")?;
    let metadata = table.metadata();
    // default_partition_spec returns &Arc<PartitionSpec>; clone the inner
    // value (not the Arc) since PartitionKey::new takes ownership of a
    // PartitionSpec.
    let pspec_ref = metadata.default_partition_spec();
    let pspec: iceberg::spec::PartitionSpec = (**pspec_ref).clone();

    let iceberg_schema = samples_iceberg_schema()?;
    let arrow_schema = samples_arrow_schema();

    let parquet_props = WriterProperties::default();
    let parquet_builder = ParquetWriterBuilder::new(parquet_props, Arc::new(iceberg_schema.clone()));
    let location_gen =
        DefaultLocationGenerator::new(metadata.clone()).context("samples location gen")?;
    // The default file-name counter resets to 0 every time we construct
    // a new generator. Multiple flushes within the same process would
    // therefore produce colliding paths (`part-00000.parquet`), and
    // FastAppend rejects re-adding an already-tracked file. Stamp each
    // flush with a UUID so paths are unique across flushes.
    let prefix = format!("part-{}", uuid::Uuid::new_v4());
    let name_gen = DefaultFileNameGenerator::new(prefix, None, DataFileFormat::Parquet);
    let rolling = RollingFileWriterBuilder::new_with_default_file_size(
        parquet_builder,
        table.file_io().clone(),
        location_gen,
        name_gen,
    );
    let data_file_builder = DataFileWriterBuilder::new(rolling);
    let mut writer = FanoutWriter::new(data_file_builder);

    let iceberg_schema_arc = Arc::new(iceberg_schema);
    for (day, by_series) in &snap.by_day {
        let pk = PartitionKey::new(
            pspec.clone(),
            iceberg_schema_arc.clone(),
            Struct::from_iter([Some(Literal::int(epoch_days(*day)))]),
        );
        let batch = build_samples_batch(arrow_schema.clone(), by_series)
            .context("build samples batch")?;
        writer
            .write(pk, batch)
            .await
            .context("fanout writer.write")?;
    }
    let data_files = writer.close().await.context("fanout writer.close")?;
    let n = data_files.len();

    let tx = Transaction::new(&table);
    let action = tx.fast_append().add_data_files(data_files);
    let tx_with_action = action
        .apply(Transaction::new(&table))
        .context("apply samples action")?;
    tx_with_action
        .commit(&*tables.catalog)
        .await
        .context("samples commit")?;
    Ok(n)
}

async fn flush_series(tables: &IcebergTables, snap: &Snapshot) -> Result<()> {
    let table = tables
        .catalog
        .load_table(&tables.series)
        .await
        .context("reload series")?;
    let metadata = table.metadata();

    let iceberg_schema = series_iceberg_schema()?;
    let arrow_schema = series_arrow_schema();

    let parquet_props = WriterProperties::default();
    let parquet_builder = ParquetWriterBuilder::new(parquet_props, Arc::new(iceberg_schema.clone()));
    let location_gen =
        DefaultLocationGenerator::new(metadata.clone()).context("series location gen")?;
    let prefix = format!("series-part-{}", uuid::Uuid::new_v4());
    let name_gen = DefaultFileNameGenerator::new(prefix, None, DataFileFormat::Parquet);
    let rolling = RollingFileWriterBuilder::new_with_default_file_size(
        parquet_builder,
        table.file_io().clone(),
        location_gen,
        name_gen,
    );
    let data_file_builder = DataFileWriterBuilder::new(rolling);
    let mut writer = data_file_builder
        .build(None)
        .await
        .context("series writer build")?;

    let batch = build_series_batch(arrow_schema, snap).context("build series batch")?;
    writer.write(batch).await.context("series writer.write")?;
    let data_files = writer.close().await.context("series writer.close")?;

    let tx = Transaction::new(&table);
    let action = tx.fast_append().add_data_files(data_files);
    let tx_with_action = action
        .apply(Transaction::new(&table))
        .context("apply series action")?;
    tx_with_action
        .commit(&*tables.catalog)
        .await
        .context("series commit")?;
    Ok(())
}

fn samples_arrow_schema() -> Arc<ArrowSchema> {
    Arc::new(ArrowSchema::new(vec![
        Field::new("series_id", DataType::Int64, false).with_metadata(field_id_metadata(
            field_ids::SAMPLES_SERIES_ID,
        )),
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        )
        .with_metadata(field_id_metadata(field_ids::TIMESTAMP)),
        Field::new("value", DataType::Float64, false)
            .with_metadata(field_id_metadata(field_ids::VALUE)),
    ]))
}

fn series_arrow_schema() -> Arc<ArrowSchema> {
    // Build the labels Map field with field-id metadata on key and value.
    let key_field = Arc::new(
        Field::new("key", DataType::Utf8, false)
            .with_metadata(field_id_metadata(field_ids::LABELS_KEY)),
    );
    let value_field = Arc::new(
        Field::new("value", DataType::Utf8, false)
            .with_metadata(field_id_metadata(field_ids::LABELS_VALUE)),
    );
    let entries_field = Arc::new(Field::new(
        "key_value",
        DataType::Struct(vec![key_field, value_field].into()),
        false,
    ));
    let labels_dt = DataType::Map(entries_field, false);

    Arc::new(ArrowSchema::new(vec![
        Field::new("series_id", DataType::Int64, false)
            .with_metadata(field_id_metadata(field_ids::SERIES_ID)),
        Field::new("metric_name", DataType::Utf8, false)
            .with_metadata(field_id_metadata(field_ids::METRIC_NAME)),
        Field::new("labels", labels_dt, false)
            .with_metadata(field_id_metadata(field_ids::LABELS)),
    ]))
}

fn field_id_metadata(id: i32) -> HashMap<String, String> {
    HashMap::from([("PARQUET:field_id".to_string(), id.to_string())])
}

fn build_samples_batch(
    schema: Arc<ArrowSchema>,
    by_series: &std::collections::BTreeMap<i64, Vec<(i64, f64)>>,
) -> Result<RecordBatch> {
    // Total length = sum of all per-series Vec lengths. Sort within each
    // series so Parquet row groups encode tighter ranges.
    let total: usize = by_series.values().map(|v| v.len()).sum();
    let mut series_ids = Vec::with_capacity(total);
    let mut timestamps = Vec::with_capacity(total);
    let mut values = Vec::with_capacity(total);
    for (sid, points) in by_series {
        let mut points = points.clone();
        points.sort_by_key(|(ts, _)| *ts);
        for (ts, v) in points {
            series_ids.push(*sid);
            timestamps.push(ts);
            values.push(v);
        }
    }
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(series_ids)) as ArrayRef,
            Arc::new(TimestampMicrosecondArray::from(timestamps)) as ArrayRef,
            Arc::new(Float64Array::from(values)) as ArrayRef,
        ],
    )?;
    Ok(batch)
}

fn build_series_batch(schema: Arc<ArrowSchema>, snap: &Snapshot) -> Result<RecordBatch> {
    let n = snap.new_series.len();
    let mut series_ids = Vec::with_capacity(n);
    let mut metrics = Vec::with_capacity(n);

    // Use MapBuilder to accumulate (k, v) pairs row by row with offsets,
    // then rebuild the MapArray with the exact field shape Iceberg expects:
    // - outer field "labels" (DataType::Map(entries_field, false))
    // - entries field "key_value" (Struct, non-null) with PARQUET:field_id=3
    //   wait — labels has field_id=3, key has 4, value has 5. Builder
    //   doesn't carry these, so we strip the inner entries off the builder
    //   output and re-wrap with our schema-matching field metadata.
    let map_fields = MapFieldNames {
        entry: "key_value".to_string(),
        key: "key".to_string(),
        value: "value".to_string(),
    };
    let mut mb = MapBuilder::new(Some(map_fields), StringBuilder::new(), StringBuilder::new());

    for (sid, entry) in &snap.new_series {
        series_ids.push(*sid);
        metrics.push(entry.metric.as_str());
        for (k, v) in &entry.labels {
            mb.keys().append_value(k);
            mb.values().append_value(v);
        }
        mb.append(true)?;
    }
    let raw_map = mb.finish();

    // Rebuild with Iceberg-expected shape (non-null value field with
    // PARQUET:field_id metadata on key and value).
    let labels_array = rewrap_labels_map(raw_map)?;

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(series_ids)) as ArrayRef,
            Arc::new(StringArray::from(metrics)) as ArrayRef,
            Arc::new(labels_array) as ArrayRef,
        ],
    )?;
    Ok(batch)
}

/// Rebuild the labels MapArray so its inner field shape matches Iceberg's
/// expectations: non-null value, PARQUET:field_id metadata on key/value.
fn rewrap_labels_map(raw: MapArray) -> Result<MapArray> {
    let (_old_entries_field, offsets, entries, nulls, ordered) = raw.into_parts();
    let (_old_struct_fields, struct_arrays, struct_nulls) = entries.into_parts();

    // struct_arrays[0] is keys (StringArray), [1] is values (StringArray).
    // Both should be non-null in a well-formed series row, but the builder
    // produces a value field marked nullable. Re-wrap with the right Field.
    let key_field = Arc::new(
        Field::new("key", DataType::Utf8, false)
            .with_metadata(field_id_metadata(field_ids::LABELS_KEY)),
    );
    let value_field = Arc::new(
        Field::new("value", DataType::Utf8, false)
            .with_metadata(field_id_metadata(field_ids::LABELS_VALUE)),
    );
    let entries_field = Arc::new(Field::new(
        "key_value",
        DataType::Struct(vec![key_field.clone(), value_field.clone()].into()),
        false,
    ));
    let new_entries = StructArray::try_new(
        vec![key_field, value_field].into(),
        struct_arrays,
        struct_nulls,
    )?;
    Ok(MapArray::new(
        entries_field,
        offsets,
        new_entries,
        nulls,
        ordered,
    ))
}

fn epoch_days(day: NaiveDate) -> i32 {
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    (day - epoch).num_days() as i32
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::iceberg_table::IcebergTables;
    use crate::ingest::buffer::{Buffer, NewSeriesEntry};
    use crate::ingest::types::ValidatedSample;
    use crate::state::DF_CATALOG_NAME;
    use crate::iceberg_table::NAMESPACE;
    use datafusion::prelude::SessionContext;
    use iceberg_datafusion::IcebergCatalogProvider;
    use std::collections::BTreeMap;
    use std::sync::Arc as StdArc;

    /// Stand up a fresh in-memory warehouse + DataFusion session per test.
    /// We create a unique warehouse URI to keep tests fully isolated even
    /// if they share a process.
    async fn fresh_setup() -> (StdArc<IcebergTables>, SessionContext) {
        let suffix = uuid::Uuid::new_v4().simple().to_string();
        let uri = format!("memory:///wh-{suffix}");
        let tables = StdArc::new(IcebergTables::open_memory(&uri).await.unwrap());
        let ctx = SessionContext::new();
        let cat = IcebergCatalogProvider::try_new(tables.catalog.clone())
            .await
            .unwrap();
        ctx.register_catalog(DF_CATALOG_NAME, StdArc::new(cat));
        (tables, ctx)
    }

    fn make_validated(metric: &str, labels: &[(&str, &str)], ts_ms: i64, value: f64) -> ValidatedSample {
        let mut bm = BTreeMap::new();
        for (k, v) in labels {
            bm.insert((*k).to_string(), (*v).to_string());
        }
        let series_id = crate::ingest::validate::derive_series_id(metric, &bm);
        ValidatedSample {
            series_id,
            metric: metric.to_string(),
            labels: bm,
            ts_us: ts_ms.saturating_mul(1_000),
            value,
        }
    }

    async fn count(ctx: &SessionContext, sql: &str) -> i64 {
        use datafusion::arrow::array::Int64Array;
        let df = ctx.sql(sql).await.unwrap();
        let batches = df.collect().await.unwrap();
        for b in batches {
            let col = b.column(0);
            if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
                if arr.len() > 0 {
                    return arr.value(0);
                }
            }
        }
        0
    }

    #[tokio::test]
    async fn empty_snapshot_does_nothing() {
        let (tables, _ctx) = fresh_setup().await;
        let snap = Buffer::new().take();
        let res = flush(&tables, snap).await.unwrap();
        assert_eq!(res.samples_files, 0);
        assert_eq!(res.samples_rows, 0);
        assert_eq!(res.series_rows, 0);
    }

    #[tokio::test]
    async fn single_sample_writes_and_queries_back() {
        let (tables, ctx) = fresh_setup().await;
        let mut b = Buffer::new();
        let s = make_validated("m", &[("a", "1")], 1_714_800_000_000, 42.0);
        b.insert_batch(1, vec![s]);
        let snap = b.take();

        let res = flush(&tables, snap).await.unwrap();
        assert_eq!(res.samples_files, 1);
        assert_eq!(res.samples_rows, 1);
        assert_eq!(res.series_rows, 1);

        let n_series = count(&ctx,
            &format!("SELECT COUNT(*) FROM {}.{}.series", DF_CATALOG_NAME, NAMESPACE)).await;
        assert_eq!(n_series, 1);
        let n_samples = count(&ctx,
            &format!("SELECT COUNT(*) FROM {}.{}.samples", DF_CATALOG_NAME, NAMESPACE)).await;
        assert_eq!(n_samples, 1);
    }

    #[tokio::test]
    async fn multi_day_snapshot_writes_one_file_per_day() {
        // Two samples on different days → FanoutWriter must split into
        // two day partitions.
        let (tables, ctx) = fresh_setup().await;
        let mut b = Buffer::new();
        let day_a_ms = 1_714_800_000_000_i64; // 2024-05-04 ish; doesn't matter, just the day
        let day_b_ms = day_a_ms + 86_400_000;
        b.insert_batch(1, vec![
            make_validated("m", &[("a", "1")], day_a_ms, 10.0),
            make_validated("m", &[("a", "1")], day_b_ms, 20.0),
        ]);
        let snap = b.take();
        let res = flush(&tables, snap).await.unwrap();
        assert_eq!(res.samples_files, 2, "one file per day expected");
        assert_eq!(res.samples_rows, 2);
        // series row dedup: same (metric, labels) → 1 series
        assert_eq!(res.series_rows, 1);

        let n_samples = count(&ctx,
            &format!("SELECT COUNT(*) FROM {}.{}.samples", DF_CATALOG_NAME, NAMESPACE)).await;
        assert_eq!(n_samples, 2);
    }

    #[tokio::test]
    async fn second_flush_appends_without_clobbering() {
        let (tables, ctx) = fresh_setup().await;
        // First flush.
        let mut b = Buffer::new();
        b.insert_batch(1, vec![
            make_validated("m1", &[("k", "v")], 1_714_800_000_000, 1.0),
        ]);
        let _ = flush(&tables, b.take()).await.unwrap();

        // Second flush: new series + new sample. Buffer carries over
        // known_series so the same (m1, k=v) wouldn't re-emit, but we test
        // a brand-new series anyway.
        let mut b2 = Buffer::with_known_series([
            make_validated("m1", &[("k", "v")], 0, 0.0).series_id,
        ]);
        b2.insert_batch(2, vec![
            make_validated("m2", &[("k", "v")], 1_714_800_000_000, 99.0),
        ]);
        let _ = flush(&tables, b2.take()).await.unwrap();

        let n_series = count(&ctx,
            &format!("SELECT COUNT(*) FROM {}.{}.series", DF_CATALOG_NAME, NAMESPACE)).await;
        assert_eq!(n_series, 2);
        let n_samples = count(&ctx,
            &format!("SELECT COUNT(*) FROM {}.{}.samples", DF_CATALOG_NAME, NAMESPACE)).await;
        assert_eq!(n_samples, 2);
    }

    #[tokio::test]
    async fn samples_only_no_new_series_is_valid() {
        let (tables, _ctx) = fresh_setup().await;
        // First, register the series.
        let mut b = Buffer::new();
        let s = make_validated("m", &[("k", "v")], 1_714_800_000_000, 1.0);
        let series_id = s.series_id;
        b.insert_batch(1, vec![s]);
        let _ = flush(&tables, b.take()).await.unwrap();

        // Now write more samples with the *same* series. Buffer carries
        // known_series → new_series stays empty.
        let mut b2 = Buffer::with_known_series([series_id]);
        b2.insert_batch(2, vec![
            make_validated("m", &[("k", "v")], 1_714_800_010_000, 2.0),
        ]);
        let snap = b2.take();
        assert_eq!(snap.new_series.len(), 0, "no new series this time");

        let res = flush(&tables, snap).await.unwrap();
        assert_eq!(res.samples_rows, 1);
        assert_eq!(res.series_rows, 0);
    }
}
