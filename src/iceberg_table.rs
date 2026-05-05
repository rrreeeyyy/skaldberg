//! Iceberg catalog and table bootstrap.
//!
//! Phase 4 uses an in-process `MemoryCatalog` for the entire warehouse —
//! no external services. Phase 5 will swap this for `S3TablesCatalog`
//! against AWS without changing call sites.
//!
//! Two tables live under namespace `skaldberg`:
//!
//! - **series** — metadata catalog. One row per `(metric, labels)` pair.
//!   Schema: `series_id BIGINT, metric_name STRING, labels MAP<STRING,STRING>`.
//!   Unpartitioned; rewritten in full each commit (small).
//!
//! - **samples** — time-series fact table. One row per measurement.
//!   Schema: `series_id BIGINT, timestamp TIMESTAMP, value DOUBLE`.
//!   Partitioned by `days(timestamp)` so a `WHERE timestamp BETWEEN ...`
//!   query reads only the relevant day files.
//!
//! Iceberg field IDs are stable across schema evolution. We assign them
//! explicitly here and they're carried in Parquet metadata as
//! `PARQUET:field_id` so writers can map Arrow columns back to Iceberg
//! columns by id, not by name.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use iceberg::io::MemoryStorageFactory;
use iceberg::memory::{MemoryCatalogBuilder, MEMORY_CATALOG_WAREHOUSE};
use iceberg::spec::{
    MapType, NestedField, PartitionSpec, PrimitiveType, Schema, Transform, Type,
    UnboundPartitionField,
};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};

pub const NAMESPACE: &str = "skaldberg";
pub const SERIES_TABLE: &str = "series";
pub const SAMPLES_TABLE: &str = "samples";

/// Iceberg field IDs. Numbered 1..N at table creation; never renumbered.
pub mod field_ids {
    pub const SERIES_ID: i32 = 1;
    pub const METRIC_NAME: i32 = 2;
    pub const LABELS: i32 = 3;
    pub const LABELS_KEY: i32 = 4;
    pub const LABELS_VALUE: i32 = 5;

    pub const SAMPLES_SERIES_ID: i32 = 1;
    pub const TIMESTAMP: i32 = 2;
    pub const VALUE: i32 = 3;
}

/// Holds shared state for catalog and the two table identifiers.
pub struct IcebergTables {
    pub catalog: Arc<dyn Catalog>,
    pub namespace: NamespaceIdent,
    pub series: TableIdent,
    pub samples: TableIdent,
}

impl IcebergTables {
    /// Build an in-process MemoryCatalog and ensure the namespace + both
    /// tables exist. Idempotent: re-opening over an already-populated
    /// catalog returns the existing tables (which is the no-op case
    /// today since MemoryCatalog is in-process).
    pub async fn open_memory(warehouse_uri: &str) -> Result<Self> {
        let catalog = MemoryCatalogBuilder::default()
            .with_storage_factory(Arc::new(MemoryStorageFactory))
            .load(
                "skaldberg",
                HashMap::from([(
                    MEMORY_CATALOG_WAREHOUSE.to_string(),
                    warehouse_uri.to_string(),
                )]),
            )
            .await
            .context("memory catalog load")?;

        let catalog: Arc<dyn Catalog> = Arc::new(catalog);
        let namespace = NamespaceIdent::from_strs([NAMESPACE])?;
        if catalog.get_namespace(&namespace).await.is_err() {
            catalog
                .create_namespace(&namespace, HashMap::new())
                .await
                .context("create namespace")?;
        }

        let series = TableIdent::new(namespace.clone(), SERIES_TABLE.to_string());
        if catalog.load_table(&series).await.is_err() {
            catalog
                .create_table(&namespace, build_series_creation()?)
                .await
                .context("create series table")?;
        }

        let samples = TableIdent::new(namespace.clone(), SAMPLES_TABLE.to_string());
        if catalog.load_table(&samples).await.is_err() {
            catalog
                .create_table(&namespace, build_samples_creation()?)
                .await
                .context("create samples table")?;
        }

        Ok(Self {
            catalog,
            namespace,
            series,
            samples,
        })
    }
}

pub fn series_iceberg_schema() -> Result<Schema> {
    let labels_key = Arc::new(NestedField::map_key_element(
        field_ids::LABELS_KEY,
        Type::Primitive(PrimitiveType::String),
    ));
    let labels_value = Arc::new(NestedField::map_value_element(
        field_ids::LABELS_VALUE,
        Type::Primitive(PrimitiveType::String),
        true,
    ));
    let labels_type = Type::Map(MapType {
        key_field: labels_key,
        value_field: labels_value,
    });

    Schema::builder()
        .with_fields(vec![
            Arc::new(NestedField::required(
                field_ids::SERIES_ID,
                "series_id",
                Type::Primitive(PrimitiveType::Long),
            )),
            Arc::new(NestedField::required(
                field_ids::METRIC_NAME,
                "metric_name",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::required(field_ids::LABELS, "labels", labels_type)),
        ])
        .build()
        .context("series schema")
}

fn build_series_creation() -> Result<TableCreation> {
    Ok(TableCreation::builder()
        .name(SERIES_TABLE.to_string())
        .schema(series_iceberg_schema()?)
        .build())
}

pub fn samples_iceberg_schema() -> Result<Schema> {
    Schema::builder()
        .with_fields(vec![
            Arc::new(NestedField::required(
                field_ids::SAMPLES_SERIES_ID,
                "series_id",
                Type::Primitive(PrimitiveType::Long),
            )),
            Arc::new(NestedField::required(
                field_ids::TIMESTAMP,
                "timestamp",
                Type::Primitive(PrimitiveType::Timestamp),
            )),
            Arc::new(NestedField::required(
                field_ids::VALUE,
                "value",
                Type::Primitive(PrimitiveType::Double),
            )),
        ])
        .build()
        .context("samples schema")
}

fn build_samples_creation() -> Result<TableCreation> {
    let schema = samples_iceberg_schema()?;
    let partition_spec = PartitionSpec::builder(schema.clone())
        .with_spec_id(0)
        .add_unbound_fields(vec![UnboundPartitionField::builder()
            .source_id(field_ids::TIMESTAMP)
            .name("timestamp_day".to_string())
            .transform(Transform::Day)
            .build()])
        .context("samples partition fields")?
        .build()
        .context("samples partition spec")?;
    Ok(TableCreation::builder()
        .name(SAMPLES_TABLE.to_string())
        .schema(schema)
        .partition_spec(partition_spec)
        .build())
}
