//! Iceberg catalog and table bootstrap.
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
//! `CatalogConfig` selects the backing catalog at startup. `Memory` is
//! the dev / test path (in-process, no external services). `S3Tables`
//! talks to AWS S3 Tables via `iceberg-catalog-s3tables`; AWS credentials
//! and region are taken from the standard SDK chain (env vars / shared
//! config / IAM role).
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
use iceberg_catalog_s3tables::{
    S3TablesCatalogBuilder, S3TABLES_CATALOG_PROP_TABLE_BUCKET_ARN,
};

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

/// Catalog selection passed in from CLI / config.
#[derive(Debug, Clone)]
pub enum CatalogConfig {
    /// In-process catalog backed by an in-memory warehouse URI.
    /// Used for unit tests and local dev only.
    Memory { warehouse_uri: String },
    /// AWS S3 Tables catalog. Region and credentials come from the
    /// standard AWS SDK chain (env vars / shared config / IAM role).
    /// `endpoint_url` is optional and primarily for LocalStack-style testing.
    S3Tables {
        table_bucket_arn: String,
        endpoint_url: Option<String>,
    },
}

/// Holds shared state for catalog and the two table identifiers.
pub struct IcebergTables {
    pub catalog: Arc<dyn Catalog>,
    pub namespace: NamespaceIdent,
    pub series: TableIdent,
    pub samples: TableIdent,
}

impl IcebergTables {
    /// Build the configured catalog and ensure the namespace + both
    /// tables exist. Idempotent.
    pub async fn open(config: &CatalogConfig) -> Result<Self> {
        let catalog: Arc<dyn Catalog> = match config {
            CatalogConfig::Memory { warehouse_uri } => {
                Arc::new(build_memory_catalog(warehouse_uri).await?)
            }
            CatalogConfig::S3Tables {
                table_bucket_arn,
                endpoint_url,
            } => Arc::new(
                build_s3tables_catalog(table_bucket_arn, endpoint_url.as_deref()).await?,
            ),
        };
        ensure_skaldberg_tables(catalog).await
    }

    /// Convenience wrapper for tests and local-dev callers.
    pub async fn open_memory(warehouse_uri: &str) -> Result<Self> {
        Self::open(&CatalogConfig::Memory {
            warehouse_uri: warehouse_uri.to_string(),
        })
        .await
    }
}

async fn build_memory_catalog(warehouse_uri: &str) -> Result<impl Catalog> {
    MemoryCatalogBuilder::default()
        .with_storage_factory(Arc::new(MemoryStorageFactory))
        .load(
            "skaldberg",
            HashMap::from([(
                MEMORY_CATALOG_WAREHOUSE.to_string(),
                warehouse_uri.to_string(),
            )]),
        )
        .await
        .context("memory catalog load")
}

async fn build_s3tables_catalog(
    table_bucket_arn: &str,
    endpoint_url: Option<&str>,
) -> Result<impl Catalog> {
    let mut builder = S3TablesCatalogBuilder::default();
    if let Some(url) = endpoint_url {
        builder = builder.with_endpoint_url(url);
    }
    builder
        .load(
            "s3tables",
            HashMap::from([(
                S3TABLES_CATALOG_PROP_TABLE_BUCKET_ARN.to_string(),
                table_bucket_arn.to_string(),
            )]),
        )
        .await
        .context("s3tables catalog load")
}

async fn ensure_skaldberg_tables(catalog: Arc<dyn Catalog>) -> Result<IcebergTables> {
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

    Ok(IcebergTables {
        catalog,
        namespace,
        series,
        samples,
    })
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
