# Skaldberg

A small, cloud-native time-series database backed by Apache Iceberg.

Status: **Phase 4 (Iceberg, in-process MemoryCatalog).** Phase 5 will swap
the catalog for AWS S3 Tables without changing call sites.

## What it is

Skaldberg ingests metric samples (Prometheus Remote Write or a JSON API),
durably stages them in a write-ahead log, batches them into Apache Iceberg
tables on commit, and exposes the data over a SQL endpoint backed by
[DataFusion](https://github.com/apache/datafusion). Storage is two
Iceberg tables under one namespace:

- `series`  — one row per `(metric, labels)` tuple. Labels live in a
  `MAP<STRING,STRING>` column.
- `samples` — one row per measurement, partitioned by `days(timestamp)`.

A small helper view `sk_metric` joins them so callers don't have to.

## Endpoints

| Method | Path                | Body                                  | Response                |
| ------ | ------------------- | ------------------------------------- | ----------------------- |
| GET    | `/healthz`          | —                                     | `200 ok`                |
| POST   | `/api/v1/sql`       | `{"sql": "...", "max_rows": 100000}`  | columns + rows + meta   |
| POST   | `/api/v1/ingest`    | `{"samples": [...]}` (JSON)           | accepted/rejected per sample |
| POST   | `/api/v1/write`     | snappy-compressed protobuf (Prom 1.0) | `204 No Content`        |

## SQL examples

```sql
-- list metrics
SELECT DISTINCT metric_name FROM iceberg.skaldberg.series ORDER BY 1;

-- count samples in a day
SELECT COUNT(*) FROM iceberg.skaldberg.samples
 WHERE timestamp BETWEEN TIMESTAMP '2026-05-04 00:00:00'
                     AND TIMESTAMP '2026-05-04 23:59:59';

-- sk_metric helper view (samples joined to series)
SELECT timestamp, value FROM sk_metric
 WHERE metric_name = 'http_requests_total'
 ORDER BY timestamp DESC LIMIT 10;

-- per-second rate via the sk_rate_of helper view
SELECT timestamp, value_per_sec FROM sk_rate_of
 WHERE metric_name = 'http_requests_total' AND series_id = ?;
```

## Architecture

```
   Prometheus / curl                         SQL client
          │                                       │
   POST /api/v1/write             POST /api/v1/sql
   POST /api/v1/ingest                            │
          │                                       │
          ▼                                       ▼
   ┌──────────────┐                       ┌──────────────┐
   │  validate +  │                       │  DataFusion  │
   │  WAL append  │                       │  SessionCtx  │
   │  (fsync)     │                       │              │
   │      │       │                       │   sk_metric  │
   │      ▼       │                       │   sk_rate_of │
   │  in-memory   │                       └──────┬───────┘
   │  buffer      │                              │
   │  (day,series)│                              │
   └──────┬───────┘                              │
          │ 5 min  /  64 MiB                     │
          ▼                                      │
   ┌──────────────┐    ┌─────────────────────────┴────┐
   │   Fanout-    │───▶│  iceberg-rust + iceberg-     │
   │   Writer →   │    │  datafusion + MemoryCatalog  │
   │   Transaction│    │                              │
   │   .commit()  │    │  warehouse layout:           │
   │              │    │   skaldberg/series/...       │
   │              │    │   skaldberg/samples/         │
   │              │    │     timestamp_day=YYYY-MM-DD │
   └──────┬───────┘    └──────────────────────────────┘
          │
          ▼
       WAL truncate
```

## Build & run

```bash
cargo build --release
./target/release/skaldberg-server \
    --wal-dir ./data/wal \
    --warehouse-uri memory:///warehouse \
    --bind 127.0.0.1:8080 \
    --flush-interval-secs 60
```

Then:

```bash
# health
curl http://127.0.0.1:8080/healthz

# ingest one sample
curl -sX POST http://127.0.0.1:8080/api/v1/ingest \
  -H 'content-type: application/json' \
  -d "$(jq -n --argjson ts $(($(date +%s) * 1000)) \
            '{samples:[{metric:"demo",labels:{job:"api"},ts:$ts,value:42.0}]}')"

# query (after the next flush)
curl -sX POST http://127.0.0.1:8080/api/v1/sql \
  -H 'content-type: application/json' \
  -d '{"sql":"SELECT * FROM iceberg.skaldberg.samples"}' | jq
```

## Tests

Unit tests:

```bash
cargo test
```

End-to-end smoke tests (start a server, drive it over HTTP, assert):

```bash
python3 scripts/smoke_ingest.py
python3 scripts/smoke_remote_write.py
```

## Design notes

- **WAL.** Every accepted sample is appended to a CRC-checked, append-only
  log and `fdatasync`'d before the request returns 200/204. On startup the
  WAL is replayed into the in-memory buffer; on successful commit the WAL
  segment is truncated.
- **Buffer.** Samples accumulate in a two-level `BTreeMap<NaiveDate,
  BTreeMap<series_id, Vec<(ts, value)>>>` so a flush can produce one
  Parquet file per day partition in a single pass via `FanoutWriter`.
- **Iceberg writes.** Each flush is `Transaction::new(&table).fast_append()
  .add_data_files(...).commit(catalog)` — atomic at the snapshot level.
- **Series catalog.** The `series` table is appended to only when the
  buffer sees a `series_id` it hasn't persisted before. Existing
  series ids are seeded from the catalog at startup.
- **Backpressure.** When the buffer reaches 256 MiB the ingest endpoints
  return 503 so producers retry rather than OOM the server.

## Roadmap

- **Phase 5.** Replace `MemoryCatalogBuilder` with `S3TablesCatalog` so
  the warehouse lives in AWS S3 Tables. The rest of the code is
  catalog-agnostic.
- Graceful shutdown that flushes the buffer before exit (currently a
  SIGTERM during the flush window leaves up to one interval of WAL to
  replay on restart).
- Real Prometheus connection smoke test.
- Grafana datasource integration.

## License

Apache License 2.0. See [LICENSE](./LICENSE).
