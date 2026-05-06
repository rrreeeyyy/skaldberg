# Skaldberg

A small, cloud-native time-series database backed by Apache Iceberg.

Status: **Phase 8 (PromQL → SQL pushdown over S3 Tables).** Storage is
Apache Iceberg tables on AWS S3 Tables (or an in-process MemoryCatalog
for dev), exposed over a Prometheus HTTP API subset that translates
PromQL queries into a single DataFusion SQL plan whenever possible.

## What it is

Skaldberg ingests metric samples (Prometheus Remote Write or a JSON API),
durably stages them in a write-ahead log, batches them into Apache Iceberg
tables on commit, and exposes the data over both a raw SQL endpoint and
a PromQL-compatible HTTP API backed by
[DataFusion](https://github.com/apache/datafusion). Storage is two
Iceberg tables under one namespace:

- `series`  — one row per `(metric, labels)` tuple. Labels live in a
  `MAP<STRING,STRING>` column.
- `samples` — one row per measurement, partitioned by `days(timestamp)`.

A small helper view `sk_metric` joins them so callers don't have to.

## Endpoints

| Method | Path                          | Body / Params                          | Response                |
| ------ | ----------------------------- | -------------------------------------- | ----------------------- |
| GET    | `/healthz`                    | —                                      | `200 ok`                |
| GET    | `/metrics`                    | —                                      | Prometheus exposition   |
| POST   | `/api/v1/sql`                 | `{"sql": "...", "max_rows": 100000}`   | columns + rows + meta   |
| POST   | `/api/v1/ingest`              | `{"samples": [...]}` (JSON)            | accepted/rejected per sample |
| POST   | `/api/v1/write`               | snappy-compressed protobuf (Prom 1.0)  | `204 No Content`        |
| GET/POST | `/api/v1/query`             | `query=`, `time=`                      | Prometheus instant vector |
| GET/POST | `/api/v1/query_range`       | `query=`, `start=`, `end=`, `step=`    | Prometheus matrix       |
| GET    | `/api/v1/labels`              | —                                      | distinct label names    |
| GET    | `/api/v1/label/{name}/values` | —                                      | distinct values for one label |
| GET    | `/api/v1/series`              | `match[]=` (repeatable)                | series matching selectors |

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

## PromQL pushdown

Queries arriving at `/api/v1/query` and `/api/v1/query_range` are
translated into one DataFusion SQL plan whenever the shape allows.
The Rust app stays thin: it parses PromQL, builds SQL, decodes Arrow,
serializes JSON. All filtering, aggregation, ranking, window math,
and label joins happen inside DataFusion (which sees the Iceberg
tables on S3 Tables transparently — no per-step Rust loop materializing
samples in memory).

Pushed entirely into SQL (instant + matrix paths):

| Shape                                          | SQL primitive                                   |
| ---------------------------------------------- | ----------------------------------------------- |
| `metric{l = "v"}` / `l != "v"`                 | `element_at(s.labels, k)[1]` predicate          |
| `sum / avg / min / max / count(...) [by (...)]` | `GROUP BY` on retained labels                   |
| `topk / bottomk(n, ...) [by (...)]`            | `ROW_NUMBER()` ranking                          |
| `n * vec` / `vec / n` / `vec > c` / `vec > bool c` | row-wise transform / filter                 |
| `rate / irate / increase / delta(metric[r])`   | `LAG()` + counter-reset `CASE`                  |
| `<agg>(rate(metric[r])) [by (...)]`            | per-series rate CTE → outer `GROUP BY`          |
| `histogram_quantile(q, rate(bucket[r]))`       | window-based cumulative interpolation           |
| `<sel> <op> <sel>` (1:1 label match)           | `string_agg`-keyed JOIN                         |
| `topk(n, rate(metric[r])) [by (...)]`          | ranking over the per-series rate CTE            |

Everything else (or richer variants) falls back to a single SQL fetch
plus a Rust post-step:

- `without (...)` modifier (would need the full label set up front)
- `on (...)` / `ignoring (...)` / `group_left` / `group_right` on `vec × vec`
- nested aggregations (`sum(sum(...))`) and other non-selector inners
- arbitrary inner expressions on `histogram_quantile`
- regex matchers (`=~`, `!~`) — applied in Rust after the SQL pull

The dispatch layer in `src/prometheus.rs` always tries SQL pushdown
first and only falls through on shapes the planner can't handle yet.

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

# In-memory catalog (dev). The default if --catalog isn't set.
./target/release/skaldberg-server \
    --wal-dir ./data/wal \
    --bind 127.0.0.1:8080 \
    --flush-interval-secs 60

# Real S3 Tables catalog. Uses the AWS credential chain
# (env / SSO / shared profile / IRSA / IMDS) — no IAM access keys.
./target/release/skaldberg-server \
    --wal-dir ./data/wal \
    --catalog s3tables \
    --table-bucket-arn "arn:aws:s3tables:<region>:<acct>:bucket/<name>" \
    --aws-region <region> \
    --bind 127.0.0.1:8080
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

# raw SQL (after the next flush)
curl -sX POST http://127.0.0.1:8080/api/v1/sql \
  -H 'content-type: application/json' \
  -d '{"sql":"SELECT * FROM iceberg.skaldberg.samples"}' | jq

# Prometheus instant query
curl -sG http://127.0.0.1:8080/api/v1/query \
  --data-urlencode 'query=sum(rate(demo[1m])) by (job)' \
  --data-urlencode "time=$(date +%s)" | jq
```

## Tests

Unit tests:

```bash
cargo test
```

End-to-end smoke tests (start a server, drive it over HTTP, assert):

```bash
# in-memory catalog
python3 scripts/smoke_ingest.py
python3 scripts/smoke_remote_write.py

# real S3 Tables (needs AWS_PROFILE + an existing bucket ARN)
SKALDBERG_TABLE_BUCKET_ARN=arn:aws:s3tables:... \
    SKALDBERG_AWS_REGION=ap-northeast-1 \
    AWS_PROFILE=<profile> \
    ./scripts/smoke_s3tables.sh
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

Done:
- **Phase 5.** S3TablesCatalog backend (AWS S3 Tables), aws-sdk-s3
  direct path (no OpenDAL bridge).
- **Phase 6.** Querier guardrails: graceful shutdown, bearer token
  auth, `/metrics`, `--query-timeout-secs`, `--max-concurrent-queries`,
  `--query-memory-limit-mb`.
- **Phase 7.** Grafana integration via the Prometheus HTTP API
  subset.
- **Phase 8.** PromQL → SQL pushdown for selectors, aggregations,
  topk/bottomk, scalar × vector, rate-family, `<agg>(rate(...))`,
  `histogram_quantile(q, rate(...))`, vector × vector,
  `topk(n, rate(...))` — instant + matrix paths. End-to-end verified
  against a real S3 Tables bucket.

Open:
- `without (...)`, `on/ignoring/group_*` modifiers in SQL pushdown.
- Compaction / retention story for the `samples` and `series` tables.
- Real Prometheus connection smoke test.

## License

Apache License 2.0. See [LICENSE](./LICENSE).
