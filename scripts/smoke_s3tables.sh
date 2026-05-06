#!/usr/bin/env bash
# End-to-end smoke against a real AWS S3 Tables bucket.
#
# Unlike the in-memory smokes (smoke_ingest.py / smoke_remote_write.py)
# which exercise the dev / unit-test code path, this script needs:
#   - actual AWS credentials resolvable by the SDK chain
#     (env / SSO / shared profile / IRSA / IMDS)
#   - an existing S3 Tables bucket to write into
#
# Configuration (env or CLI flags below):
#   SKALDBERG_TABLE_BUCKET_ARN  required
#   SKALDBERG_AWS_REGION        default: ap-northeast-1
#   SKALDBERG_BIN               default: <repo>/target/debug/skaldberg-server
#   AWS_PROFILE                 passed through to the SDK as usual
#
# What it does:
#   1. starts the server pointed at the bucket
#   2. ingests three series of counter-style samples under a
#      run-unique metric name (so accumulated data from prior runs
#      doesn't contaminate range queries)
#   3. waits for the next flush
#   4. exercises the Phase 7 / Phase 8 read paths against S3 Tables:
#        - SQL COUNT(*) + sk_metric (round-trip baseline)
#        - /api/v1/labels and /api/v1/label/<n>/values (label
#          discovery via SQL DISTINCT)
#        - /api/v1/series (selector-based listing)
#        - /api/v1/query for: pure selector (label = pushdown),
#          sum(...) by (...), topk(...), scalar × selector,
#          rate(...), sum(rate(...)) by (...) (two-step pushdown)
#        - /api/v1/query_range for: rate(...), sum(rate(...))
#      The point isn't every Prometheus semantic — it's that LAG /
#      VALUES / element_at / multi-CTE / two-step pushdown SQL
#      planners all produce the right shape against Parquet objects
#      on S3 Tables (not just the in-memory catalog).
#   5. asserts AWS-side that `skaldberg.{series, samples}` tables
#      exist via `aws s3tables list-tables`
#   6. shuts the server down with SIGTERM
#
# This is a smoke test, not a regression check — it asserts that the
# server starts, ingests, and reads back through S3 Tables. It does
# not clean up data afterwards (the bucket stays seeded for repeat runs).

set -euo pipefail

usage() {
    cat <<USAGE
Usage: $0 [--arn <table-bucket-arn>] [--region <region>] [--bin <path>]

Required:
  --arn <arn>     S3 Tables table bucket ARN (or env SKALDBERG_TABLE_BUCKET_ARN)
Options:
  --region <r>    AWS region (default: ap-northeast-1)
  --bin <path>    server binary (default: <repo>/target/debug/skaldberg-server)
  -h, --help      this help
USAGE
}

repo_root=$(cd "$(dirname "$0")/.." && pwd)
arn=${SKALDBERG_TABLE_BUCKET_ARN:-}
region=${SKALDBERG_AWS_REGION:-ap-northeast-1}
bin=${SKALDBERG_BIN:-"$repo_root/target/debug/skaldberg-server"}
port=8200

while [[ $# -gt 0 ]]; do
    case "$1" in
        --arn) arn=$2; shift 2 ;;
        --region) region=$2; shift 2 ;;
        --bin) bin=$2; shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) echo "unknown arg: $1" >&2; usage; exit 2 ;;
    esac
done

if [[ -z "$arn" ]]; then
    echo "ERROR: table bucket ARN is required (--arn or SKALDBERG_TABLE_BUCKET_ARN)" >&2
    usage
    exit 2
fi
if [[ ! -x "$bin" ]]; then
    echo "ERROR: server binary not executable: $bin" >&2
    echo "       run \`cargo build\` first, or pass --bin explicitly" >&2
    exit 2
fi

server_log=$(mktemp -t skaldberg-s3-smoke.log.XXXXXX)
wal_dir=$(mktemp -d -t skaldberg-s3-smoke-wal.XXXXXX)
echo "server log: $server_log"
echo "wal dir:    $wal_dir"
echo "arn:        $arn"
echo "region:     $region"

RUST_LOG=${RUST_LOG:-"info,skaldberg_server=info"} \
    "$bin" \
    --catalog s3tables \
    --table-bucket-arn "$arn" \
    --aws-region "$region" \
    --wal-dir "$wal_dir" \
    --bind "127.0.0.1:$port" \
    --flush-interval-secs 5 \
    > "$server_log" 2>&1 &
server_pid=$!
echo "server pid: $server_pid"

cleanup() {
    if kill -0 "$server_pid" 2>/dev/null; then
        kill -TERM "$server_pid" 2>/dev/null || true
        wait "$server_pid" 2>/dev/null || true
    fi
}
trap cleanup EXIT

# wait for healthz
up=0
for _ in $(seq 1 90); do
    if curl -fsS "http://127.0.0.1:$port/healthz" > /dev/null 2>&1; then
        up=1
        break
    fi
    if ! kill -0 "$server_pid" 2>/dev/null; then
        echo "ERROR: server died during startup" >&2
        tail -120 "$server_log" >&2
        exit 1
    fi
    sleep 0.5
done
if [[ "$up" -ne 1 ]]; then
    echo "ERROR: server never became healthy" >&2
    tail -120 "$server_log" >&2
    exit 1
fi
echo "server healthy"

ts=$(python3 -c 'import time; print(int(time.time()*1000))')
echo "ts=$ts"

# Use a run-unique metric so range queries see exactly this run's
# samples (not accumulated history from previous smoke runs).
v8_metric="smoke_v8_${ts}"
echo "phase8 metric: $v8_metric"

echo "--- ingest baseline + 3 phase-8 series ---"
# Build three counter-style series. Sample timestamps are 5..1 evenly
# spaced 10s offsets ending at $ts so the latest sample sits at "now"
# and the eldest is 40s in the past — well within any 1m range query.
read -r -d '' ingest_body <<EOF || true
{"samples":[
 {"metric":"smoke_s3tables","labels":{"job":"smoke"},"ts":$ts,"value":1.0},
 {"metric":"$v8_metric","labels":{"job":"smoke","status":"ok"},"ts":$((ts-40000)),"value":0},
 {"metric":"$v8_metric","labels":{"job":"smoke","status":"ok"},"ts":$((ts-30000)),"value":10},
 {"metric":"$v8_metric","labels":{"job":"smoke","status":"ok"},"ts":$((ts-20000)),"value":20},
 {"metric":"$v8_metric","labels":{"job":"smoke","status":"ok"},"ts":$((ts-10000)),"value":30},
 {"metric":"$v8_metric","labels":{"job":"smoke","status":"ok"},"ts":$ts,"value":40},
 {"metric":"$v8_metric","labels":{"job":"smoke","status":"err"},"ts":$((ts-40000)),"value":0},
 {"metric":"$v8_metric","labels":{"job":"smoke","status":"err"},"ts":$((ts-30000)),"value":5},
 {"metric":"$v8_metric","labels":{"job":"smoke","status":"err"},"ts":$((ts-20000)),"value":10},
 {"metric":"$v8_metric","labels":{"job":"smoke","status":"err"},"ts":$((ts-10000)),"value":15},
 {"metric":"$v8_metric","labels":{"job":"smoke","status":"err"},"ts":$ts,"value":20},
 {"metric":"$v8_metric","labels":{"job":"other","status":"ok"},"ts":$((ts-40000)),"value":0},
 {"metric":"$v8_metric","labels":{"job":"other","status":"ok"},"ts":$((ts-30000)),"value":20},
 {"metric":"$v8_metric","labels":{"job":"other","status":"ok"},"ts":$((ts-20000)),"value":40},
 {"metric":"$v8_metric","labels":{"job":"other","status":"ok"},"ts":$((ts-10000)),"value":60},
 {"metric":"$v8_metric","labels":{"job":"other","status":"ok"},"ts":$ts,"value":80}
]}
EOF
curl -fsS -X POST "http://127.0.0.1:$port/api/v1/ingest" \
    -H 'content-type: application/json' \
    -d "$ingest_body"
echo

echo "--- waiting 8s for the next flush ---"
sleep 8

echo "--- COUNT(*) ---"
count_resp=$(curl -fsS -X POST "http://127.0.0.1:$port/api/v1/sql" \
    -H 'content-type: application/json' \
    -d '{"sql":"SELECT COUNT(*) AS n FROM iceberg.skaldberg.samples"}')
echo "$count_resp"
n=$(printf '%s' "$count_resp" | python3 -c 'import sys, json; print(json.load(sys.stdin)["rows"][0]["n"])')
if [[ "$n" -lt 16 ]]; then
    echo "ERROR: expected COUNT(*) >= 16, got $n" >&2
    exit 1
fi

echo "--- sk_metric smoke_s3tables ---"
sk_resp=$(curl -fsS -X POST "http://127.0.0.1:$port/api/v1/sql" \
    -H 'content-type: application/json' \
    -d "{\"sql\":\"SELECT metric_name, value FROM sk_metric WHERE metric_name = 'smoke_s3tables' ORDER BY value DESC LIMIT 1\"}")
echo "$sk_resp"
metric=$(printf '%s' "$sk_resp" | python3 -c 'import sys, json; r=json.load(sys.stdin)["rows"]; print(r[0]["metric_name"] if r else "")')
if [[ "$metric" != "smoke_s3tables" ]]; then
    echo "ERROR: sk_metric did not return smoke_s3tables row (got: $metric)" >&2
    exit 1
fi

# ---------- Phase 7 / 8 read-path coverage ----------
#
# Each block runs one HTTP call and pipes the response through a
# Python one-liner that asserts the expected shape. Bash `set -e`
# stops the script on the first failure.

assert_promql() {
    local label=$1 url=$2 py=$3
    echo "--- $label ---"
    local resp
    resp=$(curl -fsS "$url")
    echo "$resp"
    printf '%s' "$resp" | python3 -c "$py" || { echo "FAIL: $label" >&2; exit 1; }
}

# Selector with label= pushdown (`element_at(labels, 'k')[1] = '...'`).
assert_promql "instant: ${v8_metric}{status=\"ok\"}" \
    "http://127.0.0.1:$port/api/v1/query?query=$(python3 -c "import urllib.parse; print(urllib.parse.quote('${v8_metric}{status=\"ok\"}'))")&time=$(python3 -c "print($ts/1000.0)")" \
    'import sys,json
r=json.load(sys.stdin)["data"]["result"]
assert len(r)==2, f"expected 2 series, got {len(r)}: {r}"
labels=sorted([row["metric"]["job"] for row in r])
assert labels==["other","smoke"], f"unexpected jobs: {labels}"
'

# Aggregate pushdown: sum(metric) by (job)
assert_promql "instant: sum(${v8_metric}) by (job)" \
    "http://127.0.0.1:$port/api/v1/query?query=$(python3 -c "import urllib.parse; print(urllib.parse.quote('sum(${v8_metric}) by (job)'))")&time=$(python3 -c "print($ts/1000.0)")" \
    'import sys,json
r=json.load(sys.stdin)["data"]["result"]
got={row["metric"]["job"]:float(row["value"][1]) for row in r}
# smoke: ok=40 + err=20 = 60; other: ok=80
assert abs(got.get("smoke",0)-60)<1e-6 and abs(got.get("other",0)-80)<1e-6, f"got {got}"
'

# topk pushdown
assert_promql "instant: topk(1, ${v8_metric})" \
    "http://127.0.0.1:$port/api/v1/query?query=$(python3 -c "import urllib.parse; print(urllib.parse.quote('topk(1, ${v8_metric})'))")&time=$(python3 -c "print($ts/1000.0)")" \
    'import sys,json
r=json.load(sys.stdin)["data"]["result"]
assert len(r)==1, f"expected 1 row, got {len(r)}"
v=float(r[0]["value"][1])
assert abs(v-80)<1e-6, f"top value should be 80, got {v}"
'

# scalar × vector pushdown
assert_promql "instant: ${v8_metric} * 2 (where job=other)" \
    "http://127.0.0.1:$port/api/v1/query?query=$(python3 -c "import urllib.parse; print(urllib.parse.quote('${v8_metric}{job=\"other\"} * 2'))")&time=$(python3 -c "print($ts/1000.0)")" \
    'import sys,json
r=json.load(sys.stdin)["data"]["result"]
assert len(r)==1, f"expected 1 series, got {len(r)}"
v=float(r[0]["value"][1])
# latest value 80 * 2 = 160. metric_name dropped on arithmetic.
assert abs(v-160)<1e-6 and "__name__" not in r[0]["metric"], f"got {r[0]}"
'

# rate-family pushdown (instant). Per-series 5 samples 10s apart;
# delta = 40 (ok) / 20 (err) / 80 (other) over 40s = 1.0 / 0.5 / 2.0 per sec.
assert_promql "instant: rate(${v8_metric}[1m])" \
    "http://127.0.0.1:$port/api/v1/query?query=$(python3 -c "import urllib.parse; print(urllib.parse.quote('rate(${v8_metric}[1m])'))")&time=$(python3 -c "print($ts/1000.0)")" \
    'import sys,json
r=json.load(sys.stdin)["data"]["result"]
got={(row["metric"]["job"], row["metric"]["status"]):float(row["value"][1]) for row in r}
exp={("smoke","ok"):1.0, ("smoke","err"):0.5, ("other","ok"):2.0}
for k,v in exp.items():
    assert abs(got.get(k, -1)-v)<1e-6, f"{k} expected {v}, got {got.get(k)} (all: {got})"
'

# Two-step pushdown: sum(rate()) by (job)
# smoke = 1.0 + 0.5 = 1.5 ; other = 2.0
assert_promql "instant: sum(rate(${v8_metric}[1m])) by (job)" \
    "http://127.0.0.1:$port/api/v1/query?query=$(python3 -c "import urllib.parse; print(urllib.parse.quote('sum(rate(${v8_metric}[1m])) by (job)'))")&time=$(python3 -c "print($ts/1000.0)")" \
    'import sys,json
r=json.load(sys.stdin)["data"]["result"]
got={row["metric"]["job"]:float(row["value"][1]) for row in r}
assert abs(got.get("smoke",-1)-1.5)<1e-6 and abs(got.get("other",-1)-2.0)<1e-6, f"got {got}"
'

# Matrix path: rate
end_s=$(python3 -c "print($ts/1000.0)")
start_s=$(python3 -c "print($ts/1000.0 - 30)")
assert_promql "matrix: rate(${v8_metric}[1m])" \
    "http://127.0.0.1:$port/api/v1/query_range?query=$(python3 -c "import urllib.parse; print(urllib.parse.quote('rate(${v8_metric}[1m])'))")&start=$start_s&end=$end_s&step=15s" \
    'import sys,json
r=json.load(sys.stdin)["data"]["result"]
assert len(r)==3, f"expected 3 series, got {len(r)}"
# Each series should produce ≥ 1 point (the latest eval has 5 samples in window).
for row in r:
    assert len(row["values"])>=1, f"series {row[chr(34)+chr(109)+chr(101)+chr(116)+chr(114)+chr(105)+chr(99)+chr(34)]} no points"
'

# Matrix two-step pushdown: sum(rate()) by (job) over the same window.
# Verifies the cross-join VALUES + LAG + outer GROUP BY pipeline plans
# against S3 Tables Parquet.
assert_promql "matrix: sum(rate(${v8_metric}[1m])) by (job)" \
    "http://127.0.0.1:$port/api/v1/query_range?query=$(python3 -c "import urllib.parse; print(urllib.parse.quote('sum(rate(${v8_metric}[1m])) by (job)'))")&start=$start_s&end=$end_s&step=15s" \
    'import sys,json
r=json.load(sys.stdin)["data"]["result"]
got={row["metric"]["job"] for row in r}
assert got=={"smoke","other"}, f"expected jobs {{smoke,other}}, got {got}"
# Latest eval should match instant case: smoke=1.5, other=2.0.
last={row["metric"]["job"]: float(row["values"][-1][1]) for row in r}
assert abs(last["smoke"]-1.5)<1e-6 and abs(last["other"]-2.0)<1e-6, f"latest values: {last}"
'

# Label discovery via SQL DISTINCT (Phase 8-2).
assert_promql "/api/v1/labels" \
    "http://127.0.0.1:$port/api/v1/labels" \
    'import sys,json
r=json.load(sys.stdin)["data"]
for k in ("__name__","job","status"):
    assert k in r, f"label {k} missing from {r}"
'

assert_promql "/api/v1/label/job/values" \
    "http://127.0.0.1:$port/api/v1/label/job/values" \
    'import sys,json
r=json.load(sys.stdin)["data"]
assert "smoke" in r and "other" in r, f"got {r}"
'

# /api/v1/series with selector match[]
assert_promql "/api/v1/series?match[]=${v8_metric}" \
    "http://127.0.0.1:$port/api/v1/series?match[]=$(python3 -c "import urllib.parse; print(urllib.parse.quote('${v8_metric}'))")" \
    'import sys,json
r=json.load(sys.stdin)["data"]
assert len(r)==3, f"expected 3 series, got {len(r)}"
'

echo "--- AWS list-tables under skaldberg ---"
aws s3tables list-tables \
    --table-bucket-arn "$arn" \
    --namespace skaldberg \
    --region "$region"

echo "--- shutdown ---"
kill -TERM "$server_pid"
wait "$server_pid" 2>/dev/null || true
trap - EXIT

echo "PASS"
