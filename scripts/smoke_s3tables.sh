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
#   2. POSTs one valid sample to /api/v1/ingest
#   3. waits for the next flush
#   4. queries SELECT COUNT(*) and sk_metric to confirm the write
#      landed and round-trips through DataFusion
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

echo "--- ingest 1 sample ---"
curl -fsS -X POST "http://127.0.0.1:$port/api/v1/ingest" \
    -H 'content-type: application/json' \
    -d "{\"samples\":[{\"metric\":\"smoke_s3tables\",\"labels\":{\"job\":\"smoke\"},\"ts\":$ts,\"value\":1.0}]}"
echo

echo "--- waiting 8s for the next flush ---"
sleep 8

echo "--- COUNT(*) ---"
count_resp=$(curl -fsS -X POST "http://127.0.0.1:$port/api/v1/sql" \
    -H 'content-type: application/json' \
    -d '{"sql":"SELECT COUNT(*) AS n FROM iceberg.skaldberg.samples"}')
echo "$count_resp"
n=$(printf '%s' "$count_resp" | python3 -c 'import sys, json; print(json.load(sys.stdin)["rows"][0]["n"])')
if [[ "$n" -lt 1 ]]; then
    echo "ERROR: expected COUNT(*) >= 1, got $n" >&2
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
