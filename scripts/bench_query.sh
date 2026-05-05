#!/usr/bin/env bash
# Quick latency check for /api/v1/sql.
#
# Companion to bench_ingest.sh. Same disclaimer applies: not a
# benchmark suite, just a "did this PR make queries 10x slower?"
# probe. Defaults run a `SELECT COUNT(*) FROM iceberg.skaldberg.samples`
# 2000 times at 50-way concurrency.
#
# Server is expected to be already running with whatever data shape
# you want to measure against (otherwise an empty `samples` table
# is still a valid case — it exercises the catalog/datafusion plan
# path). Override the SQL via `--sql` for ad-hoc shapes.
#
# Requires `oha` (cargo install oha).
#
# Usage:
#   ./scripts/bench_query.sh
#   ./scripts/bench_query.sh -n 5000 -c 100
#   ./scripts/bench_query.sh --sql "SELECT * FROM sk_metric LIMIT 10"
#   SKALDBERG_API_TOKEN=...  appended as bearer when set

set -euo pipefail

n=2000
c=50
url=http://127.0.0.1:8080/api/v1/sql
sql="SELECT COUNT(*) AS n FROM iceberg.skaldberg.samples"

usage() {
    cat <<USAGE
Usage: $0 [options]

Options:
  -n <n>          total requests (default: 2000)
  -c <c>          concurrent connections (default: 50)
  --url <url>     SQL URL (default: $url)
  --sql <sql>     SQL to run (default: $sql)
  -h | --help     this help

Env:
  SKALDBERG_API_TOKEN  bearer token added to every request when set
USAGE
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        -n) n=$2; shift 2 ;;
        -c) c=$2; shift 2 ;;
        --url) url=$2; shift 2 ;;
        --sql) sql=$2; shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) echo "unknown arg: $1" >&2; usage; exit 2 ;;
    esac
done

if ! command -v oha >/dev/null 2>&1; then
    echo "oha not found. install with: cargo install oha" >&2
    exit 1
fi

body=$(mktemp -t skaldberg-bench-query.XXXXXX)
trap 'rm -f "$body"' EXIT

# Build the JSON body via Python so the SQL string is escaped
# correctly (handles quotes, backslashes, etc.).
SQL="$sql" python3 - > "$body" <<'PY'
import json, os
print(json.dumps({"sql": os.environ["SQL"]}))
PY

headers=(-H 'content-type: application/json')
if [[ -n "${SKALDBERG_API_TOKEN:-}" ]]; then
    headers+=(-H "Authorization: Bearer $SKALDBERG_API_TOKEN")
fi

echo "warming up: 1 request..."
curl -fsS -X POST "${headers[@]}" --data-binary "@$body" "$url" > /dev/null

echo "running: oha -n $n -c $c (sql=$sql)"
oha -n "$n" -c "$c" -m POST "${headers[@]}" -D "$body" "$url"
