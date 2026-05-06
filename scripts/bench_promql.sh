#!/usr/bin/env bash
# Quick latency check for /api/v1/query (PromQL).
#
# Companion to bench_query.sh. Same disclaimer applies: not a
# benchmark suite, just a "did this PR make queries 10x slower?"
# probe for the PromQL pushdown paths added in Phase 8.
#
# Defaults run `topk(5, rate(http_requests_total[1m]))` 500 times
# at 10-way concurrency. Override with `--promql` for ad-hoc shapes.
#
# Server is expected to be already running with whatever data shape
# you want to measure against. The script doesn't ingest anything.
#
# Requires `oha` (cargo install oha).
#
# Usage:
#   ./scripts/bench_promql.sh
#   ./scripts/bench_promql.sh -n 2000 -c 50 \
#       --promql 'sum(rate(http_requests_total[5m])) by (job)'
#   ./scripts/bench_promql.sh --range 1h --step 30s \
#       --promql 'histogram_quantile(0.9, rate(latency_bucket[5m]))'
#   SKALDBERG_API_TOKEN=...  appended as bearer when set

set -euo pipefail

n=500
c=10
host="http://127.0.0.1:8080"
promql='topk(5, rate(http_requests_total[1m]))'
mode="instant"        # or "range"
range_seconds=900     # only used in range mode (15 min default)
step="30s"

usage() {
    cat <<USAGE
Usage: $0 [options]

Options:
  -n <n>            total requests (default: $n)
  -c <c>            concurrent connections (default: $c)
  --host <url>      server base (default: $host)
  --promql <q>      PromQL expression (default: $promql)
  --range <secs>    switch to /api/v1/query_range with a window of
                    <secs> seconds ending at now (default: instant)
  --step <step>     step for --range (default: $step)
  -h | --help       this help

Env:
  SKALDBERG_API_TOKEN   bearer token added to every request when set
USAGE
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        -n) n=$2; shift 2 ;;
        -c) c=$2; shift 2 ;;
        --host) host=$2; shift 2 ;;
        --promql) promql=$2; shift 2 ;;
        --range) mode=range; range_seconds=$2; shift 2 ;;
        --step) step=$2; shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) echo "unknown arg: $1" >&2; usage; exit 2 ;;
    esac
done

if ! command -v oha >/dev/null 2>&1; then
    echo "oha not found. install with: cargo install oha" >&2
    exit 1
fi

now=$(python3 -c 'import time; print(int(time.time()))')

# Build a form-encoded body. Both /api/v1/query and /query_range
# accept POST with application/x-www-form-urlencoded bodies, which
# avoids URL-length issues on long PromQL expressions.
body=$(mktemp -t skaldberg-bench-promql.XXXXXX)
trap 'rm -f "$body"' EXIT

if [[ "$mode" == "range" ]]; then
    start=$((now - range_seconds))
    # `end=""` so curl --data-binary doesn't pick up a trailing newline,
    # which would otherwise turn `time=12345` into `time=12345\n` and
    # fail server-side timestamp parsing.
    QUERY="$promql" START="$start" END="$now" STEP="$step" python3 - > "$body" <<'PY'
import os, urllib.parse
print(urllib.parse.urlencode({
    "query": os.environ["QUERY"],
    "start": os.environ["START"],
    "end":   os.environ["END"],
    "step":  os.environ["STEP"],
}), end="")
PY
    url="$host/api/v1/query_range"
else
    QUERY="$promql" TIME="$now" python3 - > "$body" <<'PY'
import os, urllib.parse
print(urllib.parse.urlencode({
    "query": os.environ["QUERY"],
    "time":  os.environ["TIME"],
}), end="")
PY
    url="$host/api/v1/query"
fi

headers=(-H 'content-type: application/x-www-form-urlencoded')
if [[ -n "${SKALDBERG_API_TOKEN:-}" ]]; then
    headers+=(-H "Authorization: Bearer $SKALDBERG_API_TOKEN")
fi

echo "warming up: 1 request..."
curl -fsS -X POST "${headers[@]}" --data-binary "@$body" "$url" > /dev/null

echo "running: oha -n $n -c $c (mode=$mode promql=$promql)"
oha -n "$n" -c "$c" -m POST "${headers[@]}" -D "$body" "$url"
