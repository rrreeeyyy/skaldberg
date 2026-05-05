#!/usr/bin/env bash
# Quick throughput / latency check for /api/v1/ingest.
#
# Not a regression baseline. The goal is "notice when ingest goes
# from 'fine' to 'wow that's wrong'" — for example, a regression
# that introduces a per-request fdatasync, or one that allocates a
# new String for every label on every sample. Useful before a PR
# merge to spot-check that the hot path didn't pick up a 10x slowdown.
#
# The server is expected to be already running. Start it however
# suits — `cargo run --release -- ...` for a realistic number,
# `cargo run -- ...` for a debug build with the usual warnings.
#
# Requires `oha` (cargo install oha).
#
# Usage:
#   ./scripts/bench_ingest.sh                  # 5000 requests, 50 conns, port 8080
#   ./scripts/bench_ingest.sh -n 50000 -c 200  # heavier
#   --url / --metrics-url to override targets
#   SKALDBERG_API_TOKEN=...                    # added as bearer if set

set -euo pipefail

n=5000
c=50
url=http://127.0.0.1:8080/api/v1/ingest
metrics_url=http://127.0.0.1:8080/metrics

usage() {
    cat <<USAGE
Usage: $0 [options]

Options:
  -n <n>             total requests (default: 5000)
  -c <c>             concurrent connections (default: 50)
  --url <url>        ingest URL (default: $url)
  --metrics-url <u>  metrics URL for the post-run dump
                     (default: $metrics_url)
  -h | --help        this help

Env:
  SKALDBERG_API_TOKEN   bearer token added to every request when set
USAGE
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        -n) n=$2; shift 2 ;;
        -c) c=$2; shift 2 ;;
        --url) url=$2; shift 2 ;;
        --metrics-url) metrics_url=$2; shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) echo "unknown arg: $1" >&2; usage; exit 2 ;;
    esac
done

if ! command -v oha >/dev/null 2>&1; then
    echo "oha not found. install with: cargo install oha" >&2
    exit 1
fi

body=$(mktemp -t skaldberg-bench-body.XXXXXX)
trap 'rm -f "$body"' EXIT

python3 - > "$body" <<'PY'
import json, time
ts = int(time.time() * 1000)
print(json.dumps({"samples": [
    {"metric": "bench_metric_a", "labels": {"job": "oha"}, "ts": ts, "value": 1.0},
    {"metric": "bench_metric_b", "labels": {"job": "oha"}, "ts": ts, "value": 2.0},
    {"metric": "bench_metric_c", "labels": {"job": "oha"}, "ts": ts, "value": 3.0},
]}))
PY

headers=(-H 'content-type: application/json')
if [[ -n "${SKALDBERG_API_TOKEN:-}" ]]; then
    headers+=(-H "Authorization: Bearer $SKALDBERG_API_TOKEN")
fi

echo "warming up..."
curl -fsS -X POST "${headers[@]}" --data-binary "@$body" "$url" > /dev/null

echo "running: oha -n $n -c $c $url"
oha -n "$n" -c "$c" -m POST "${headers[@]}" -D "$body" "$url"

echo
echo "--- /metrics after run (ingest/flush only) ---"
curl -sS "$metrics_url" | grep -E '^skaldberg_(ingest|flush)' | sort
