#!/usr/bin/env python3
"""Synthetic metric emitter for the Grafana dogfood scenario.

Loops every `--interval` seconds and POSTs a realistic-shaped batch
to `/api/v1/ingest`:

  app_requests_total                counter   (job, route, status)
  app_latency_seconds_bucket        histogram (job, route, le)
  app_inflight                      gauge     (job)

Counter / bucket values are monotonically increasing per series so
`rate()` / `histogram_quantile(rate())` produce well-defined values.
The gauge oscillates so panels show movement.

Usage:
  ./scripts/emit_demo_metrics.py
  ./scripts/emit_demo_metrics.py --host http://127.0.0.1:8080 --interval 5
  SKALDBERG_API_TOKEN=xxx ./scripts/emit_demo_metrics.py

Stop with Ctrl-C. Doesn't clean up after itself; let the next demo
run reuse the bucket or wipe the WAL dir.
"""
import argparse
import json
import math
import os
import random
import sys
import time
import urllib.error
import urllib.request


DEFAULTS = {
    "host": "http://127.0.0.1:8080",
    "interval": 5.0,
    "jobs": ["api", "worker", "ingester"],
    "routes_per_job": {
        "api": ["/login", "/checkout", "/healthz", "/items"],
        "worker": ["/job/run"],
        "ingester": ["/ingest", "/flush"],
    },
    "statuses": ["200", "200", "200", "200", "500"],  # weighted: ~80% 200
    "latency_buckets_seconds": ["0.005", "0.01", "0.025", "0.05",
                                "0.1", "0.25", "0.5", "1.0", "2.5", "+Inf"],
}


def now_ms():
    return int(time.time() * 1000)


def post_ingest(host, token, samples):
    body = json.dumps({"samples": samples}).encode()
    req = urllib.request.Request(
        f"{host}/api/v1/ingest",
        data=body,
        headers={"content-type": "application/json"},
        method="POST",
    )
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status, json.load(r)
    except urllib.error.HTTPError as e:
        return e.code, {"error": e.read().decode()[:300]}


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--host", default=os.environ.get("SKALDBERG_HOST", DEFAULTS["host"]))
    ap.add_argument("--interval", type=float, default=DEFAULTS["interval"],
                    help="Seconds between ingest batches (default: 5)")
    ap.add_argument("--quiet", action="store_true", help="Suppress per-batch log lines")
    args = ap.parse_args()
    token = os.environ.get("SKALDBERG_API_TOKEN")

    # Persistent counter state per (metric, labelset).
    counters: dict[tuple, float] = {}
    bucket_state: dict[tuple, float] = {}

    def bump_counter(key: tuple, by: float) -> float:
        new = counters.get(key, 0.0) + by
        counters[key] = new
        return new

    def bump_bucket(job: str, route: str, le: str, by: float) -> float:
        key = (job, route, le)
        new = bucket_state.get(key, 0.0) + by
        bucket_state[key] = new
        return new

    def step():
        ts = now_ms()
        samples = []
        for job in DEFAULTS["jobs"]:
            for route in DEFAULTS["routes_per_job"][job]:
                # Per-route req volume varies a bit per tick. /healthz
                # gets >> traffic so panels show realistic skew.
                base_rate = 200.0 if route == "/healthz" else 5.0
                tick_rate = max(0.5, base_rate + random.uniform(-1.0, 1.5))
                for status in DEFAULTS["statuses"]:
                    portion = tick_rate / len(DEFAULTS["statuses"])
                    cum = bump_counter(("app_requests_total", job, route, status), portion)
                    samples.append({
                        "metric": "app_requests_total",
                        "labels": {"job": job, "route": route, "status": status},
                        "ts": ts,
                        "value": cum,
                    })
                # Histogram. Pretend latency is exponential with a
                # job baseline. Cumulative bucket counts so rate()
                # over the bucket gives per-second per-bucket counts.
                base_latency = 0.02 if job != "ingester" else 0.05
                obs = [base_latency * random.expovariate(1.0)
                       for _ in range(int(tick_rate * 2))]
                for le in DEFAULTS["latency_buckets_seconds"]:
                    if le == "+Inf":
                        cnt = len(obs)
                    else:
                        thr = float(le)
                        cnt = sum(1 for v in obs if v <= thr)
                    cum = bump_bucket(job, route, le, cnt)
                    samples.append({
                        "metric": "app_latency_seconds_bucket",
                        "labels": {"job": job, "route": route, "le": le},
                        "ts": ts,
                        "value": cum,
                    })
        # Gauge: oscillates 0..50 with a slow sine + noise, per job.
        for i, job in enumerate(DEFAULTS["jobs"]):
            t = ts / 1000.0
            base = 25 + 20 * math.sin(t / (60.0 + 10 * i))
            samples.append({
                "metric": "app_inflight",
                "labels": {"job": job},
                "ts": ts,
                "value": max(0.0, base + random.uniform(-3.0, 3.0)),
            })
        status, body = post_ingest(args.host, token, samples)
        return status, body, len(samples)

    print(f"emitting to {args.host} every {args.interval}s — Ctrl-C to stop")
    try:
        while True:
            t0 = time.time()
            try:
                status, body, n = step()
            except urllib.error.URLError as e:
                if not args.quiet:
                    print(f"[{time.strftime('%H:%M:%S')}] connect error: {e}",
                          file=sys.stderr)
                time.sleep(args.interval)
                continue
            if not args.quiet:
                rejected = body.get("rejected", []) if isinstance(body, dict) else []
                print(f"[{time.strftime('%H:%M:%S')}] sent={n} status={status} "
                      f"accepted={body.get('accepted', '?')} rejected={len(rejected)}")
            elapsed = time.time() - t0
            time.sleep(max(0.0, args.interval - elapsed))
    except KeyboardInterrupt:
        print("\nstopped")


if __name__ == "__main__":
    main()
