#!/usr/bin/env python3
"""End-to-end smoke for Phase 4 /api/v1/ingest.

Flow:
  1. Start server with a fresh wal dir + short flush interval.
  2. POST a mix of valid and invalid samples; assert per-sample report.
  3. Wait for flush.
  4. SELECT against the Iceberg-backed catalog and verify rows came through.
  5. Restart server (same wal dir, fresh in-process catalog) → confirm
     server log shows wal replay path runs.

Note: Phase 4 uses an in-process MemoryCatalog, so commits don't survive a
restart. The test only asserts that WAL replay code path exercises
correctly across a restart, not that data persists.
"""
import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request

SERVER_BIN = "/home/claude/skaldberg-server/target/debug/skaldberg-server"


def start_server(wal_dir, port, flush_secs=2):
    env = os.environ.copy()
    env["RUST_LOG"] = "info,skaldberg_server=info"
    log_path = f"/tmp/server-ingest-p4-{port}.log"
    proc = subprocess.Popen(
        [
            SERVER_BIN,
            "--wal-dir", wal_dir,
            "--bind", f"127.0.0.1:{port}",
            "--flush-interval-secs", str(flush_secs),
        ],
        stdout=open(log_path, "w"),
        stderr=subprocess.STDOUT,
        env=env,
    )
    for _ in range(50):
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{port}/healthz", timeout=0.3) as r:
                if r.status == 200:
                    return proc, log_path
        except (urllib.error.URLError, ConnectionResetError):
            time.sleep(0.2)
    proc.kill()
    raise RuntimeError(f"server didn't come up; see {log_path}")


def stop_server(proc):
    try:
        proc.send_signal(signal.SIGTERM)
        proc.wait(timeout=3)
    except Exception:
        proc.kill()


def post_json(url, body):
    req = urllib.request.Request(
        url, data=json.dumps(body).encode(),
        headers={"content-type": "application/json"}, method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status, json.load(r)
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read().decode("utf-8", "replace"))


def query_sql(port, sql):
    s, r = post_json(f"http://127.0.0.1:{port}/api/v1/sql", {"sql": sql})
    assert s == 200, f"SQL failed: {s} {r}"
    return r


def now_ms():
    return int(time.time() * 1000)


def main():
    passed, failed = [], []

    def check(name, cond, detail=""):
        (passed if cond else failed).append((name, detail))
        print(f"  {'PASS' if cond else 'FAIL'}  {name}{'' if cond else '  ' + detail}")

    tmpdir = tempfile.mkdtemp(prefix="skaldberg-p4-ingest-")
    print(f"wal dir: {tmpdir}")
    port = 8200
    proc, log_path = start_server(tmpdir, port, flush_secs=2)
    try:
        ts = now_ms()

        # ---------- T1: mix valid + invalid ----------
        body = {
            "samples": [
                {"metric": "http_requests_total",
                 "labels": {"job": "api", "status": "200"},
                 "ts": ts, "value": 100.0},
                {"metric": "http_requests_total",
                 "labels": {"job": "api", "status": "500"},
                 "ts": ts, "value": 5.0},
                {"metric": "cpu_usage_seconds",
                 "labels": {"job": "api", "instance": "i-1"},
                 "ts": ts, "value": 42.5},
                # Invalid: metric name with space
                {"metric": "bad metric", "labels": {}, "ts": ts, "value": 1.0},
                # Invalid: reserved label
                {"metric": "m2", "labels": {"__name__": "x"},
                 "ts": ts, "value": 1.0},
            ]
        }
        s, r = post_json(f"http://127.0.0.1:{port}/api/v1/ingest", body)
        check("T1 ingest 200", s == 200, f"got {s}")
        check("T1 accepted == 3", r.get("accepted") == 3,
              f"got accepted={r.get('accepted')}")
        check("T1 rejected == 2", len(r.get("rejected", [])) == 2,
              f"got {r.get('rejected')}")
        idx = sorted(x["index"] for x in r.get("rejected", []))
        check("T1 rejected indices [3, 4]", idx == [3, 4], f"got {idx}")

        # ---------- T2: empty samples ----------
        s, r = post_json(f"http://127.0.0.1:{port}/api/v1/ingest", {"samples": []})
        check("T2 empty array → 200 accepted=0",
              s == 200 and r.get("accepted") == 0, f"got {s} {r}")

        # ---------- T3: wait for flush, query ----------
        time.sleep(3.5)
        result = query_sql(port,
            "SELECT COUNT(*) AS n FROM iceberg.skaldberg.series")
        n = result["rows"][0]["n"]
        check("T3 series count == 3", n == 3, f"got {n}")

        result = query_sql(port,
            "SELECT COUNT(*) AS n FROM iceberg.skaldberg.samples")
        n = result["rows"][0]["n"]
        check("T3 samples count == 3", n == 3, f"got {n}")

        # ---------- T4: labels MAP integrity ----------
        result = query_sql(port,
            "SELECT metric_name, labels FROM iceberg.skaldberg.series "
            "WHERE metric_name = 'http_requests_total' "
            "ORDER BY series_id")
        check("T4 found 2 http_requests_total rows", len(result["rows"]) == 2,
              f"got {len(result['rows'])}")
        for row in result["rows"]:
            labels = row.get("labels", {})
            null_keys = [k for k, v in labels.items() if v is None]
            check(f"T4 labels {labels} have no null keys", not null_keys,
                  f"null keys: {null_keys}")
            check(f"T4 labels {labels} have job",
                  "job" in labels, f"got {labels}")

        # ---------- T5: partition pruning works ----------
        # All samples have the same ts → all in same day partition.
        from datetime import datetime, timezone
        day = datetime.fromtimestamp(ts / 1000, tz=timezone.utc).date()
        result = query_sql(port,
            f"SELECT COUNT(*) AS n FROM iceberg.skaldberg.samples "
            f"WHERE timestamp >= TIMESTAMP '{day} 00:00:00' "
            f"AND timestamp < TIMESTAMP '{day} 23:59:59'")
        check("T5 partition-bound query returns 3", result["rows"][0]["n"] == 3,
              f"got {result['rows']}")

        # ---------- T6: WAL replay path on restart ----------
        # Send a sample, then kill the server before flush. The new process
        # opens the same WAL dir; we expect to see "replayed wal records"
        # in its log.
        ts2 = now_ms()
        s, r = post_json(f"http://127.0.0.1:{port}/api/v1/ingest", {
            "samples": [{"metric": "preflush", "labels": {"k": "v"},
                         "ts": ts2, "value": 99.0}],
        })
        check("T6 pre-restart ingest accepted",
              s == 200 and r["accepted"] == 1, f"got {s} {r}")
        # Kill RIGHT NOW so flush hasn't run.
        proc.kill()
        proc.wait(timeout=3)

        port2 = port + 1
        proc2, log2 = start_server(tmpdir, port2, flush_secs=60)
        try:
            time.sleep(0.5)
            with open(log2) as f:
                txt = f.read()
            check("T6 restart log shows wal replay",
                  "replayed wal records" in txt,
                  f"log tail: {txt[-400:]}")
        finally:
            stop_server(proc2)
    finally:
        try:
            stop_server(proc)
        except Exception:
            pass

    print()
    print(f"=== summary === passed: {len(passed)}  failed: {len(failed)}")
    if failed:
        print("\n=== server log tail ===")
        with open(log_path) as f:
            for line in f.readlines()[-30:]:
                print(f"  {line.rstrip()}")
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
