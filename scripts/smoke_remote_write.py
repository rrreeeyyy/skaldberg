#!/usr/bin/env python3
"""End-to-end smoke for /api/v1/write (Prometheus Remote-Write 1.0).

We construct a WriteRequest by hand using the protobuf wire format
encoder. This avoids generating Python bindings from a .proto file —
the wire format is small and stable for this message shape.

Verifies:
  - 204 No Content on a valid request
  - flush + SQL query confirms the data landed correctly
  - __name__ becomes the metric, other labels become labels
  - bad protobuf returns 400
  - empty WriteRequest still returns 204
"""
import os
import pathlib
import signal
import struct
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request

import snappy

# Resolve to `<repo>/target/debug/skaldberg-server` by default. Override
# with `SKALDBERG_BIN=...` (e.g. for release builds or CI artifacts).
SERVER_BIN = os.environ.get(
    "SKALDBERG_BIN",
    str(pathlib.Path(__file__).resolve().parent.parent / "target" / "debug" / "skaldberg-server"),
)

# ----------------- protobuf wire-format encoders -----------------
#
# Prometheus 1.0 schema:
#   message WriteRequest { repeated TimeSeries timeseries = 1; }
#   message TimeSeries   { repeated Label labels = 1;
#                          repeated Sample samples = 2; }
#   message Label        { string name = 1; string value = 2; }
#   message Sample       { double value = 1; int64 timestamp = 2; }
#
# Wire types we need:
#   0 = varint (int64)
#   1 = 64-bit fixed (double)
#   2 = length-delimited (string, bytes, embedded message)


def varint(n):
    out = bytearray()
    while True:
        if n < 0x80:
            out.append(n)
            return bytes(out)
        out.append((n & 0x7F) | 0x80)
        n >>= 7


def zigzag(n):
    return (n << 1) ^ (n >> 63)


def tag(field_num, wire_type):
    return varint((field_num << 3) | wire_type)


def encode_string(field_num, s):
    b = s.encode("utf-8")
    return tag(field_num, 2) + varint(len(b)) + b


def encode_int64(field_num, v):
    # Prometheus's `Sample.timestamp` is plain int64 (not sint64), so it
    # uses the regular varint encoding. Negative values would expand to 10
    # bytes; for our test data ts > 0 so this is fine.
    return tag(field_num, 0) + varint(v if v >= 0 else (v + (1 << 64)))


def encode_double(field_num, v):
    return tag(field_num, 1) + struct.pack("<d", v)


def encode_message(field_num, body):
    return tag(field_num, 2) + varint(len(body)) + body


def encode_label(name, value):
    return encode_string(1, name) + encode_string(2, value)


def encode_sample(timestamp_ms, value):
    return encode_double(1, value) + encode_int64(2, timestamp_ms)


def encode_timeseries(labels, samples):
    # labels and samples are lists of pre-encoded bytes (label fragment
    # body + sample fragment body).
    body = b""
    for lbl in labels:
        body += encode_message(1, lbl)
    for s in samples:
        body += encode_message(2, s)
    return body


def encode_write_request(timeseries_bodies):
    body = b""
    for ts in timeseries_bodies:
        body += encode_message(1, ts)
    return body


def build_request(metrics):
    """metrics is a list of (name, [(label_k, label_v)...], [(ts_ms, value)...])"""
    ts_bodies = []
    for name, labels, samples in metrics:
        label_bodies = [encode_label("__name__", name)]
        for k, v in labels:
            label_bodies.append(encode_label(k, v))
        sample_bodies = [encode_sample(t, v) for t, v in samples]
        ts_bodies.append(encode_timeseries(label_bodies, sample_bodies))
    return encode_write_request(ts_bodies)


# ----------------- server lifecycle -----------------

def start_server(data_dir, port, flush_secs=2):
    env = os.environ.copy()
    env["LD_LIBRARY_PATH"] = "/tmp/libduckdb"
    env["RUST_LOG"] = "info,skaldberg_server=info"
    log_path = f"/tmp/server-rw-{port}.log"
    proc = subprocess.Popen(
        [SERVER_BIN, "--wal-dir", data_dir, "--bind", f"127.0.0.1:{port}",
         "--flush-interval-secs", str(flush_secs)],
        stdout=open(log_path, "w"), stderr=subprocess.STDOUT, env=env,
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


def post_remote_write(port, body_bytes):
    compressed = snappy.compress(body_bytes)
    req = urllib.request.Request(
        f"http://127.0.0.1:{port}/api/v1/write",
        data=compressed,
        headers={
            "Content-Type": "application/x-protobuf",
            "Content-Encoding": "snappy",
            "X-Prometheus-Remote-Write-Version": "0.1.0",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status, r.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()


def post_raw(port, body_bytes, content_encoding="snappy"):
    """Send raw bytes without snappy-compressing first — used to test bad-input handling."""
    req = urllib.request.Request(
        f"http://127.0.0.1:{port}/api/v1/write",
        data=body_bytes,
        headers={
            "Content-Type": "application/x-protobuf",
            "Content-Encoding": content_encoding,
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status, r.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()


def query_sql(port, sql):
    import json
    body = json.dumps({"sql": sql}).encode()
    req = urllib.request.Request(
        f"http://127.0.0.1:{port}/api/v1/sql",
        data=body,
        headers={"content-type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.load(r)


# ----------------- tests -----------------

def main():
    passed, failed = [], []

    def check(name, cond, detail=""):
        (passed if cond else failed).append((name, detail))
        print(f"  {'PASS' if cond else 'FAIL'}  {name}{'' if cond else '  ' + detail}")

    tmpdir = tempfile.mkdtemp(prefix="skaldberg-rw-")
    print(f"data dir: {tmpdir}")
    port = 8100
    proc, log_path = start_server(tmpdir, port, flush_secs=1)
    try:
        anchor = int(time.time() * 1000) - 60_000

        # ---------- T1: minimal valid request ----------
        body = build_request([
            ("http_requests_total",
             [("job", "api"), ("status", "200")],
             [(anchor, 100.0), (anchor + 1000, 105.0)]),
        ])
        status, _ = post_remote_write(port, body)
        check("T1 single-series 2-sample → 204", status == 204, f"got {status}")

        # ---------- T2: multiple series, different metrics ----------
        body = build_request([
            ("cpu_usage_seconds",
             [("job", "api"), ("instance", "i-1")],
             [(anchor, 50.0)]),
            ("memory_resident_bytes",
             [("job", "api"), ("instance", "i-1")],
             [(anchor, 1024.0 * 1024 * 200)]),
        ])
        status, _ = post_remote_write(port, body)
        check("T2 multi-series → 204", status == 204, f"got {status}")

        # ---------- T3: empty WriteRequest is still 204 ----------
        status, _ = post_remote_write(port, b"")
        check("T3 empty body → 204", status == 204, f"got {status}")

        # ---------- T4: garbage body → 400 ----------
        # Send raw bytes that are not valid snappy.
        status, body = post_raw(port, b"\xff\xfe\xfd not snappy")
        check("T4 garbage snappy → 400", status == 400, f"got {status} {body!r}")

        # ---------- T5: valid snappy of garbage protobuf → 400 ----------
        garbage = snappy.compress(b"\xff" * 32)
        status, body = post_raw(port, garbage)
        check("T5 garbage protobuf → 400", status == 400, f"got {status} {body!r}")

        # ---------- T6: wait for flush, query data ----------
        time.sleep(1.5)
        result = query_sql(port, "SELECT COUNT(*) AS n FROM iceberg.skaldberg.samples")
        # T1: 2 samples, T2: 2 samples → 4 total. Plus the stub, but the
        # stub has 0 rows. So COUNT(*) == 4.
        n = result["rows"][0]["n"]
        check("T6 samples count == 4", n == 4, f"got {n}")

        result = query_sql(port, "SELECT COUNT(*) AS n FROM iceberg.skaldberg.series")
        n = result["rows"][0]["n"]
        check("T6 series count == 3", n == 3, f"got {n}")

        # ---------- T7: __name__ became metric_name, not a label ----------
        result = query_sql(port,
            "SELECT metric_name, labels FROM iceberg.skaldberg.series ORDER BY metric_name")
        rows = result["rows"]
        check("T7 first metric is cpu_usage_seconds",
              rows[0]["metric_name"] == "cpu_usage_seconds", f"got {rows[0]}")
        check("T7 metric_name not present as a label",
              "__name__" not in rows[0]["labels"], f"got labels={rows[0]['labels']}")
        check("T7 expected labels present",
              rows[0]["labels"] == {"job": "api", "instance": "i-1"},
              f"got {rows[0]['labels']}")

        # ---------- T8: SQL join across samples + series (sk_metric equivalent) ----------
        result = query_sql(port,
            "SELECT sa.timestamp, sa.value FROM iceberg.skaldberg.samples sa "
            "JOIN iceberg.skaldberg.series s ON sa.series_id = s.series_id "
            "WHERE s.metric_name = 'http_requests_total' "
            "ORDER BY sa.timestamp")
        check("T8 join returns 2 rows for http_requests_total",
              len(result["rows"]) == 2, f"got {len(result['rows'])}")
        if len(result["rows"]) == 2:
            check("T8 first value == 100.0",
                  abs(result["rows"][0]["value"] - 100.0) < 1e-9,
                  f"got {result['rows'][0]['value']}")
            check("T8 second value == 105.0",
                  abs(result["rows"][1]["value"] - 105.0) < 1e-9,
                  f"got {result['rows'][1]['value']}")

        # ---------- T9: NaN/Inf samples are silently rejected by validate(),
        #               but the request still 204s ----------
        body = build_request([
            ("ok_metric", [], [(anchor, 1.0)]),
            ("bad_nan", [], [(anchor, float("nan"))]),
        ])
        status, _ = post_remote_write(port, body)
        check("T9 mixed valid+NaN still 204", status == 204, f"got {status}")

        time.sleep(1.5)
        result = query_sql(port, "SELECT COUNT(*) AS n FROM iceberg.skaldberg.series WHERE metric_name='ok_metric'")
        check("T9 ok_metric series exists", result["rows"][0]["n"] == 1)
        result = query_sql(port, "SELECT COUNT(*) AS n FROM iceberg.skaldberg.series WHERE metric_name='bad_nan'")
        check("T9 bad_nan series does NOT exist (rejected)",
              result["rows"][0]["n"] == 0, f"got {result['rows']}")

        # ---------- T10: series with no __name__ is silently dropped ----------
        # We can't easily build this with our helper since it always inserts
        # __name__. Hand-craft a single TimeSeries with only "job" label.
        labels = [encode_label("job", "x")]
        samples = [encode_sample(anchor, 1.0)]
        ts_body = encode_timeseries(labels, samples)
        wr_body = encode_write_request([ts_body])
        status, _ = post_remote_write(port, wr_body)
        check("T10 no-__name__ series → still 204 (drop, not error)",
              status == 204, f"got {status}")

    finally:
        stop_server(proc)

    print()
    print(f"=== summary === passed: {len(passed)}  failed: {len(failed)}")
    if failed:
        print("\n=== server log tail ===")
        with open(log_path) as f:
            for line in f.readlines()[-40:]:
                print(f"  {line.rstrip()}")
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
