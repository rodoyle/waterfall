#!/usr/bin/env bash
# Local end-to-end smoke test — no cluster, no B210.
#
# Proves the whole software path before anything is deployed:
#
#   vita49_tone_sender.py --UDP:4820--> waterfall-consumer --POST /ingest-->
#   waterfall-bridge --/chunks--> assertions
#
# The sender emits a phase-continuous tone at an exact FFT bin, so the last
# assertion can check the peak BIN and its dBFS as they arrive over HTTP — the
# same numbers the live RF path must produce.
set -uo pipefail
cd "$(dirname "$0")/.."

BRIDGE_PORT=${BRIDGE_PORT:-4780}
UDP_PORT=${UDP_PORT:-4820}
STATS_PORT=${STATS_PORT:-4830}
PACKETS=${PACKETS:-400}
TONE_BIN=${TONE_BIN:-512}

echo "==> building release binaries (the deployed configuration)"
cargo build --release --bins >/dev/null 2>&1 || { echo "FAIL: cargo build --release --bins"; exit 1; }

WATERFALL_LISTEN="127.0.0.1:${BRIDGE_PORT}" ./target/release/waterfall \
    --source ingest --static . --center-freq 915000000 --sample-rate 2000000 \
    > /tmp/wf-bridge.log 2>&1 &
BRIDGE_PID=$!

./target/release/waterfall-consumer \
    --listen "127.0.0.1:${UDP_PORT}" \
    --bridge-url "http://127.0.0.1:${BRIDGE_PORT}" \
    --stats-listen "127.0.0.1:${STATS_PORT}" \
    --publish-hz 30 --hexdump-first 2 --rcvbuf 8388608 \
    > /tmp/wf-consumer.log 2>&1 &
CONSUMER_PID=$!

cleanup() {
    kill "$BRIDGE_PID" "$CONSUMER_PID" 2>/dev/null
    wait "$BRIDGE_PID" "$CONSUMER_PID" 2>/dev/null
}
trap cleanup EXIT

sleep 2

echo "==> sending ${PACKETS} tone packets at bin ${TONE_BIN} to UDP ${UDP_PORT}"
python bin/vita49_tone_sender.py --port "$UDP_PORT" --count "$PACKETS" \
    --rate 976.5625 --bin "$TONE_BIN" --amp 16384 || { echo "FAIL: sender"; exit 1; }
sleep 2

echo "==> asserting the path end to end"
BRIDGE_PORT="$BRIDGE_PORT" STATS_PORT="$STATS_PORT" TONE_BIN="$TONE_BIN" python - <<'PY'
import json
import math
import os
import sys
import urllib.request

bridge = int(os.environ["BRIDGE_PORT"])
stats_port = int(os.environ["STATS_PORT"])
tone_bin = int(os.environ["TONE_BIN"])
fft_size = 4096

failures = []


def get(port, path):
    with urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=10) as response:
        return json.load(response)


def check(label, condition, detail=""):
    print(f"  {'ok  ' if condition else 'FAIL'}  {label}{' — ' + detail if detail else ''}")
    if not condition:
        failures.append(label)


meta = get(bridge, "/meta")
check("meta.source == vita49", meta["source"] == "vita49", meta["source"])
check("meta.stale is false", meta["stale"] is False, f"stale_ms={meta['stale_ms']}")
check("meta.center_hz == 915 MHz", meta["center_hz"] == 915_000_000, str(meta["center_hz"]))
check("meta.sample_rate_hz == 2 MS/s", meta["sample_rate_hz"] == 2_000_000, str(meta["sample_rate_hz"]))
check("meta.rows_ingested > 0", meta["rows_ingested"] > 0, str(meta["rows_ingested"]))
check("meta.bins == 4096", meta["bins"] == fft_size, str(meta["bins"]))

rows = get(bridge, "/chunks?size=5")
check("chunks returned rows", len(rows) > 0, f"{len(rows)} rows")
if rows:
    row = rows[-1]
    check("row has 4096 bins", len(row["intensity"]) == fft_size, str(len(row["intensity"])))
    bins = row["intensity"]
    spread = max(bins) - min(bins)
    check("intensity is non-flat (>20 dB spread)", spread > 20.0, f"{spread:.1f} dB")

    half = bins[: fft_size // 2]
    peak_bin = max(range(len(half)), key=lambda i: half[i])
    peak_dbfs = half[peak_bin]
    check(f"peak bin == {tone_bin}", peak_bin == tone_bin, f"got {peak_bin}")
    expected_dbfs = 20.0 * math.log10(64 / 127.0)
    check(
        f"peak dBFS ~= {expected_dbfs:.2f} (amp 64 of 127)",
        abs(peak_dbfs - expected_dbfs) <= 1.5,
        f"got {peak_dbfs:.2f}",
    )
    # 512 * (2e6 / 4096) = 250 kHz
    check(
        "peak bin == 250 kHz",
        abs(peak_bin * 2_000_000 / fft_size - 250_000) < 1.0,
        f"{peak_bin * 2_000_000 / fft_size / 1e3:.1f} kHz",
    )

consumer = get(stats_port, "/stats")
check("consumer received packets", consumer["packets_received"] > 0, str(consumer["packets_received"]))
check(
    "consumer analyzed every packet",
    consumer["packets_analyzed"] == consumer["packets_received"],
    f"{consumer['packets_analyzed']}/{consumer['packets_received']}",
)
check("no gaps over a contiguous feed", consumer["gaps"] == 0, str(consumer["gaps"]))
check("no analysis drops at release speed", consumer["stft_dropped"] == 0, str(consumer["stft_dropped"]))
check("no parse errors", consumer["parse_errors"] == 0, str(consumer["parse_errors"]))
check("size field matched on every packet", consumer["size_field_mismatch"] == 0, str(consumer["size_field_mismatch"]))
check("rows produced", consumer["rows_produced"] > 0, str(consumer["rows_produced"]))
check("rows published to bridge", consumer["rows_published"] > 0, str(consumer["rows_published"]))
check("no bridge errors", consumer["bridge_errors"] == 0, str(consumer["bridge_errors"]))
check("SO_RCVBUF granted", consumer["rcvbuf_bytes"] > 0, str(consumer["rcvbuf_bytes"]))

print()
if failures:
    print(f"LOCAL SMOKE: FAIL ({len(failures)}): {', '.join(failures)}")
    sys.exit(1)
print("LOCAL SMOKE: PASS")
PY
STATUS=$?

echo
echo "==> consumer log (SPIKE + stats)"
grep -E "SPIKE|consumer stats|bound UDP|SO_RCVBUF" /tmp/wf-consumer.log | head -20
echo
echo "==> bridge log"
head -5 /tmp/wf-bridge.log

exit $STATUS
