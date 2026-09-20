# waterfall — spectrum analyzer

A packet radio / SDR waterfall spectrum analyzer. End-to-end pipeline:

```
[RF hardware: USRP/SDR via UHD] -- VITA49 UDP --> consumer -- I/Q bytes -->
STFT (CPU rustfft / Apple MLX) -- dB rows --> bridge server (WebSocket+REST) -->
front end (HTML5 Canvas waterfall)
```

See `docs/plans/` for the master plan (`overview.md`) and per-component plans.

## Current status

- **M0 — build stabilized.** `cargo build`/`cargo test` and
  `cargo build --features mlx`/`cargo test --features mlx` all pass.
- **M1 — bridge server + front end live data.** A Rust binary embeds the STFT
  processor and serves real spectrogram rows to the front end over WebSocket
  push + `/chunks` REST fallback, replacing the old random `generateSweep()` demo.
- **M2 — VITA49 consumer + live RF (in progress).** The UDP consumer owns the
  network boundary and feeds real RF to the same STFT. Two tiers, one image:
  `waterfall-consumer` (middleware: UDP → VITA49 → sc16 → i8 → STFT → paced
  `POST /ingest`) and `waterfall-bridge` (web tier: `/ws`, `/chunks`, `/meta`,
  `/stats`, static front end). The synthetic generator still exists for local
  development (`--source synthetic`, the default) but is **not** reachable in the
  deployed path, which runs `--source ingest` and reports `stale` rather than
  fabricating rows.

  The wire format is sc16 (not 8-bit), one packet per datagram, 8212 bytes at
  full rate, header `0x10D00804`; the sender's framing bug that made the first
  send panic is fixed in `sigproc` (commit `7c8f80e`). The RF path keeps **full
  16-bit resolution** (`StftProcessor::push_sc16`, dBFS referenced to 32767)
  because the live 915 MHz band peaks at `|sc16| = 46` — an 8-bit `>> 8`
  downshift would collapse it to `{0, -1}` with a DC bias and flatten the
  waterfall. The 8-bit path is retained and tested. See
  `docs/plans/vita49-consumer.md` for the corrected assumptions and
  `deploy/README.md` for the apply order.
- **M3 / M4** (RF collector, end-to-end hardening) — not started.

## Run the waterfall

### Local development (synthetic feed, no cluster)

```bash
# Build and run the bridge server (default 127.0.0.1:4780):
cargo run

# Options:
cargo run -- --sweep-ms 40          # synthetic STFT cadence
cargo run -- --static /path/dir     # serve static front end from elsewhere
WATERFALL_LISTEN=0.0.0.0:4780 cargo run   # bind any host

# Optional GPU-accelerated STFT on Apple Silicon:
cargo run --features mlx
```

Then open **<http://127.0.0.1:4780/>**. The synthetic source is baseband, so the
axis stays relative Hz.

### Live RF (middleware + web tier)

```bash
# Middleware: binds UDP 4820, publishes to the bridge over POST /ingest.
cargo run --bin waterfall-consumer -- \
  --bridge-url http://127.0.0.1:4780 --hexdump-first 3

# Web tier with no synthetic fallback (this is what the cluster runs).
cargo run -- --source ingest --center-freq 915000000 --sample-rate 2000000
```

With a live RF feed the front end labels an **absolute** frequency axis
(915.000 MHz ± 1 MHz, from `GET /meta`) and shows a status readout with the
producer, packet rate and loss counters.

In-cluster deployment: see **`deploy/README.md`** (image build, manifests, and
the apply order that stops sigproc's one-shot DNS resolution from firing before
the consumer is listening).

### Server endpoints

- `GET /ws` — WebSocket push of `{type:"sweep", bins, samples, intensity[]}`
  dBFS rows as they arrive.
- `GET /chunks?time=<start>&size=<n>` — REST fallback, rows in ascending time.
- `POST /ingest` — row batches from `waterfall-consumer` (the live data path).
- `GET /meta` — RF centre/sample rate, producer, freshness, consumer counters
  (drives the front end's absolute axis and status readout).
- `GET /stats` — ingest counters.
- `*` — static files (`index.html`, `main.js`, `dataClient.js`, `axis.js`).

The consumer additionally serves `GET /stats` and `GET /healthz` on `--stats-listen`
(default `0.0.0.0:4830`).

### Tests

```bash
cargo test                 # unit + integration (VITA49 framing, gaps, tone/dBFS)
cargo test --features mlx  # optional Apple MLX backend
bun test                   # front-end frequency-axis mapping (axis.test.js)
bin/check_mlx.sh           # verify the MLX build environment
```

## Palantir prototype

Open `http://127.0.0.1:4780/palantir.html` after starting the bridge server to try the second frontend view. It is a vanilla WebGL prototype: submit a plain-text question, watch the mist resolve, and use Reset to replay. The mock adapter in `palantir.js` follows `docs/palantir-response.schema.json` (`palantir.v1`), whose ordered items support `image`, `video`, `video-frame`, and `raster-text` entries with `src`, `caption`, and optional `effect`. Replace `mock()` with the backend fetch when the response endpoint is available.

## Front end

Vanilla JS + HTML5 Canvas (no React). `dataClient.js` connects the WebSocket,
falls back to `/chunks` polling, and feeds normalized sweeps to `main.js`, whose
renderer (`appendSweep`/`renderPlot`) is unchanged from its standalone form. A
lightweight controls panel (top-right, collapsible) exposes bin-count, rows,
sweep interval, dB range, linear/log frequency axis, and run/pause — all bound
to the existing `config` object, no framework.
