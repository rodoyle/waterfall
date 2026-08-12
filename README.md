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
- **M1 — bridge server + front end live data (this milestone).** A Rust binary
  embeds the STFT processor and serves real spectrogram rows to the front end
  over WebSocket push + `/chunks` REST fallback, replacing the old random
  `generateSweep()` demo. Until the VITA49 consumer exists (M2), the STFT is fed
  by a synthetic I/Q generator, so the waterfall shows real STFT output.
- **M2 / M3 / M4** (VITA49 consumer, RF collector, end-to-end) — not started.

## Run the waterfall (M1)

```bash
# Build and run the bridge server (default 127.0.0.1:4780):
cargo run

# Options:
cargo run -- --sweep-ms 40          # STFT cadence (ranges/sec feed)
cargo run -- --static /path/dir     # serve static front end from elsewhere
WATERFALL_LISTEN=0.0.0.0:4780 cargo run   # bind any host

# Optional GPU-accelerated STFT on Apple Silicon:
cargo run --features mlx
```

Then open **<http://127.0.0.1:4780/>** — the waterfall is live. The status in the
time label (`live`/`polling`/`offline`) reflects the data transport.

### Server endpoints

- `GET /ws` — WebSocket push of `{type:"sweep", bins, samples, intensity[]}`
  dBFS rows as the STFT produces them.
- `GET /chunks?time=<start>&size=<n>` — REST fallback, rows in ascending time.
- `*` — static files (`index.html`, `main.js`, `dataClient.js`).

### Tests

```bash
cargo test               # CPU + rayon backend
cargo test --features mlx  # optional Apple MLX backend
bin/check_mlx.sh         # verify the MLX build environment
```

## Front end

Vanilla JS + HTML5 Canvas (no React). `dataClient.js` connects the WebSocket,
falls back to `/chunks` polling, and feeds normalized sweeps to `main.js`, whose
renderer (`appendSweep`/`renderPlot`) is unchanged from its standalone form. A
lightweight controls panel (top-right, collapsible) exposes bin-count, rows,
sweep interval, dB range, linear/log frequency axis, and run/pause — all bound
to the existing `config` object, no framework.
