# Front End — Waterfall Plot Visualizer

**Plan file:** `docs/plans/frontend.md`
**Status:** Partially implemented — standalone renderer works, no live data path.

## What exists today (grounded in `main.js`, `index.html`)

- **Vanilla JS + HTML5 Canvas**, single-page, loaded from `index.html` with
  `<script src="main.js">`. No React, no build step needed to run.
- **Rendering model (already correct for the job):**
  - Off-screen `plotCanvas` sized `numBins × numRows` (256 × 220), re-drawn by
    shifting the image up one row and stamping the newest sweep at the bottom
    (`appendSweep` + `drawSweepIntoSweep`).
  - A full-window `renderPlot()` blits the off-screen buffer with
    `imageSmoothingEnabled = false` (crisp pixel-art spectrogram), draws axes,
    frequency labels, and a dB color bar (−100..0 dB).
  - Color mapping: a 5-stop heatmap (blue→cyan→green→yellow→red) via
    `intensityToColor`.
  - Continuous update via `setInterval(config.sweepIntervalMs)`.
- **Data source today:** `generateSweep()` produces **random synthetic sweeps** for
  demo purposes only. There is **no client** for a WebSocket or REST feed, and no
  server.

## Target architecture

```
[bridge server: WebSocket push + GET /chunks?time=<start>&size=...]
        │
        │ WebSocket: 'sweep' messages { bins, samples, intensity[] }   (push)
        │ REST fallback: GET /chunks?time=&size= -> same shape          (poll)
        ▼
   Front end:
   +-----------------+
   | dataClient.js   |  WS connect / auto-reconnect / poll fallback
   +-----------------+            │
        │ Float32Array intensity[] (per row of dB values)
        ▼
   +-----------------+
   | appendSweep()   |  existing off-screen-buffer + blit logic — UNCHANGED
   +-----------------+
        ▼
   renderPlot()  (existing canvas + axes + color bar)
```

The key change is only at the **data source boundary**: replace `generateSweep()`
with rows pushed from the live feed. All rendering logic already exists and should
**not** be rewritten into React.

## Message contract (proposed)

- **WebSocket** message per sweep (JSON):

  ```json
  { "type": "sweep", "bins": 4096, "samples": 128, "intensity": [ -70.2, -68.1, ... ] }
  ```

  `intensity` is `Float32Array`-shaped dB magnitude aligned to `config.numBins` /
  the frequency axis. On startup the server may send `{ "type": "snapshot" }` with
  the full history so the screen fills without waiting.
- **REST fallback:** `GET /chunks?time=<start>&size=<n>` returns a JSON array of
  sweeps in ascending time order; the client polls when WS is down.

## Implementation steps

1. **Add `dataClient.js`** with:
   - A `WebSocket` wrapper: connect to `ws://<host>/ws`, buffer/miss-handling,
     exponential backoff reconnection, and a health check.
   - Graceful fallback to `setInterval` polling of `/chunks` when WS fails, plus a
     visible status indicator ("live" vs "polling" vs "offline").
   - Normalization of incoming `intensity[]` to the `Float32Array` shape
     `appendSweep` expects.
2. **Toggle data source in `main.js`:** gate `generateSweep()` behind a
   `config.demo` flag; when a live client is present, feed `dataClient` sweeps into
   `appendSweep` and drive redraw from the data arrival (or keep the interval as a
   clock and just consume freshest frame).
3. **Add controls (`config` panel):** bin-count, rows, sweep interval, dB range,
   frequency axis scale (linear/log), and a run/pause toggle. Lightweight — reuse
   the existing `config` object, no framework.
4. **Frequency axis alignment:** ensure the front end's `freqMin/freqMax`
   corresponds to the STFT's bin-to-frequency mapping documented in `stft-mlx.md`.

## Verification

- Serve the page, run the bridge server, confirm rows advance from live frames and
  the "demo" marker disappears.
- Kill the WS server: client auto-falls back to polling, then reconnects when back.
- Browser DevTools → Network: confirm WS handshake and `200/204` on `/chunks`.
- Resize the window: `onResize` re-renders without losing the waterfall history.
- Compare per-pixel output with a known injected tone (see STFT sine test) to
  confirm bin alignment end to end.

## Pitfalls

- **Do not port to React** just because the old `docs/plan/` says so — the canvas
  renderer is complete and state-light. React adds cost without benefit here.
- Keep `imageSmoothingEnabled = false`; blurriness silently kills spectrogram
  readability.
- WebSocket backpressure: if the server outpaces render, drop or coalesce frames
  rather than queueing unboundedly and leaking memory.
- The spectrogram is 4096 bins today but the canvas is 256 `numBins`. Decide the
  downsampling point (server-side aggregate or `ctx.drawImage` scaling) early.
