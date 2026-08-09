# Waterfall Project — Overview, Status & Milestones

This is the master plan for the four components of the waterfall spectrum-analyzer
system. Each component has its own plan file in `docs/plans/`:

| # | Component | Plan file | Status |
| --- | ----------- | ----------- | -------- |
| 1 | Waterfall plot **front end** | `frontend.md` | **Partially implemented** |
| 2 | Rust + **UHD RF collector** | `rf-collector.md` | **Not started** |
| 3 | UDP **VITA49 consumer** (8-bit I/Q) | `vita49-consumer.md` | **Not started** |
| 4 | **MLX-accelerated STFT** processor | `stft-mlx.md` | **Partially implemented** |

## Data flow (target)

```
 [RF hardware: USRP/SDR via UHD]
            │  complex baseband I/Q, 8-bit interleaved
            ▼
  (2) RF collector (Rust + UHD lib)          ── local / remote ──┐
            │  VITA49 frames over UDP                            │
            ▼                                                     │
  (3) VITA49 consumer (Rust, UDP socket)                           │
            │  8-bit I/Q bytes                                    │
            ▼                                                     │
  (4) MLX STFT processor (Rust, rustfft fallback)                  │
            │  magnitude in dB (frame × 4096 bins)                │
            ▼                                                     │
  [bridge server: WebSocket push + REST /chunks]                  │
            │                                                     │
            ▼                                                     ▼
  (1) Front end (HTML5 Canvas waterfall)  ─────────────────────────┘
```

The pipeline is currently **split where the bridge server should be**: the STFT
processor and the front end each exist in isolation, and neither has been connected
to the other or to the RF data path.

## Current status (verified against the repo, not aspirational)

**1. Front end — partial, works standalone.**

- `main.js` (vanilla JS, no React) is a complete HTML5-Canvas waterfall renderer:
  off-screen sweep buffer, scrolling, heatmap color mapping (−100..0 dB), axes and
  color bar. Runs as a static page via `index.html`.
- Data source is **random on-the-fly generation** only — there is no WebSocket
  client, no REST polling, no server it talks to.
- The `docs/plan/` files describe a **React** app with proxied `App.jsx`; this does
  **not** match the actual vanilla-JS implementation. Treat them as out-of-date.

**2. RF collector — not started.** No Rust code exists. SDR prerequisites are
installed on this machine (`uhd`, `hackrf`, `soapysdr`, `librtlsdr` via Homebrew).
Build-env helper `bin/check_mlx.sh` exists for the MLX side only.

**3. VITA49 consumer — not started.** No UDP socket code, no VITA49 frame parser.

**4. MLX STFT — partial, MLX path compiles.**

- `src/lib.rs` implements the STFT (4096-point FFT, 1024-sample hop / 75% overlap)
  with two backends: CPU `rustfft` + `rayon`, and Apple GPU via `mlx-sys`.
- Unit tests cover I/Q conversion, output shape, sine-tone bin placement, and edge
  cases. `cargo build --features mlx` and `cargo test --features mlx` pass.
- **BUG (must fix first):** `cargo build` with **no features fails**. The
  `FftBackend::Mlx` enum variant and its match arm in `process()` reference the
  `#[cfg(feature = "mlx")]`-gated `mlx_integration` module unconditionally
  (E0433 unresolved module + E0282 type annotation). Fix: gate the `Mlx` variant
  and its match arm on `#[cfg(feature = "mlx")]`.
- No binary, no server, no connection to the consumer or front end.

## Cross-cutting gaps (what links everything together)

These are the pieces that exist in the plan but are **absent from the codebase**:

1. **A Rust binary / entry point.** `src/lib.rs` is a library; nothing runs the STFT
   against live data. Need a `main.rs` (likely a workspace `bin`).
2. **The bridge server.** Something must accept STFT output and serve it to the
   front end — WebSocket push + `GET /chunks?time=<start>&size=...`. The likely home
   is a small Bun/Node or Rust HTTP+WS server (the `index.ts` Bun entry exists as a
   stub printing `Hello via Bun!`).
3. **Packet → STFT wiring.** The consumer's 8-bit I/Q bytes must be chunked,
   windowed, and fed to the STFT; the complex spectrogram must be converted to
   dB magnitude (power) before the front end displays it.
4. **The collector↔consumer transport contract.** VITA49 frame format, UDP
   port/address, sample rate, and byte order must be pinned down (see
   `vita49-consumer.md`).

## Milestones

- **M0 — Stabilize the build (immediate):** fix the no-feature compile so `cargo
  build` (CPU) and `cargo build --features mlx` both succeed; run the test suite.
- **M1 — Bridge server + front end live data:** a working server that streams
  current STFT frames to the existing `main.js`, replacing random generation.
- **M2 — VITA49 consumer:** UDP receiver that parses VITA49 frames, extracts 8-bit
  I/Q, feeds the STFT. Test with a synthetic sender first.
- **M3 — RF collector:** Rust bindings to UHD, streaming captured I/Q as VITA49
  over UDP (local or remote host).
- **M4 — End-to-end:** USRP → VITA49 UDP → consumer → MLX STFT → bridge → front end
  renders a live waterfall. Bench CPU vs MLX throughput.

## Repo hygiene

- The git repo has **no commits yet** and everything is staged. Consider an initial
  commit after M0 stabilizes the build.
- `docs/plan/` (singular) is a stale, React-flavored duplicate; consolidate into
  `docs/plans/` and delete or archive the old directory.
- `src/lib.rs` (library) and the front end (`main.js`) live in one crate; plan a
  layout that keeps the Rust signal pipeline and the bridge server coherent (see
  `rf-collector.md` / `stft-mlx.md` for the proposed crate structure).
