# MLX STFT Processor — Rust signal back end

**Plan file:** `docs/plans/stft-mlx.md`
**Status:** Partially implemented. The STFT core + both FFT backends exist in
`src/lib.rs`; the no-feature build is broken and there is no binary/server/UI
connection.

## Role in the pipeline

```
(3) VITA49 consumer ── 8-bit I/Q bytes ─▶ (4) STFT processor ─▶ bridge server ─▶ (1) front end
                                          4096-bin FFT, 1024-hop,
                                          mlx / rustfft fallback
```

The processor turns the consumer's 8-bit I/Q into a magnitude-in-dB spectrogram
(frame × 4096 bins) that the front end renders as a waterfall row.

## What exists today (grounded in `src/lib.rs`)

- **STFT core:** constant `FFT_SIZE = 4096`, `HOP_SIZE = 1024` (75% overlap).
  Frames the input and runs one 4096-point forward FFT per frame in parallel via
  `rayon`.
- **Two backends behind one `FftBackend` enum:**
  - CPU: `rustfft` planner, `plan_fft_forward(FFT_SIZE)`.
  - MLX: `mlx_integration::MlxFft` (module `#[cfg(feature = "mlx")]`) — packs the
    `Complex<f32>` buffer into an MLX `COMPLEX64` array, calls `mlx_fft_fft`,
    evals, copies back. Falls back to CPU if MLX setup fails.
- **I/Q conversion:** `bytes_to_complex` decodes interleaved signed 8-bit I/Q
  (`[I0,Q0,...]`) to `Complex<f32>`. Window application is a no-op placeholder
  (assumed applied upstream).
- **Magnitude in tests** is computed as `norm_sqr()`; the sine test verifies
  tone-to-bin mapping.
- **Tests (present):** I/Q round-trip, output shape, sine-tone bin placement,
  random-data-not-zeros, edge-case input length.

## Current bugs / gaps (must fix first)

1. **Default `cargo build` fails** (verified): the `FftBackend::Mlx` enum variant
   and its match arm in `process()` reference the `mlx_integration` module
   unconditionally, but that module is `#[cfg(feature = "mlx")]`.
   - Error 1: `E0433` "unresolved module or unlinked crate `mlx_integration`" at
     the enum declaration.
   - Error 2: `E0282` "type annotations needed" on the `process()` match arm's
     `|e| e.to_string()` closure.
   - Fix: gate the `Mlx(mlx_integration::MlxFft)` variant and the `Mlx` match arm
     on `#[cfg(feature = "mlx")]` exactly as the `plan()` impls already are.
     `cargo build` (CPU) and `cargo build --features mlx` must both compile, and
     `cargo test` must pass for both.
2. **Library-only, no binary:** `src/lib.rs` is a lib with a `#[cfg]`-guarded
   `main()`. There is no production entry point that consumes I/Q and emits
   spectrogram frames.
3. **No magnitude/dB conversion exposed:** only raw `Complex<f32>` comes out. The
   front end needs dB power per bin. Add a `spectrogram_to_dB` (log-magnitude,
   with a floor) and define ADC full-scale ↔ dBFS mapping.
4. **No streaming/backpressure interface:** `stft` takes a full input `&[i8]`
   (batch). For a continuous waterfall you need an incremental framing/hop-window
   API that emits one row per hop with overlap carry-over.
5. **Default `FFT_SIZE`/hop hard-coded as constants.** Keep them configurable
   (per stream context from the VITA49 consumer) without losing the optimized
   constants as defaults.

## Target interface

```
struct StftProcessor {
    fft_size: usize,   // 4096 default
    hop: usize,        // 1024 default
    backend: FftBackend,
}
impl StftProcessor {
    fn push(&mut self, iq: &[i8]) -> Vec<Row>;       // incremental, overlap carry
}
struct Row { sample: u64, bins: Vec<Complex<f32>> }   // or Vec<f32> dB directly
// plus: fn to_db(bins) -> Vec<f32>
```

## Implementation steps

1. **Fix the feature-gate bug** (above) so both builds/tests pass. Add a CI/script
   check that a no-feature build stays green.
2. Add a **dB conversion** (`20*log10(|x|)` with floor) and unit-test it against
   the front end's −100..0 display range.
3. Turn the batch `stft` into an **incremental hop-window stream** that keeps the
   overlap buffer across `push` calls, so the consumer can feed it continuously.
4. Expose the processor through a Rust binary (or a shared lib the bridge server
   links) — the bridge server then owns WebSocket/REST to the front end (see
   `overview.md` cross-cutting gaps).
5. Add a **benchmark**: `cargo bench`-style harness comparing CPU-backend vs
   MLX-backend FFT throughput/frame on this Mac.
6. Wire end to end with a synthetic sender → consumer → processor → front end
   before touching UHD (Milestone M2/M4).

## Verification

- `cargo build` and `cargo build --features mlx` both succeed.
- `cargo test` and `cargo test --features mlx` pass (full suite).
- Sine-tone test confirms correct bin after switching from batch to incremental.
- Backpressure: feeding faster-than-realtime I/Q produces the expected number of
  frames with correct hop spacing and no silent frame loss.
- Benchmark shows MLX-backend throughput ≥ CPU for reasonable frame sizes (justify
  keeping MLX as default on Apple Silicon).
- End-to-end: a known tone appears at the expected bin/position in the waterfall.

## Pitfalls

- **The MLX path is `unsafe`** (raw `mlx_sys` pointers): any missed size check
  (already present: `buffer.len() != n`) or ABI mismatch corrupts memory. Keep the
  existing checks and add a test that runs under the MLX feature.
- `unsafe impl Sync for FftBackend` assumes the MLX default device/stream are safe
  across rayon threads — revisit if you ever switch to per-thread streams.
- MLX `COMPLEX64` ↔ `num_complex::Complex<f32>` copy must stay explicit/byte-for-
  byte (it already is) — don't "simplify" it to a cast.
- A 75% overlap + 4096-bin windows at high sample rates is heavy; choose frame
  size/hop to match the UDP arrival rate and UI refresh (see `frontend.md`,
  `vita49-consumer.md`).
- Don't let the batch `stft`'s `Vec` allocations per frame hurt throughput; reuse
  buffers in the streaming version.
