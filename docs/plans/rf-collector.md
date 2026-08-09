# RF Collector — Rust + UHD

**Plan file:** `docs/plans/rf-collector.md`
**Status:** Not started — no code exists. Target that collects the RF signal and
emits VITA49 frames over UDP.

## Role in the pipeline

```
[SDR / USRP hardware]
        │  complex baseband, 8-bit interleaved I/Q
        ▼
(2) RF collector (Rust + UHD)  ─── VITA49 frames over UDP ───▶ (3) VITA49 consumer
```

The collector is the only component that talks to hardware (or a recorded source).
It streamlines: configure the device → tune/gain/sample-rate → read complex
baseband I/Q → packetize as VITA49 → send UDP to a configurable host/port. The
consumer (this repo or a remote host) receives those packets.

## What exists today

- **None.** `src/` contains only `lib.rs` (the STFT library). No `main.rs`, no UHD
  bindings, no networking code.
- Machine prerequisites **are installed**: `uhd`, `hackrf`, `soapysdr`,
  `librtlsdr`, `cubicsdr` via Homebrew. `bin/check_mlx.sh` covers the MLX build
  environment only; it does **not** validate UHD.

## UHD access from Rust (decision needed)

UHD ships as C++ (with a Python API); there is no blessed first-party Rust crate.
The viable approaches, in increasing effort/maturity trade-off:

1. **`uhd-sys`/`rust-uhd`** — community FFI bindings. Fastest to stand up but
   historically stale relative to UHD releases; verify against the installed UHD
   version (`uhd_config_info` / `uhd_find_devices`).
2. **C shim + `#[link(name = "uhd")]`** — write a thin C shim over libuhd's
   streaming API, expose it to Rust via an FFI module. More control, more work.
3. **`uhd::usrp::multi_usrp` via `rust-uhd`** and fall back to invoking
   `uhd_rx_file`/`uhd_rx_cfile` as a subprocess for a v0 — simplest to validate
   end-to-end while real bindings mature.
4. **Alternative source for v0:** a **recorded I/Q file** (e.g. output of
   `uhd_rx_cfile` or `hackrf_transfer`) or a loopback generator, so the rest of the
   pipeline (consumer → STFT → front end) can be built and verified before the UHD
   bindings are complete. Recommended to de-risk.

**Recommendation:** build the VITA49 consumer + STFT + front end against a file or
synthetic source first (Milestone M2/M4), and add the collector last (M3), so the
collector only has to emit real packets into an already-verified consumer.

## Proposed crate layout

Introduce a Cargo workspace so the library, the collector, and the consumer are
separate binaries sharing one repo:

```
Cargo.toml            <- [workspace] members = ["crates/collector", "crates/consumer", "crates/stft"]
src/lib.rs            <- move to crates/stft (existing STFT, see stft-mlx.md)
crates/collector/
  src/main.rs          <- CLI: --device, --rate, --gain, --freq, --host, --port
  src/vita49_tx.rs     <- packetize I/Q + transmit timestamp into VITA49 frames
  src/uhd.rs          <- FFI/bindings layer (per decision above)
crates/consumer/       <- UDP VITA49 consumer (see vita49-consumer.md)
crates/stft/           <- moved STFT library + mlx
```

(If a full workspace is too much for v0, a single `src/bin/` split in one crate is
acceptable; the workspace is the cleaner end state.)

## VITA49 transmission contract

The collector emits what the consumer expects — pin this down now so both sides
match (see `vita49-consumer.md` for the parser side):

- **UDP** transport, configurable destination host/port (default
  `127.0.0.1:48000` or similar).
- **VITA49 (VRT) framing:** 32-bit word-aligned; VITA49 packet header + stream id
  - payload. Use **vector data** (more recent tags) or classic if/then packets —
  decide once, document in one place.
- **Payload:** 8-bit signed I/Q interleaved `[I0,Q0,I1,Q1,...]` — this is exactly
  the format `bytes_to_complex` already consumes in `src/lib.rs`.
- **Fields the consumer needs:** stream id / context (sample rate, num bins,
  scaling), and per-packet frame count or timestamp so the consumer can detect
  gaps. Include a context packet (VITA49 context type) at stream start.

## Implementation steps

1. Decide the UHD binding approach (above) and stand up a minimal
   "configure device + read N I/Q samples" spike.
2. Implement the VITA49 transmit packetizer with a unit test that round-trips
   against the consumer's parser.
3. CLI + config: `--freq`, `--sample-rate`, `--gain`, `--frame-size`, `--host`,
   `--port`, `--source=file|usrp|noise`.
4. Streaming loop: read I/Q → packetize → `sendto` UDP; handle backpressure
   (drop frames vs block) and continuous (non-stop) acquisition.
5. Add the file/noise sources so the collector can run without hardware.

## Non-functional requirements

- **Throughput:** full-rate I/Q streaming over UDP; the consumer must keep up.
  Reconcile frame size with the STFT's 4096-size / 1024-hop framing
  (`stft-mlx.md`) so frames align to STFT windows.
- **Timestamping:** sequence numbers / timestamps in the stream so the consumer
  can reassemble and detect dropped packets.
- Graceful shutdown (Ctrl-C drains and flushes the stream).

## Verification

- Unit test: packetizer output parses cleanly with the consumer's decoder
  (round-trip, no crate boundary shared — both sides test against the same fixture
  bytes).
- With a file source: `collector --source=file` → consumer → STFT produces the
  expected spectrogram (compare to the STFT sine-tone test).
- With real hardware: tune to a known signal (e.g. broadcast FM clip) and confirm
  the waterfall shows the expected band structure.
- Latency/throughput: measure sustained Mp/s of I/Q through the UDP path.

## Pitfalls

- UHD crate staleness vs installed UHD version — verify ABI before committing to
  a binding.
- Byte order and alignment: VITA49 is big-endian, 32-bit word aligned; an
  off-by-one in packet length silently corrupts every frame. Round-trip tests are
  mandatory.
- 8-bit I/Q scaling/overflow: define the ADC full-scale ↔ dBFS mapping now so the
  front end's dB range (−100..0) is meaningful.
- Do not block the STFT on slow UDP; separate the network receive thread from the
  processing thread.
