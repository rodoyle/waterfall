# VITA49 Consumer — Rust UDP → 8-bit I/Q

**Plan file:** `docs/plans/vita49-consumer.md`
**Status:** Not started — the UDP socket + VITA49 parser do not exist. The first
built half of the signal-processing back end.

## Role in the pipeline

```
(2) RF collector (local or remote host)  ── VITA49 over UDP ──┐
                                                              ▼
(3) VITA49 consumer (Rust)  ── 8-bit I/Q bytes ──▶ (4) MLX STFT processor
```

The consumer owns the **network boundary**: it binds a UDP socket, parses VITA49
packets, validates stream/context, and hands clean interleaved 8-bit I/Q bytes to
the STFT processor. "Local or remote host" simply means the UDP source address is
configurable — the code is identical either way.

## What exists today

- **None.** There is no networking, no parser, no `main.rs`. The only relevant
  existing piece is `bytes_to_complex` + the STFT framing in `src/lib.rs`, which
  are downstream consumers of the I/Q bytes this module would produce.

## What the consumer must do

1. **Bind & receive** UDP on a configurable address/port (default
   `0.0.0.0:48000` or a chosen port), a dedicated receive thread/async task.
2. **Parse VITA49 (VRT) frames:**
   - Parse the 32-bit VITA49 packet header (packet type, count, class, fields:
     stream id present, packet count, sequence, TSI/TSF).
   - Consume optional fields per the header: **stream id**, **TSI (timestamps)**,
     **TSF (fractional timestamps)**.
   - Parse **context packets** (sample rate, number of channels/bits, scaling,
     data format) and build an internal context for the stream.
   - Extract the **payload**: 8-bit signed interleaved I/Q →
     `[I0,Q0, I1,Q1, ...]` (this is precisely the input `bytes_to_complex`
     expects).
   - Handle word alignment padding and **multiple packets per UDP datagram**
     (common), plus a datagram possibly splitting one packet across datagrams
     (rare; decide support level).
3. **Reassemble / sequence-check:** use the per-packet count / sequence /
   timestamps to detect lost packets; report gaps. Do **not** silently fabricate
   data on loss — surface it (metric/log) so a dropped RF frame isn't mistaken for
   a blackband.
4. **Normalize & hand off:** convert the raw stream context (any bit depth, scale)
   into the canonical 8-bit interleaved I/Q buffer the STFT consumes, with the
   scale documented (ADC full-scale ↔ dB in `stft-mlx.md` / `rf-collector.md`).

## Message / framing contract (shared, single source of truth)

Write the framing rules **once** and have both collector and consumer test against
the same byte fixtures. Minimum to agree on (proposed values, adjust as needed):

- UDP datagram → one or more **VITA49 packets**, each 32-bit word aligned.
- Packet payload: `[I0,Q0, I1,Q1, ...]`, 8-bit **signed** I/Q.
- A **context packet** at stream start carrying sample rate + bits + scale so the
  consumer self-configures.
- Per-packet **stream id** so multiple streams don't cross.
- **Packet count / sequence** for loss detection.
- Big-endian VITA49 framing (VITA specifies big-endian header words).

## Implementation steps

1. Spike: bind a UDP socket, `recvfrom`, hex-dump incoming datagrams against a
   fixture capture — confirms the byte layout before any parsing logic.
2. Implement the VITA49 header/field parser (pure functions + unit tests; no I/O).
   Test by hand-building packet bytes in `#[cfg(test)]`.
3. Add payload extraction + context-packet handling (bit depth, scale).
4. Wire the receive loop to a simple internal channel/Air-Queue that delivers
   8-bit I/Q buffers downstream (so it doesn't block on the STFT).
5. Add loss/gap detection and a CLI (`--port`, `--host`, verbosity, `--dump`
   hex mode for debugging).
6. Build a **synthetic sender** (test-only) that emits the same fixtures as a UDP
   stream, so end-to-end tests run without an RF collector.

## Verification

- Unit: hand-built VITA49 packets parse to the expected I/Q byte sequence.
- Round-trip: the consumer decodes the collector's packetizer output byte-for-byte
  (shared fixture, no shared crate) — see `rf-collector.md`.
- Synthetic sender → consumer → STFT: the sine-tone bin appears in the spectrogram
  (reuse the STFT sine test at the integration level).
- Loss injection: drop N% of datagrams and confirm loss is reported (not assumed
  zero).
- Sustained throughput: confirm the consumer keeps pace with full-rate input.

## Pitfalls

- **VITA49 layout is easy to get subtly wrong** (header bit fields, padding,
  endianness). Always test against captured bytes, not just self-generated ones.
- **Backpressure:** if the STFT is slower than the UDP arrival, buffer or drop
  deliberately — never let the receiver thread block the processing (and never
  swallow frames silently).
- **Multiple packets per datagram** is the normal case for high-rate I/Q; a parser
  that assumes exactly-one-packet-per-datagram will drop most of the stream.
- Do not hardcode `127.0.0.1`; "remote host" is an explicit requirement — the
  bind/source address must be config.
- Timestamp/sequence bookkeeping pays off; without it a small dropout looks like a
  legitimate narrowband feature on the waterfall.
