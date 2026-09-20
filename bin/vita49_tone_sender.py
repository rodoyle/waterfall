#!/usr/bin/env python3
"""Synthetic VITA 49 tone sender for local end-to-end smoke tests.

Emits exactly what `sigproc` emits (post framing-fix), so the middleware, the
STFT and the dBFS calibration can be exercised without the B210 or the cluster:

  * one IF Data packet per UDP datagram,
  * 20-byte header (5 big-endian 32-bit words) + sc16 interleaved I/Q payload,
  * 2048 complex samples per packet -> 2053 words -> 8212 bytes,
  * header 0x10D00804 (type 0x1, TSI=3 free-running, TSF=1 sample counts),
  * phase-continuous tone so consecutive packets do not smear across bins.

The tone is placed at an exact FFT bin, so the receiver can assert the peak bin
and its dBFS independently of the sender.
"""

import argparse
import math
import socket
import struct
import sys
import time

HEADER_BYTES = 20
NOMINAL_SAMPLES = 2048


def build_packet(ts_int, first_sample, bin_index, fft_size, amplitude_sc16, n_samples):
    """One VITA 49 IF Data packet, big-endian, sized from its own word count."""
    total_words = 5 + n_samples  # 2 sc16 components = 1 32-bit word
    buf = bytearray(total_words * 4)

    header = 0x10D00000 | (total_words - 1)  # size field = total words - 1
    struct.pack_into(">I", buf, 0, header)
    struct.pack_into(">I", buf, 4, 0)  # stream id: always 0 today
    struct.pack_into(">I", buf, 8, ts_int)  # timestamp integer seconds
    struct.pack_into(">Q", buf, 12, first_sample)  # fraction: SAMPLE COUNTS

    two_pi = 2.0 * math.pi
    for k in range(n_samples):
        n = first_sample + k
        theta = two_pi * bin_index * n / fft_size
        struct.pack_into(
            ">hh",
            buf,
            HEADER_BYTES + k * 4,
            int(amplitude_sc16 * math.cos(theta)),
            int(amplitude_sc16 * math.sin(theta)),
        )
    return bytes(buf)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=4820)
    ap.add_argument("--count", type=int, default=400, help="packets to send")
    ap.add_argument("--rate", type=float, default=976.5625, help="packets/second")
    ap.add_argument("--bin", type=int, default=512, help="tone bin (512 -> 250 kHz)")
    ap.add_argument("--amp", type=int, default=64 << 8, help="tone amplitude in sc16")
    ap.add_argument("--fft", type=int, default=4096)
    ap.add_argument("--sample-rate", type=float, default=2_000_000.0)
    args = ap.parse_args()

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    target = (args.host, args.port)
    interval = 1.0 / args.rate if args.rate > 0 else 0.0
    # Hoisted and validated once: the sample counter is integer arithmetic.
    # `round` rather than `int` so a fractional rate (e.g. 1_999_999.5 Hz) is
    # accepted rather than truncated, and an invalid rate fails here rather
    # than mid-stream.
    sample_rate = round(args.sample_rate)
    if sample_rate <= 0:
        ap.error("--sample-rate must be a positive number of Hz")
    sent = 0
    next_at = time.monotonic()
    datagram = b""  # bound for the --count 0 edge case

    while sent < args.count:
        counter = sent * NOMINAL_SAMPLES
        datagram = build_packet(
            ts_int=counter // sample_rate,
            first_sample=counter % sample_rate,
            bin_index=args.bin,
            fft_size=args.fft,
            amplitude_sc16=args.amp,
            n_samples=NOMINAL_SAMPLES,
        )
        try:
            sock.sendto(datagram, target)
        except OSError as exc:
            print(f"sender: {exc}", file=sys.stderr)
            return 1
        sent += 1

        next_at += interval
        delay = next_at - time.monotonic()
        if delay > 0:
            time.sleep(delay)

    expected_dbfs = 20.0 * math.log10((args.amp >> 8) / 127.0)
    print(
        f"sender: {sent} packets of {len(datagram)} bytes to "
        f"{args.host}:{args.port} | tone bin {args.bin} "
        f"({args.bin * args.sample_rate / args.fft / 1e3:.1f} kHz) "
        f"| expected peak ~{expected_dbfs:.2f} dBFS"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
