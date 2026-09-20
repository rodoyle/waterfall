//! VITA 49.0 IF Data packet parsing for the sigproc UDP stream.
//!
//! Pure functions only — no I/O, no sockets, no clocks — so the wire format is
//! testable from hand-built byte fixtures. The receive loop lives in
//! `src/bin/waterfall-consumer.rs`.
//!
//! # The wire format as actually sent by `sigproc`
//!
//! One packet per UDP datagram, big-endian 32-bit words:
//!
//! ```text
//!   offset  size  content
//!   0       4     word 0      packet header
//!   4       4     word 1      stream id            (always 0 today)
//!   8       4     word 2      timestamp seconds    (u32, free-running)
//!   12      8     words 3-4   timestamp fraction   (u64, sample counts)
//!   20      N*4   words 5+    payload: sc16 interleaved [I0,Q0,I1,Q1,...]
//! ```
//!
//! Header word bits: `31-28` packet type (`0x1` = IF Data; context packets are
//! never sent), `27-26` class id, `25-24` trailer, `23-22` TSI (`0x3` = "other",
//! i.e. free-running — NOT UTC), `21-20` TSF (`0x1` = fraction is in sample
//! counts), `19-16` packet count (always 0 today), `15-0` packet size in 32-bit
//! words minus one.
//!
//! A full packet (2048 complex samples) is 2053 words = **8212 bytes** with
//! header `0x10D00804`. `N` is smaller when the UHD read comes up short, so the
//! parser sizes everything from the datagram length and never assumes 8212.
//!
//! # Deviation from `docs/plans/vita49-consumer.md`
//!
//! That plan assumed 8-bit I/Q, context packets, a populated stream id and
//! packet count, and multiple packets per datagram. All four are wrong; this
//! module is authoritative. It was corrected against `sigproc/src/vita49.rs`
//! while fixing the sender's framing bug (the sender previously declared 4101
//! pseudo-words for an 8202-byte datagram whose payload ended at byte 8212).

/// Bytes of VRT header + timestamp fields before the payload (5 x 32-bit words).
pub const HEADER_BYTES: usize = 20;

/// VITA 49.0 packet type for an IF Data packet.
pub const IF_DATA_PACKET_TYPE: u8 = 0x1;

/// Free-running ("other") timestamp integer — not UTC.
pub const TSI_OTHER: u8 = 0x3;

/// Timestamp fraction expressed as a sample count.
pub const TSF_SAMPLE_COUNT: u8 = 0x1;

/// Complex samples in a full-rate sigproc packet (`capture.rs` `SAMPLES_PER_READ`).
pub const NOMINAL_COMPLEX_SAMPLES: usize = 2048;

/// Bytes in a full-rate packet: 20-byte header + 2048 complex sc16 samples.
pub const NOMINAL_DATAGRAM_BYTES: usize = HEADER_BYTES + NOMINAL_COMPLEX_SAMPLES * 4;

/// Nominal sample rate of the sigproc stream (2 MS/s, see its ConfigMap).
pub const NOMINAL_SAMPLE_RATE: f64 = 2_000_000.0;

/// A datagram must carry the 20-byte header and at least one whole complex
/// sample (4 bytes) to be usable. Anything shorter is treated as truncated.
pub const MIN_DATAGRAM_BYTES: usize = HEADER_BYTES + 4;

/// Decoded fields of the packet header word.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Header {
    /// Bits 31-28.
    pub packet_type: u8,
    /// Bit 27 — class id present (the sender never sets it).
    pub class_id_present: bool,
    /// Bit 25 — trailer present (the sender never sets it).
    pub trailer_present: bool,
    /// Bits 23-22.
    pub tsi: u8,
    /// Bits 21-20.
    pub tsf: u8,
    /// Bits 19-16 — sequence count. Always 0 today: do not depend on it.
    pub packet_count: u8,
    /// Bits 15-0 — declared packet length in 32-bit words, minus one.
    pub size_words: u16,
    /// The undecoded header word, for logs and fixtures.
    pub raw: u32,
}

impl Header {
    /// The declared packet length in bytes, if the size field is taken at face
    /// value (`(size_words + 1) * 4`).
    pub fn declared_bytes(&self) -> usize {
        (self.size_words as usize + 1) * 4
    }
}

/// Decode the packet header word.
pub fn parse_header(word: u32) -> Header {
    Header {
        packet_type: (word >> 28) as u8,
        class_id_present: (word >> 27) & 1 == 1,
        trailer_present: (word >> 25) & 1 == 1,
        tsi: ((word >> 22) & 0x3) as u8,
        tsf: ((word >> 20) & 0x3) as u8,
        packet_count: ((word >> 16) & 0xF) as u8,
        size_words: (word & 0xFFFF) as u16,
        raw: word,
    }
}

/// Reasons a datagram cannot be used at all.
///
/// Note what is *not* here: a size field that disagrees with the datagram
/// length is reported on [`Packet::size_field_matches`] rather than rejected.
/// UDP preserves datagram boundaries, so the delivered length is authoritative,
/// and the sender's own size convention has not been stable; refusing the
/// packet would turn a cosmetic field into total data loss.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseError {
    /// Fewer than [`MIN_DATAGRAM_BYTES`] arrived.
    TooShort { len: usize, need: usize },
    /// Not an IF Data packet (context packets are never sent, so this is a
    /// misdirected or foreign datagram).
    UnsupportedPacketType(u8),
    /// User-data trailer bit set; the payload would end with a trailer word this
    /// parser does not know how to strip, so it refuses rather than guessing.
    UnsupportedTrailer,
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ParseError::TooShort { len, need } => {
                write!(f, "datagram too short: {len} bytes, need at least {need}")
            }
            ParseError::UnsupportedPacketType(t) => {
                write!(f, "unsupported VITA49 packet type 0x{t:x} (expected IF Data 0x1)")
            }
            ParseError::UnsupportedTrailer => {
                write!(f, "VITA49 trailer bit set; payload trailer not supported")
            }
        }
    }
}

impl std::error::Error for ParseError {}

/// One parsed IF Data packet, borrowing the payload straight out of the receive
/// buffer (no copy on the hot path).
#[derive(Debug, Clone, Copy)]
pub struct Packet<'a> {
    pub header: Header,
    /// Word 1. Always 0 today: informational only, never dispatch on it.
    pub stream_id: u32,
    /// Word 2 — free-running seconds.
    pub ts_int: u32,
    /// Words 3-4 — fractional timestamp in sample counts.
    pub ts_frac: u64,
    /// Payload bytes, truncated down to whole 32-bit words.
    pub payload: &'a [u8],
    /// Bytes after the last whole payload word (0..=3); discarded, counted.
    pub trailing_bytes: usize,
    /// Whether the header's size field agrees with the delivered datagram.
    pub size_field_matches: bool,
}

impl<'a> Packet<'a> {
    /// Complete complex samples carried by this packet.
    pub fn complex_samples(&self) -> usize {
        self.payload.len() / 4
    }

    /// Number of interleaved i16 components in the payload.
    pub fn components(&self) -> usize {
        self.payload.len() / 2
    }

    /// Monotonic sample counter built from both timestamp fields.
    ///
    /// `ts_int * sample_rate + ts_frac`, matching how the sender encoded it. The
    /// step between consecutive packets is the ONLY loss signal available: the
    /// stream id and packet count are both 0.
    pub fn sample_counter(&self, sample_rate: f64) -> u64 {
        (self.ts_int as u64)
            .saturating_mul(sample_rate as u64)
            .saturating_add(self.ts_frac)
    }

    /// Convert the sc16 payload to the interleaved 8-bit I/Q the STFT consumes.
    ///
    /// Each component is shifted right by 8: sc16 full scale 32767 >> 8 == 127,
    /// which is exactly the reference `waterfall::to_db` calibrates against, so
    /// dBFS stays meaningful and `lib.rs` needs no change.
    ///
    /// The bytes must NOT be reinterpreted as i8 — that yields interleaved
    /// garbage with a plausible-looking noise floor.
    pub fn to_i8(&self) -> Vec<i8> {
        self.payload
            .chunks_exact(2)
            .map(|c| (i16::from_be_bytes([c[0], c[1]]) >> 8) as i8)
            .collect()
    }
}

/// Parse one UDP datagram into an IF Data packet.
///
/// Sizes the payload from the datagram length, floor-aligned to 32-bit words.
pub fn parse_datagram(buf: &[u8]) -> Result<Packet<'_>, ParseError> {
    if buf.len() < MIN_DATAGRAM_BYTES {
        return Err(ParseError::TooShort {
            len: buf.len(),
            need: MIN_DATAGRAM_BYTES,
        });
    }

    let header = parse_header(u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]));

    if header.packet_type != IF_DATA_PACKET_TYPE {
        return Err(ParseError::UnsupportedPacketType(header.packet_type));
    }
    if header.trailer_present {
        return Err(ParseError::UnsupportedTrailer);
    }

    let stream_id = u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);
    let ts_int = u32::from_be_bytes([buf[8], buf[9], buf[10], buf[11]]);
    let ts_frac = u64::from_be_bytes([
        buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18], buf[19],
    ]);

    let available = buf.len() - HEADER_BYTES;
    let payload_len = available & !0x3; // whole 32-bit words only

    Ok(Packet {
        header,
        stream_id,
        ts_int,
        ts_frac,
        payload: &buf[HEADER_BYTES..HEADER_BYTES + payload_len],
        trailing_bytes: available - payload_len,
        size_field_matches: header.declared_bytes() == buf.len(),
    })
}

// ---------------- Loss tracking ----------------

/// One detected discontinuity in the sample counter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Gap {
    /// Sample counter of the packet that closed the gap.
    pub at_counter: u64,
    /// Samples never delivered between the two packets.
    pub missing_samples: u64,
}

/// Tracks the unwrapped sample counter across packets and reports gaps.
///
/// An unmarked dropout on a waterfall looks exactly like a legitimate narrowband
/// signal, so gaps are counted and surfaced instead of being interpolated over.
#[derive(Debug, Default, Clone)]
pub struct GapTracker {
    last: Option<u64>,
    /// Expected step between consecutive full packets, in samples.
    expected_step: u64,
    pub gaps: u64,
    pub missing_samples: u64,
    pub last_step: u64,
    /// Packets that arrived with a non-monotonic (reordered/stale) counter.
    pub out_of_order: u64,
}

impl GapTracker {
    /// `expected_step` is the nominal samples per packet (2048 for a full read).
    pub fn new(expected_step: u64) -> Self {
        GapTracker {
            expected_step,
            ..Default::default()
        }
    }

    /// Observe a packet's sample counter. Returns `Some(Gap)` when the step
    /// exceeded the expected spacing, i.e. samples were lost or never sent.
    ///
    /// A *smaller* step is not loss (overlapping frames, or a short read), and a
    /// counter that went backwards is counted separately rather than treated as
    /// a gap of unbounded size.
    pub fn observe(&mut self, counter: u64, samples_in_packet: u64) -> Option<Gap> {
        let Some(previous) = self.last else {
            self.last = Some(counter);
            return None;
        };

        if counter < previous {
            self.out_of_order += 1;
            self.last = Some(counter);
            return None;
        }

        let step = counter - previous;
        self.last_step = step;
        self.last = Some(counter);

        // Allow one sample of slack for float/truncation effects at the sender,
        // and treat an unpopulated packet count as "no information", not loss.
        if step > self.expected_step + 1 {
            let missing = step - self.expected_step;
            self.gaps += 1;
            self.missing_samples += missing;
            return Some(Gap {
                at_counter: counter,
                missing_samples: missing,
            });
        }

        // A short read legitimately advances less than the expected step.
        let _ = samples_in_packet;
        None
    }

    /// Current sample counter, if any packet has been seen.
    pub fn last_counter(&self) -> Option<u64> {
        self.last
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE_RATE: f64 = NOMINAL_SAMPLE_RATE;

    /// Hand-built fixture: a full 2048-sample packet, bytes written explicitly
    /// (not by the sender's builder) so the test proves the layout independently.
    fn full_packet(ts_int: u32, ts_frac: u64) -> Vec<u8> {
        let samples = NOMINAL_COMPLEX_SAMPLES;
        let total_words = 5 + samples; // 2053
        let mut buf = vec![0u8; total_words * 4]; // 8212

        // 0x1 << 28 | 0x3 << 22 | 0x1 << 20 | (total_words - 1)
        let header: u32 = 0x10D0_0000 | (total_words as u32 - 1);
        buf[0..4].copy_from_slice(&header.to_be_bytes());
        buf[4..8].copy_from_slice(&0u32.to_be_bytes()); // stream id (0 today)
        buf[8..12].copy_from_slice(&ts_int.to_be_bytes());
        buf[12..20].copy_from_slice(&ts_frac.to_be_bytes());

        // Ramp payload: I = i, Q = -i, so layout errors are visible.
        for i in 0..samples {
            let (re, im) = (i as i16, -(i as i16));
            let off = HEADER_BYTES + i * 4;
            buf[off..off + 2].copy_from_slice(&re.to_be_bytes());
            buf[off + 2..off + 4].copy_from_slice(&im.to_be_bytes());
        }
        buf
    }

    #[test]
    fn header_bit_fields_are_decoded() {
        let h = parse_header(0x10D0_0804);
        assert_eq!(h.packet_type, IF_DATA_PACKET_TYPE);
        assert!(!h.class_id_present);
        assert!(!h.trailer_present);
        assert_eq!(h.tsi, TSI_OTHER);
        assert_eq!(h.tsf, TSF_SAMPLE_COUNT);
        assert_eq!(h.packet_count, 0);
        assert_eq!(h.size_words, 0x0804);
        assert_eq!(h.declared_bytes(), NOMINAL_DATAGRAM_BYTES);
        assert_eq!(h.raw, 0x10D0_0804);
    }

    #[test]
    fn header_class_id_and_trailer_bits_are_surfaced() {
        let h = parse_header(0x1 << 28 | 1 << 27 | 1 << 25);
        assert!(h.class_id_present);
        assert!(h.trailer_present);
    }

    #[test]
    fn full_packet_parses_with_twenty_byte_header() {
        let datagram = full_packet(7, 1024);
        let pkt = parse_datagram(&datagram).expect("full packet parses");

        assert_eq!(datagram.len(), NOMINAL_DATAGRAM_BYTES);
        assert_eq!(pkt.header.raw, 0x10D0_0804);
        assert_eq!(pkt.header.size_words, 2052);
        assert!(pkt.size_field_matches);
        assert_eq!(pkt.stream_id, 0);
        assert_eq!(pkt.ts_int, 7);
        assert_eq!(pkt.ts_frac, 1024);
        assert_eq!(pkt.payload.len(), 8192);
        assert_eq!(pkt.complex_samples(), NOMINAL_COMPLEX_SAMPLES);
        assert_eq!(pkt.trailing_bytes, 0);

        // First and last complex samples of the ramp survive big-endian decoding.
        // Note -1 >> 8 == -1 (arithmetic shift keeps the sign), which is the
        // sc16 → i8 behaviour the dBFS calibration depends on.
        let iq = pkt.to_i8();
        assert_eq!(iq.len(), NOMINAL_COMPLEX_SAMPLES * 2);
        assert_eq!(&iq[0..4], &[0, 0, 0, -1], "i=0 -> 0, i=1 -> (0, -1)");
        let last = iq.len() - 2;
        assert_eq!(iq[last], (2047i16 >> 8) as i8);
        assert_eq!(iq[last + 1], ((-2047i16) >> 8) as i8);
    }

    #[test]
    fn sign_and_scale_of_the_sc16_to_i8_conversion_is_exact() {
        // Full-scale magnitudes must map onto the 127 reference `to_db` expects.
        let mut buf = full_packet(0, 0);
        buf[HEADER_BYTES..HEADER_BYTES + 2].copy_from_slice(&32767i16.to_be_bytes());
        buf[HEADER_BYTES + 2..HEADER_BYTES + 4].copy_from_slice(&(-32768i16).to_be_bytes());
        let pkt = parse_datagram(&buf).unwrap();
        let iq = pkt.to_i8();
        assert_eq!(iq[0], 127, "32767 >> 8 == 127 == i8 full scale");
        assert_eq!(iq[1], -128, "-32768 >> 8 == -128");
    }

    #[test]
    fn short_packet_is_sized_from_the_datagram_not_assumed_full() {
        // Three complex samples: 5 + 3 = 8 words = 32 bytes, size field 7.
        let mut buf = vec![0u8; HEADER_BYTES + 3 * 4];
        buf[0..4].copy_from_slice(&(0x10D0_0000u32 | 7).to_be_bytes());
        buf[8..12].copy_from_slice(&99u32.to_be_bytes());
        buf[12..20].copy_from_slice(&2048u64.to_be_bytes());
        for i in 0..3 {
            let off = HEADER_BYTES + i * 4;
            buf[off..off + 2].copy_from_slice(&(256i16 * (i as i16 + 1)).to_be_bytes());
        }

        let pkt = parse_datagram(&buf).expect("short packet parses");
        assert_eq!(pkt.header.size_words, 7);
        assert!(pkt.size_field_matches);
        assert_eq!(pkt.complex_samples(), 3);
        assert_eq!(pkt.to_i8()[0], 1); // 256 >> 8
    }

    #[test]
    fn legacy_8202_byte_layout_is_tolerated_but_flagged() {
        // The pre-fix sender declared 4101 pseudo-words for an 8202-byte
        // datagram whose payload ran to byte 8212. datagram length wins.
        let mut buf = vec![0u8; 8202];
        buf[0..4].copy_from_slice(&0x10D0_1004u32.to_be_bytes());
        buf[12..20].copy_from_slice(&1500u64.to_be_bytes());

        let pkt = parse_datagram(&buf).expect("legacy datagram still parses");
        assert_eq!(pkt.header.size_words, 4100);
        assert!(!pkt.size_field_matches, "declared 16404 bytes, delivered 8202");
        assert_eq!(pkt.payload.len(), 8180, "whole words only");
        assert_eq!(pkt.complex_samples(), 2045);
        assert_eq!(pkt.trailing_bytes, 2);
        assert_eq!(pkt.ts_frac, 1500);
    }

    #[test]
    fn truncated_datagrams_are_rejected_without_panicking() {
        for len in 0..MIN_DATAGRAM_BYTES {
            let buf = vec![0u8; len];
            let err = parse_datagram(&buf).expect_err("must reject");
            assert_eq!(err, ParseError::TooShort { len, need: MIN_DATAGRAM_BYTES });
        }

        // 27 bytes was the plan's "reject" threshold for a 28-byte header; the
        // real header is 20 bytes, so 27 bytes carries one usable sample and the
        // 3 leftover bytes are discarded rather than failing the packet.
        let mut buf = vec![0u8; 27];
        buf[0..4].copy_from_slice(&(0x10D0_0000u32 | 6).to_be_bytes());
        let pkt = parse_datagram(&buf).expect("20-byte header + 7 payload bytes");
        assert_eq!(pkt.complex_samples(), 1);
        assert_eq!(pkt.trailing_bytes, 3);
        assert!(!pkt.size_field_matches);
    }

    #[test]
    fn foreign_and_trailerized_datagrams_are_rejected() {
        let mut ctx = full_packet(0, 0);
        // Context packet (type 0x4) must never be accepted as I/Q.
        ctx[0] = 0x40;
        assert_eq!(
            parse_datagram(&ctx).unwrap_err(),
            ParseError::UnsupportedPacketType(0x4)
        );

        let mut trailer = full_packet(0, 0);
        let header = u32::from_be_bytes([trailer[0], trailer[1], trailer[2], trailer[3]]);
        trailer[0..4].copy_from_slice(&(header | (1 << 25)).to_be_bytes());
        assert_eq!(
            parse_datagram(&trailer).unwrap_err(),
            ParseError::UnsupportedTrailer
        );
    }

    #[test]
    fn timestamp_fraction_rolls_over_into_seconds() {
        // Fraction near the top of one second, then a carry: the unwrapped
        // counter must stay monotonic across the boundary.
        let before_datagram = full_packet(100, 1_999_000);
        let after_datagram = full_packet(101, 1024);
        let before = parse_datagram(&before_datagram).unwrap();
        let after = parse_datagram(&after_datagram).unwrap();

        let c0 = before.sample_counter(SAMPLE_RATE);
        let c1 = after.sample_counter(SAMPLE_RATE);
        assert!(c1 > c0, "counter must be monotonic across ts_int carry");
        assert_eq!(c0, 100 * 2_000_000 + 1_999_000);
        assert_eq!(c1, 101 * 2_000_000 + 1024);
        // The step is real, visible loss: 1000 samples remained in the first
        // second when the previous packet was stamped, plus 1024 into the next.
        let step = c1 - c0;
        assert_eq!(step, (2_000_000 - 1_999_000) + 1024);
    }

    #[test]
    fn counter_wraps_ts_frac_at_the_sample_rate() {
        // ts_frac is 0..sample_rate-1 and must be interpreted in sample counts,
        // never as a sub-second fraction.
        let datagram = full_packet(0, 1_999_999);
        let pkt = parse_datagram(&datagram).unwrap();
        assert_eq!(pkt.sample_counter(SAMPLE_RATE), 1_999_999);
        assert!(pkt.ts_frac < NOMINAL_SAMPLE_RATE as u64);
    }

    #[test]
    fn gap_tracker_reports_only_real_discontinuities() {
        let mut t = GapTracker::new(NOMINAL_COMPLEX_SAMPLES as u64);

        assert_eq!(t.observe(0, 2048), None);
        assert_eq!(t.observe(2048, 2048), None, "contiguous");
        assert_eq!(t.observe(4096, 2048), None, "contiguous");

        // Unsigned shift in 1 sample (sender truncation): not loss.
        assert_eq!(t.observe(6143, 2048), None);
        assert_eq!(t.gaps, 0);

        // Short read: step below nominal is not loss either.
        assert_eq!(t.observe(7167, 1024), None);
        assert_eq!(t.gaps, 0);

        // A real dropout of three packets.
        let gap = t.observe(7167 + 2048 * 4, 2048).expect("gap detected");
        assert_eq!(gap.missing_samples, 2048 * 3);
        assert_eq!(t.gaps, 1);
        assert_eq!(t.missing_samples, 2048 * 3);

        // Backwards counter: counted, never treated as a huge gap.
        assert_eq!(t.observe(100, 2048), None);
        assert_eq!(t.out_of_order, 1);
        assert_eq!(t.gaps, 1);

        assert_eq!(t.last_counter(), Some(100));
    }

    #[test]
    fn gap_tracker_counts_a_non_contiguous_injected_sequence() {
        // Feed a deliberately gapped sequence: 0, 2048, [drop 2], 8192.
        let mut t = GapTracker::new(2048);
        for counter in [0u64, 2048, 8192] {
            t.observe(counter, 2048);
        }
        assert_eq!(t.gaps, 1);
        assert_eq!(t.missing_samples, 4096);
    }
}
