//! The datagram → rows pipeline, shared by the consumer binary and the tests.
//!
//! I/O policy (binding sockets, threading, pacing, HTTP) stays in
//! `src/bin/waterfall-consumer.rs`; everything that decides *what a datagram
//! means* lives here so an integration test can drive it over a real UDP socket.

use crate::vita49::{self, Packet, ParseError};
use crate::{Row, StftProcessor, FFT_SIZE, HOP_SIZE};

/// What one datagram produced.
#[derive(Debug)]
pub struct Outcome {
    /// Frames completed by this datagram (0..n, in time order).
    pub rows: Vec<Row>,
    /// Why the datagram was unusable, if it was.
    pub parse_error: Option<ParseError>,
    /// Complex samples carried (0 when malformed).
    pub complex_samples: usize,
    /// Unwrapped sample counter (0 when malformed).
    pub sample_counter: u64,
    /// Whether the header size field agreed with the delivered length.
    pub size_field_matches: bool,
}

impl Outcome {
    fn malformed(error: ParseError) -> Self {
        Outcome {
            rows: Vec::new(),
            parse_error: Some(error),
            complex_samples: 0,
            sample_counter: 0,
            size_field_matches: false,
        }
    }
}

/// Stateful VITA49 → STFT pipeline.
///
/// Holds the FFT overlap buffer. Not `Sync`: it is driven from a single analysis
/// thread, which is what keeps the FFT off the socket.
///
/// Gap tracking is deliberately NOT here. This processor only sees the datagrams
/// that survived the analysis queue, so counting discontinuities here would
/// report the consumer's own deliberate drops as RF loss — a misdiagnosis that
/// makes a healthy link look lossy. Loss is tracked where every datagram is
/// observed: the receive loop.
pub struct PacketProcessor {
    processor: StftProcessor,
    sample_rate: f64,
    malformed: u64,
    samples_seen: u64,
    abs_sum: u64,
    peak_abs: i16,
}

impl PacketProcessor {
    /// `sample_rate` is needed to unwrap the timestamp pair into a sample counter.
    pub fn new(sample_rate: f64) -> Self {
        PacketProcessor {
            processor: StftProcessor::new(FFT_SIZE, HOP_SIZE),
            sample_rate,
            malformed: 0,
            samples_seen: 0,
            abs_sum: 0,
            peak_abs: 0,
        }
    }

    /// Parse one datagram, convert sc16 → i8, and advance the STFT.
    pub fn observe(&mut self, datagram: &[u8]) -> Outcome {
        let packet: Packet<'_> = match vita49::parse_datagram(datagram) {
            Ok(p) => p,
            Err(e) => {
                self.malformed += 1;
                return Outcome::malformed(e);
            }
        };

        let complex_samples = packet.complex_samples();
        let counter = packet.sample_counter(self.sample_rate);

        // Full 16-bit resolution, dBFS referenced to the sc16 full scale
        // (32767). Live sigproc RF peaks around ±46, so the i8 `>> 8` path would
        // quantize it to {0, -1} and flatten the waterfall; see
        // `to_db_with_reference` and the quiet-tone test in tests/tone_db.rs.
        let iq = packet.payload_i16();
        for &component in &iq {
            self.samples_seen += 1;
            self.abs_sum += component.unsigned_abs() as u64;
            self.peak_abs = self.peak_abs.max(component.saturating_abs());
        }
        let rows = self.processor.push_sc16(&iq);

        Outcome {
            rows,
            parse_error: None,
            complex_samples,
            sample_counter: counter,
            size_field_matches: packet.size_field_matches,
        }
    }

    /// Datagrams rejected as malformed.
    pub fn malformed(&self) -> u64 {
        self.malformed
    }

    /// Largest absolute component seen, in sc16 units.
    ///
    /// Surfaced because it is the number that decides whether an 8-bit
    /// downshift would be lossy: live RF around ±46 collapses under `>> 8`.
    pub fn peak_abs(&self) -> i16 {
        self.peak_abs
    }

    /// Components observed (2 per complex sample).
    pub fn samples_seen(&self) -> u64 {
        self.samples_seen
    }

    /// Mean absolute component level, in sc16 units.
    pub fn mean_abs(&self) -> f64 {
        if self.samples_seen == 0 {
            return 0.0;
        }
        self.abs_sum as f64 / self.samples_seen as f64
    }

    /// Samples consumed by the STFT so far.
    pub fn samples_consumed(&self) -> u64 {
        self.processor.samples_consumed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vita49::{GapTracker, NOMINAL_COMPLEX_SAMPLES, NOMINAL_SAMPLE_RATE};

    /// Minimal independent packet builder (mirrors the corrected sigproc framing).
    fn packet(ts_int: u32, ts_frac: u64, samples: &[(i16, i16)]) -> Vec<u8> {
        let total_words = 5 + samples.len();
        let mut buf = vec![0u8; total_words * 4];
        let header = 0x10D0_0000u32 | (total_words as u32 - 1);
        buf[0..4].copy_from_slice(&header.to_be_bytes());
        buf[8..12].copy_from_slice(&ts_int.to_be_bytes());
        buf[12..20].copy_from_slice(&ts_frac.to_be_bytes());
        for (i, (re, im)) in samples.iter().enumerate() {
            let off = 20 + i * 4;
            buf[off..off + 2].copy_from_slice(&re.to_be_bytes());
            buf[off + 2..off + 4].copy_from_slice(&im.to_be_bytes());
        }
        buf
    }

    #[test]
    fn full_packets_produce_two_rows_per_packet_in_steady_state() {
        let mut p = PacketProcessor::new(NOMINAL_SAMPLE_RATE);
        let samples: Vec<(i16, i16)> = (0..NOMINAL_COMPLEX_SAMPLES)
            .map(|i| ((i as i16).wrapping_mul(7), 0))
            .collect();

        // The 4096-point FFT cannot emit until a full window has arrived.
        let first = p.observe(&packet(0, 0, &samples));
        assert_eq!(first.parse_error, None);
        assert_eq!(first.complex_samples, NOMINAL_COMPLEX_SAMPLES);
        assert!(first.rows.is_empty(), "2048 samples is half a window");

        let second = p.observe(&packet(0, NOMINAL_COMPLEX_SAMPLES as u64, &samples));
        assert_eq!(second.rows.len(), 1, "window fills at sample 4096");
        assert_eq!(second.rows[0].sample, 0);

        // Steady state: 2048 samples in, 2 frames out (hop 1024).
        let third = p.observe(&packet(0, (NOMINAL_COMPLEX_SAMPLES * 2) as u64, &samples));
        assert_eq!(third.rows.len(), 2);
        assert_eq!(third.rows[0].sample, 1024);
        assert_eq!(third.rows[1].sample, 2048);

        let fourth = p.observe(&packet(0, (NOMINAL_COMPLEX_SAMPLES * 3) as u64, &samples));
        assert_eq!(fourth.rows.len(), 2);
        assert_eq!(fourth.rows[0].sample, 3072);
        // 4 packets = 8192 samples in; 5 frames emitted (1 + 2 + 2) at hop 1024
        // leaves 3072 samples of overlap buffered for the next window.
        assert_eq!(p.samples_consumed(), 5 * 1024);
    }

    #[test]
    fn a_timestamp_jump_is_reported_and_not_interpolated() {
        // Loss accounting is receive-side: the processor supplies the counter,
        // the GapTracker (owned by the receive path) decides if it is a gap.
        let mut p = PacketProcessor::new(NOMINAL_SAMPLE_RATE);
        let mut tracker = GapTracker::new(NOMINAL_COMPLEX_SAMPLES as u64);
        let samples: Vec<(i16, i16)> = (0..NOMINAL_COMPLEX_SAMPLES).map(|_| (1, 1)).collect();

        let first = p.observe(&packet(0, 0, &samples));
        assert_eq!(
            tracker.observe(first.sample_counter, first.complex_samples as u64),
            None,
            "first packet cannot be a gap"
        );

        // Skip three packets' worth of timestamps.
        let jumped = p.observe(&packet(0, 2048 * 4, &samples));
        let gap = tracker
            .observe(jumped.sample_counter, jumped.complex_samples as u64)
            .expect("gap detected");
        assert_eq!(gap.missing_samples, 2048 * 3);
        assert_eq!(tracker.gaps, 1);
        assert_eq!(tracker.missing_samples, 6144);
    }

    #[test]
    fn malformed_datagrams_are_counted_and_do_not_disturb_the_stream() {
        let mut p = PacketProcessor::new(NOMINAL_SAMPLE_RATE);
        let samples: Vec<(i16, i16)> = (0..8).map(|_| (5, -5)).collect();

        let short = p.observe(&[0u8; 7]);
        assert!(short.parse_error.is_some());
        assert!(short.rows.is_empty());

        // A good packet after a bad one still flows.
        let good = p.observe(&packet(0, 0, &samples));
        assert_eq!(good.parse_error, None);
        assert_eq!(p.malformed(), 1);
    }
}
