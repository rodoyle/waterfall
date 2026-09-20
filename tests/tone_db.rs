//! Tone + bit-depth gate: synthetic VITA49 sender → UDP → consumer pipeline → STFT.
//!
//! This is the test that proves the sc16 → i8 conversion and the dBFS
//! calibration are right, rather than merely plausible. It drives the REAL
//! consumer pipeline (`waterfall::consumer::PacketProcessor`) over a real UDP
//! socket with hand-built datagrams, so a framing, endianness, bit-depth or
//! scaling mistake fails here.
//!
//! Numbers this asserts, for a 2 MS/s stream with a 4096-point FFT:
//!   * a tone at bin 512 is 250 kHz above DC (`passband_bin * rate / FFT`),
//!   * amplitude 64 (of the 127 i8 full scale) reads 20*log10(64/127) = -5.94 dBFS,
//!   * the window is a no-op in this codebase (`apply_window` is a placeholder),
//!     so coherent gain is 1.0 — the tolerance only has to cover i16 rounding
//!     and the arithmetic shift's truncation.
//!
//! It also pins down the trap the task description warns about: reinterpreting
//! the sc16 bytes as i8 produces interleaved garbage with a plausible-looking
//! noise floor, so the wrong conversion is asserted to be WRONG here.

use std::net::UdpSocket;
use waterfall::consumer::PacketProcessor;
use waterfall::vita49::{parse_datagram, GapTracker, NOMINAL_COMPLEX_SAMPLES, NOMINAL_SAMPLE_RATE};
use waterfall::{StftProcessor, FFT_SIZE, HOP_SIZE};

/// Passband bin of the test tone: 512 * (2e6 / 4096) = 250 kHz.
const TONE_BIN: usize = 512;
/// Tone amplitude in sc16 units. `16384 >> 8 == 64`, well inside i8 range.
const TONE_AMPLITUDE_SC16: i16 = 64 << 8;
/// Expected dBFS for amplitude 64 against the 127 full-scale reference.
const EXPECTED_DBFS: f32 = -5.94;
/// i16 rounding + arithmetic-shift truncation, plus slack for the peak bin.
const DBFS_TOLERANCE: f32 = 1.5;

/// Independent packet builder: the corrected sigproc layout, written here from
/// the byte offsets rather than reusing sender or parser code.
fn build_packet(ts_int: u32, ts_frac: u64, samples: &[(i16, i16)]) -> Vec<u8> {
    let total_words = 5 + samples.len(); // header + stream id + ts(3 words) + payload
    let mut buf = vec![0u8; total_words * 4];
    let header: u32 = 0x10D0_0000 | (total_words as u32 - 1);
    buf[0..4].copy_from_slice(&header.to_be_bytes());
    buf[4..8].copy_from_slice(&0u32.to_be_bytes());
    buf[8..12].copy_from_slice(&ts_int.to_be_bytes());
    buf[12..20].copy_from_slice(&ts_frac.to_be_bytes());
    for (i, (re, im)) in samples.iter().enumerate() {
        let off = 20 + i * 4;
        buf[off..off + 2].copy_from_slice(&re.to_be_bytes());
        buf[off + 2..off + 4].copy_from_slice(&im.to_be_bytes());
    }
    buf
}

/// One full-rate packet of an analytic (single-sided) complex tone at `bin`.
///
/// `first_sample` is the tone's absolute sample index, so consecutive packets
/// stay phase-continuous and the tone does not smear across bins.
fn tone_packet(ts_int: u32, first_sample: u64, bin: usize) -> Vec<u8> {
    let mut samples = Vec::with_capacity(NOMINAL_COMPLEX_SAMPLES);
    for k in 0..NOMINAL_COMPLEX_SAMPLES {
        let n = (first_sample + k as u64) as f64;
        let theta = 2.0 * std::f64::consts::PI * bin as f64 * n / FFT_SIZE as f64;
        samples.push((
            (TONE_AMPLITUDE_SC16 as f64 * theta.cos()) as i16,
            (TONE_AMPLITUDE_SC16 as f64 * theta.sin()) as i16,
        ));
    }
    build_packet(ts_int, first_sample, &samples)
}

/// Round-trip `count` datagrams through a real UDP socket (loopback).
///
/// Proves the datagram arrives byte-identical — the transport boundary the
/// consumer actually owns, not a call to the parser directly.
fn over_udp(datagrams: &[Vec<u8>]) -> Vec<Vec<u8>> {
    let socket = UdpSocket::bind("127.0.0.1:0").expect("bind loopback");
    let target = socket.local_addr().expect("local addr");
    for d in datagrams {
        socket.send_to(d, target).expect("send datagram");
    }

    let mut received = Vec::with_capacity(datagrams.len());
    let mut buf = vec![0u8; 65_536];
    for _ in 0..datagrams.len() {
        let (n, _) = socket.recv_from(&mut buf).expect("receive datagram");
        received.push(buf[..n].to_vec());
    }
    received
}

/// Peak (bin index, dBFS) over the negative-frequency-free half of the row.
fn peak(bins: &[f32]) -> (usize, f32) {
    bins.iter()
        .enumerate()
        .take(bins.len() / 2)
        .fold(
            (0usize, f32::MIN),
            |acc, (i, &v)| {
                if v > acc.1 {
                    (i, v)
                } else {
                    acc
                }
            },
        )
}

/// Second-highest bin, used to prove the energy really is concentrated.
fn second_peak(bins: &[f32]) -> f32 {
    let (top, _) = peak(bins);
    bins.iter()
        .enumerate()
        .take(bins.len() / 2)
        .filter(|(i, _)| *i != top)
        .fold(f32::MIN, |acc, (_, &v)| acc.max(v))
}

fn row_from(processor: &mut PacketProcessor, datagrams: &[Vec<u8>]) -> waterfall::Row {
    let mut last = None;
    for datagram in datagrams {
        let outcome = processor.observe(datagram);
        assert!(outcome.parse_error.is_none(), "datagram must parse");
        assert!(
            outcome.size_field_matches,
            "header size field must agree with the delivered datagram"
        );
        if let Some(row) = outcome.rows.last() {
            last = Some(row.clone());
        }
    }
    last.expect("a full window was fed, so at least one row must be emitted")
}

#[test]
fn a_tone_lands_on_its_expected_bin_at_its_expected_dbfs() {
    // Four full packets = 8192 samples, comfortably past the 4096 window.
    let packets: Vec<Vec<u8>> = (0..4)
        .map(|p| tone_packet(0, p * NOMINAL_COMPLEX_SAMPLES as u64, TONE_BIN))
        .collect();

    let received = over_udp(&packets);
    assert_eq!(received[0].len(), 8212, "corrected framing is 8212 bytes");
    assert_eq!(
        received[0], packets[0],
        "the datagram must survive the UDP round trip byte-for-byte"
    );

    let mut processor = PacketProcessor::new(NOMINAL_SAMPLE_RATE);
    let row = row_from(&mut processor, &received);

    assert_eq!(row.bins.len(), FFT_SIZE);

    let (bin, dbfs) = peak(&row.bins);
    assert_eq!(bin, TONE_BIN, "tone bin: 512 -> 250 kHz at 2 MS/s");
    assert!(
        (dbfs - EXPECTED_DBFS).abs() <= DBFS_TOLERANCE,
        "expected ~{EXPECTED_DBFS} dBFS for amplitude 64 of 127 full scale, got {dbfs}"
    );

    // Energy must be concentrated: a smeared conversion would lift neighbours.
    let second = second_peak(&row.bins);
    assert!(
        dbfs - second > 20.0,
        "peak {dbfs} dBFS vs next {second} dBFS: tone is not clean"
    );
}

#[test]
fn two_tone_amplitudes_track_the_full_scale_reference_in_dbfs() {
    // Halving the amplitude must read ~6 dB lower, which only holds if the
    // reference (127) and the >> 8 scaling are both right.
    let loud: Vec<Vec<u8>> = (0..4)
        .map(|p| tone_packet(0, p * NOMINAL_COMPLEX_SAMPLES as u64, TONE_BIN))
        .collect();
    let mut loud_processor = PacketProcessor::new(NOMINAL_SAMPLE_RATE);
    let (_, loud_dbfs) = peak(&row_from(&mut loud_processor, &over_udp(&loud)).bins);

    // amplitude >> 2 == 16 of 127.
    let quiet_packets: Vec<Vec<u8>> = (0..4)
        .map(|p| {
            let mut samples = Vec::with_capacity(NOMINAL_COMPLEX_SAMPLES);
            let amplitude = TONE_AMPLITUDE_SC16 >> 2;
            for k in 0..NOMINAL_COMPLEX_SAMPLES {
                let n = (p * NOMINAL_COMPLEX_SAMPLES + k) as f64;
                let theta = 2.0 * std::f64::consts::PI * TONE_BIN as f64 * n / FFT_SIZE as f64;
                samples.push((
                    (amplitude as f64 * theta.cos()) as i16,
                    (amplitude as f64 * theta.sin()) as i16,
                ));
            }
            build_packet(0, (p * NOMINAL_COMPLEX_SAMPLES) as u64, &samples)
        })
        .collect();
    let mut quiet_processor = PacketProcessor::new(NOMINAL_SAMPLE_RATE);
    let (_, quiet_dbfs) = peak(&row_from(&mut quiet_processor, &over_udp(&quiet_packets)).bins);

    let delta = loud_dbfs - quiet_dbfs;
    assert!(
        (delta - 12.04).abs() <= 1.0,
        "amplitude /4 must read ~12 dB lower, got {delta} ({loud_dbfs} vs {quiet_dbfs})"
    );
}

#[test]
fn reinterpreting_sc16_bytes_as_i8_is_detectably_wrong() {
    // The trap: the payload is big-endian i16, so reading its bytes as i8 gives
    // interleaved garbage. It must not reproduce the tone.
    let packets: Vec<Vec<u8>> = (0..4)
        .map(|p| tone_packet(0, p * NOMINAL_COMPLEX_SAMPLES as u64, TONE_BIN))
        .collect();
    let received = over_udp(&packets);

    let correct = {
        let mut processor = PacketProcessor::new(NOMINAL_SAMPLE_RATE);
        row_from(&mut processor, &received)
    };
    let (correct_bin, correct_dbfs) = peak(&correct.bins);

    // Same bytes, wrong interpretation.
    let mut wrong_processor = StftProcessor::new(FFT_SIZE, HOP_SIZE);
    let mut wrong_bins = Vec::new();
    for datagram in &received {
        let packet = parse_datagram(datagram).expect("parses");
        let misread: Vec<i8> = packet.payload.iter().map(|b| *b as i8).collect();
        for row in wrong_processor.push_bytes(&misread) {
            wrong_bins = row.bins;
        }
    }
    let (wrong_bin, wrong_dbfs) = peak(&wrong_bins);

    assert_eq!(correct_bin, TONE_BIN);
    assert!(
        (correct_dbfs - EXPECTED_DBFS).abs() <= DBFS_TOLERANCE,
        "correct conversion must hit the expected dBFS"
    );
    assert!(
        wrong_bin != TONE_BIN || (wrong_dbfs - EXPECTED_DBFS).abs() > 3.0,
        "misreading sc16 as i8 must not reproduce the tone (bin {wrong_bin}, {wrong_dbfs} dBFS)"
    );
}

#[test]
fn a_socket_level_timestamp_jump_is_reported_as_a_gap() {
    let packets = vec![
        tone_packet(0, 0, TONE_BIN),
        tone_packet(0, NOMINAL_COMPLEX_SAMPLES as u64, TONE_BIN),
        // Skip three packets' worth of samples.
        tone_packet(0, NOMINAL_COMPLEX_SAMPLES as u64 * 5, TONE_BIN),
    ];

    let mut processor = PacketProcessor::new(NOMINAL_SAMPLE_RATE);
    // Wire-loss accounting is receive-side (see consumer.rs), so the test drives
    // the same GapTracker the receive loop owns, on the same counters.
    let mut tracker = GapTracker::new(NOMINAL_COMPLEX_SAMPLES as u64);
    let mut gaps = 0;
    let mut missing = 0;
    for datagram in over_udp(&packets) {
        let outcome = processor.observe(&datagram);
        assert!(outcome.parse_error.is_none());
        if let Some(gap) = tracker.observe(outcome.sample_counter, outcome.complex_samples as u64) {
            gaps += 1;
            missing += gap.missing_samples;
        }
    }

    assert_eq!(gaps, 1, "exactly one discontinuity");
    assert_eq!(missing, 2048 * 3, "three packets' worth of samples missing");
    assert_eq!(tracker.gaps, 1);
    assert_eq!(tracker.missing_samples, 2048 * 3);
}

/// The REAL datagram captured from live sigproc traffic on 2026-09-20
/// (deployed consumer, `--capture-fixture`), committed so the parser is verified
/// against actual bytes rather than only against bytes this repo generated.
///
/// Measured content: 8212 bytes, header 0x10D00804, 2048 complex sc16 samples of
/// genuine 915 MHz band RF — and a PEAK ABSOLUTE COMPONENT OF only 46. That
/// quietness is the whole reason the live path keeps 16-bit resolution.
#[test]
fn real_captured_sigproc_datagram_parses() {
    let datagram: &[u8] = include_bytes!("fixtures/sigproc-full-packet.bin");

    assert_eq!(datagram.len(), 8212, "live full-rate packet is 2053 words");
    assert_eq!(
        &datagram[0..4],
        &[0x10, 0xd0, 0x08, 0x04],
        "header 0x10D00804 (type 0x1, TSI 3, TSF 1, size 2052)"
    );

    let packet = parse_datagram(datagram).expect("live datagram parses");
    assert_eq!(packet.header.packet_type, 1);
    assert_eq!(packet.header.tsi, 3, "free-running timestamps");
    assert_eq!(packet.header.tsf, 1, "fraction in sample counts");
    assert_eq!(packet.header.packet_count, 0, "not populated by the sender");
    assert_eq!(packet.header.size_words, 2052);
    assert!(packet.size_field_matches, "declared size == delivered length");
    assert_eq!(packet.stream_id, 0, "always 0 today");
    assert_eq!(packet.complex_samples(), NOMINAL_COMPLEX_SAMPLES);
    assert_eq!(packet.trailing_bytes, 0);

    let components = packet.payload_i16();
    assert_eq!(components.len(), NOMINAL_COMPLEX_SAMPLES * 2);

    // Genuine RF: many distinct values, mostly non-zero.
    let distinct: std::collections::HashSet<i16> = components.iter().copied().collect();
    assert!(distinct.len() > 20, "real noise floor, {} distinct", distinct.len());
    let non_zero = components.iter().filter(|&&v| v != 0).count();
    assert!(non_zero * 10 > components.len() * 9, ">90% non-zero");

    // THE POINT: the live band is quiet, far below the 8-bit LSB.
    let peak = components.iter().map(|v| v.saturating_abs()).max().unwrap();
    assert!(
        peak < 256,
        "live peak |sc16| is {peak}: below the `>> 8` LSB, so an 8-bit \
         downshift would destroy this signal (see the test below)"
    );

    // The sample counter is reconstructible from the real timestamp pair.
    assert_eq!(
        packet.sample_counter(NOMINAL_SAMPLE_RATE),
        packet.ts_int as u64 * NOMINAL_SAMPLE_RATE as u64 + packet.ts_frac
    );

    // The sc16 path renders this quiet packet with real structure.
    let mut processor = PacketProcessor::new(NOMINAL_SAMPLE_RATE);
    let mut rows = processor.observe(datagram).rows;
    rows.extend(processor.observe(datagram).rows);
    assert!(!rows.is_empty(), "two live datagrams fill a 4096-sample window");
    let row = &rows[rows.len() - 1];
    assert_eq!(row.bins.len(), FFT_SIZE);
    let spread = row.bins.iter().cloned().fold(f32::MIN, f32::max)
        - row.bins.iter().cloned().fold(f32::MAX, f32::min);
    assert!(
        spread > 5.0,
        "live spectrum has structure at 16-bit resolution (spread {spread:.1} dB)"
    );
    assert_eq!(processor.peak_abs(), peak, "processor reports the live level");
    assert!(processor.mean_abs() > 1.0, "mean level {}", processor.mean_abs());
}

/// The 8-bit downshift is only lossless for full-scale input. On this live band
/// it collapses the signal to `{0, -1}` with a ~-0.5 LSB DC bias, which is what
/// the waterfall would render as a flat field with a DC spur.
#[test]
fn i8_downshift_destroys_live_level_signal() {
    let datagram: &[u8] = include_bytes!("fixtures/sigproc-full-packet.bin");
    let packet = parse_datagram(datagram).expect("parses");

    let iq = packet.to_i8();
    let distinct: std::collections::HashSet<i8> = iq.iter().copied().collect();
    assert!(
        distinct.len() <= 2,
        "downshifted live RF collapses to {distinct:?}"
    );
    assert!(
        distinct.iter().all(|v| *v == 0 || *v == -1),
        "only {{0,-1}} survive: {distinct:?}"
    );

    // Negative components bias to -1 while positives vanish -> DC offset, which
    // is not a noise floor, it is an artifact.
    let mean = iq.iter().map(|&v| v as f64).sum::<f64>() / iq.len() as f64;
    assert!(mean < -0.4, "hard DC bias introduced by >>8: mean {mean}");

    // Contrast with the full-resolution path, which preserves the same bytes.
    let components = packet.payload_i16();
    let peak = components.iter().map(|v| v.saturating_abs()).max().unwrap();
    assert!(peak > 0, "the signal exists in the original samples");
    assert!(
        iq.iter().all(|&v| v == 0 || v == -1) && peak >= 1,
        "the information was in the low bits"
    );
}

/// The sc16 path must place a quiet tone (at live levels) on its bin and report
/// its level, which the 8-bit path cannot do.
#[test]
fn quiet_tones_keep_their_bin_and_level_on_the_sc16_path() {
    // Amplitude 46 == the measured peak of the live band.
    const QUIET_SC16: i16 = 46;
    let packets: Vec<Vec<u8>> = (0..4)
        .map(|p| {
            let mut samples = Vec::with_capacity(NOMINAL_COMPLEX_SAMPLES);
            for k in 0..NOMINAL_COMPLEX_SAMPLES {
                let n = (p * NOMINAL_COMPLEX_SAMPLES + k) as f64;
                let theta = 2.0 * std::f64::consts::PI * TONE_BIN as f64 * n / FFT_SIZE as f64;
                samples.push((
                    (QUIET_SC16 as f64 * theta.cos()) as i16,
                    (QUIET_SC16 as f64 * theta.sin()) as i16,
                ));
            }
            build_packet(0, (p * NOMINAL_COMPLEX_SAMPLES) as u64, &samples)
        })
        .collect();

    let mut processor = PacketProcessor::new(NOMINAL_SAMPLE_RATE);
    let row = row_from(&mut processor, &over_udp(&packets));

    let (bin, dbfs) = peak(&row.bins);
    assert_eq!(bin, TONE_BIN, "quiet tone still lands on its bin");

    // 20*log10(46 / 32767) = -57.06 dBFS against the 16-bit reference.
    let expected = 20.0 * (QUIET_SC16 as f32 / 32767.0).log10();
    assert!(
        (dbfs - expected).abs() <= 1.5,
        "expected ~{expected:.2} dBFS for a quiet sc16 tone, got {dbfs:.2}"
    );
    assert!(
        dbfs - second_peak(&row.bins) > 15.0,
        "quiet tone is still clean (peak {dbfs:.1} vs next {:.1})",
        second_peak(&row.bins)
    );
}

/// A full-scale sc16 tone reads 0 dBFS on the 16-bit reference, so the
/// calibration keeps the same meaning as the 8-bit reference.
#[test]
fn full_scale_sc16_tone_reads_zero_dbfs() {
    let packets: Vec<Vec<u8>> = (0..4)
        .map(|p| {
            let mut samples = Vec::with_capacity(NOMINAL_COMPLEX_SAMPLES);
            for k in 0..NOMINAL_COMPLEX_SAMPLES {
                let n = (p * NOMINAL_COMPLEX_SAMPLES + k) as f64;
                let theta = 2.0 * std::f64::consts::PI * TONE_BIN as f64 * n / FFT_SIZE as f64;
                samples.push((
                    (32767.0 * theta.cos()).round() as i16,
                    (32767.0 * theta.sin()).round() as i16,
                ));
            }
            build_packet(0, (p * NOMINAL_COMPLEX_SAMPLES) as u64, &samples)
        })
        .collect();

    let mut processor = PacketProcessor::new(NOMINAL_SAMPLE_RATE);
    let (bin, dbfs) = peak(&row_from(&mut processor, &over_udp(&packets)).bins);
    assert_eq!(bin, TONE_BIN);
    assert!(
        dbfs >= -1.0,
        "a full-scale sc16 tone must read ~0 dBFS (clamped at 0), got {dbfs}"
    );
}
