//! waterfall-consumer — the signal-middleware tier.
//!
//! Owns the network boundary: binds UDP (default `0.0.0.0:4820`), parses the
//! VITA 49 stream that `sigproc` pushes, converts sc16 → interleaved i8,
//! analyses EVERY packet through the STFT, and publishes paced rows to the web
//! tier (`waterfall-bridge`) over `POST /ingest`.
//!
//! Design constraints that are load-bearing:
//!   * The receive loop must NEVER block on analysis or on the network. A full
//!     datagram is 8212 bytes and arrives as ~6 IP fragments at 1450-byte pod
//!     MTU, so a stalled reader drops fragments and loses whole packets.
//!   * A large receive buffer is required: at ~8 MB/s the default buffer
//!     overflows and shows up as phantom loss.
//!   * Every packet is analysed (no RF decimation); only *publication* is paced.
//!     Drops are deliberately counted, never silent.
//!   * No synthetic fallback. If the feed stops, the stats say so; fabricated
//!     rows would look exactly like real signal.
//!
//! Usage:
//!   waterfall-consumer [--listen 0.0.0.0:4820] [--bridge-url URL]
//!                      [--sample-rate 2000000] [--center-freq 915000000]
//!                      [--publish-hz 30] [--stats-listen 0.0.0.0:4830]
//!                      [--rcvbuf 8388608] [--hexdump-first N]
//!                      [--capture-fixture PATH]

use axum::{routing::get, Json, Router};
use serde::Serialize;
use std::net::{SocketAddr, UdpSocket as StdUdpSocket};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use waterfall::cli;
use waterfall::consumer::PacketProcessor;
use waterfall::publish::{IngestMeta, IngestRequest, RowBatch, RowDto};
use waterfall::vita49::{self, GapTracker, ParseError, NOMINAL_COMPLEX_SAMPLES};

/// Max UDP datagram we will read (datagrams are 8202-8212 bytes today).
const RECV_BUF_BYTES: usize = 65_536;

// ---------------- Counters ----------------

/// Every drop and every rate signal, so loss can never be silent.
#[derive(Debug, Default)]
struct Counters {
    packets_received: AtomicU64,
    packets_analyzed: AtomicU64,
    samples_received: AtomicU64,
    socket_errors: AtomicU64,
    parse_errors: AtomicU64,
    too_short: AtomicU64,
    wrong_type: AtomicU64,
    trailerized: AtomicU64,
    stft_dropped: AtomicU64,
    rows_produced: AtomicU64,
    rows_published: AtomicU64,
    batches_sent: AtomicU64,
    bridge_errors: AtomicU64,
    rows_lost_to_bridge: AtomicU64,
    publish_dropped: AtomicU64,
    size_field_mismatch: AtomicU64,
    gaps: AtomicU64,
    missing_samples: AtomicU64,
    out_of_order: AtomicU64,
    /// Last sample counter, or -1 before the first packet.
    last_counter: AtomicI64,
    /// Effective SO_RCVBUF granted by the kernel.
    rcvbuf_bytes: AtomicU64,
}

impl Counters {
    fn bump(c: &AtomicU64) {
        c.fetch_add(1, Ordering::Relaxed);
    }
}

/// Snapshot served by `GET /stats` and logged periodically.
#[derive(Debug, Clone, Serialize)]
struct StatsSnapshot {
    source: &'static str,
    uptime_secs: u64,
    center_hz: f64,
    sample_rate_hz: f64,
    /// Receive rate over the sampling window (packets/second).
    packets_per_sec: f64,
    packets_received: u64,
    packets_analyzed: u64,
    samples_received: u64,
    parse_errors: u64,
    too_short: u64,
    wrong_type: u64,
    trailerized: u64,
    size_field_mismatch: u64,
    stft_dropped: u64,
    rows_produced: u64,
    rows_published: u64,
    batches_sent: u64,
    bridge_errors: u64,
    rows_lost_to_bridge: u64,
    publish_dropped: u64,
    gaps: u64,
    missing_samples: u64,
    out_of_order: u64,
    last_sample_counter: Option<u64>,
    rcvbuf_bytes: u64,
}

/// Configuration resolved from flags/environment.
#[derive(Debug, Clone)]
struct Config {
    listen: SocketAddr,
    bridge_url: String,
    sample_rate: f64,
    center_hz: f64,
    publish_hz: f64,
    stats_listen: SocketAddr,
    rcvbuf: usize,
    hexdump_first: u64,
    capture_fixture: Option<String>,
}

fn env_parse<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn load_config() -> Config {
    let args: Vec<String> = std::env::args().collect();

    // Both `--flag value` and `--flag=value` are accepted (see waterfall::cli);
    // the manifests use the equals form, and silently ignoring a flag must not
    // be possible in a deployed path.
    const KNOWN: &[&str] = &[
        "--listen",
        "--bridge-url",
        "--sample-rate",
        "--center-freq",
        "--publish-hz",
        "--stats-listen",
        "--rcvbuf",
        "--hexdump-first",
        "--capture-fixture",
    ];
    let unknown = cli::unknown_flags(&args, KNOWN);
    if !unknown.is_empty() {
        eprintln!(
            "waterfall-consumer: ignoring unrecognized flag(s): {}",
            unknown.join(", ")
        );
    }

    Config {
        listen: env_parse(
            "WATERFALL_CONSUMER_LISTEN",
            cli::parse_or(&args, "--listen", "0.0.0.0:4820".to_string()),
        )
        .parse()
        .expect("invalid --listen address"),
        bridge_url: std::env::var("WATERFALL_BRIDGE_URL").unwrap_or_else(|_| {
            cli::parse_or(
                &args,
                "--bridge-url",
                "http://waterfall-ui.default.svc.cluster.local:4780".to_string(),
            )
        }),
        sample_rate: cli::parse_or(&args, "--sample-rate", 2_000_000.0_f64),
        center_hz: cli::parse_or(&args, "--center-freq", 915_000_000.0_f64),
        publish_hz: cli::parse_or(&args, "--publish-hz", 30.0_f64),
        stats_listen: cli::parse_or(&args, "--stats-listen", "0.0.0.0:4830".to_string())
            .parse()
            .expect("invalid --stats-listen address"),
        rcvbuf: cli::parse_or(&args, "--rcvbuf", 8 * 1024 * 1024_usize),
        hexdump_first: cli::parse_or(&args, "--hexdump-first", 0_u64),
        capture_fixture: cli::flag_value(&args, "--capture-fixture"),
    }
}

// ---------------- Socket ----------------

/// Bind the receive socket with a large SO_RCVBUF, reporting what was granted.
///
/// Linux caps SO_RCVBUF at `net.core.rmem_max` (often 212992) and silently
/// halves/doubles the reported value. Because fragment loss looks exactly like
/// packet loss, the effective value is read back and surfaced rather than
/// assumed — an 8 MB buffer is what keeps ~977 pkt/s of 8212-byte datagrams from
/// overflowing.
async fn bind_receive_socket(
    cfg: &Config,
    stats: &Arc<Counters>,
) -> std::io::Result<tokio::net::UdpSocket> {
    let socket = socket2::Socket::new(
        socket2::Domain::IPV4,
        socket2::Type::DGRAM,
        Some(socket2::Protocol::UDP),
    )?;
    socket.set_reuse_address(true)?;
    socket.set_recv_buffer_size(cfg.rcvbuf)?;
    let effective = socket.recv_buffer_size().unwrap_or(0) as u64;
    stats.rcvbuf_bytes.store(effective, Ordering::Relaxed);

    socket.set_nonblocking(true)?;
    socket.bind(&cfg.listen.into())?;

    let std_socket: StdUdpSocket = socket.into();
    let socket = tokio::net::UdpSocket::from_std(std_socket)?;

    println!(
        "waterfall-consumer: bound UDP {} | bridge {} | center {:.0} Hz | {:.0} Hz sample rate",
        cfg.listen, cfg.bridge_url, cfg.center_hz, cfg.sample_rate
    );
    println!(
        "waterfall-consumer: SO_RCVBUF requested {} bytes, kernel reports {} bytes{}",
        cfg.rcvbuf,
        effective,
        if (effective as usize) < cfg.rcvbuf {
            " (CAPTURED by net.core.rmem_max — see deploy/README notes)"
        } else {
            ""
        }
    );
    Ok(socket)
}

// ---------------- Spike helpers ----------------

/// Hexdump + shape assertion for the first datagrams on the wire.
///
/// This exists to prove the transport before trusting a parser: length, header
/// word, decoded fields, timestamp and payload plausibility.
fn spike_inspect(n: u64, datagram: &[u8], cfg: &Config) {
    let header = if datagram.len() >= 4 {
        u32::from_be_bytes([datagram[0], datagram[1], datagram[2], datagram[3]])
    } else {
        0
    };
    let h = vita49::parse_header(header);

    let shown = datagram.len().min(64);
    let hex: Vec<String> = datagram[..shown]
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    let tail_start = datagram.len().saturating_sub(16);
    let tail: Vec<String> = if datagram.len() > shown {
        datagram[tail_start..]
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect()
    } else {
        Vec::new()
    };

    println!(
        "SPIKE datagram #{n}: len={} header=0x{header:08X} type=0x{:x} tsi={} tsf={} pkt_count={} size_words={} (declared {} bytes)",
        datagram.len(),
        h.packet_type,
        h.tsi,
        h.tsf,
        h.packet_count,
        h.size_words,
        h.declared_bytes()
    );
    println!("SPIKE   first {shown} bytes: {}", hex.join(" "));
    if !tail.is_empty() {
        println!("SPIKE   last 16 bytes:    {}", tail.join(" "));
    }

    match vita49::parse_datagram(datagram) {
        Ok(pkt) => {
            // Plausibility: real RF is neither constant nor a reinterpreted byte
            // soup. A constant or all-zero payload is the only thing this
            // heuristic can honestly call wrong (dead ADC, misframed datagram).
            // A clean bin-centred tone legitimately quantizes to a handful of
            // distinct values, so "few distinct values" must NOT be reported as
            // implausible — bit-depth mistakes are caught by the spectrum instead
            // (see tests/tone_db.rs).
            let mut values = std::collections::HashSet::new();
            let mut nonzero = 0usize;
            for c in pkt.payload.chunks_exact(2) {
                let v = i16::from_be_bytes([c[0], c[1]]);
                if v != 0 {
                    nonzero += 1;
                }
                values.insert(v);
            }
            let total = pkt.components().max(1);
            let degenerate = values.len() <= 1;
            println!(
                "SPIKE   parsed: stream_id={} ts_int={} ts_frac={} samples={} counter={} trailing={} size_field_matches={}",
                pkt.stream_id,
                pkt.ts_int,
                pkt.ts_frac,
                pkt.complex_samples(),
                pkt.sample_counter(cfg.sample_rate),
                pkt.trailing_bytes,
                pkt.size_field_matches
            );
            println!(
                "SPIKE   payload: {} components, {} distinct i16 values, {} non-zero ({:.0}%) -> degenerate_payload={}",
                pkt.components(),
                values.len(),
                nonzero,
                100.0 * nonzero as f64 / total as f64,
                degenerate
            );
            if !pkt.size_field_matches {
                println!("SPIKE   NOTE: header size field disagrees with the delivered datagram length (length wins)");
            }
        }
        Err(e) => println!("SPIKE   parse failed: {e}"),
    }
}

// ---------------- Receive loop ----------------

async fn receive_loop(
    socket: tokio::net::UdpSocket,
    tx: mpsc::Sender<Vec<u8>>,
    stats: Arc<Counters>,
    cfg: Arc<Config>,
    fixture_written: Arc<AtomicBool>,
) {
    let mut buf = vec![0u8; RECV_BUF_BYTES];

    // Loss is tracked HERE, where every delivered datagram is observed — not in
    // the analysis worker, which only sees datagrams that survived the bounded
    // queue. Counting gaps there would report the consumer's own deliberate
    // drops as RF loss and make a healthy link look lossy.
    let mut tracker = GapTracker::new(NOMINAL_COMPLEX_SAMPLES as u64);

    loop {
        let (n, _src) = match socket.recv_from(&mut buf).await {
            Ok(v) => v,
            Err(e) => {
                Counters::bump(&stats.socket_errors);
                eprintln!("waterfall-consumer: recv error: {e}");
                tokio::time::sleep(Duration::from_millis(10)).await;
                continue;
            }
        };

        let received = stats.packets_received.fetch_add(1, Ordering::Relaxed) + 1;
        let datagram = &buf[..n];

        if received <= cfg.hexdump_first {
            spike_inspect(received, datagram, &cfg);
        }

        // Cheap rejection here keeps counters honest in the receive path, and
        // this is the only place that sees EVERY datagram, so it owns both the
        // wire-loss accounting and the sample counter.
        match vita49::parse_datagram(datagram) {
            Ok(pkt) => {
                if !pkt.size_field_matches {
                    Counters::bump(&stats.size_field_mismatch);
                }

                stats
                    .samples_received
                    .fetch_add(pkt.components() as u64, Ordering::Relaxed);

                // The unwrapped sample counter is the only loss signal available:
                // stream_id and packet_count are both 0 today.
                let counter = pkt.sample_counter(cfg.sample_rate);
                stats
                    .last_counter
                    .store(counter.min(i64::MAX as u64) as i64, Ordering::Relaxed);

                if let Some(gap) = tracker.observe(counter, pkt.complex_samples() as u64) {
                    stats.gaps.store(tracker.gaps, Ordering::Relaxed);
                    stats
                        .missing_samples
                        .store(tracker.missing_samples, Ordering::Relaxed);
                    stats
                        .out_of_order
                        .store(tracker.out_of_order, Ordering::Relaxed);
                    if tracker.gaps <= 5 || tracker.gaps.is_multiple_of(100) {
                        println!(
                            "waterfall-consumer: GAP #{} at counter {} — {} samples missing (total gaps={}, missing={})",
                            tracker.gaps,
                            gap.at_counter,
                            gap.missing_samples,
                            tracker.gaps,
                            tracker.missing_samples
                        );
                    }
                }

                // Capture one real datagram for the committed test fixture.
                if let Some(path) = cfg.capture_fixture.as_deref() {
                    if !fixture_written.swap(true, Ordering::Relaxed)
                        && pkt.complex_samples() == NOMINAL_COMPLEX_SAMPLES
                    {
                        match std::fs::write(path, datagram) {
                            Ok(()) => {
                                println!("waterfall-consumer: captured {n}-byte fixture to {path}")
                            }
                            Err(e) => {
                                eprintln!("waterfall-consumer: fixture write failed: {e}")
                            }
                        }
                    }
                }
            }
            Err(e) => {
                Counters::bump(&stats.parse_errors);
                match e {
                    ParseError::TooShort { .. } => Counters::bump(&stats.too_short),
                    ParseError::UnsupportedPacketType(_) => Counters::bump(&stats.wrong_type),
                    ParseError::UnsupportedTrailer => Counters::bump(&stats.trailerized),
                }
                continue;
            }
        }

        // Hand the raw datagram to the analysis worker. Never block the socket:
        // a full queue is a counted drop, not backpressure.
        match tx.try_send(datagram.to_vec()) {
            Ok(()) => {
                Counters::bump(&stats.packets_analyzed);
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                Counters::bump(&stats.stft_dropped);
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                eprintln!("waterfall-consumer: analysis worker stopped; receive loop exiting");
                return;
            }
        }
    }
}

// ---------------- Analysis worker ----------------

/// Convert (sc16 → i8) and run the STFT over every admitted datagram on a
/// dedicated thread, so the FFT can never stall the socket.
///
/// Wire-loss accounting deliberately lives in the receive loop instead: this
/// worker only sees datagrams that survived the bounded queue.
fn analysis_worker(
    mut rx: mpsc::Receiver<Vec<u8>>,
    batch: Arc<Mutex<RowBatch>>,
    stats: Arc<Counters>,
    sample_rate: f64,
) {
    let mut pipeline = PacketProcessor::new(sample_rate);

    while let Some(datagram) = rx.blocking_recv() {
        let outcome = pipeline.observe(&datagram);

        for row in outcome.rows {
            Counters::bump(&stats.rows_produced);
            let mut b = batch.lock().expect("batch mutex poisoned");
            b.push(RowDto {
                sample: row.sample,
                bins: row.bins,
            });
            stats.publish_dropped.store(b.dropped(), Ordering::Relaxed);
        }
    }
}

// ---------------- Publisher ----------------

async fn publish_loop(batch: Arc<Mutex<RowBatch>>, stats: Arc<Counters>, cfg: Arc<Config>) {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .expect("http client");

    let period = Duration::from_secs_f64(1.0 / cfg.publish_hz.max(1.0));
    let mut ticker = tokio::time::interval(period);
    let url = format!("{}/ingest", cfg.bridge_url.trim_end_matches('/'));

    loop {
        ticker.tick().await;

        let rows = {
            let mut b = batch.lock().expect("batch mutex poisoned");
            let dropped = b.dropped();
            stats.publish_dropped.store(dropped, Ordering::Relaxed);
            b.drain()
        };
        if rows.is_empty() {
            continue;
        }

        let meta = IngestMeta {
            source: "vita49".to_string(),
            center_hz: cfg.center_hz,
            sample_rate_hz: cfg.sample_rate,
            last_sample_counter: stats.last_counter.load(Ordering::Relaxed).max(0) as u64,
            gaps: stats.gaps.load(Ordering::Relaxed),
            missing_samples: stats.missing_samples.load(Ordering::Relaxed),
            packets_received: stats.packets_received.load(Ordering::Relaxed),
            parse_errors: stats.parse_errors.load(Ordering::Relaxed),
            stft_dropped: stats.stft_dropped.load(Ordering::Relaxed),
            publish_dropped: stats.publish_dropped.load(Ordering::Relaxed),
            rcvbuf_bytes: stats.rcvbuf_bytes.load(Ordering::Relaxed),
        };

        let count = rows.len() as u64;
        let request = IngestRequest { rows, meta };

        match client.post(&url).json(&request).send().await {
            Ok(response) if response.status().is_success() => {
                stats.batches_sent.fetch_add(1, Ordering::Relaxed);
                stats.rows_published.fetch_add(count, Ordering::Relaxed);
            }
            Ok(response) => {
                stats.bridge_errors.fetch_add(1, Ordering::Relaxed);
                stats
                    .rows_lost_to_bridge
                    .fetch_add(count, Ordering::Relaxed);
                let errors = stats.bridge_errors.load(Ordering::Relaxed);
                if errors <= 5 || errors.is_multiple_of(60) {
                    eprintln!(
                        "waterfall-consumer: bridge POST {} -> HTTP {} (errors={})",
                        url,
                        response.status(),
                        errors
                    );
                }
            }
            Err(e) => {
                stats.bridge_errors.fetch_add(1, Ordering::Relaxed);
                stats
                    .rows_lost_to_bridge
                    .fetch_add(count, Ordering::Relaxed);
                let errors = stats.bridge_errors.load(Ordering::Relaxed);
                if errors <= 5 || errors.is_multiple_of(60) {
                    eprintln!("waterfall-consumer: bridge POST failed: {e} (errors={errors})");
                }
            }
        }
    }
}

// ---------------- Stats ----------------

fn snapshot(
    stats: &Counters,
    cfg: &Config,
    uptime: Duration,
    window: Duration,
    window_packets: u64,
) -> StatsSnapshot {
    let last = stats.last_counter.load(Ordering::Relaxed);
    StatsSnapshot {
        source: "vita49",
        uptime_secs: uptime.as_secs(),
        center_hz: cfg.center_hz,
        sample_rate_hz: cfg.sample_rate,
        packets_per_sec: if window.as_secs_f64() > 0.0 {
            window_packets as f64 / window.as_secs_f64()
        } else {
            0.0
        },
        packets_received: stats.packets_received.load(Ordering::Relaxed),
        packets_analyzed: stats.packets_analyzed.load(Ordering::Relaxed),
        samples_received: stats.samples_received.load(Ordering::Relaxed),
        parse_errors: stats.parse_errors.load(Ordering::Relaxed),
        too_short: stats.too_short.load(Ordering::Relaxed),
        wrong_type: stats.wrong_type.load(Ordering::Relaxed),
        trailerized: stats.trailerized.load(Ordering::Relaxed),
        size_field_mismatch: stats.size_field_mismatch.load(Ordering::Relaxed),
        stft_dropped: stats.stft_dropped.load(Ordering::Relaxed),
        rows_produced: stats.rows_produced.load(Ordering::Relaxed),
        rows_published: stats.rows_published.load(Ordering::Relaxed),
        batches_sent: stats.batches_sent.load(Ordering::Relaxed),
        bridge_errors: stats.bridge_errors.load(Ordering::Relaxed),
        rows_lost_to_bridge: stats.rows_lost_to_bridge.load(Ordering::Relaxed),
        publish_dropped: stats.publish_dropped.load(Ordering::Relaxed),
        gaps: stats.gaps.load(Ordering::Relaxed),
        missing_samples: stats.missing_samples.load(Ordering::Relaxed),
        out_of_order: stats.out_of_order.load(Ordering::Relaxed),
        last_sample_counter: if last < 0 { None } else { Some(last as u64) },
        rcvbuf_bytes: stats.rcvbuf_bytes.load(Ordering::Relaxed),
    }
}

async fn stats_loop(stats: Arc<Counters>, cfg: Arc<Config>) {
    let start = Instant::now();
    let mut previous_packets = 0u64;
    let mut previous_at = Instant::now();

    loop {
        tokio::time::sleep(Duration::from_secs(10)).await;

        let now = Instant::now();
        let packets = stats.packets_received.load(Ordering::Relaxed);
        let snap = snapshot(
            &stats,
            &cfg,
            start.elapsed(),
            now.duration_since(previous_at),
            packets - previous_packets,
        );
        previous_packets = packets;
        previous_at = now;

        // One line, everything an auditor needs: rate, gaps, every drop path.
        println!(
            "consumer stats: {:.1} pkt/s | rx={} analyzed={} samples={} | gaps={} missing_samples={} out_of_order={} | parse_errors={} (short={} wrong_type={} trailer={}) size_field_mismatch={} | rows_produced={} published={} publish_dropped={} stft_dropped={} | bridge_errors={} | last_counter={:?} rcvbuf={}",
            snap.packets_per_sec,
            snap.packets_received,
            snap.packets_analyzed,
            snap.samples_received,
            snap.gaps,
            snap.missing_samples,
            snap.out_of_order,
            snap.parse_errors,
            snap.too_short,
            snap.wrong_type,
            snap.trailerized,
            snap.size_field_mismatch,
            snap.rows_produced,
            snap.rows_published,
            snap.publish_dropped,
            snap.stft_dropped,
            snap.bridge_errors,
            snap.last_sample_counter,
            snap.rcvbuf_bytes,
        );
    }
}

#[tokio::main]
async fn main() {
    let cfg = Arc::new(load_config());
    let stats = Arc::new(Counters::default());
    let batch = Arc::new(Mutex::new(RowBatch::new(64)));

    let socket = bind_receive_socket(&cfg, &stats)
        .await
        .expect("failed to bind VITA49 receive socket");

    // Bounded, but deep enough to absorb a burst: the analysis worker must
    // never push back on the socket, yet dropping a packet is a real loss of
    // analysis (counted, and reported separately from wire loss).
    let (tx, rx) = mpsc::channel::<Vec<u8>>(512);

    let worker_batch = Arc::clone(&batch);
    let worker_stats = Arc::clone(&stats);
    let worker_rate = cfg.sample_rate;
    std::thread::Builder::new()
        .name("analysis".into())
        .spawn(move || analysis_worker(rx, worker_batch, worker_stats, worker_rate))
        .expect("failed to spawn analysis worker");

    tokio::spawn(receive_loop(
        socket,
        tx,
        Arc::clone(&stats),
        Arc::clone(&cfg),
        Arc::new(AtomicBool::new(false)),
    ));
    tokio::spawn(publish_loop(
        Arc::clone(&batch),
        Arc::clone(&stats),
        Arc::clone(&cfg),
    ));
    tokio::spawn(stats_loop(Arc::clone(&stats), Arc::clone(&cfg)));

    // Stats/health endpoint for the middleware tier.
    let stats_state = Arc::clone(&stats);
    let cfg_state = Arc::clone(&cfg);
    let app = Router::new()
        .route(
            "/stats",
            get(move || {
                let stats = Arc::clone(&stats_state);
                let cfg = Arc::clone(&cfg_state);
                async move { Json(snapshot(&stats, &cfg, Duration::ZERO, Duration::ZERO, 0)) }
            }),
        )
        .route("/healthz", get(|| async { "ok" }));

    let listener = tokio::net::TcpListener::bind(cfg.stats_listen)
        .await
        .expect("failed to bind stats listener");
    println!(
        "waterfall-consumer: stats on http://{}/stats",
        cfg.stats_listen
    );

    axum::serve(listener, app)
        .await
        .expect("stats server error");
}
