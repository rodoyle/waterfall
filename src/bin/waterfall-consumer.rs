//! waterfall-consumer — the signal-middleware tier.
//!
//! Owns the network boundary: binds UDP (default `0.0.0.0:4820`), parses the
//! VITA 49 stream that `sigproc` pushes, converts sc16 → interleaved i8,
//! analyses EVERY packet through the STFT, and publishes paced rows to the web
//! tier (`waterfall-bridge`) over `POST /ingest`.
//!
//! Design constraints that are load-bearing:
//!   * The receive loop must NEVER block on analysis or on the network; a
//!     stalled reader can drop datagrams and lose samples.
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
//!                      [--multicast-group G] [--multicast-iface ADDR]
//!
//! Multicast is ADDITIVE. `--multicast-group` (env `WATERFALL_MULTICAST_GROUP`)
//! joins a LAN copy of the same stream on the already-bound socket, and
//! `--multicast-iface` (env `WATERFALL_MULTICAST_IFACE`) names the *local*
//! interface address to join on. The unicast bind is never conditional on either,
//! so a failed join degrades to exactly the behaviour that already works. While
//! both paths deliver, the second copy of a datagram is dropped and counted
//! (`duplicates_dropped`), never analysed twice and never reported as loss.

use axum::{routing::get, Json, Router};
use serde::Serialize;
use std::collections::{HashMap, VecDeque};
use std::net::{Ipv4Addr, SocketAddr, UdpSocket as StdUdpSocket};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use waterfall::cli;
use waterfall::consumer::PacketProcessor;
use waterfall::publish::{IngestMeta, IngestRequest, RowBatch, RowDto};
use waterfall::shutdown;
use waterfall::vita49::{self, DataStreamTracker, ParseError};

/// Buffer large enough for any UDP datagram.
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
    sequence_gaps: AtomicU64,
    /// Last sample counter, or -1 before the first packet.
    last_counter: AtomicI64,
    /// Effective SO_RCVBUF granted by the kernel.
    rcvbuf_bytes: AtomicU64,
    /// Largest |sc16| component seen — the number that decides whether an 8-bit
    /// downshift would be lossy (live RF peaks around ±46).
    signal_peak_sc16: AtomicU64,
    /// Mean |sc16| component level, in thousandths.
    signal_mean_milli: AtomicU64,
    /// Second copies of a datagram dropped because two transport paths
    /// (multicast + unicast) delivered the same packet. Positive evidence that
    /// both paths are live.
    duplicates_dropped: AtomicU64,
    /// Duplicates whose payload differed from the copy already analysed — a real
    /// defect in the parallel path, counted rather than silently accepted.
    duplicate_mismatch: AtomicU64,
    /// Duplicates whose bytes were actually compared against the retained copy.
    /// Together with `duplicates_fallback` this says how much of the duplicate
    /// stream is genuinely verified rather than assumed.
    duplicates_compared: AtomicU64,
    /// Duplicates whose original copy was evicted from the retained window, so
    /// only the counter matched and the bytes could not be checked.
    duplicates_fallback: AtomicU64,
    /// Type-4 IF Context packets seen. They carry no samples, so they are not
    /// analysed, but they are deduped and counted so the parallel path is
    /// accounted for end to end.
    context_packets: AtomicU64,
    /// Context packets dropped as duplicates of one already seen.
    context_duplicates_dropped: AtomicU64,
    /// Context duplicates whose bytes differed from the retained copy.
    context_duplicate_mismatch: AtomicU64,
    /// Datagram count per source address, bounded. This is how a third delivery
    /// path is attributed: two addresses for one stream means unicast plus
    /// multicast, three means something else is also forwarding.
    source_counts: Mutex<HashMap<SocketAddr, u64>>,
    /// Whether the multicast group join actually succeeded in this process.
    multicast_joined: AtomicBool,
    /// Why the join failed, if it did: `/stats` stays honest about a degraded path.
    multicast_join_error: Mutex<Option<String>>,
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
    sequence_gaps: u64,
    last_sample_counter: Option<u64>,
    rcvbuf_bytes: u64,
    /// Peak and mean |sc16| level of the live RF (see note in PacketProcessor).
    signal_peak_sc16: u64,
    signal_mean_abs: f64,
    /// Multicast state: the group requested, the interface joined on, whether the
    /// join succeeded, and why it did not. `unicast_only` remains a working state.
    multicast_group: Option<String>,
    multicast_iface: Option<String>,
    multicast_joined: bool,
    multicast_join_error: Option<String>,
    duplicates_dropped: u64,
    duplicate_mismatch: u64,
    duplicates_compared: u64,
    duplicates_fallback: u64,
    context_packets: u64,
    context_duplicates_dropped: u64,
    context_duplicate_mismatch: u64,
    /// Datagram count per source address, highest first, bounded.
    sources: Vec<SourceCount>,
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
    /// Additive LAN multicast copy of the same stream; `Off` = today's behaviour.
    multicast: MulticastConfig,
}

/// Where the additive multicast copy of the stream comes from, resolved from the
/// flags/environment before any socket exists.
///
/// Deliberately a tri-state rather than two `Option`s: "no group" and "group but
/// no interface" mean different things at join time, and an unparseable value must
/// never silently become "let the kernel decide".
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[allow(dead_code)]
enum MulticastConfig {
    /// No join. Today's unicast-only behaviour.
    #[default]
    Off,
    /// Group without an interface: the kernel picks the egress interface by
    /// route. Only correct on a single-homed host, and never used by the manifest.
    KernelChosen(Ipv4Addr),
    /// Group plus the local interface address to join on (the deployed form).
    Joined { group: Ipv4Addr, iface: Ipv4Addr },
}

impl MulticastConfig {
    fn group(&self) -> Option<Ipv4Addr> {
        match *self {
            MulticastConfig::Off => None,
            MulticastConfig::KernelChosen(group) => Some(group),
            MulticastConfig::Joined { group, .. } => Some(group),
        }
    }

    fn iface(&self) -> Option<Ipv4Addr> {
        match *self {
            MulticastConfig::Joined { iface, .. } => Some(iface),
            _ => None,
        }
    }
}

/// Resolve `--multicast-group` / `--multicast-iface` without ever refusing to start.
///
/// A malformed value disables the join and says so on stderr. It must not fail
/// startup: the unicast stream is the path the deployment lives on, so a typo in
/// an additive feature cannot be allowed to take it down.
fn parse_multicast(group: Option<&str>, iface: Option<&str>) -> MulticastConfig {
    let Some(group) = group.map(str::trim).filter(|value| !value.is_empty()) else {
        return MulticastConfig::Off;
    };
    let group = match group.parse::<Ipv4Addr>() {
        Ok(group) if group.is_multicast() => group,
        Ok(other) => {
            eprintln!(
                "waterfall-consumer: --multicast-group {other} is not a multicast address \
                 (224.0.0.0/4); multicast disabled, unicast unaffected"
            );
            return MulticastConfig::Off;
        }
        Err(error) => {
            eprintln!(
                "waterfall-consumer: --multicast-group {group:?} is not an IPv4 address \
                 ({error}); multicast disabled, unicast unaffected"
            );
            return MulticastConfig::Off;
        }
    };

    match iface.map(str::trim).filter(|value| !value.is_empty()) {
        None => MulticastConfig::KernelChosen(group),
        Some(raw) => match raw.parse::<Ipv4Addr>() {
            // `0.0.0.0` is the explicit spelling of "kernel chooses".
            Ok(addr) if addr.is_unspecified() => MulticastConfig::KernelChosen(group),
            Ok(addr) => MulticastConfig::Joined { group, iface: addr },
            Err(error) => {
                eprintln!(
                    "waterfall-consumer: --multicast-iface {raw:?} is not an IPv4 address \
                     ({error}); multicast disabled, unicast unaffected"
                );
                MulticastConfig::Off
            }
        },
    }
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
        "--multicast-group",
        "--multicast-iface",
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
                // Canonical Service name (the pre-rename `waterfall-ui` no longer
                // exists, so a stale default would fail DNS for anyone running
                // this binary without an explicit --bridge-url).
                "http://orbweaver-ui.default.svc.cluster.local:4780".to_string(),
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
        multicast: parse_multicast(
            std::env::var("WATERFALL_MULTICAST_GROUP")
                .ok()
                .or_else(|| cli::flag_value(&args, "--multicast-group"))
                .as_deref(),
            std::env::var("WATERFALL_MULTICAST_IFACE")
                .ok()
                .or_else(|| cli::flag_value(&args, "--multicast-iface"))
                .as_deref(),
        ),
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

    // Additive multicast join, deliberately AFTER the unicast bind and deliberately
    // non-fatal: the unicast path is what the deployment lives on, so an unusable
    // group or interface must degrade to today's behaviour, not refuse to start.
    if let Err(error) = join_multicast(&socket, cfg, stats) {
        eprintln!(
            "waterfall-consumer: multicast join failed ({error}); continuing with unicast {} only",
            cfg.listen
        );
    }

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

/// Join the additive multicast copy of the stream on the already-bound socket.
///
/// One socket receives both copies: the sender emits the unicast datagram and the
/// multicast datagram to the same port, and `SO_REUSEADDR` (set above) is what lets
/// a wildcard-bound port take a group membership at all. Membership needs no
/// capability — `SO_BINDTODEVICE` would need CAP_NET_RAW, which the pod drops — so
/// the interface is identified by its local address, exactly as IP_ADD_MEMBERSHIP
/// expects.
fn join_multicast(
    socket: &socket2::Socket,
    cfg: &Config,
    stats: &Arc<Counters>,
) -> std::io::Result<()> {
    let group = match cfg.multicast {
        MulticastConfig::Off => return Ok(()),
        MulticastConfig::KernelChosen(group) => {
            eprintln!(
                "waterfall-consumer: no multicast interface given; the kernel picks the join \
                 interface by route, which is only correct on a single-homed host"
            );
            group
        }
        MulticastConfig::Joined { group, .. } => group,
    };
    let iface = cfg.multicast.iface().unwrap_or(Ipv4Addr::UNSPECIFIED);

    match socket.join_multicast_v4(&group, &iface) {
        Ok(()) => {
            stats.multicast_joined.store(true, Ordering::Relaxed);
            println!(
                "waterfall-consumer: joined multicast {group} on {iface_label} (additive: unicast {listen} stays bound, second copies are dropped and counted)",
                iface_label = if iface.is_unspecified() {
                    "the kernel-chosen interface".to_string()
                } else {
                    iface.to_string()
                },
                listen = cfg.listen,
            );
            Ok(())
        }
        Err(error) => {
            if let Ok(mut slot) = stats.multicast_join_error.lock() {
                *slot = Some(error.to_string());
            }
            Err(error)
        }
    }
}

// ---------------- Parallel-path dedupe ----------------

/// Verdict for one observed datagram when a second transport path may deliver it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Duplicate {
    /// First copy of this packet inside the window: analyse it.
    New,
    /// Second copy, byte-identical to the copy already analysed.
    Exact,
    /// Second copy carrying different bytes for the same counter — a real defect
    /// in the parallel path, so it is counted rather than quietly accepted.
    Mismatch,
    /// Duplicate whose original copy has been evicted from the retained window,
    /// so only the counter could be checked, not the bytes.
    Fallback,
}

/// Drops the second copy of a datagram when two transport paths deliver it.
///
/// Membership is by `(stream_id, sample_counter)` over a short sliding window,
/// because the two copies are not adjacent in time: the unicast and multicast
/// datagrams can interleave with later packets. A "same as the last packet" test
/// would miss reordered copies and let the duplicate reach the STFT, which doubles
/// that packet's contribution to the sample clock and to the loss tracker.
struct DuplicateFilter {
    /// Counters retained for membership. Larger than `retained_window` so a late
    /// duplicate is still recognised even after its bytes have been evicted.
    seen: HashMap<u32, VecDeque<u64>>,
    window: usize,
    /// The most recent copies per stream, kept so a duplicate can be compared
    /// byte for byte against the copy that was analysed. Bounded to
    /// `retained_window` entries so memory stays finite.
    retained: HashMap<u32, VecDeque<(u64, Vec<u8>)>>,
    retained_window: usize,
}

impl DuplicateFilter {
    /// Slots for the membership window: long enough for two paths to swap order
    /// across many packets, still bounded.
    const WINDOW: usize = 64;
    /// Slots retained WITH bytes. Smaller than `WINDOW` so memory stays bounded
    /// (one datagram per slot) while a duplicate several packets later can still
    /// be byte-compared.
    const RETAINED_WINDOW: usize = 16;

    fn new() -> Self {
        DuplicateFilter {
            seen: HashMap::new(),
            window: Self::WINDOW,
            retained: HashMap::new(),
            retained_window: Self::RETAINED_WINDOW,
        }
    }

    /// Narrow both windows, so a test can prove the bounds are real.
    #[cfg(test)]
    fn with_window(window: usize) -> Self {
        DuplicateFilter {
            window: window.max(1),
            retained_window: window.max(1),
            ..DuplicateFilter::new()
        }
    }

    fn observe(&mut self, stream_id: u32, counter: u64, datagram: &[u8]) -> Duplicate {
        let already_seen = self
            .seen
            .get(&stream_id)
            .is_some_and(|window| window.contains(&counter));

        if already_seen {
            // Search the retained window for the ORIGINAL copy of this counter.
            // Comparing against "the last packet seen" is what made an interleaved
            // duplicate (A, B, A) look Exact without ever touching its bytes.
            return match self
                .retained
                .get(&stream_id)
                .and_then(|window| window.iter().find(|(c, _)| *c == counter))
            {
                Some((_, bytes)) => {
                    if bytes.as_slice() == datagram {
                        Duplicate::Exact
                    } else {
                        Duplicate::Mismatch
                    }
                }
                // Counter still in `seen`, but its bytes have been evicted.
                None => Duplicate::Fallback,
            };
        }

        let seen_window = self.seen.entry(stream_id).or_default();
        seen_window.push_back(counter);
        while seen_window.len() > self.window {
            seen_window.pop_front();
        }

        let retained_window = self.retained.entry(stream_id).or_default();
        retained_window.push_back((counter, datagram.to_vec()));
        while retained_window.len() > self.retained_window {
            retained_window.pop_front();
        }

        Duplicate::New
    }
}

/// Cap on distinct source addresses tracked. A spoofed flood would otherwise grow
/// the map without bound; everything past the cap folds into one bucket.
const MAX_SOURCES: usize = 16;

/// One row of the per-source attribution table served by `/stats`.
#[derive(Debug, Clone, Serialize)]
struct SourceCount {
    addr: String,
    packets: u64,
}

/// Fold one datagram's source address into the bounded per-source counter.
///
/// This is the diagnosis tool for the duplicate rate: sigproc sends the unicast
/// copy to the ClusterIP and the multicast copy to the group, so the addresses
/// should show one of each. Three or more addresses means something else is
/// forwarding a copy, which is what a >50% duplicate rate would indicate.
fn record_source(stats: &Counters, src: SocketAddr) {
    let Ok(mut map) = stats.source_counts.lock() else {
        return;
    };
    if let Some(count) = map.get_mut(&src) {
        *count += 1;
        return;
    }
    if map.len() < MAX_SOURCES {
        map.insert(src, 1);
        return;
    }
    *map.entry(SocketAddr::from(([0, 0, 0, 0], 0))).or_insert(0) += 1;
}

/// Key for a Type-4 IF Context packet. The stream id and timestamp sit at the same
/// offsets as a data packet, so the same counter formula applies; the packet just
/// carries no samples. Returns None when the datagram is too short to key on.
fn context_key(datagram: &[u8], sample_rate: f64) -> Option<(u32, u64)> {
    if datagram.len() < 20 {
        return None;
    }
    let stream_id = u32::from_be_bytes([datagram[4], datagram[5], datagram[6], datagram[7]]);
    let ts_int = u32::from_be_bytes([datagram[8], datagram[9], datagram[10], datagram[11]]);
    let ts_frac = u64::from_be_bytes([
        datagram[12],
        datagram[13],
        datagram[14],
        datagram[15],
        datagram[16],
        datagram[17],
        datagram[18],
        datagram[19],
    ]);
    Some((
        stream_id,
        (ts_int as u64)
            .saturating_mul(sample_rate as u64)
            .saturating_add(ts_frac),
    ))
}

/// Snapshot of the per-source attribution table, highest count first.
///
/// Sorted so the dominant delivery path is first: with both paths live we expect
/// one unicast address and one multicast address, and an unexpected third entry
/// is what explains a duplicate rate above ~50%.
fn source_snapshot(stats: &Counters) -> Vec<SourceCount> {
    let mut sources: Vec<SourceCount> = match stats.source_counts.lock() {
        Ok(map) => map
            .iter()
            .map(|(addr, packets)| SourceCount {
                addr: addr.to_string(),
                packets: *packets,
            })
            .collect(),
        // A poisoned map is not worth failing a stats response over; report none
        // rather than a fake table.
        Err(_) => Vec::new(),
    };
    sources.sort_by_key(|source| std::cmp::Reverse(source.packets));
    sources
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
            let mut abs_sum = 0u64;
            let mut peak_abs = 0i16;
            for c in pkt.payload.as_chunks::<2>().0 {
                let v = i16::from_be_bytes(*c);
                if v != 0 {
                    nonzero += 1;
                }
                abs_sum += v.unsigned_abs() as u64;
                peak_abs = peak_abs.max(v.saturating_abs());
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
                "SPIKE   payload: {} components, {} distinct i16 values, {} non-zero ({:.0}%), peak |sc16|={}, mean |sc16|={:.1} -> degenerate_payload={}",
                pkt.components(),
                values.len(),
                nonzero,
                100.0 * nonzero as f64 / total as f64,
                peak_abs,
                abs_sum as f64 / total as f64,
                degenerate
            );
            if !pkt.size_field_matches {
                println!("SPIKE   NOTE: header size field disagrees with the delivered datagram length (length wins)");
            }
        }
        Err(ParseError::ContextPacket) => println!("SPIKE   IF context packet (no samples)"),
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
    let mut tracker = DataStreamTracker::default();
    // Dedupe only when a second transport path actually exists. With one path a
    // repeated counter is a sender defect, and hiding it would be dishonest.
    let mut dedupe = stats
        .multicast_joined
        .load(Ordering::Relaxed)
        .then(DuplicateFilter::new);

    // Context packets carry no samples, so they never reach the analysis queue,
    // but a duplicate copy still must not be counted twice. They get their own
    // filter so a context counter can never collide with a data counter.
    let mut context_dedupe = stats
        .multicast_joined
        .load(Ordering::Relaxed)
        .then(DuplicateFilter::new);
    loop {
        let (n, src) = match socket.recv_from(&mut buf).await {
            Ok(v) => v,
            Err(e) => {
                Counters::bump(&stats.socket_errors);
                eprintln!("waterfall-consumer: recv error: {e}");
                tokio::time::sleep(Duration::from_millis(10)).await;
                continue;
            }
        };
        // Attribute this datagram to its sender. With both transport paths live this
        // is how we tell unicast from multicast from an unexpected third copy.
        record_source(&stats, src);

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

                let counter = pkt.sample_counter(cfg.sample_rate);

                // Both transport paths deliver the same packet to this socket, so the
                // second copy must be dropped HERE: before the loss tracker (a duplicated
                // counter is not loss) and before the analysis queue (a duplicate row
                // would double that packet's contribution to the sample clock).
                if let Some(filter) = dedupe.as_mut() {
                    match filter.observe(pkt.stream_id, counter, datagram) {
                        Duplicate::New => {}
                        Duplicate::Exact => {
                            Counters::bump(&stats.duplicates_dropped);
                            Counters::bump(&stats.duplicates_compared);
                            continue;
                        }
                        Duplicate::Mismatch => {
                            Counters::bump(&stats.duplicates_dropped);
                            Counters::bump(&stats.duplicates_compared);
                            Counters::bump(&stats.duplicate_mismatch);
                            continue;
                        }
                        Duplicate::Fallback => {
                            // The original copy aged out of the retained window, so
                            // the bytes could not be compared. Still a duplicate, so
                            // drop it — but count it as unverified, not as a match.
                            Counters::bump(&stats.duplicates_dropped);
                            Counters::bump(&stats.duplicates_fallback);
                            continue;
                        }
                    }
                }

                stats
                    .samples_received
                    .fetch_add(pkt.components() as u64, Ordering::Relaxed);

                // Sample timestamps and data sequence counts are tracked per stream.
                stats
                    .last_counter
                    .store(counter.min(i64::MAX as u64) as i64, Ordering::Relaxed);

                let gap = tracker.observe(&pkt, cfg.sample_rate);
                stats.gaps.store(tracker.gaps, Ordering::Relaxed);
                stats
                    .missing_samples
                    .store(tracker.missing_samples, Ordering::Relaxed);
                stats
                    .out_of_order
                    .store(tracker.out_of_order, Ordering::Relaxed);
                stats
                    .sequence_gaps
                    .store(tracker.sequence_gaps, Ordering::Relaxed);
                if let Some(gap) = gap {
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
                    if !fixture_written.swap(true, Ordering::Relaxed) {
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
            Err(ParseError::ContextPacket) => {
                // Context packets carry no samples, so they are never analysed, but
                // they still arrive twice on the parallel paths and must be counted
                // and deduped exactly like data packets.
                Counters::bump(&stats.context_packets);
                if let Some(filter) = context_dedupe.as_mut() {
                    if let Some((stream_id, counter)) = context_key(datagram, cfg.sample_rate) {
                        match filter.observe(stream_id, counter, datagram) {
                            Duplicate::New => {}
                            Duplicate::Exact => {
                                Counters::bump(&stats.context_duplicates_dropped);
                                Counters::bump(&stats.duplicates_compared);
                            }
                            Duplicate::Mismatch => {
                                Counters::bump(&stats.context_duplicates_dropped);
                                Counters::bump(&stats.duplicates_compared);
                                Counters::bump(&stats.context_duplicate_mismatch);
                            }
                            Duplicate::Fallback => {
                                Counters::bump(&stats.context_duplicates_dropped);
                                Counters::bump(&stats.duplicates_fallback);
                            }
                        }
                    }
                }
                continue;
            }
            Err(e) => {
                Counters::bump(&stats.parse_errors);
                match e {
                    ParseError::ContextPacket => unreachable!("handled above"),
                    ParseError::ContextLengthMismatch { .. } => {}
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

        stats
            .signal_peak_sc16
            .store(pipeline.peak_abs() as u64, Ordering::Relaxed);
        stats
            .signal_mean_milli
            .store((pipeline.mean_abs() * 1000.0) as u64, Ordering::Relaxed);

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
        sequence_gaps: stats.sequence_gaps.load(Ordering::Relaxed),
        last_sample_counter: if last < 0 { None } else { Some(last as u64) },
        rcvbuf_bytes: stats.rcvbuf_bytes.load(Ordering::Relaxed),
        signal_peak_sc16: stats.signal_peak_sc16.load(Ordering::Relaxed),
        signal_mean_abs: stats.signal_mean_milli.load(Ordering::Relaxed) as f64 / 1000.0,
        multicast_group: cfg.multicast.group().map(|group| group.to_string()),
        multicast_iface: cfg.multicast.iface().map(|iface| iface.to_string()),
        multicast_joined: stats.multicast_joined.load(Ordering::Relaxed),
        multicast_join_error: stats
            .multicast_join_error
            .lock()
            .ok()
            .and_then(|error| error.clone()),
        duplicates_dropped: stats.duplicates_dropped.load(Ordering::Relaxed),
        duplicate_mismatch: stats.duplicate_mismatch.load(Ordering::Relaxed),
        duplicates_compared: stats.duplicates_compared.load(Ordering::Relaxed),
        duplicates_fallback: stats.duplicates_fallback.load(Ordering::Relaxed),
        context_packets: stats.context_packets.load(Ordering::Relaxed),
        context_duplicates_dropped: stats.context_duplicates_dropped.load(Ordering::Relaxed),
        context_duplicate_mismatch: stats.context_duplicate_mismatch.load(Ordering::Relaxed),
        sources: source_snapshot(stats),
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
            "consumer stats: {:.1} pkt/s | rx={} analyzed={} samples={} | gaps={} missing_samples={} out_of_order={} sequence_gaps={} | parse_errors={} (short={} wrong_type={} trailer={}) size_field_mismatch={} | rows_produced={} published={} publish_dropped={} stft_dropped={} | bridge_errors={} | last_counter={:?} rcvbuf={}",
            snap.packets_per_sec,
            snap.packets_received,
            snap.packets_analyzed,
            snap.samples_received,
            snap.gaps,
            snap.missing_samples,
            snap.out_of_order,
            snap.sequence_gaps,
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
        println!(
            "consumer signal: peak |sc16|={} mean |sc16|={:.1} (an 8-bit downshift is lossless only above 256)",
            snap.signal_peak_sc16, snap.signal_mean_abs
        );
        // The parallel-path line: `duplicates_dropped` climbing at about half of rx is
        // the positive proof that multicast AND unicast are both delivering.
        println!(
            "consumer path: multicast_group={:?} iface={:?} joined={} join_error={:?} | duplicates_dropped={} duplicate_mismatch={} compared={} fallback={} | context_packets={} context_duplicates_dropped={} context_duplicate_mismatch={}",
            snap.multicast_group,
            snap.multicast_iface,
            snap.multicast_joined,
            snap.multicast_join_error,
            snap.duplicates_dropped,
            snap.duplicate_mismatch,
            snap.duplicates_compared,
            snap.duplicates_fallback,
            snap.context_packets,
            snap.context_duplicates_dropped,
            snap.context_duplicate_mismatch,
        );
        // Per-source attribution: how many delivery paths actually feed this socket.
        // More than unicast + multicast is what explains a duplicate rate above ~50%.
        let sources: Vec<String> = snap
            .sources
            .iter()
            .map(|source| format!("{}={}", source.addr, source.packets))
            .collect();
        println!("consumer sources: {}", sources.join(" | "));
    }
}

/// Packet-rate meter for the `/stats` endpoint.
///
/// `stats_loop` computes its own 10 s rate, but that is a local variable for
/// the log line, not shared state. `/stats` therefore measures between
/// requests. The first request has no previous sample and falls back to the
/// average since start, so the reported rate is never a fabricated zero.
struct RateMeter {
    start: Instant,
    last_at: Instant,
    last_packets: u64,
}

impl RateMeter {
    fn new(start: Instant) -> Self {
        Self {
            start,
            last_at: start,
            last_packets: 0,
        }
    }

    /// Returns `(uptime, window, window_packets)` for the current request.
    fn observe(&mut self, packets: u64) -> (Duration, Duration, u64) {
        self.observe_at(Instant::now(), packets)
    }

    /// [`RateMeter::observe`] against an explicit clock reading.
    ///
    /// Split out so the rate arithmetic can be tested without sleeping: the
    /// monotonic clock is not the thing under test here.
    fn observe_at(&mut self, now: Instant, packets: u64) -> (Duration, Duration, u64) {
        let uptime = now.duration_since(self.start);
        let window = now.duration_since(self.last_at);
        let window_packets = packets.saturating_sub(self.last_packets);
        self.last_at = now;
        self.last_packets = packets;
        if window.as_secs_f64() < 1.0 {
            // Back-to-back requests would divide by a near-zero window and
            // report a nonsense rate; the lifetime average is the honest
            // fallback for both uptime and window.
            return (uptime, uptime, packets);
        }
        (uptime, window, window_packets)
    }
}
#[tokio::main]
async fn main() {
    let cfg = Arc::new(load_config());
    let stats = Arc::new(Counters::default());
    let batch = Arc::new(Mutex::new(RowBatch::new(64)));
    let meter = Arc::new(Mutex::new(RateMeter::new(Instant::now())));

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
    let meter_state = Arc::clone(&meter);
    let app = Router::new()
        .route(
            "/stats",
            get(move || {
                let stats = Arc::clone(&stats_state);
                let cfg = Arc::clone(&cfg_state);
                let meter = Arc::clone(&meter_state);
                async move {
                    // Uptime and rate come from the meter, not from constants:
                    // `/stats` previously reported `uptime_secs: 0` and
                    // `packets_per_sec: 0.0` forever, which made the two fields
                    // useless for spotting a stalled stream.
                    let packets = stats.packets_received.load(Ordering::Relaxed);
                    let (uptime, window, window_packets) = {
                        let mut meter = meter.lock().expect("rate meter mutex poisoned");
                        meter.observe(packets)
                    };
                    Json(snapshot(&stats, &cfg, uptime, window, window_packets))
                }
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

    // Graceful shutdown so a rollout exits zero instead of panicking the serve
    // loop and showing up as an `Error` pod (see waterfall::shutdown).
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown::signal())
        .await
        .expect("stats server error");
}

#[cfg(test)]
mod tests {
    use super::*;

    fn data(seq: u8, counter: u64, samples: usize) -> Vec<u8> {
        let mut packet = vec![0u8; 20 + 4 * samples];
        let header = 0x10d0_0000 | ((seq as u32) << 16) | (5 + samples as u32);
        packet[..4].copy_from_slice(&header.to_be_bytes());
        packet[4..8].copy_from_slice(&5u32.to_be_bytes());
        packet[12..20].copy_from_slice(&counter.to_be_bytes());
        packet
    }

    #[tokio::test]
    async fn receive_loop_skips_context_and_tracks_variable_data_packets() {
        let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let target = socket.local_addr().unwrap();
        let sender = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let stats = Arc::new(Counters::default());
        let cfg = Arc::new(Config {
            listen: target,
            bridge_url: String::new(),
            sample_rate: 2_000_000.0,
            center_hz: 0.0,
            publish_hz: 30.0,
            stats_listen: target,
            rcvbuf: 0,
            hexdump_first: 0,
            capture_fixture: None,
            multicast: MulticastConfig::Off,
        });
        let (tx, mut rx) = mpsc::channel(8);
        let task = tokio::spawn(receive_loop(
            socket,
            tx,
            Arc::clone(&stats),
            cfg,
            Arc::new(AtomicBool::new(false)),
        ));
        let mut context = vec![0u8; 60];
        context[..4].copy_from_slice(&0x40d9_000fu32.to_be_bytes());
        let packets = [
            context.clone(),
            data(15, 0, 360),
            context.clone(),
            data(0, 360, 12),
            data(2, 384, 8),
            context[..24].to_vec(),
            data(3, 392, 1),
        ];
        for packet in &packets {
            sender.send_to(packet, target).await.unwrap();
        }
        for _ in 0..4 {
            tokio::time::timeout(Duration::from_secs(2), rx.recv())
                .await
                .unwrap()
                .unwrap();
        }
        tokio::time::timeout(Duration::from_secs(2), async {
            while stats.packets_analyzed.load(Ordering::Relaxed) < 4 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(stats.packets_received.load(Ordering::Relaxed), 7);
        assert_eq!(stats.packets_analyzed.load(Ordering::Relaxed), 4);
        // Only the two well-formed context packets reach the context path; the
        // truncated third copy fails the declared-length check first and is counted
        // as a parse error instead.
        assert_eq!(stats.context_packets.load(Ordering::Relaxed), 2);
        assert_eq!(stats.parse_errors.load(Ordering::Relaxed), 1);
        assert_eq!(stats.size_field_mismatch.load(Ordering::Relaxed), 0);
        assert_eq!(stats.gaps.load(Ordering::Relaxed), 1);
        assert_eq!(stats.missing_samples.load(Ordering::Relaxed), 12);
        assert_eq!(stats.sequence_gaps.load(Ordering::Relaxed), 1);
        task.abort();
    }

    #[tokio::test]
    async fn context_packets_are_deduped_on_the_parallel_path() {
        // Context packets carry no samples and are never analysed, but they still
        // arrive twice when both transport paths are live. Dropping the second copy
        // keeps the context accounting honest instead of double-counting it.
        let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let target = socket.local_addr().unwrap();
        let sender = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let stats = Arc::new(Counters::default());
        stats.multicast_joined.store(true, Ordering::Relaxed);
        let cfg = Arc::new(Config {
            listen: target,
            bridge_url: String::new(),
            sample_rate: 2_000_000.0,
            center_hz: 0.0,
            publish_hz: 30.0,
            stats_listen: target,
            rcvbuf: 0,
            hexdump_first: 0,
            capture_fixture: None,
            multicast: MulticastConfig::Joined {
                group: "239.255.49.2".parse().unwrap(),
                iface: "127.0.0.1".parse().unwrap(),
            },
        });
        let (tx, _rx) = mpsc::channel(8);
        let task = tokio::spawn(receive_loop(
            socket,
            tx,
            Arc::clone(&stats),
            cfg,
            Arc::new(AtomicBool::new(false)),
        ));

        // One context packet, delivered twice by the two paths.
        let mut context = vec![0u8; 60];
        context[..4].copy_from_slice(&0x40d9_000fu32.to_be_bytes());
        // A distinct stream id so this cannot collide with a data packet key.
        context[4..8].copy_from_slice(&0x0000_00ffu32.to_be_bytes());
        for _ in 0..2 {
            sender.send_to(&context, target).await.unwrap();
        }

        tokio::time::timeout(Duration::from_secs(2), async {
            while stats.context_packets.load(Ordering::Relaxed) < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        assert_eq!(stats.context_packets.load(Ordering::Relaxed), 2);
        assert_eq!(
            stats.context_duplicates_dropped.load(Ordering::Relaxed),
            1,
            "the second context copy must be dropped, not counted twice"
        );
        assert_eq!(
            stats.duplicates_compared.load(Ordering::Relaxed),
            1,
            "the dropped copy was byte-compared, so it counts as verified"
        );
        assert_eq!(
            stats.context_duplicate_mismatch.load(Ordering::Relaxed),
            0,
            "identical copies must not report a mismatch"
        );
        task.abort();
    }

    #[test]
    fn duplicate_filter_drops_the_second_copy_and_keeps_the_first() {
        let mut filter = DuplicateFilter::new();
        let first = data(4, 1024, 8);
        assert_eq!(filter.observe(7, 1024, &first), Duplicate::New);
        assert_eq!(filter.observe(7, 1024, &first), Duplicate::Exact);
        // Same counter on a different stream is a different packet.
        assert_eq!(filter.observe(8, 1024, &first), Duplicate::New);
        // A new counter on the first stream is not a duplicate.
        assert_eq!(filter.observe(7, 4096, &data(5, 4096, 8)), Duplicate::New);
    }

    #[test]
    fn duplicate_filter_compares_bytes_not_just_counters() {
        let mut filter = DuplicateFilter::new();
        let original = data(4, 1024, 8);
        assert_eq!(filter.observe(7, 1024, &original), Duplicate::New);

        let mut altered = original.clone();
        let last = altered.len() - 1;
        altered[last] ^= 0xff;
        assert_eq!(
            filter.observe(7, 1024, &altered),
            Duplicate::Mismatch,
            "same counter with different bytes must not pass as an identical copy"
        );
    }

    #[test]
    fn duplicate_filter_compares_interleaved_duplicates() {
        // The regression this guards: retaining only the MOST RECENT datagram meant
        // an interleaved duplicate (A, B, A') was compared against B, so a differing
        // A' was reported Exact without its bytes ever being examined. Interleaving
        // is the normal case here — the unicast and multicast copies are not adjacent.
        let mut filter = DuplicateFilter::new();
        let a = data(4, 1024, 8);
        let b = data(4, 1536, 8);

        assert_eq!(filter.observe(7, 1024, &a), Duplicate::New);
        assert_eq!(filter.observe(7, 1536, &b), Duplicate::New);

        // A' for the older counter, arriving after B. Must be byte-compared to A.
        let mut a_prime = a.clone();
        let last = a_prime.len() - 1;
        a_prime[last] ^= 0xff;
        assert_eq!(
            filter.observe(7, 1024, &a_prime),
            Duplicate::Mismatch,
            "an interleaved duplicate with different bytes must be a mismatch"
        );

        // And a faithful copy of A still reads as Exact, not as a second mismatch.
        assert_eq!(filter.observe(7, 1024, &a), Duplicate::Exact);
    }

    #[test]
    fn duplicate_filter_reports_fallback_when_bytes_were_evicted() {
        // The membership window is longer than the retained window, so a duplicate
        // that is still "seen" can be older than the bytes we kept. That must be
        // reported honestly as unverified rather than silently claimed as Exact.
        let mut filter = DuplicateFilter::new();
        let first = data(4, 0, 8);
        assert_eq!(filter.observe(9, 0, &first), Duplicate::New);

        // Push the first copy's bytes out of the retained window without leaving the
        // membership window.
        for i in 1..=DuplicateFilter::RETAINED_WINDOW {
            let counter = i as u64 * 512;
            assert_eq!(
                filter.observe(9, counter, &data(4, counter, 8)),
                Duplicate::New
            );
        }

        assert_eq!(
            filter.observe(9, 0, &first),
            Duplicate::Fallback,
            "a duplicate whose bytes were evicted cannot be verified"
        );
    }

    #[test]
    fn duplicate_filter_window_is_bounded_so_a_real_repeat_stays_visible() {
        let mut filter = DuplicateFilter::with_window(2);
        for counter in [0, 512, 1024] {
            assert_eq!(
                filter.observe(1, counter, &data(0, counter, 4)),
                Duplicate::New
            );
        }
        // Counter 0 has fallen out of a window of 2: a repeat is new again, not hidden.
        assert_eq!(filter.observe(1, 0, &data(0, 0, 4)), Duplicate::New);
        // The most recent counters are still recognised as copies.
        assert_eq!(filter.observe(1, 1024, &data(0, 1024, 4)), Duplicate::Exact);
    }

    #[test]
    fn multicast_config_is_opt_in_and_never_fatal() {
        assert_eq!(parse_multicast(None, None), MulticastConfig::Off);
        assert_eq!(
            parse_multicast(Some("  "), Some("192.168.0.48")),
            MulticastConfig::Off
        );
        assert_eq!(
            parse_multicast(Some("239.255.49.2"), Some("192.168.0.48")),
            MulticastConfig::Joined {
                group: "239.255.49.2".parse().unwrap(),
                iface: "192.168.0.48".parse().unwrap(),
            }
        );
        assert_eq!(
            parse_multicast(Some("239.255.49.2"), None),
            MulticastConfig::KernelChosen("239.255.49.2".parse().unwrap())
        );
        // A unicast address is not a group: disabled, never accepted.
        assert_eq!(
            parse_multicast(Some("192.168.0.48"), Some("192.168.0.48")),
            MulticastConfig::Off
        );
        // Garbage in the interface slot disables the join; it must not silently
        // become "let the kernel choose".
        assert_eq!(
            parse_multicast(Some("239.255.49.2"), Some("not-an-address")),
            MulticastConfig::Off
        );
    }

    #[test]
    fn joining_with_an_address_that_is_not_local_reports_an_error() {
        // The degraded path must be observable, not a panic: a wrong
        // --multicast-iface leaves the unicast stream working.
        let socket = socket2::Socket::new(
            socket2::Domain::IPV4,
            socket2::Type::DGRAM,
            Some(socket2::Protocol::UDP),
        )
        .unwrap();
        socket
            .bind(&"0.0.0.0:0".parse::<SocketAddr>().unwrap().into())
            .unwrap();
        let stats = Arc::new(Counters::default());
        let cfg = Config {
            listen: "0.0.0.0:0".parse().unwrap(),
            bridge_url: String::new(),
            sample_rate: 2_000_000.0,
            center_hz: 0.0,
            publish_hz: 30.0,
            stats_listen: "0.0.0.0:0".parse().unwrap(),
            rcvbuf: 0,
            hexdump_first: 0,
            capture_fixture: None,
            multicast: MulticastConfig::Joined {
                group: "239.255.49.2".parse().unwrap(),
                // TEST-NET-3: guaranteed not to be a local interface address.
                iface: "203.0.113.9".parse().unwrap(),
            },
        };
        let error = join_multicast(&socket, &cfg, &stats).expect_err("not a local address");
        assert!(!error.to_string().is_empty());
        assert!(!stats.multicast_joined.load(Ordering::Relaxed));
        assert_eq!(
            stats
                .multicast_join_error
                .lock()
                .expect("join error slot")
                .as_deref(),
            Some(error.to_string().as_str())
        );
    }

    #[test]
    fn multicast_is_off_by_default_so_nothing_joins_unless_asked() {
        let cfg = Config {
            listen: "0.0.0.0:4820".parse().unwrap(),
            bridge_url: String::new(),
            sample_rate: 2_000_000.0,
            center_hz: 0.0,
            publish_hz: 30.0,
            stats_listen: "0.0.0.0:4830".parse().unwrap(),
            rcvbuf: 0,
            hexdump_first: 0,
            capture_fixture: None,
            multicast: MulticastConfig::Off,
        };
        let socket = socket2::Socket::new(
            socket2::Domain::IPV4,
            socket2::Type::DGRAM,
            Some(socket2::Protocol::UDP),
        )
        .unwrap();
        let stats = Arc::new(Counters::default());
        assert!(join_multicast(&socket, &cfg, &stats).is_ok());
        assert!(!stats.multicast_joined.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn a_second_copy_of_the_same_datagram_is_not_analysed_twice() {
        // The parallel-run signature: unicast and multicast deliver identical bytes
        // to one socket, so one packet must produce one row source, not two.
        let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let target = socket.local_addr().unwrap();
        let sender = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let stats = Arc::new(Counters::default());
        stats.multicast_joined.store(true, Ordering::Relaxed);
        let cfg = Arc::new(Config {
            listen: target,
            bridge_url: String::new(),
            sample_rate: 2_000_000.0,
            center_hz: 0.0,
            publish_hz: 30.0,
            stats_listen: target,
            rcvbuf: 0,
            hexdump_first: 0,
            capture_fixture: None,
            multicast: MulticastConfig::Joined {
                group: "239.255.49.2".parse().unwrap(),
                iface: "127.0.0.1".parse().unwrap(),
            },
        });
        let (tx, mut rx) = mpsc::channel(8);
        let task = tokio::spawn(receive_loop(
            socket,
            tx,
            Arc::clone(&stats),
            cfg,
            Arc::new(AtomicBool::new(false)),
        ));

        let packet = data(9, 8192, 16);
        for _ in 0..2 {
            sender.send_to(&packet, target).await.unwrap();
        }
        tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while stats.packets_received.load(Ordering::Relaxed) < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        assert_eq!(
            stats.packets_analyzed.load(Ordering::Relaxed),
            1,
            "one packet must reach the STFT once, not twice"
        );
        assert_eq!(stats.duplicates_dropped.load(Ordering::Relaxed), 1);
        assert_eq!(stats.duplicate_mismatch.load(Ordering::Relaxed), 0);
        assert_eq!(
            stats.out_of_order.load(Ordering::Relaxed),
            0,
            "a duplicate is not reordering and must not be reported as loss"
        );
        assert_eq!(stats.gaps.load(Ordering::Relaxed), 0);
        task.abort();
    }

    #[test]
    fn rate_meter_reports_real_values_not_zeros() {
        // The bug this guards: `/stats` used to pass Duration::ZERO for both
        // uptime and window, so a live stream reported uptime_secs=0 and
        // packets_per_sec=0.0 forever — indistinguishable from a dead stream.
        // The clock is injected so this asserts arithmetic, not scheduler speed.
        let start = Instant::now();
        let mut meter = RateMeter::new(start);

        // First sample: no previous reading, so the sub-second guard reports the
        // lifetime average rather than dividing by an empty window.
        let first = start + Duration::from_secs(5);
        assert_eq!(
            meter.observe_at(first, 1_000),
            (Duration::from_secs(5), Duration::from_secs(5), 1_000),
            "first sample falls back to the lifetime average"
        );

        // Second sample 2 s later: uptime spans the whole run, the window is
        // only the last interval, and the count is the delta between samples.
        let second = first + Duration::from_secs(2);
        assert_eq!(
            meter.observe_at(second, 1_347),
            (Duration::from_secs(7), Duration::from_secs(2), 347),
            "later samples report the interval, not the run total"
        );

        // A counter that appears to go backwards must not underflow into a
        // gigantic rate; saturating_sub floors the delta at zero.
        assert_eq!(
            meter.observe_at(second + Duration::from_secs(3), 1_347).2,
            0
        );
    }
}
