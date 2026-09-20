// src/main.rs — Waterfall bridge server (web tier).
//
// Serves the front end and fans out spectrogram rows from whichever producer is
// configured:
//
//   * `GET  /ws`      — WebSocket push of `{type:"sweep", bins, samples, intensity[]}`
//                       dBFS rows as they arrive.
//   * `GET  /chunks?time=<start>&size=<n>` — REST fallback returning the last
//                       rows in ascending time order (the front end polls this
//                       when the WebSocket is unavailable).
//   * `POST /ingest`  — row batches from `waterfall-consumer` (the middleware
//                       tier). This is the deployed data path.
//   * `GET  /meta`    — RF metadata for the front end: centre frequency, sample
//                       rate, producer, freshness, and the consumer's loss
//                       counters. Drives the absolute MHz frequency axis.
//   * `GET  /stats`   — ingest counters for operators and verification.
//   * fallback        — static files (index.html, main.js, dataClient.js).
//
// `--source synthetic|ingest` selects the producer. `synthetic` (the default)
// keeps the M1 local development loop working with no cluster and no RF. The
// deployed Deployment runs `--source ingest`, where there is deliberately NO
// synthetic fallback: if the RF feed stops, `/meta` reports stale and the rows
// stop, because fabricated rows look exactly like real signal on a waterfall.

use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Query, State,
    },
    http::StatusCode,
    response::{IntoResponse, Json},
    routing::{get, post},
    Router,
};
use num_complex::Complex;
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::{
    collections::VecDeque,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};
use tower_http::services::ServeDir;
use waterfall::cli;
use waterfall::publish::IngestRequest;
use waterfall::{complex_vec_to_bytes, StftProcessor, HOP_SIZE};

/// Default sample rate of the synthetic source. Bins 0..N map to 0..SR Hz.
const SAMPLE_RATE: f32 = 48_000.0;
/// Max rows retained for `/chunks` (~8 MB at 4096 bins/row).
const MAX_CHUNKS: usize = 512;
/// A feed with no row for this long is reported as stale.
const STALE_AFTER: Duration = Duration::from_secs(3);

/// One spectrogram row served to the front end.
#[derive(Clone, Serialize)]
struct SweepEntry {
    #[serde(rename = "type")]
    kind: &'static str,
    bins: usize,
    samples: u64,
    intensity: Vec<f32>,
}

impl SweepEntry {
    fn from_row(row: waterfall::Row) -> Self {
        SweepEntry {
            kind: "sweep",
            bins: row.bins.len(),
            samples: row.sample,
            intensity: row.bins,
        }
    }
}

/// Producer state: what fed the ring, and how fresh it is.
#[derive(Debug, Default)]
struct ProducerMeta {
    /// Which producer last delivered a row: "synthetic" or "vita49".
    source: String,
    center_hz: f64,
    sample_rate_hz: f64,
    last_row_at: Option<SystemTime>,
    rows_ingested: u64,
    batches_ingested: u64,
    bins: usize,
    /// Latest loss/health report from the middleware tier.
    consumer: Option<waterfall::publish::IngestMeta>,
}

/// Shared server state: fan-out channel, bounded history ring, producer state.
struct AppState {
    tx: tokio::sync::broadcast::Sender<SweepEntry>,
    ring: Mutex<VecDeque<SweepEntry>>,
    producer: Mutex<ProducerMeta>,
    /// Producer selected at startup (`synthetic` or `ingest`).
    configured_source: &'static str,
}

#[derive(Deserialize)]
struct ChunksQuery {
    time: Option<u64>,
    size: Option<usize>,
}

/// Served by `GET /meta`: everything the front end needs to label the plot.
#[derive(Serialize)]
struct MetaResponse {
    /// Producer that last delivered rows.
    source: String,
    /// Producer this bridge was started with.
    configured_source: &'static str,
    center_hz: f64,
    sample_rate_hz: f64,
    /// Bins per row (0 until the first row arrives).
    bins: usize,
    /// Milliseconds since the last row, if any.
    stale_ms: Option<u128>,
    /// True when nothing has arrived for `STALE_AFTER`.
    stale: bool,
    rows_ingested: u64,
    batches_ingested: u64,
    ring_len: usize,
    consumer: Option<waterfall::publish::IngestMeta>,
}

/// Served by `GET /stats`.
#[derive(Serialize)]
struct StatsResponse {
    source: String,
    configured_source: &'static str,
    rows_ingested: u64,
    batches_ingested: u64,
    ring_len: usize,
    ring_capacity: usize,
    subscribers: usize,
    stale: bool,
    consumer: Option<waterfall::publish::IngestMeta>,
}

#[derive(Serialize)]
struct IngestAck {
    accepted: usize,
    ring_len: usize,
}

fn source_meta(
    state: &AppState,
) -> (
    String,
    f64,
    f64,
    usize,
    Option<u128>,
    bool,
    u64,
    u64,
    Option<waterfall::publish::IngestMeta>,
) {
    let p = state.producer.lock().unwrap();
    let stale_ms = p.last_row_at.and_then(|t| {
        SystemTime::now()
            .duration_since(t)
            .ok()
            .map(|d| d.as_millis())
    });
    let stale = match stale_ms {
        Some(ms) => ms > STALE_AFTER.as_millis(),
        None => true,
    };
    (
        p.source.clone(),
        p.center_hz,
        p.sample_rate_hz,
        p.bins,
        stale_ms,
        stale,
        p.rows_ingested,
        p.batches_ingested,
        p.consumer.clone(),
    )
}

// ---------------- Routing ----------------

fn app(state: Arc<AppState>, static_dir: &str) -> Router {
    Router::new()
        .route("/ws", get(ws_handler))
        .route("/chunks", get(chunks_handler))
        .route("/ingest", post(ingest_handler))
        .route("/meta", get(meta_handler))
        .route("/stats", get(stats_handler))
        .fallback_service(ServeDir::new(static_dir))
        .with_state(state)
}

/// Accept a batch of rows from the middleware tier and fan it out.
///
/// Returns 202 immediately: the consumer must never wait on the web tier.
async fn ingest_handler(
    State(st): State<Arc<AppState>>,
    Json(request): Json<IngestRequest>,
) -> impl IntoResponse {
    let accepted = request.rows.len();

    {
        let mut p = st.producer.lock().unwrap();
        p.source = request.meta.source.clone();
        p.center_hz = request.meta.center_hz;
        p.sample_rate_hz = request.meta.sample_rate_hz;
        p.last_row_at = Some(SystemTime::now());
        p.rows_ingested += accepted as u64;
        p.batches_ingested += 1;
        p.consumer = Some(request.meta);
    }

    let mut ring = st.ring.lock().unwrap();
    for row in request.rows {
        let entry = SweepEntry {
            kind: "sweep",
            bins: row.bins.len(),
            samples: row.sample,
            intensity: row.bins,
        };
        {
            let mut p = st.producer.lock().unwrap();
            p.bins = entry.bins;
        }
        let _ = st.tx.send(entry.clone()); // ignore "no subscribers"
        ring.push_back(entry);
        while ring.len() > MAX_CHUNKS {
            ring.pop_front();
        }
    }
    let ring_len = ring.len();
    drop(ring);

    (StatusCode::ACCEPTED, Json(IngestAck { accepted, ring_len }))
}

/// RF metadata for the front end's frequency axis, with freshness and loss.
async fn meta_handler(State(st): State<Arc<AppState>>) -> impl IntoResponse {
    let (source, center_hz, sample_rate_hz, bins, stale_ms, stale, rows, batches, consumer) =
        source_meta(&st);
    Json(MetaResponse {
        source,
        configured_source: st.configured_source,
        center_hz,
        sample_rate_hz,
        bins,
        stale_ms,
        stale,
        rows_ingested: rows,
        batches_ingested: batches,
        ring_len: st.ring.lock().unwrap().len(),
        consumer,
    })
}

async fn stats_handler(State(st): State<Arc<AppState>>) -> impl IntoResponse {
    let (source, _c, _s, _b, _ms, stale, rows, batches, consumer) = source_meta(&st);
    Json(StatsResponse {
        source,
        configured_source: st.configured_source,
        rows_ingested: rows,
        batches_ingested: batches,
        ring_len: st.ring.lock().unwrap().len(),
        ring_capacity: MAX_CHUNKS,
        subscribers: st.tx.receiver_count(),
        stale,
        consumer,
    })
}

/// WebSocket: stream every row as it is produced. No backpressure beyond the
/// broadcast buffer — a slow client just misses frames, which is fine for a
/// waterfall.
async fn ws_handler(ws: WebSocketUpgrade, State(st): State<Arc<AppState>>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| websocket_loop(socket, st))
}

async fn websocket_loop(mut socket: WebSocket, st: Arc<AppState>) {
    let mut rx = st.tx.subscribe();
    loop {
        let Ok(entry) = rx.recv().await else { break };
        let text = serde_json::to_string(&entry).unwrap_or_default();
        if socket.send(Message::Text(text)).await.is_err() {
            break;
        }
    }
}

/// REST fallback: rows with `samples >= time`, ascending, up to `size`.
async fn chunks_handler(
    Query(q): Query<ChunksQuery>,
    State(st): State<Arc<AppState>>,
) -> impl IntoResponse {
    let time = q.time.unwrap_or(0);
    let size = q.size.unwrap_or(128).min(MAX_CHUNKS);
    let ring = st.ring.lock().unwrap();
    let start = ring
        .iter()
        .position(|e| e.samples >= time)
        .unwrap_or(ring.len());
    let rows: Vec<_> = ring.iter().skip(start).take(size).cloned().collect();
    Json(rows)
}

// ---------------- Synthetic I/Q source (development only) ----------------

/// Generate `n` complex baseband samples: a slow-drifting tone plus noise.
/// Produces a visible moving band on the waterfall so the local dev feed is
/// obviously real STFT output, not random per-bin noise.
fn gen_block(n: usize, phase: &mut u64, rng: &mut impl Rng) -> Vec<i8> {
    let mut samples = Vec::with_capacity(n);
    for _ in 0..n {
        let t = *phase as f32 / SAMPLE_RATE;
        *phase += 1;
        // Center frequency drifts between ~300 Hz and ~6 kHz over ~30 s.
        let drift = (t / 30.0).sin() * 0.5 + 0.5; // 0..1
        let f = 300.0 + (5_700.0 * drift);
        let a = 60.0 * (t / 1.5).sin().abs().max(0.25); // slow amplitude swell
        let re = a * (2.0 * std::f32::consts::PI * f * t).cos() + rng.gen_range(-2.0..2.0);
        let im = a * (2.0 * std::f32::consts::PI * f * t).sin() + rng.gen_range(-2.0..2.0);
        samples.push(Complex::new(re, im));
    }
    complex_vec_to_bytes(&samples)
}

/// Background engine: generate I/Q, run it through the STFT, fan each row out to
/// WebSocket subscribers and the history ring. Runs on its own thread so a busy
/// FFT never stalls the HTTP/WS runtime. Only started for `--source synthetic`.
fn engine_loop(st: Arc<AppState>, sweep_ms: u64, center_hz: f64, sample_rate_hz: f64) {
    let mut proc = StftProcessor::new(waterfall::FFT_SIZE, HOP_SIZE);
    let mut phase = 0u64;
    let mut rng = rand::thread_rng();
    {
        let mut p = st.producer.lock().unwrap();
        p.source = "synthetic".to_string();
        p.center_hz = center_hz;
        p.sample_rate_hz = sample_rate_hz;
    }
    loop {
        let block = gen_block(HOP_SIZE, &mut phase, &mut rng);
        for row in proc.push_bytes(&block) {
            let entry = SweepEntry::from_row(row);
            {
                let mut p = st.producer.lock().unwrap();
                p.last_row_at = Some(SystemTime::now());
                p.bins = entry.bins;
                p.rows_ingested += 1;
            }
            let _ = st.tx.send(entry.clone()); // ignore "no subscribers"
            let mut ring = st.ring.lock().unwrap();
            ring.push_back(entry);
            while ring.len() > MAX_CHUNKS {
                ring.pop_front();
            }
        }
        std::thread::sleep(Duration::from_millis(sweep_ms));
    }
}

// ---------------- Entry point ----------------

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();

    // Both `--flag value` and `--flag=value` are accepted (see waterfall::cli).
    // The equals form matters: a Deployment once passed `--source=ingest`, this
    // parser only matched the spaced form, and the bridge silently came up in
    // synthetic mode. Unrecognized flags are reported instead of ignored.
    const KNOWN: &[&str] = &[
        "--source",
        "--static",
        "--sweep-ms",
        "--center-freq",
        "--sample-rate",
    ];
    let unknown = cli::unknown_flags(&args, KNOWN);
    if !unknown.is_empty() {
        eprintln!(
            "waterfall bridge: ignoring unrecognized flag(s): {}",
            unknown.join(", ")
        );
    }

    let listen = std::env::var("WATERFALL_LISTEN").unwrap_or_else(|_| "127.0.0.1:4780".into());
    let static_dir = cli::flag_value(&args, "--static").unwrap_or_else(|| ".".into());
    let sweep_ms: u64 = cli::parse_or(&args, "--sweep-ms", 60_u64);
    let source = std::env::var("WATERFALL_SOURCE")
        .ok()
        .or_else(|| cli::flag_value(&args, "--source"))
        .unwrap_or_else(|| "synthetic".into());
    let center_hz: f64 = cli::parse_or(&args, "--center-freq", 915_000_000.0_f64);
    let sample_rate_hz: f64 = cli::parse_or(&args, "--sample-rate", 2_000_000.0_f64);

    let configured_source: &'static str = match source.as_str() {
        "ingest" | "vita49" => "ingest",
        _ => "synthetic",
    };

    let (tx, _) = tokio::sync::broadcast::channel(128);
    let state = Arc::new(AppState {
        tx,
        ring: Mutex::new(VecDeque::new()),
        producer: Mutex::new(ProducerMeta::default()),
        configured_source,
    });

    if configured_source == "synthetic" {
        let engine = Arc::clone(&state);
        std::thread::spawn(move || engine_loop(engine, sweep_ms, center_hz, sample_rate_hz));
        println!("waterfall bridge: source=synthetic (development feed; no RF)");
    } else {
        println!(
            "waterfall bridge: source=ingest — awaiting POST /ingest from waterfall-consumer; \
             NO synthetic fallback (a dead feed reports stale, it does not fabricate rows)"
        );
    }
    let addr: SocketAddr = listen.parse().expect("invalid WATERFALL_LISTEN address");
    println!(
        "waterfall bridge: http://{addr}  (static: {static_dir}, centre {:.0} Hz, {:.0} Hz sample rate)",
        center_hz, sample_rate_hz
    );
    let router = app(state, &static_dir);
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("bind failed");
    axum::serve(listener, router).await.expect("server error");
}
