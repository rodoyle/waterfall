// src/main.rs — Waterfall bridge server (Milestone M1).
//
// A single Rust binary that owns the STFT processor and serves the front end:
//
//   * `GET /ws`    — WebSocket push of `{type:"sweep", bins, samples, intensity[]}`
//                    dBFS rows as they are produced by the STFT.
//   * `GET /chunks?time=<start>&size=<n>` — REST fallback returning the last
//                    rows in ascending time order (the front end polls this when
//                    the WebSocket is unavailable).
//   * fallback      — static files (index.html, main.js, dataClient.js).
//
// Until the VITA49 consumer exists (Milestone M2), the STFT is fed by a
// background *synthetic* I/Q generator so the front end shows live, real
// STFT-processed data instead of random on-the-fly sweeps. When M2 lands, the
// consumer feeds the same `waterfall::StftProcessor` and only this generator
// changes.

use axum::{
    extract::{ws::{Message, WebSocket, WebSocketUpgrade}, Query, State},
    response::{IntoResponse, Json},
    routing::get,
    Router,
};
use num_complex::Complex;
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::{
    collections::VecDeque,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};
use tower_http::services::ServeDir;
use waterfall::{complex_vec_to_bytes, StftProcessor, HOP_SIZE};

/// Default sample rate of the synthetic source. Bins 0..N map to 0..SR Hz.
const SAMPLE_RATE: f32 = 48_000.0;
/// Max rows retained for `/chunks`.
const MAX_CHUNKS: usize = 512;

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

/// Shared server state: fan-out channel + bounded history ring.
struct AppState {
    tx: tokio::sync::broadcast::Sender<SweepEntry>,
    ring: Mutex<VecDeque<SweepEntry>>,
}

#[derive(Deserialize)]
struct ChunksQuery {
    time: Option<u64>,
    size: Option<usize>,
}

// ---------------- Routing ----------------

fn app(state: Arc<AppState>, static_dir: &str) -> Router {
    Router::new()
        .route("/ws", get(ws_handler))
        .route("/chunks", get(chunks_handler))
        .fallback_service(ServeDir::new(static_dir))
        .with_state(state)
}

/// WebSocket: stream every STFT row as it is produced. No backpressure beyond
/// the broadcast buffer — a slow client just misses frames, which is fine for a
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
    let start = ring.iter().position(|e| e.samples >= time).unwrap_or(ring.len());
    let rows: Vec<_> = ring.iter().skip(start).take(size).cloned().collect();
    Json(rows)
}

// ---------------- Synthetic I/Q source ----------------

/// Generate `n` complex baseband samples: a slow-drifting tone plus noise.
/// Produces a visible moving band on the waterfall so the live feed is obviously
/// real STFT output, not random per-bin noise.
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
/// FFT never stalls the HTTP/WS runtime.
fn engine_loop(st: Arc<AppState>, sweep_ms: u64) {
    let mut proc = StftProcessor::new(waterfall::FFT_SIZE, HOP_SIZE);
    let mut phase = 0u64;
    let mut rng = rand::thread_rng();
    loop {
        let block = gen_block(HOP_SIZE, &mut phase, &mut rng);
        for row in proc.push_bytes(&block) {
            let entry = SweepEntry::from_row(row);
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
    let listen = std::env::var("WATERFALL_LISTEN").unwrap_or_else(|_| "127.0.0.1:4780".into());
    let static_dir = args
        .iter()
        .position(|a| a == "--static")
        .and_then(|i| args.get(i + 1).cloned())
        .unwrap_or_else(|| ".".into());
    let sweep_ms: u64 = args
        .iter()
        .position(|a| a == "--sweep-ms")
        .and_then(|i| args.get(i + 1))
        .and_then(|v| v.parse().ok())
        .unwrap_or(60);

    let (tx, _) = tokio::sync::broadcast::channel(128);
    let state = Arc::new(AppState {
        tx,
        ring: Mutex::new(VecDeque::new()),
    });

    // Synthetic-feed engine thread.
    let engine = Arc::clone(&state);
    std::thread::spawn(move || engine_loop(engine, sweep_ms));

    let addr: SocketAddr = listen.parse().expect("invalid WATERFALL_LISTEN address");
    println!("waterfall bridge: http://{addr}  (static: {static_dir})");
    let router = app(state, &static_dir);
    let listener = tokio::net::TcpListener::bind(addr).await.expect("bind failed");
    axum::serve(listener, router).await.expect("server error");
}