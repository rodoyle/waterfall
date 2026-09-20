# waterfall — bridge (web tier) + VITA49 consumer (middleware) image (linux/amd64)
#
# Builds BOTH binaries plus the static front end into one image; each Deployment
# selects which binary runs. No platform pin: kaniko builds this natively on an
# amd64 cluster node (there is no QEMU in the path).
#
# All dependencies are pure Rust (rustfft, rayon, tokio, axum, reqwest without
# TLS), so unlike sigproc there is no UHD/system library to install, and
# cargo-chef keeps the dependency layer cached across source edits.

# ── Stage 1: Chef — toolchain + cargo-chef ────────────────────────────────────
FROM rust:latest AS chef
RUN cargo install cargo-chef

# ── Stage 2: Planner — dependency recipe from the real source tree ────────────
FROM chef AS planner
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src/ src/
RUN cargo chef prepare --recipe-path recipe.json

# ── Stage 3: Builder — cached deps, then the real build ───────────────────────
FROM chef AS builder
WORKDIR /build

# Cook dependencies only — this layer is cached while recipe.json is unchanged.
COPY --from=planner /build/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json

# Real sources: only this layer invalidates on edits.
COPY Cargo.toml Cargo.lock ./
COPY src/ src/
# Both binaries are built from the same crate; `--bins` covers
# src/main.rs (waterfall) and src/bin/waterfall-consumer.rs.
RUN cargo build --release --bins

# ── Stage 4: Runtime ─────────────────────────────────────────────────────────
FROM debian:trixie-slim

# The bridge serves these as static files (see --static /app/static).
WORKDIR /app/static
COPY index.html main.js dataClient.js axis.js ./
COPY palantir.html palantir.css palantir.js ./

# Middleware + web tier binaries.
COPY --from=builder /build/target/release/waterfall /usr/local/bin/waterfall
COPY --from=builder /build/target/release/waterfall-consumer /usr/local/bin/waterfall-consumer

# Default entrypoint is the bridge; waterfall-consumer.yaml overrides it.
ENTRYPOINT ["/usr/local/bin/waterfall"]
CMD ["--source", "ingest", "--static", "/app/static"]
