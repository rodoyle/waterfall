//! Shared types for the middleware → web-tier hop, plus the latest-wins publish
//! buffer that keeps the receive loop off the network path.
//!
//! The consumer analyses every packet but cannot publish every row: at 2 MS/s
//! with a 4096-point FFT and 1024-sample hop the STFT produces ~1953 rows/s of
//! 4096 bins (~64 MB/s of JSON), far more than a waterfall display or a LAN hop
//! wants. Rows are therefore queued in a bounded, latest-wins buffer and drained
//! on a fixed cadence. Drops are always counted, never silent.

use serde::{Deserialize, Serialize};
use std::collections::VecDeque;

/// One spectrogram row on the wire.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RowDto {
    /// Absolute index of this frame's first sample across the whole stream.
    pub sample: u64,
    /// Per-bin dBFS power.
    pub bins: Vec<f32>,
}

/// Metadata the consumer reports alongside its rows.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IngestMeta {
    /// Which producer this batch came from: `"vita49"` in the deployed path.
    pub source: String,
    /// RF centre frequency in Hz.
    pub center_hz: f64,
    /// Sample rate in Hz.
    pub sample_rate_hz: f64,
    /// Unwrapped last sample counter seen by the receiver.
    pub last_sample_counter: u64,
    /// Detected sample-counter discontinuities.
    pub gaps: u64,
    /// Samples never delivered across those gaps.
    pub missing_samples: u64,
    /// Packets received since start.
    pub packets_received: u64,
    /// Packets that failed to parse.
    pub parse_errors: u64,
    /// Packets dropped because the analysis channel was full (no blocking).
    pub stft_dropped: u64,
    /// Rows dropped by this buffer before publishing.
    pub publish_dropped: u64,
    /// Effective SO_RCVBUF the kernel granted (may be capped by rmem_max).
    pub rcvbuf_bytes: u64,
}

/// Body of `POST /ingest`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IngestRequest {
    pub rows: Vec<RowDto>,
    pub meta: IngestMeta,
}

/// Latest-wins bounded batch of rows awaiting publication.
///
/// Bounded: when full, the OLDEST row is discarded so the newest data always
/// survives (a waterfall that lags behind is worse than one that skips).
#[derive(Debug)]
pub struct RowBatch {
    rows: VecDeque<RowDto>,
    capacity: usize,
    dropped: u64,
}

impl RowBatch {
    /// `capacity` is the maximum number of rows held between publications.
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "capacity must be positive");
        RowBatch {
            rows: VecDeque::with_capacity(capacity),
            capacity,
            dropped: 0,
        }
    }

    /// Queue a row, evicting the oldest if at capacity.
    pub fn push(&mut self, row: RowDto) {
        if self.rows.len() == self.capacity {
            self.rows.pop_front();
            self.dropped += 1;
        }
        self.rows.push_back(row);
    }

    /// Take everything queued, oldest first.
    pub fn drain(&mut self) -> Vec<RowDto> {
        self.rows.drain(..).collect()
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Rows evicted before publication.
    pub fn dropped(&self) -> u64 {
        self.dropped
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(sample: u64) -> RowDto {
        RowDto {
            sample,
            bins: vec![sample as f32],
        }
    }

    #[test]
    fn batch_keeps_the_newest_rows_when_full() {
        let mut batch = RowBatch::new(3);
        for s in 0..5 {
            batch.push(row(s));
        }
        assert_eq!(batch.len(), 3);
        assert_eq!(batch.dropped(), 2);
        let drained = batch.drain();
        assert_eq!(
            drained.iter().map(|r| r.sample).collect::<Vec<_>>(),
            vec![2, 3, 4],
            "oldest rows are the ones discarded"
        );
        assert!(batch.is_empty());
        assert_eq!(batch.dropped(), 2, "drop count survives a drain");
    }

    #[test]
    fn ingest_request_round_trips_through_json() {
        let req = IngestRequest {
            rows: vec![row(0), row(1024)],
            meta: IngestMeta {
                source: "vita49".into(),
                center_hz: 915_000_000.0,
                sample_rate_hz: 2_000_000.0,
                last_sample_counter: 2048,
                gaps: 1,
                missing_samples: 4096,
                packets_received: 3,
                parse_errors: 0,
                stft_dropped: 0,
                publish_dropped: 0,
                rcvbuf_bytes: 8_388_608,
            },
        };
        let json = serde_json::to_string(&req).expect("serializes");
        let back: IngestRequest = serde_json::from_str(&json).expect("deserializes");
        assert_eq!(back, req);
    }
}
