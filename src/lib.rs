// src/lib.rs
//
// Short-Time Fourier Transform (STFT) of a wide complex baseband signal.
//
// Data arrives as interleaved 8-bit I/Q samples (VITA49 / UHD): bytes
// [I0,Q0, I1,Q1, ...]. Each 2 bytes = one complex16 sample, converted to
// Complex<f32> and fed through a 4096-point forward FFT per STFT frame, with
// frames advanced by a 1024-sample hop (75% overlap).
//
// Two backends:
//   * default (no features): SIMD-optimized CPU FFT via rustfft + rayon.
//   * `--features mlx`: GPU/Metal accelerated FFT via Apple MLX (mlx-sys).
//
// A window function is assumed to have already been applied by the caller.

use num_complex::Complex;
use rayon::prelude::*;
use rustfft::FftPlanner;

/// VITA 49.0 wire-format parsing for the sigproc UDP stream (pure functions).
pub mod vita49;

/// Types shared by the middleware (consumer) and web tier (bridge), plus the
/// latest-wins publish buffer.
pub mod publish;

/// The datagram → STFT pipeline (parse, sc16 → i8, FFT, gap tracking), shared by
/// the consumer binary and the integration tests.
pub mod consumer;

/// Flag parsing shared by both binaries (`--flag value` and `--flag=value`).
pub mod cli;

// Constants derived from the spec.
pub const FFT_SIZE: usize = 4096;
pub const HOP_SIZE: usize = 1024; // 75% overlap: (4096-1024)/4096

// ---------------- MLX (Apple GPU / Metal) backend ----------------
#[cfg(all(feature = "mlx", target_os = "macos"))]
mod mlx_integration {
    use mlx_sys as m;
    use num_complex::Complex;
    use std::os::raw::c_int;

    /// Error type for the MLX path (mlx_sys reports failures via `c_int` code
    /// or a null pointer).
    #[derive(Debug)]
    pub struct MlxError(pub String);
    impl std::fmt::Display for MlxError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "MLX Error: {}", self.0)
        }
    }
    impl std::error::Error for MlxError {}

    /// A plan for an N-point forward FFT on the MLX Metal backend.
    pub struct MlxFft {
        #[allow(dead_code)] // device held for the lifetime of the plan
        device: m::mlx_device,
        stream: m::mlx_stream,
        n: c_int,
    }

    // SAFETY: the MLX default device/stream are process-global singletons AND
    // every process() call takes MLX_LOCK, so handing this plan to rayon threads
    // is safe (the lock serializes concurrent Metal command-buffer encoding).
    unsafe impl Sync for MlxFft {}

    /// Serializes GPU access. Metal forbids concurrent encoding into the default
    /// stream/command buffer from multiple threads; rayon invokes `process` in
    /// parallel, so all MLX FFI calls must be serialized.
    static MLX_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    impl MlxFft {
        /// Execute the forward FFT of `buffer` in-place on the MLX backend.
        pub fn process(&self, buffer: &mut [Complex<f32>]) -> Result<(), MlxError> {
            let n = self.n as usize;
            if buffer.len() != n {
                return Err(MlxError(format!(
                    "MlxFft::process got {} elements, expected {}",
                    buffer.len(),
                    n
                )));
            }
            let _guard = MLX_LOCK.lock().unwrap();
            unsafe {
                // Pack input into a complex64 1-D MLX array, shape [n].
                let shape = [n as c_int];
                let input = m::mlx_array_new_data(
                    buffer.as_ptr() as *const std::ffi::c_void,
                    shape.as_ptr(),
                    1,
                    m::mlx_dtype__MLX_COMPLEX64,
                );

                // `res` is an out-param: mlx_fft_fft allocates it. Do NOT
                // preallocate (mlx_array_set_data with a NULL data pointer
                // dereferences NULL and segfaults).
                let mut output = std::mem::zeroed::<m::mlx_array>();

                // Forward FFT (axis 0, full size).
                let rc = m::mlx_fft_fft(&mut output, input, self.n, 0, self.stream);
                if rc != 0 {
                    m::mlx_array_free(input);
                    m::mlx_array_free(output);
                    return Err(MlxError(format!("mlx_fft_fft failed: {rc}")));
                }

                // Evaluate, verify size, and copy back.
                m::mlx_array_eval(output);
                if m::mlx_array_size(output) != n {
                    m::mlx_array_free(input);
                    m::mlx_array_free(output);
                    return Err(MlxError("MLX FFT output size mismatch".to_string()));
                }
                let data = m::mlx_array_data_complex64(output);
                if data.is_null() {
                    m::mlx_array_free(input);
                    m::mlx_array_free(output);
                    return Err(MlxError("MLX FFT output is null after eval".to_string()));
                }
                // `complex float *` from the C ABI is layout-identical to
                // num_complex::Complex<f32>; copy raw bytes to be explicit.
                let nbytes = n * std::mem::size_of::<Complex<f32>>();
                let src = std::slice::from_raw_parts(data as *const u8, nbytes);
                let dst = std::slice::from_raw_parts_mut(buffer.as_mut_ptr() as *mut u8, nbytes);
                dst.copy_from_slice(src);

                m::mlx_array_free(input);
                m::mlx_array_free(output);
                Ok(())
            }
        }
    }

    /// Build an N-point forward-FFT plan backed by MLX (Metal).
    pub fn plan_fft_forward(n: usize) -> Result<MlxFft, MlxError> {
        unsafe {
            // Default device.
            let mut device = std::mem::zeroed::<m::mlx_device>();
            let rc = m::mlx_get_default_device(&mut device);
            if rc != 0 {
                return Err(MlxError(format!("mlx_get_default_device failed: {rc}")));
            }
            // Default stream for that device.
            let mut stream = std::mem::zeroed::<m::mlx_stream>();
            let rc = m::mlx_get_default_stream(&mut stream, device);
            if rc != 0 {
                return Err(MlxError(format!("mlx_get_default_stream failed: {rc}")));
            }
            Ok(MlxFft {
                device,
                stream,
                n: n as c_int,
            })
        }
    }
}

// ---------------- FFT backend abstraction ----------------

fn plan_cpu_fft(n: usize) -> std::sync::Arc<dyn rustfft::Fft<f32> + Send + Sync> {
    let mut planner = FftPlanner::<f32>::new();
    planner.plan_fft_forward(n)
}

/// A unified forward-FFT over the selected backend (CPU / MLX).
enum FftBackend {
    Cpu(std::sync::Arc<dyn rustfft::Fft<f32> + Send + Sync>),
    #[cfg(all(feature = "mlx", target_os = "macos"))]
    Mlx(mlx_integration::MlxFft),
}

// SAFETY: both variants are shareable across rayon threads.
unsafe impl Sync for FftBackend {}

#[cfg(all(feature = "mlx", target_os = "macos"))]
impl FftBackend {
    fn plan(n: usize) -> FftBackend {
        match mlx_integration::plan_fft_forward(n) {
            Ok(p) => FftBackend::Mlx(p),
            Err(e) => {
                eprintln!("{e}; falling back to CPU FFT.");
                FftBackend::Cpu(plan_cpu_fft(n))
            }
        }
    }
}

#[cfg(any(not(feature = "mlx"), not(target_os = "macos")))]
impl FftBackend {
    fn plan(n: usize) -> FftBackend {
        FftBackend::Cpu(plan_cpu_fft(n))
    }
}

#[cfg(all(feature = "mlx", target_os = "macos"))]
impl FftBackend {
    /// Run the forward FFT, dispatching to MLX when the backend is MLX.
    fn process(&self, buffer: &mut [Complex<f32>]) -> Result<(), String> {
        match self {
            FftBackend::Cpu(f) => {
                f.process(buffer);
                Ok(())
            }
            FftBackend::Mlx(p) => p.process(buffer).map_err(|e| e.to_string()),
        }
    }
}

#[cfg(any(not(feature = "mlx"), not(target_os = "macos")))]
impl FftBackend {
    /// Run the forward FFT on the CPU backend.
    fn process(&self, buffer: &mut [Complex<f32>]) -> Result<(), String> {
        match self {
            FftBackend::Cpu(f) => {
                f.process(buffer);
                Ok(())
            }
        }
    }
}

// ---------------- Data conversion ----------------

/// Decode interleaved signed 8-bit I/Q into Complex<f32>.
fn bytes_to_complex(bytes: &[i8]) -> Vec<Complex<f32>> {
    bytes
        .chunks_exact(2)
        .map(|c| Complex::new(c[0] as f32, c[1] as f32))
        .collect()
}

/// Window already applied upstream (spec); no-op placeholder.
fn apply_window(_samples: &mut [Complex<f32>]) {}

// ---------------- STFT ----------------

/// Compute the STFT spectrogram of a complex baseband signal.
///
/// * `input_bytes` – interleaved 8-bit I/Q (VITA49 / UHD).
/// * Returns a flat `Vec<Complex<f32>>` in row-major `frame * FFT_SIZE + bin`
///   order (each frame is a 4096-bin complex FFT; frames advance by 1024).
pub fn stft(input_bytes: &[i8]) -> Vec<Complex<f32>> {
    let complex_samples = bytes_to_complex(input_bytes);
    let num_complex_samples = complex_samples.len();

    if num_complex_samples < FFT_SIZE {
        eprintln!(
            "STFT Error: input has too few samples ({}) for FFT size ({}). Minimum required: {}.",
            num_complex_samples, FFT_SIZE, FFT_SIZE
        );
        return Vec::new();
    }

    let num_frames = (num_complex_samples.saturating_sub(FFT_SIZE)) / HOP_SIZE + 1;

    // Select backend.
    let fft = FftBackend::plan(FFT_SIZE);

    // Frame the input and run one FFT per frame in parallel (rayon).
    let frames: Vec<Result<Vec<Complex<f32>>, String>> = (0..num_frames)
        .into_par_iter()
        .map(|frame| {
            let start = frame * HOP_SIZE;
            let mut window = complex_samples[start..start + FFT_SIZE].to_vec();
            apply_window(&mut window);
            fft.process(&mut window)?;
            Ok(window)
        })
        .collect();

    // Assemble the spectrogram, logging any frame-level errors.
    let mut spectrogram: Vec<Complex<f32>> = Vec::with_capacity(num_frames * FFT_SIZE);
    for frame_result in frames {
        match frame_result {
            Ok(frame) => spectrogram.extend(frame),
            Err(e) => {
                eprintln!("STFT frame error: {e}; inserting zero frame.");
                spectrogram.extend(std::iter::repeat_n(Complex::new(0.0, 0.0), FFT_SIZE));
            }
        }
    }

    spectrogram
}

// ---------------- dB conversion + incremental streaming ----------------

/// dB floor: values below this are clipped, matching the front end's −100..0
/// display range (see `docs/plans/frontend.md`)
const DB_FLOOR: f32 = -100.0;

/// Convert complex FFT bins to dBFS power values.
///
/// Mapping: an 8-bit signed I/Q sample has full-scale amplitude 127 per
/// component. A complex tone of amplitude `A` (analytic, single-sided) lands at
/// bin magnitude `|X[k]| ≈ A * N` for an `N`-point forward FFT, so normalizing by
/// `N` and referencing 127 gives `0 dBFS` for a full-scale tone and a
/// meaningful [DB_FLOOR, 0] range for everything else.
///
/// ```text
/// dbfs = 20 * log10( |X[k]| / N ) - 20 * log10(127),  clamped to [DB_FLOOR, 0]
/// ```
/// Convert complex FFT bins to dBFS power.
///
/// * `bins` – one row of complex FFT output (length = FFT size).
/// * Returns one dBFS value per bin in [DB_FLOOR, 0].
pub fn to_db(bins: &[Complex<f32>]) -> Vec<f32> {
    let n = bins.len().max(1) as f32;
    bins.iter()
        .map(|c| {
            // #/N = amplitude (FFT-gain normalization); reference full-scale 8-bit
            // amplitude 127 so a full-scale tone reads 0 dBFS. Log floor on `amp`
            // avoids log(0) and maps into [DB_FLOOR, 0].
            let amp = (c.norm() / n).max(1e-12);
            let dbfs = 20.0 * (amp / 127.0).log10();
            dbfs.clamp(DB_FLOOR, 0.0)
        })
        .collect()
}

/// One spectrogram row emitted by [`StftProcessor`]; consumed by the bridge
/// server and ultimately rendered as a waterfall line by the front end.
#[derive(Debug, Clone)]
pub struct Row {
    /// Absolute index of this frame's first sample across the whole stream.
    pub sample: u64,
    /// Per-bin dBFS power (length = fft_size).
    pub bins: Vec<f32>,
}

/// Incremental short-time Fourier transform with overlap carry-over.
///
/// Takes interleaved 8-bit I/Q bytes via [`StftProcessor::push_bytes`], keeps
/// an overlap buffer across calls so a continuous stream is framed at `hop`
/// spacing (75% overlap by default), and emits one [`Row`] of dBFS bins per
/// completed frame. The only public entry point consuming I/Q bytes; designed so
/// the VITA49 consumer (Milestone M2) and the bridge generator both feed it.
pub struct StftProcessor {
    fft_size: usize,
    hop: usize,
    backend: FftBackend,
    pending: Vec<Complex<f32>>,
    next_sample: u64,
}

impl StftProcessor {
    /// Create a processor with the given FFT size and hop.
    pub fn new(fft_size: usize, hop: usize) -> Self {
        assert!(hop > 0, "hop must be positive");
        assert!(fft_size >= hop, "fft_size must be >= hop");
        StftProcessor {
            fft_size,
            hop,
            backend: FftBackend::plan(fft_size),
            pending: Vec::with_capacity(fft_size),
            next_sample: 0,
        }
    }

    pub fn fft_size(&self) -> usize {
        self.fft_size
    }
    pub fn hop(&self) -> usize {
        self.hop
    }

    /// Feed interleaved signed 8-bit I/Q bytes, returning any frames completed
    /// by this call in time order (0..n rows).
    pub fn push_bytes(&mut self, iq: &[i8]) -> Vec<Row> {
        self.pending.extend(bytes_to_complex(iq));
        let mut out = Vec::new();
        while self.pending.len() >= self.fft_size {
            let mut window: Vec<Complex<f32>> = self.pending[..self.fft_size].to_vec();
            apply_window(&mut window);
            let _ = self.backend.process(&mut window);
            out.push(Row {
                sample: self.next_sample,
                bins: to_db(&window),
            });
            // Consume `hop` samples, keeping `fft_size - hop` overlap for the
            // next frame.
            self.pending.drain(..self.hop);
            self.next_sample += self.hop as u64;
        }
        out
    }

    /// Absolute index of the next sample to be consumed.
    pub fn samples_consumed(&self) -> u64 {
        self.next_sample
    }
}

// ---------------- Shared helper ----------------

/// Round-trip helper: Complex<f32> -> interleaved i8 I/Q (tests + demo).
pub fn complex_vec_to_bytes(complex_samples: &[Complex<f32>]) -> Vec<i8> {
    let mut bytes = Vec::with_capacity(complex_samples.len() * 2);
    for s in complex_samples {
        bytes.push(s.re.clamp(-128.0, 127.0) as i8);
        bytes.push(s.im.clamp(-128.0, 127.0) as i8);
    }
    bytes
}

// ---------------- Tests ----------------

#[cfg(test)]
mod tests {
    use super::*;
    use rand::Rng;
    use std::f32::consts::PI;

    #[test]
    fn test_bytes_to_complex_conversion() {
        let original = vec![
            Complex::new(10.0, 20.0),
            Complex::new(-5.0, 0.0),
            Complex::new(127.0, -128.0),
            Complex::new(128.0, -129.0), // clamps into i8 range
        ];
        let bytes = complex_vec_to_bytes(&original);
        assert_eq!(bytes, vec![10, 20, -5, 0, 127, -128, 127, -128]);

        let decoded = bytes_to_complex(&bytes);
        assert_eq!(decoded.len(), 4);
        assert_eq!(decoded[0], Complex::new(10.0, 20.0));
        assert_eq!(decoded[3], Complex::new(127.0, -128.0));
    }

    #[test]
    fn test_stft_output_shape() {
        // 2 frames: need FFT_SIZE + HOP_SIZE complex samples.
        let num_samples = FFT_SIZE + HOP_SIZE;
        let input_bytes = vec![0i8; num_samples * 2];
        let spectrogram = stft(&input_bytes);
        assert_eq!(spectrogram.len(), 2 * FFT_SIZE);
    }

    #[test]
    fn test_stft_sine_wave() {
        let freq_hz = 50.0;
        let sample_rate = 48000.0;
        let num_samples = (4 * HOP_SIZE) + FFT_SIZE;
        let mut signal = Vec::with_capacity(num_samples);
        for i in 0..num_samples {
            let t = i as f32 / sample_rate;
            // cos + i sin = e^{iωt}: positive-frequency analytic tone.
            signal.push(Complex::new(
                100.0 * (2.0 * PI * freq_hz * t).cos(),
                100.0 * (2.0 * PI * freq_hz * t).sin(),
            ));
        }
        let input_bytes = complex_vec_to_bytes(&signal);
        let spectrogram = stft(&input_bytes);

        let num_complex_samples = input_bytes.len() / 2;
        let num_frames = (num_complex_samples.saturating_sub(FFT_SIZE)) / HOP_SIZE + 1;
        assert_eq!(spectrogram.len(), num_frames * FFT_SIZE);

        // The injected tone should land at bin = round(freq/fs * NFFT).
        let bin_index = (freq_hz * FFT_SIZE as f32 / sample_rate).round() as usize;
        assert!(bin_index < FFT_SIZE);

        let first_frame = &spectrogram[..FFT_SIZE];
        let search_radius = 2;
        let start = bin_index.saturating_sub(search_radius);
        let end = (bin_index + search_radius + 1).min(FFT_SIZE);

        let mut max_energy = 0.0f32;
        let mut peak_bin = None;
        for (i, bin) in first_frame.iter().enumerate().take(end).skip(start) {
            let e = bin.norm_sqr();
            if e > max_energy {
                max_energy = e;
                peak_bin = Some(i);
            }
        }
        let peak_bin = peak_bin.expect("no peak near expected bin");
        println!("Sine test: expected bin {bin_index}, peak at {peak_bin}");
        assert!(
            peak_bin >= start && peak_bin < end,
            "peak {peak_bin} outside [{start},{end})"
        );

        // Tone should dominate the frame (allowing for noise floor).
        let total_energy: f32 = first_frame.iter().map(|c| c.norm_sqr()).sum();
        assert!(
            max_energy > total_energy * 0.000001,
            "tone not dominant (peak {max_energy} vs total {total_energy})"
        );
    }

    #[test]
    fn test_stft_random_data_not_all_zeros() {
        let mut rng = rand::thread_rng();
        let num_bytes = (FFT_SIZE + HOP_SIZE) * 2; // two frames
        let input_bytes: Vec<i8> = (0..num_bytes).map(|_| rng.gen_range(-128..=127)).collect();
        let spectrogram = stft(&input_bytes);
        let num_samples = input_bytes.len() / 2;
        let num_frames = (num_samples.saturating_sub(FFT_SIZE)) / HOP_SIZE + 1;
        assert_eq!(spectrogram.len(), num_frames * FFT_SIZE);
        assert!(
            spectrogram.iter().any(|c| c.re != 0.0 || c.im != 0.0),
            "spectrogram of random input was all zeros"
        );
    }

    #[test]
    fn test_stft_edge_case_input_length() {
        // Too short -> empty.
        assert!(stft(&vec![0i8; (FFT_SIZE - 100) * 2]).is_empty());
        // Exactly one frame.
        assert_eq!(stft(&vec![0i8; FFT_SIZE * 2]).len(), FFT_SIZE);
    }

    #[test]
    fn test_to_db_fullscale_is_zero() {
        // A complex tone of full-scale amplitude 127 at one bin -> magnitude
        // n*127 -> exactly 0 dBFS.
        let n = 1024;
        let mut bins = vec![Complex::new(0.0, 0.0); n];
        bins[17] = Complex::new(127.0 * n as f32, 0.0);
        let db = to_db(&bins);
        assert!(
            (db[17].abs()) < 1e-3,
            "full-scale bin should read ~0, got {}",
            db[17]
        );
        assert!(db.iter().filter(|v| **v < -90.0).count() >= n - 1);
    }

    #[test]
    fn test_to_db_silence_clipped_to_floor() {
        let n = 256;
        let bins = vec![Complex::new(0.0, 0.0); n];
        let db = to_db(&bins);
        assert!(db.iter().all(|v| (*v - (-100.0)).abs() < 1e-3));
    }

    #[test]
    fn test_stft_processor_incremental_matches_batch() {
        // Feed the same contiguous I/Q through the incremental processor
        // (split across push calls) and compare, frame-by-frame, to the batch
        // `stft` + `to_db` at the default frame geometry.
        let num_samples = (2 * HOP_SIZE) + FFT_SIZE;
        let mut signal = Vec::with_capacity(num_samples);
        for i in 0..num_samples {
            signal.push(Complex::new(
                80.0 * (2.0 * PI * (i as f32) / 64.0).sin(),
                80.0 * (2.0 * PI * (i as f32) / 64.0).cos(),
            ));
        }
        let bytes = complex_vec_to_bytes(&signal);

        // Reference: batch path.
        let raw = stft(&bytes);
        let expected_frames: Vec<Vec<f32>> = raw.chunks_exact(FFT_SIZE).map(to_db).collect();

        // Incremental: split the byte stream into odd-sized chunks so the
        // carry-over boundary falls mid-frame.
        let mut proc = StftProcessor::new(FFT_SIZE, HOP_SIZE);
        let mut collected: Vec<Row> = Vec::new();
        let mut i = 0;
        let chunk = 7000; // does not divide 2*hop samples, exercises carry-over
        while i < bytes.len() {
            let end = (i + chunk).min(bytes.len());
            collected.extend(proc.push_bytes(&bytes[i..end]));
            i = end;
        }
        assert_eq!(collected.len(), expected_frames.len());
        for (got, exp) in collected.iter().zip(expected_frames.iter()) {
            assert_eq!(got.bins.len(), exp.len());
            for (j, expected) in exp.iter().enumerate().take(got.bins.len()) {
                assert!(
                    (got.bins[j] - expected).abs() < 1e-3,
                    "bin {j} mismatch: incremental {} vs batch {}",
                    got.bins[j],
                    expected
                );
            }
        }
        // Sample positions advance by `hop` from 0.
        for (k, r) in collected.iter().enumerate() {
            assert_eq!(r.sample, (k as u64) * HOP_SIZE as u64);
        }
    }

    #[test]
    fn test_stft_processor_sine_tone_bin() {
        let fft_size = 4096;
        let hop = 1024;
        let sample_rate = 48000.0;
        let freq_hz = 500.0;
        let num_samples = (4 * hop) + fft_size;
        let mut signal = Vec::with_capacity(num_samples);
        for i in 0..num_samples {
            let t = i as f32 / sample_rate;
            // cos + i sin = e^{iωt}: positive-frequency analytic tone.
            signal.push(Complex::new(
                100.0 * (2.0 * PI * freq_hz * t).cos(),
                100.0 * (2.0 * PI * freq_hz * t).sin(),
            ));
        }
        let bytes = complex_vec_to_bytes(&signal);
        let mut proc = StftProcessor::new(fft_size, hop);
        let rows = proc.push_bytes(&bytes);
        assert!(!rows.is_empty());
        let bin_index = (freq_hz * fft_size as f32 / sample_rate).round() as usize;
        let first = &rows[0].bins;
        let search_radius = 2;
        let start = bin_index.saturating_sub(search_radius);
        let end = (bin_index + search_radius + 1).min(fft_size);
        let peak_bin = (start..end)
            .max_by(|a, b| first[*a].partial_cmp(&first[*b]).unwrap())
            .unwrap();
        assert!(
            peak_bin >= start && peak_bin < end,
            "peak {peak_bin} outside [{start},{end})"
        );
        // Tone should read near 0 dBFS (amplitude 100 -> ~ -2 dB).
        assert!(
            first[peak_bin] > -5.0 && first[peak_bin] <= 0.0,
            "got {}",
            first[peak_bin]
        );
    }
}
