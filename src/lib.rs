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
use std::f32::consts::PI;

// Constants derived from the spec.
const FFT_SIZE: usize = 4096;
const HOP_SIZE: usize = 1024; // 75% overlap: (4096-1024)/4096

// ---------------- MLX (Apple GPU / Metal) backend ----------------
#[cfg(feature = "mlx")]
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

fn plan_cpu_fft() -> std::sync::Arc<dyn rustfft::Fft<f32> + Send + Sync> {
    let mut planner = FftPlanner::<f32>::new();
    planner.plan_fft_forward(FFT_SIZE)
}

/// A unified forward-FFT over the selected backend (CPU / MLX).
enum FftBackend {
    Cpu(std::sync::Arc<dyn rustfft::Fft<f32> + Send + Sync>),
    #[cfg(feature = "mlx")]
    Mlx(mlx_integration::MlxFft),
}

// SAFETY: both variants are shareable across rayon threads.
unsafe impl Sync for FftBackend {}

#[cfg(feature = "mlx")]
impl FftBackend {
    fn plan() -> FftBackend {
        match mlx_integration::plan_fft_forward(FFT_SIZE) {
            Ok(p) => FftBackend::Mlx(p),
            Err(e) => {
                eprintln!("{e}; falling back to CPU FFT.");
                FftBackend::Cpu(plan_cpu_fft())
            }
        }
    }
}

#[cfg(not(feature = "mlx"))]
impl FftBackend {
    fn plan() -> FftBackend {
        FftBackend::Cpu(plan_cpu_fft())
    }
}

#[cfg(feature = "mlx")]
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

#[cfg(not(feature = "mlx"))]
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
    let fft = FftBackend::plan();

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
            signal.push(Complex::new(
                100.0 * (2.0 * PI * freq_hz * t).sin(),
                100.0 * (2.0 * PI * freq_hz * t).cos(),
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
        for i in start..end {
            let e = first_frame[i].norm_sqr();
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
}

// ---------------- Demo / main ----------------

#[allow(dead_code)]
fn main() {
    println!("Starting STFT demo...");
    let freq_hz = 100.0;
    let sample_rate = 48000.0;
    let num_samples = (4 * HOP_SIZE) + FFT_SIZE;

    let mut signal = Vec::with_capacity(num_samples);
    for i in 0..num_samples {
        let t = i as f32 / sample_rate;
        signal.push(Complex::new(
            100.0 * (2.0 * PI * freq_hz * t).sin(),
            100.0 * (2.0 * PI * freq_hz * t).cos(),
        ));
    }
    let input_bytes = complex_vec_to_bytes(&signal);
    println!(
        "Generated {} bytes of input data ({} complex samples).",
        input_bytes.len(),
        input_bytes.len() / 2
    );

    let spectrogram = stft(&input_bytes);
    let num_frames = if FFT_SIZE > 0 {
        spectrogram.len() / FFT_SIZE
    } else {
        0
    };
    println!(
        "STFT computed. Spectrogram size: {} complex samples ({} frames * {} bins).",
        spectrogram.len(),
        num_frames,
        FFT_SIZE
    );

    // Sanity check on the dominant bin of the first frame.
    if !spectrogram.is_empty() && FFT_SIZE > 0 {
        let bin_index = (freq_hz * FFT_SIZE as f32 / sample_rate).round() as usize;
        if bin_index < FFT_SIZE {
            let first_frame = &spectrogram[..FFT_SIZE];
            let search_radius = 2;
            let start = bin_index.saturating_sub(search_radius);
            let end = (bin_index + search_radius + 1).min(FFT_SIZE);

            let mut max_energy = 0.0f32;
            let mut peak_bin = None;
            for i in start..end {
                let e = first_frame[i].norm_sqr();
                if e > max_energy {
                    max_energy = e;
                    peak_bin = Some(i);
                }
            }
            if let Some(peak) = peak_bin {
                println!(
                    "Injected {freq_hz:.1} Hz maps to bin {bin_index}; peak found at bin {peak} ({max_energy:.2})."
                );
                let lo = peak.saturating_sub(3);
                let hi = (peak + 4).min(FFT_SIZE);
                for i in lo..hi {
                    println!(
                        "  Bin {i}: {:.2} + {:.2}j  |.|^2 = {:.2}",
                        spectrogram[i].re,
                        spectrogram[i].im,
                        spectrogram[i].norm_sqr()
                    );
                }
            } else {
                println!("Demo warning: no peak near expected bin {bin_index}.");
            }
        }
    }
}
