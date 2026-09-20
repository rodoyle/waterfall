// axis.js — frequency-axis mapping for the waterfall front end.
//
// The STFT bins are baseband: bin 0 is the lowest frequency the sample rate can
// represent and the last bin is the highest. sigproc streams 2 MS/s centred on
// 915 MHz, so a bin only becomes meaningful once it is mapped to an absolute RF
// frequency. This module owns that mapping and the label formatting, because it
// is the part worth testing (`axis.test.js`, run with `bun test`).
//
// Loaded as a module (`<script type="module">`) so it is available to the
// classic scripts by the time they boot on DOMContentLoaded, and exported so the
// tests can import it directly.

/** Fallback centre frequency: the sigproc ConfigMap's 915 MHz ISM band. */
export const DEFAULT_CENTER_HZ = 915_000_000;

/** Fallback sample rate: 2 MS/s, straight from the sigproc ConfigMap. */
export const DEFAULT_SAMPLE_RATE_HZ = 2_000_000;

/**
 * Absolute RF frequency of a display bin.
 *
 * `binCount` is the number of bins spanning the captured band, so each bin is
 * `sampleRateHz / binCount` wide and bin centres are offset by half a bin.
 */
export function binToHz(bin, binCount, centerHz, sampleRateHz) {
    if (!(binCount > 0)) return centerHz;
    const perBin = sampleRateHz / binCount;
    return centerHz - sampleRateHz / 2 + (bin + 0.5) * perBin;
}

/** Inverse of {@link binToHz} (used to place cursors/markers by frequency). */
export function hzToBin(hz, binCount, centerHz, sampleRateHz) {
    if (!(binCount > 0) || !(sampleRateHz > 0)) return 0;
    const perBin = sampleRateHz / binCount;
    return (hz - (centerHz - sampleRateHz / 2)) / perBin - 0.5;
}

/**
 * Human label for an absolute frequency.
 *
 * MHz with three decimals is what makes a 915 MHz band readable to the 1 kHz
 * level (915.000 MHz), which is the resolution a 2 MS/s waterfall shows.
 */
export function formatHzLabel(hz) {
    if (!Number.isFinite(hz)) return '--';
    const abs = Math.abs(hz);
    if (abs >= 1e6) return `${(hz / 1e6).toFixed(3)} MHz`;
    if (abs >= 1e3) return `${(hz / 1e3).toFixed(3)} kHz`;
    return `${hz.toFixed(1)} Hz`;
}

/**
 * Derive the axis configuration from the bridge's `/meta` payload.
 *
 * A real RF stream always yields an absolute axis; a baseband-only or
 * synthetic development source keeps the old relative behaviour.
 */
export function axisFromMeta(meta, fallback = {}) {
    const centerHz = Number(meta && meta.center_hz) || fallback.centerHz || DEFAULT_CENTER_HZ;
    const sampleRateHz =
        Number(meta && meta.sample_rate_hz) || fallback.sampleRateHz || DEFAULT_SAMPLE_RATE_HZ;
    const absolute = Boolean(meta && meta.center_hz && meta.sample_rate_hz);
    return {
        centerHz,
        sampleRateHz,
        absoluteAxis: absolute,
        freqMin: centerHz - sampleRateHz / 2,
        freqMax: centerHz + sampleRateHz / 2,
        // A log scale over a ~2 MHz window around 915 MHz is meaningless.
        freqScale: absolute ? 'linear' : fallback.freqScale || 'linear',
        source: (meta && meta.source) || fallback.source || 'unknown',
        stale: Boolean(meta && meta.stale),
    };
}

/** Apply `/meta` to the live config object in place (returns it for chaining). */
export function applyMetaToConfig(config, meta) {
    Object.assign(config, axisFromMeta(meta, config));
    return config;
}

/**
 * Evenly spaced axis labels across `config.freqMin..config.freqMax`.
 *
 * Returns `{ hz, text, fraction }` with `fraction` 0 at the bottom of the plot
 * and 1 at the top, so the renderer stays layout-only.
 */
export function axisLabels(config, count = 5) {
    const labels = [];
    for (let i = 0; i < count; i++) {
        const fraction = count === 1 ? 0 : i / (count - 1);
        const hz = config.freqMin + (config.freqMax - config.freqMin) * fraction;
        labels.push({
            hz,
            text: config.absoluteAxis ? formatHzLabel(hz) : `${hz.toFixed(hz >= 1000 ? 0 : 1)} Hz`,
            fraction,
        });
    }
    return labels;
}

const api = {
    binToHz,
    hzToBin,
    formatHzLabel,
    axisFromMeta,
    applyMetaToConfig,
    axisLabels,
    DEFAULT_CENTER_HZ,
    DEFAULT_SAMPLE_RATE_HZ,
};

// Classic scripts (main.js) read this global; module importers use the exports.
if (typeof globalThis !== 'undefined') globalThis.WaterfallAxis = api;

export default api;
