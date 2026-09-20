// axis.test.js — `bun test`
//
// Gates the front-end frequency axis: sigproc streams 2 MS/s centred on 915 MHz,
// so a bin must map to a real RF frequency and the plot must carry MHz labels.

import { test, expect } from 'bun:test';
import {
    binToHz,
    hzToBin,
    formatHzLabel,
    axisFromMeta,
    applyMetaToConfig,
    axisLabels,
    autoIntensityRange,
} from './axis.js';

const CENTER = 915_000_000;
const RATE = 2_000_000;
const BINS = 256;

test('bins map to an absolute span centred on the RF centre frequency', () => {
    const perBin = RATE / BINS;
    // Bin centres carry a half-bin offset, so the extremes are not the edges.
    expect(binToHz(0, BINS, CENTER, RATE)).toBeCloseTo(CENTER - RATE / 2 + perBin / 2, 3);
    expect(binToHz(BINS - 1, BINS, CENTER, RATE)).toBeCloseTo(
        CENTER + RATE / 2 - perBin / 2,
        3
    );

    // Full sweep: monotonic, and spanning the sample rate.
    let previous = -Infinity;
    for (let b = 0; b < BINS; b++) {
        const hz = binToHz(b, BINS, CENTER, RATE);
        expect(hz).toBeGreaterThan(previous);
        previous = hz;
    }
    const span = binToHz(BINS - 1, BINS, CENTER, RATE) - binToHz(0, BINS, CENTER, RATE);
    expect(span).toBeCloseTo(RATE - perBin, 3);
});

test('the middle of the band is the centre frequency within one bin', () => {
    const middle = binToHz(BINS / 2, BINS, CENTER, RATE);
    expect(Math.abs(middle - CENTER)).toBeLessThanOrEqual(RATE / BINS);
    // And the inverse mapping round-trips the bin number.
    for (const b of [0, 1, 128, 255]) {
        expect(hzToBin(binToHz(b, BINS, CENTER, RATE), BINS, CENTER, RATE)).toBeCloseTo(b, 6);
    }
});

test('labels span 914.000-916.000 MHz around the 915 MHz centre', () => {
    expect(formatHzLabel(binToHz(0, BINS, CENTER, RATE))).toBe('914.004 MHz');
    expect(formatHzLabel(binToHz(255, BINS, CENTER, RATE))).toBe('915.996 MHz');
    expect(formatHzLabel(CENTER)).toBe('915.000 MHz');
    expect(formatHzLabel(24_000)).toBe('24.000 kHz');
    expect(formatHzLabel(500)).toBe('500.0 Hz');
});

test('axis metadata is absolute and forces the linear scale', () => {
    const derived = axisFromMeta({ center_hz: CENTER, sample_rate_hz: RATE, source: 'vita49' });
    expect(derived.freqMin).toBe(CENTER - RATE / 2);
    expect(derived.freqMax).toBe(CENTER + RATE / 2);
    expect(derived.absoluteAxis).toBe(true);
    expect(derived.freqScale).toBe('linear');

    // A live config that had log selected must be forced back to linear.
    const config = { freqScale: 'log', centreLabel: 'x' };
    applyMetaToConfig(config, { center_hz: CENTER, sample_rate_hz: RATE, source: 'vita49' });
    expect(config.freqScale).toBe('linear');
    expect(config.freqMin).toBe(914_000_000);
    expect(config.freqMax).toBe(916_000_000);
    expect(config.centreLabel).toBe('x');
});

test('labels are MHz on an absolute axis and span the configured range', () => {
    const config = axisFromMeta({ center_hz: CENTER, sample_rate_hz: RATE });
    const labels = axisLabels(config, 5);
    expect(labels.map((l) => l.text)).toEqual([
        '914.000 MHz',
        '914.500 MHz',
        '915.000 MHz',
        '915.500 MHz',
        '916.000 MHz',
    ]);
    expect(labels.map((l) => l.fraction)).toEqual([0, 0.25, 0.5, 0.75, 1]);
});

test('a baseband-only source keeps the relative Hz axis', () => {
    const derived = axisFromMeta({ source: 'synthetic' }, { centerHz: 0, sampleRateHz: 48_000 });
    expect(derived.absoluteAxis).toBe(false);
    const labels = axisLabels({ ...derived, freqMin: 0, freqMax: 24_000, absoluteAxis: false }, 3);
    expect(labels.map((l) => l.text)).toEqual(['0.0 Hz', '12000 Hz', '24000 Hz']);
});

test('auto range fits the quiet live band so structure is visible', () => {
    // Two rows in the measured live range: mostly at the floor, peaks ~-85.
    const quiet = new Float32Array(4096).fill(-140);
    for (let i = 0; i < 400; i++) quiet[100 + i * 7] = -95 + i * 0.02;
    quiet[512] = -85.2;
    const range = autoIntensityRange([quiet, quiet]);
    expect(range).not.toBeNull();
    expect(range.max).toBeCloseTo(-81.2, 1); // -85.2 + 4 dB margin
    expect(range.min).toBeCloseTo(-144, 1); // -140 - 4 dB margin
    // The span must cover the signal, not the old fixed [-100, 0] window.
    expect(range.max - range.min).toBeGreaterThan(50);
});

test('auto range never returns a degenerate or out-of-bounds window', () => {
    // Flat feed: the minimum span keeps the colour scale meaningful.
    const flat = new Float32Array(64).fill(-100);
    const flatRange = autoIntensityRange([flat]);
    expect(flatRange.max - flatRange.min).toBeGreaterThanOrEqual(12);

    // Clamped to the display limits.
    const loud = new Float32Array([0, -160, -3]);
    const loudRange = autoIntensityRange([loud]);
    expect(loudRange.min).toBeGreaterThanOrEqual(-160);
    expect(loudRange.max).toBeLessThanOrEqual(0);

    // Nothing usable.
    expect(autoIntensityRange([])).toBeNull();
    expect(autoIntensityRange([new Float32Array([NaN, Infinity])])).toBeNull();
});
