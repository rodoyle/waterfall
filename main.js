// main.js

// --- Data model ---
// A waterfall/spectrogram is a 2D heatmap: rows = sweeps (time), columns = frequency
// bins, and color encodes intensity (power in dB). New sweeps push older ones down.

const config = {
    numBins: 256,          // frequency bins (columns)
    numRows: 220,          // rows held on screen (time history)
    sweepIntervalMs: 90,   // how often a new sweep arrives
    freqMin: 0,
    freqMax: 24000,        // Hz: Nyquist of the 48 kHz STFT source (informational axis)
    intensityMin: -140,    // dB scale (plot properties); -140 matches DB_FLOOR in lib.rs
    intensityMax: 0,
    autoRange: true,       // fit the dB range to live data until the user overrides it
    freqScale: 'linear',   // frequency-axis label mapping: 'linear' | 'log'
    paused: false,         // run/pause toggle for the animation clock
    demo: true,            // use random generation only until real sweeps arrive
    status: 'offline',     // live | polling | offline (from dataClient)
    // Absolute RF axis, filled in from the bridge's /meta (see axis.js). When
    // `absoluteAxis` is true the plot labels real MHz instead of baseband Hz.
    absoluteAxis: false,
    centerHz: null,
    sampleRateHz: null,
    source: 'unknown',     // 'vita49' (live RF) | 'synthetic' (dev feed)
    stale: true,           // no rows recently -> show it rather than fake data
    consumer: null,        // middleware loss/rate counters from /meta
};

// Off-screen image we draw each sweep into, then blit onto the canvas with an
// offset that shifts rows downward -> the "waterfall" motion.
const plotCanvas = document.createElement('canvas');
plotCanvas.width = config.numBins;
plotCanvas.height = config.numRows;
const plotCtx = plotCanvas.getContext('2d');
console.log("Off-screen plotCanvas created:", plotCanvas.width, "x", plotCanvas.height);

let rowOffset = 0; // number of rows scrolled so far

const canvas = document.getElementById('waterfallCanvas');
const ctx = canvas.getContext('2d');

// Set canvas size to fill the window
function onResize() {
    canvas.width = window.innerWidth;
    canvas.height = window.innerHeight;
    renderPlot(); // Re-render on resize
}
window.addEventListener('resize', onResize);
onResize(); // Initial resize

console.log("Waterfall Plot Visualizer frontend started.");

// Generate one sweep: an array of `numBins` intensity values.
// Random on-the-fly generation; only used while `config.demo` is true (i.e.
// until the bridge server delivers a real STFT sweep).
function generateSweep() {
    const sweep = new Float32Array(config.numBins);
    const center = Math.random() * config.numBins; // moving band
    const width = 20 + Math.random() * 60;
    for (let i = 0; i < config.numBins; i++) {
        // A raised peak on top of noise, so it reads like a signal band.
        const gaussian = Math.exp(-(((i - center) / width) ** 2));
        sweep[i] = gaussian * (70 + Math.random() * 30) + (Math.random() * 18 - 70);
    }
    return sweep;
}

// Map an intensity value (dB) to a color along a heatmap gradient.
function intensityToColor(v) {
    const t = Math.max(0, Math.min(1, (v - config.intensityMin) / (config.intensityMax - config.intensityMin)));
    // stops: blue -> cyan -> green -> yellow -> red
    const stops = [
        [0, 0, 90],
        [0, 200, 255],
        [0, 235, 120],
        [255, 235, 40],
        [220, 40, 20],
    ];
    const f = t * (stops.length - 1);
    const i = Math.min(stops.length - 2, Math.floor(f));
    const u = f - i;
    const c = stops[i].map((ch, k) => Math.round(ch + (stops[i + 1][k] - ch) * u));
    return `rgb(${c[0]},${c[1]},${c[2]})`;
}

// Draw a single sweep as one row of colored columns into the off-screen image.
function drawSweepIntoSweep(sweep, row) {
    // console.log(`Drawing sweep into row ${row}...`); // Log entry point
    for (let bin = 0; bin < config.numBins; bin++) {
        plotCtx.fillStyle = intensityToColor(sweep[bin]);
        plotCtx.fillRect(bin, row, 1, 1);
    }
}

// Shift the accumulated history one row up (dropping the oldest) and stamp a new
// sweep at the bottom.
function appendSweep(sweep) {
    // console.log("Appending sweep..."); // Log entry point
    if (!plotCanvas || !plotCtx) {
        console.error("plotCanvas or plotCtx is not initialized for appendSweep.");
        return;
    }
    // Shift the existing image up by one pixel
    plotCtx.drawImage(plotCanvas, 0, 1, config.numBins, config.numRows - 1, 0, 0, config.numBins, config.numRows - 1);
    // Draw the new sweep at the bottom row
    drawSweepIntoSweep(sweep, config.numRows - 1);
    rowOffset++;
    // console.log(`Sweep appended. Total rows offset: ${rowOffset}`);
}

// Full-screen re-render: axes, labels, color bar, and the spectrogram surface.
function renderPlot() {
    console.log("Rendering plot..."); // Log entry point
    ctx.clearRect(0, 0, canvas.width, canvas.height);

    const padLeft = 70, padRight = 30, padTop = 40, padBottom = 55;
    const plotW = canvas.width - padLeft - padRight;
    const plotH = canvas.height - padTop - padBottom;

    if (plotW <= 0 || plotH <= 0) {
        console.warn("Canvas plot area has non-positive dimensions. Skipping render.");
        return;
    }

    ctx.imageSmoothingEnabled = false; // Important for pixel-art like spectrograms
    console.log("Drawing off-screen plotCanvas to main canvas...");
    try {
        ctx.drawImage(plotCanvas, 0, 0, config.numBins, config.numRows,
                      padLeft, padTop, plotW, plotH);
        console.log("drawImage complete.");
    } catch (e) {
        console.error("Error during ctx.drawImage:", e);
    }


    // Axes.
    ctx.strokeStyle = 'rgba(0,0,0,0.6)';
    ctx.lineWidth = 1;
    ctx.strokeRect(padLeft, padTop, plotW, plotH);

    // Frequency labels (y axis). On a live RF stream these are absolute MHz
    // (axis.js); on the synthetic baseband feed they stay relative Hz. Log
    // spacing only applies to the relative axis — a log scale across a 2 MHz
    // window around 915 MHz would be meaningless.
    ctx.fillStyle = '#222';
    ctx.font = '12px sans-serif';
    ctx.textAlign = 'right';
    ctx.textBaseline = 'middle';
    const fLo = Math.max(1, config.freqMin); // log(0) is -inf; clamp floor
    const fHi = Math.max(fLo + 1, config.freqMax);
    const useLog = config.freqScale === 'log' && !config.absoluteAxis;
    for (let i = 0; i <= 4; i++) {
        let f;
        if (useLog) {
            // log-space between fLo and fHi
            const t = i / 4;
            f = fLo * (fHi / fLo) ** t;
            if (i === 0) f = config.freqMin; // anchor the bottom edge exactly
        } else {
            f = config.freqMin + (config.freqMax - config.freqMin) * i / 4;
        }
        const y = padTop + plotH - (plotH * i / 4);
        const label = config.absoluteAxis
            ? WaterfallAxis.formatHzLabel(f)
            : `${f.toFixed(f >= 1000 ? 0 : 1)} Hz`;
        ctx.fillText(label, padLeft - 8, y);
    }

    // Band annotation: on a live RF stream state the centre and span so the
    // plot is self-describing without the controls panel.
    if (config.absoluteAxis) {
        ctx.textAlign = 'left';
        ctx.textBaseline = 'top';
        ctx.fillText(
            `${WaterfallAxis.formatHzLabel(config.centerHz)} centre  ·  ` +
            `${(config.sampleRateHz / 1e6).toFixed(2)} MS/s  ·  ` +
            `${WaterfallAxis.formatHzLabel(config.freqMin)} – ${WaterfallAxis.formatHzLabel(config.freqMax)}`,
            padLeft + 6, padTop + 6
        );
    }

    // Time label (x axis).
    ctx.textAlign = 'center';
    ctx.textBaseline = 'top';
    ctx.fillText(`Time  (sweeps: ${rowOffset}, ${config.status})`, padLeft + plotW / 2, padTop + plotH + 10);

    // Color bar: horizontal strip under the plot, gradient left (min) to right (max).
    const barH = 12, barY = padTop + plotH + 28, barPad = 8;
    ctx.textAlign = 'center';
    ctx.textBaseline = 'alphabetic';
    const barW = plotW / 2;
    for (let x = 0; x < barW; x++) {
        ctx.fillStyle = intensityToColor(config.intensityMin + (config.intensityMax - config.intensityMin) * x / barW);
        ctx.fillRect(barPad + x, barY, 1, barH);
    }
    ctx.fillStyle = '#222';
    ctx.font = '11px sans-serif';
    ctx.fillText(`${config.intensityMin} dB`, barPad - 2, barY + barH + 12);
    ctx.fillText(`${config.intensityMax} dB`, barPad + barW + 2, barY + barH + 12);
    ctx.fillText('Intensity', barPad + barW / 2, barY - 6);
}

// --- Live data client (replaces random generation once a bridge is reachable) ---
// The bridge stream / general source of sweeps.
let pendingLive = null;    // freshest live sweep awaiting a clock tick
const recentSweeps = [];   // last few live rows, for dB auto-ranging
function pushLiveSweep(arr) {
    pendingLive = arr;
    recentSweeps.push(arr);
    if (recentSweeps.length > 32) recentSweeps.shift();
    // Real data present; stop the random demo source.
    config.demo = false;
}

// The live 915 MHz band sits far below 0 dBFS (measured: most bins at the
// floor, peaks around -85 dB), so a fixed [-100, 0] colour range renders an
// almost uniform field. Fit the range to the data actually on screen; the dB
// controls below take over as soon as the user touches them.
let lastAutoRangeAt = 0;
function maybeAutoRange() {
    if (!config.autoRange || recentSweeps.length === 0) return;
    const now = Date.now();
    if (now - lastAutoRangeAt < 1000) return;
    lastAutoRangeAt = now;
    const range = WaterfallAxis.autoIntensityRange(recentSweeps);
    if (!range) return;
    const min = Math.floor(range.min);
    const max = Math.ceil(range.max);
    if (min === config.intensityMin && max === config.intensityMax) return;
    config.intensityMin = min;
    config.intensityMax = max;
    syncDbInputs();
}
let liveClient = createLiveClient({
    numBins: config.numBins,
    onSweep: pushLiveSweep,
    onStatus: (s) => { config.status = s; },
    onMeta: applyMeta,
});

// --- RF metadata (bridge /meta) ---
// The bridge reports the producer, the RF centre/sample rate and the
// middleware's loss counters. The axis is derived from it, and the readout
// makes a dead feed visibly dead instead of quietly showing stale rows.
function applyMeta(meta) {
    if (!meta) return;
    WaterfallAxis.applyMetaToConfig(config, meta);
    config.source = meta.source || config.source;
    config.stale = Boolean(meta.stale);
    config.consumer = meta.consumer || null;
    updateStatusReadout();
    renderPlot();
}

function updateStatusReadout() {
    const el = document.getElementById('statusReadout');
    if (!el) return;
    const c = config.consumer;
    const parts = [
        `source: ${config.source}`,
        config.absoluteAxis
            ? `${WaterfallAxis.formatHzLabel(config.centerHz)} ± ${(config.sampleRateHz / 2e6).toFixed(2)} MHz`
            : 'baseband',
        config.status,
        config.stale ? 'stale' : 'fresh',
    ];
    if (c) {
        const rate = Number(c.packets_received) > 0 ? `rx ${c.packets_received} pkts` : 'rx 0 pkts';
        parts.push(rate, `gaps ${c.gaps}`, `missing ${c.missing_samples} samples`);
        if (c.stft_dropped) parts.push(`stft_dropped ${c.stft_dropped}`);
        if (c.publish_dropped) parts.push(`publish_dropped ${c.publish_dropped}`);
        if (c.parse_errors) parts.push(`parse_errors ${c.parse_errors}`);
    }
    el.textContent = parts.join('  ·  ');
    el.style.color = config.stale ? '#e66' : '#8c8';
}

// --- Animation clock (run/pause + sweep-rate aware) ---
// Keep the interval as a clock and consume the freshest live frame when one
// exists; otherwise fall back to generated demo sweeps.
function tick() {
    let sweep;
    if (config.demo) {
        sweep = generateSweep();
    } else if (pendingLive) {
        sweep = pendingLive;
        pendingLive = null;
    } else {
        return; // live, but no fresh frame yet — keep the last render
    }
    appendSweep(sweep);
    maybeAutoRange();
    renderPlot();
}

let animationInterval = null;
function startClock() {
    stopClock();
    animationInterval = setInterval(tick, config.sweepIntervalMs);
    config.paused = false;
}
function stopClock() {
    if (animationInterval !== null) {
        clearInterval(animationInterval);
        animationInterval = null;
    }
    config.paused = true;
}

// (Re)apply geometry: resize the off-screen plotCanvas and re-seed it with
// silence so live rows fill in cleanly after a bin/row-count change.
function applyGeometry() {
    plotCanvas.width = config.numBins;
    plotCanvas.height = config.numRows;
    rowOffset = 0;
    for (let i = 0; i < config.numRows; i++) {
        appendSweep(new Float32Array(config.numBins));
    }
    // The live client downsamples server bins to `numBins`; recreate it so the
    // new width takes effect for incoming sweeps.
    try { liveClient.close(); } catch {}
    liveClient = createLiveClient({
        numBins: config.numBins,
        onSweep: pushLiveSweep,
        onStatus: (s) => { config.status = s; },
        onMeta: applyMeta,
    });
    liveClient.start();
    renderPlot();
}

// --- Controls panel (plan step 3) ---
// Lightweight: read the DOM inputs into `config`, then apply only the side
// effects each change requires. No framework, reuses the existing config object.
function wireControls() {
    const $ = (id) => document.getElementById(id);
    const panel = $('controls');
    const toggle = $('controlsToggle');
    toggle.addEventListener('click', () => panel.classList.toggle('collapsed'));

    const numBinsInput = $('ctlNumBins');
    const numRowsInput = $('ctlNumRows');
    const sweepMsInput = $('ctlSweepMs');
    const dbMinInput = $('ctlDbMin');
    const dbMaxInput = $('ctlDbMax');
    const freqScaleSelect = $('ctlFreqScale');
    const runPauseBtn = $('ctlRunPause');

    function syncRunPauseBtn() {
        runPauseBtn.textContent = config.paused ? 'Run' : 'Pause';
        runPauseBtn.classList.toggle('running', !config.paused);
    }

    numBinsInput.addEventListener('change', () => {
        const v = Math.max(16, Math.min(4096, parseInt(numBinsInput.value, 10) | 0));
        numBinsInput.value = v;
        config.numBins = v;
        applyGeometry();
    });
    numRowsInput.addEventListener('change', () => {
        const v = Math.max(16, Math.min(2000, parseInt(numRowsInput.value, 10) | 0));
        numRowsInput.value = v;
        config.numRows = v;
        applyGeometry();
    });
    sweepMsInput.addEventListener('change', () => {
        const v = Math.max(10, Math.min(2000, parseInt(sweepMsInput.value, 10) | 0));
        sweepMsInput.value = v;
        config.sweepIntervalMs = v;
        if (!config.paused) startClock(); // restart interval at the new cadence
    });
    dbMinInput.addEventListener('change', () => {
        const v = Math.max(-160, Math.min(0, parseInt(dbMinInput.value, 10) | 0));
        dbMinInput.value = v;
        config.intensityMin = v;
        config.autoRange = false; // user took manual control of the colour range
        renderPlot();
    });
    dbMaxInput.addEventListener('change', () => {
        const v = Math.max(-160, Math.min(0, parseInt(dbMaxInput.value, 10) | 0));
        dbMaxInput.value = v;
        config.intensityMax = v;
        config.autoRange = false;
        renderPlot();
    });
    freqScaleSelect.addEventListener('change', () => {
        config.freqScale = freqScaleSelect.value === 'log' ? 'log' : 'linear';
        renderPlot();
    });
    runPauseBtn.addEventListener('click', () => {
        if (config.paused) startClock();
        else stopClock();
        syncRunPauseBtn();
    });

    syncRunPauseBtn();
}

/// Keep the dB inputs showing the range actually in use (auto-range moves them).
function syncDbInputs() {
    const min = document.getElementById('ctlDbMin');
    const max = document.getElementById('ctlDbMax');
    if (min) min.value = String(config.intensityMin);
    if (max) max.value = String(config.intensityMax);
}

// Seed the screen with silence so real rows fill in from the bottom upward
// (no random demo junk behind live data), then connect. Boot waits for
// DOMContentLoaded so `axis.js` (a module) is loaded first.
function bootFrontend() {
    for (let i = 0; i < config.numRows; i++) {
        appendSweep(new Float32Array(config.numBins));
    }
    renderPlot();

    // Connect last so the very first delivered sweep lands on a seeded screen.
    liveClient.start();
    startClock();
    wireControls();
    updateStatusReadout();
}

if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', bootFrontend);
} else {
    bootFrontend();
}
