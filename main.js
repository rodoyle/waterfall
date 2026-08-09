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
    intensityMin: -100,    // dB scale (plot properties)
    intensityMax: 0,
    demo: true,            // use random generation only until real sweeps arrive
    status: 'offline',     // live | polling | offline (from dataClient)
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

    // Frequency labels (y axis).
    ctx.fillStyle = '#222';
    ctx.font = '12px sans-serif';
    ctx.textAlign = 'right';
    ctx.textBaseline = 'middle';
    for (let i = 0; i <= 4; i++) {
        const f = config.freqMin + (config.freqMax - config.freqMin) * i / 4;
        const y = padTop + plotH - (plotH * i / 4);
        ctx.fillText(`${f} Hz`, padLeft - 8, y);
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
function pushLiveSweep(arr) {
    pendingLive = arr;
    // Real data present; stop the random demo source.
    config.demo = false;
}
const liveClient = createLiveClient({
    numBins: config.numBins,
    onSweep: pushLiveSweep,
    onStatus: (s) => { config.status = s; },
});

// Keep the interval as a clock and consume the freshest live frame when one
// exists; otherwise fall back to generated demo sweeps.
const animationInterval = setInterval(() => {
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
    renderPlot();
}, config.sweepIntervalMs);

// Seed the screen with silence so real rows fill in from the bottom upward
// (no random demo junk behind live data).
for (let i = 0; i < config.numRows; i++) {
    appendSweep(new Float32Array(config.numBins));
}
renderPlot();

// Connect last so the very first delivered sweep lands on a seeded screen.
liveClient.start();
