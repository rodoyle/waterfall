// live_ui_check.cjs — headless verification of the live waterfall UI.
//
// Same playwright already used by palantir.smoke.cjs. Run it against the live
// feed either through the Ingress host or a port-forward:
//
//   node bin/live_ui_check.cjs                     # default: http://127.0.0.1:4780/
//   WATERFALL_URL=http://orbweaver.apps.home.arpa/ node bin/live_ui_check.cjs
//   kubectl -n default port-forward svc/orbweaver-ui 4780:4780
//
// Asserts, against the REAL feed rather than a fixture:
//   * zero page errors,
//   * /meta reports the VITA49 producer, fresh rows, 4096 bins and 915 MHz,
//   * the on-screen status readout carries the live source and the absolute
//     RF band (so the axis metadata actually reached the UI),
//   * the rendered canvas is structured — many distinct pixel values, i.e. a
//     real waterfall, not a flat synthetic field,
//   * and it writes a screenshot artifact for visual confirmation.

const { chromium } = require('playwright');

const URL = process.env.WATERFALL_URL || 'http://127.0.0.1:4780/';
const SHOT = process.env.WATERFALL_SHOT || 'waterfall-live.png';
const MIN_DISTINCT_COLOURS = 40;

const failures = [];

function check(label, condition, detail = '') {
    const ok = Boolean(condition);
    console.log(`  ${ok ? 'ok  ' : 'FAIL'}  ${label}${detail ? ` — ${detail}` : ''}`);
    if (!ok) failures.push(label);
}

(async () => {
    const browser = await chromium.launch({ headless: true });
    const page = await browser.newPage({ viewport: { width: 1280, height: 800 } });

    const errors = [];
    page.on('pageerror', (e) => errors.push(e.message));

    await page.goto(URL, { waitUntil: 'load' });

    // The status readout only says "vita49" once /meta has been polled.
    await page.waitForFunction(
        () => {
            const el = document.getElementById('statusReadout');
            return el && /source:\s*vita49/.test(el.textContent);
        },
        { timeout: 30000 }
    );

    const status = await page.evaluate(
        () => document.getElementById('statusReadout').textContent
    );
    const meta = await page.evaluate(async () => (await fetch('/meta')).json());

    // Let the waterfall accumulate real rows before inspecting pixels. The front
    // end seeds `numRows` empty rows and appends live data at its own clock rate
    // (sweepIntervalMs), so allow enough time for the seeds to scroll out
    // (~220 rows x 90 ms) or the screenshot shows a half-filled plot.
    await page.waitForTimeout(25000);

    const pixels = await page.evaluate(() => {
        const canvas = document.getElementById('waterfallCanvas');
        const ctx = canvas.getContext('2d');
        const w = Math.min(canvas.width, 800);
        const h = Math.min(canvas.height, 600);
        const data = ctx.getImageData(0, 0, w, h).data;
        const seen = new Set();
        for (let i = 0; i < data.length; i += 4) {
            seen.add((data[i] << 16) | (data[i + 1] << 8) | data[i + 2]);
        }
        return { width: canvas.width, height: canvas.height, distinct: seen.size };
    });

    const metaAfter = await page.evaluate(async () => (await fetch('/meta')).json());

    // Loss is judged as a RATIO, not as "gaps === 0": over hundreds of thousands
    // of packets one lost fragment legitimately appears as a single gap (2048
    // samples), and the objective's criterion is "gap count approximately zero".
    const consumer = meta.consumer || {};
    const received = Number(consumer.packets_received) || 0;
    const gaps = Number(consumer.gaps) || 0;
    const missingSamples = Number(consumer.missing_samples) || 0;
    const expectedSamples = received * 2048;
    const lossRatio = expectedSamples > 0 ? missingSamples / expectedSamples : 0;

    await page.screenshot({ path: SHOT });

    console.log(`  screenshot: ${SHOT}`);
    check('no page errors', errors.length === 0, errors.join('; '));
    check('meta.source is vita49', meta.source === 'vita49', String(meta.source));
    check('meta.configured_source is ingest', meta.configured_source === 'ingest', String(meta.configured_source));
    check('meta.stale is false', meta.stale === false, `stale_ms=${meta.stale_ms}`);
    check('meta.center_hz == 915 MHz', meta.center_hz === 915000000, String(meta.center_hz));
    check('meta.sample_rate_hz == 2 MS/s', meta.sample_rate_hz === 2000000, String(meta.sample_rate_hz));
    check('meta.bins == 4096', meta.bins === 4096, String(meta.bins));
    check('rows are arriving', meta.rows_ingested > 0, String(meta.rows_ingested));
    check(
        'rows still arriving during the check',
        metaAfter.rows_ingested > meta.rows_ingested,
        `${meta.rows_ingested} -> ${metaAfter.rows_ingested}`
    );
    check('status readout shows the live source', /source:\s*vita49/.test(status), status);
    check('status readout shows the 915 MHz band', /915\.000 MHz/.test(status), status);
    check('status readout shows fresh (not stale)', /fresh/.test(status), status);
    check(
        'canvas rendered (non-zero size)',
        pixels.width > 0 && pixels.height > 0,
        `${pixels.width}x${pixels.height}`
    );
    check(
        `canvas is structured (>= ${MIN_DISTINCT_COLOURS} distinct colours)`,
        pixels.distinct >= MIN_DISTINCT_COLOURS,
        `${pixels.distinct} distinct`
    );
    check(
        'packet loss is negligible (<0.1% of samples)',
        lossRatio < 0.001,
        `${missingSamples} of ~${expectedSamples} samples missing ` +
            `(${(lossRatio * 100).toFixed(4)}%), ${gaps} gap(s)`
    );
    check(
        'loss counters are reported by the middleware',
        meta.consumer !== null && meta.consumer !== undefined
    );

    await browser.close();

    console.log();
    if (failures.length) {
        console.log(`LIVE UI: FAIL (${failures.length}): ${failures.join(', ')}`);
        process.exit(1);
    }
    console.log('LIVE UI: PASS');
})().catch((e) => {
    console.error(e);
    process.exit(1);
});
