// live_ui_check.cjs — headless verification of the live waterfall UI.
//
// Same playwright already used by palantir.smoke.cjs. Run it with the live feed
// reachable (e.g. `kubectl -n default port-forward svc/waterfall-ui 4780:4780`):
//
//   node bin/live_ui_check.cjs
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

    // Let the waterfall accumulate real rows before inspecting pixels.
    await page.waitForTimeout(8000);

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
    check('gaps reported as zero in /meta', meta.consumer && meta.consumer.gaps === 0,
        meta.consumer ? String(meta.consumer.gaps) : 'no consumer meta');

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
