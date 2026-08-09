// dataClient.js — live data source for the waterfall front end (Milestone M1).
//
// Wraps the bridge server's transport and normalizes incoming sweeps to the
// Float32Array shape `appendSweep` expects:
//
//   * WebSocket `ws://<host>/ws` gets `{type:"sweep", bins, samples, intensity[]}`
//     pushed per STFT row. Auto-reconnects with exponential backoff.
//   * When the WebSocket isn't talking (never connected, or dropped), it falls
//     back to polling `GET /chunks?time=<cursor>&size=<n>` on an interval.
//   * Report a status string to the UI: "live" | "polling" | "offline".

((global) => {
    

    // Average every `chunk` server bins into one display bin. Keeps the 4096-bin
    // STFT resolution but reduces to the canvas row width without aliasing a
    // narrow band into a speck.
    function downsample(src, outBins) {
        const inBins = src.length;
        if (outBins === inBins) return src;
        const out = new Float32Array(outBins);
        const step = inBins / outBins;
        for (let b = 0; b < outBins; b++) {
            const start = Math.floor(b * step);
            const end = Math.max(start + 1, Math.floor((b + 1) * step));
            let sum = 0;
            for (let i = start; i < end; i++) sum += src[i];
            out[b] = sum / (end - start);
        }
        return out;
    }

    // The live client. `opts`:
    //   numBins     display row width (downsampled to)
    //   onSweep     (Float32Array) one normalized sweep
    //   onStatus    ("live"|"polling"|"offline")
    //   wsPath      default "/ws"
    //   chunksPath  default "/chunks"
    //   pollMs      fallback poll interval (default 250)
    function createLiveClient(opts) {
        const wsPath = opts.wsPath || '/ws';
        const chunksPath = opts.chunksPath || '/chunks';
        const pollMs = opts.pollMs || 250;
        const numBins = opts.numBins;

        let ws = null;
        let reconnects = 0;
        let reconnectTimer = null;
        let pollTimer = null;
        let lastSamples = 0; // poll cursor: only fetch rows newer than this
        let closed = false;

        function setStatus(s) {
            if (opts.onStatus) opts.onStatus(s);
        }

        function deliver(entry) {
            if (!entry || entry.type !== 'sweep' || !entry.intensity) return;
            if (entry.samples !== undefined) lastSamples = Math.max(lastSamples, entry.samples);
            const arr = downsample(entry.intensity, numBins);
            if (arr && opts.onSweep) opts.onSweep(arr);
        }

        // ---- WebSocket ----
        function wsUrl() {
            const proto = location.protocol === 'https:' ? 'wss' : 'ws';
            return `${proto}://${location.host}${wsPath}`;
        }

        function scheduleReconnect() {
            if (closed || reconnectTimer) return;
            // exponential backoff, capped at 5 s
            const delay = Math.min(5000, 250 * 2 ** reconnects);
            reconnectTimer = setTimeout(() => {
                reconnectTimer = null;
                reconnects++;
                openWs();
            }, delay);
        }

        function openWs() {
            if (closed) return;
            try {
                ws = new WebSocket(wsUrl());
            } catch (_) {
                setStatus('polling');
                startPolling();
                return;
            }
            ws.onopen = () => {
                reconnects = 0;
                setStatus('live');
                stopPolling();
            };
            ws.onmessage = (ev) => {
                try { deliver(JSON.parse(ev.data)); } catch (_) { /* ignore malformed */ }
            };
            ws.onclose = () => {
                ws = null;
                if (closed) return;
                setStatus('polling');
                startPolling();
                scheduleReconnect();
            };
            ws.onerror = () => {
                // onclose will follow and drive reconnect/poll
                if (ws) try { ws.close(); } catch (_) {}
            };
        }

        // ---- REST fallback ----
        function pollOnce() {
            const url = `${chunksPath}?time=${lastSamples}`;
            fetch(url)
                .then((r) => (r.ok ? r.json() : null))
                .then((rows) => {
                    if (closed) return;
                    if (Array.isArray(rows) && rows.length) {
                        rows.forEach(deliver);
                    }
                })
                .catch(() => {});
        }

        function startPolling() {
            if (!closed && !pollTimer) {
                pollOnce();
                pollTimer = setInterval(pollOnce, pollMs);
            }
        }

        function stopPolling() {
            if (pollTimer) { clearInterval(pollTimer); pollTimer = null; }
        }

        function start() {
            setStatus('polling');
            openWs();
            startPolling();
        }

        function close() {
            closed = true;
            if (ws) { try { ws.close(); } catch (_) {} ws = null; }
            if (reconnectTimer) { clearTimeout(reconnectTimer); reconnectTimer = null; }
            stopPolling();
        }

        return { start, close };
    }

    global.createLiveClient = createLiveClient;
})(window);