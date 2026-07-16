/**
 * End-to-end API tests for the streaming endpoints:
 *   POST /subscribe     - SSE streaming subscription
 *   GET  /subscriptions - list active subscriptions
 *
 * SSE event format (observed from lib/http/http_stream_handler.cpp):
 *   retry: 5000\n\n                              (sent once at stream start)
 *   id: <seq>\nevent: data\ndata: {"series":[{"measurement":...,"tags":{...},
 *       "fields":{"<f>":{"timestamps":[...],"values":[...]}}}]}\n\n
 *
 * NOTE ON WRITE VOLUME: the server buffers the SSE output stream and does
 * not push small/sparse events to the socket promptly (even the response
 * headers and the initial "retry:" line are held back until several KB of
 * events accumulate). These tests therefore write batches of points in
 * waves until events flow. Sparse single-point streams currently never
 * reach the client — tracked as a known server bug.
 */

const httpMod = require('http');
const axios = require('axios');

const HOST = process.env.TIMESTAR_HOST || 'localhost';
const PORT = process.env.TIMESTAR_PORT || 8086;
const BASE_URL = `http://${HOST}:${PORT}`;

jest.setTimeout(30000);

const api = axios.create({ baseURL: BASE_URL, validateStatus: () => true });

const ts = Date.now();
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// --- helpers -------------------------------------------------------------

// Write a wave of points (fire-and-forget) so SSE events accumulate.
function writeWave(measurement, pointCount = 100) {
  const writes = [];
  const base = Date.now() * 1000000;
  for (let i = 0; i < pointCount; i++) {
    writes.push({
      measurement,
      tags: { host: 'host' + (i % 5) },
      fields: { value: i * 1.5 },
      timestamp: base + i * 1000,
    });
  }
  api.post('/write', { writes }).catch(() => {});
}

// Parse complete SSE event blocks out of a text buffer.
// Returns { events, rest } where rest is the trailing incomplete block.
function parseSSE(buffer) {
  const events = [];
  const blocks = buffer.split('\n\n');
  const rest = blocks.pop(); // possibly incomplete
  for (const block of blocks) {
    if (!block.trim()) continue;
    const ev = { raw: block };
    for (const line of block.split('\n')) {
      if (line.startsWith('id: ')) ev.id = line.slice(4);
      else if (line.startsWith('event: ')) ev.event = line.slice(7);
      else if (line.startsWith('data: ')) ev.data = line.slice(6);
      else if (line.startsWith('retry: ')) ev.retry = line.slice(7);
    }
    events.push(ev);
  }
  return { events, rest };
}

// Open an SSE subscription. Resolves with a handle exposing the collected
// events, response headers/status, and a destroy() method.
function openSubscription(requestBody, { collectMs } = {}) {
  return new Promise((resolve, reject) => {
    const body = JSON.stringify(requestBody);
    const req = httpMod.request({
      host: HOST,
      port: PORT,
      path: '/subscribe',
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(body),
      },
    });

    const handle = {
      events: [],
      statusCode: null,
      headers: null,
      errorBody: null,
      destroyed: false,
      destroy() {
        this.destroyed = true;
        req.destroy();
      },
      waitForEvent(predicate, timeoutMs) {
        const self = this;
        return new Promise((res) => {
          const deadline = Date.now() + timeoutMs;
          (function poll() {
            const found = self.events.find(predicate);
            if (found) return res(found);
            if (Date.now() > deadline) return res(null);
            setTimeout(poll, 100);
          })();
        });
      },
    };

    let buf = '';
    let resolved = false;

    req.on('response', (res) => {
      handle.statusCode = res.statusCode;
      handle.headers = res.headers;
      res.setEncoding('utf8');

      if (res.statusCode !== 200) {
        // Error responses are small JSON bodies, not streams
        let errBuf = '';
        res.on('data', (c) => (errBuf += c));
        res.on('end', () => {
          handle.errorBody = errBuf;
          if (!resolved) {
            resolved = true;
            resolve(handle);
          }
        });
        return;
      }

      res.on('data', (chunk) => {
        buf += chunk;
        const { events, rest } = parseSSE(buf);
        buf = rest;
        handle.events.push(...events);
      });

      if (!resolved) {
        resolved = true;
        resolve(handle);
      }
    });

    req.on('error', (e) => {
      if (!handle.destroyed && !resolved) {
        resolved = true;
        reject(e);
      }
    });

    req.end(body);

    // For streaming subscriptions the server holds back the response headers
    // until event bytes flush, so also resolve after a grace period — the
    // caller can keep polling handle.statusCode / handle.events.
    setTimeout(() => {
      if (!resolved) {
        resolved = true;
        resolve(handle);
      }
    }, collectMs || 5000);
  });
}

// Poll GET /subscriptions until predicate over the subscription list holds.
async function pollSubscriptions(predicate, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  let last = null;
  for (;;) {
    const res = await api.get('/subscriptions');
    expect(res.status).toBe(200);
    last = res.data;
    if (predicate(last.subscriptions)) return last;
    if (Date.now() > deadline) return null;
    await sleep(200);
  }
}

// --- tests ---------------------------------------------------------------

describe('Subscriptions API', () => {
  describe('GET /subscriptions', () => {
    test('returns a valid shape', async () => {
      const res = await api.get('/subscriptions');

      expect(res.status).toBe(200);
      expect(Array.isArray(res.data.subscriptions)).toBe(true);
      expect(typeof res.data.total_subscriptions).toBe('number');
      expect(res.data.total_subscriptions).toBe(res.data.subscriptions.length);

      // Every entry has the documented stat fields
      for (const s of res.data.subscriptions) {
        expect(typeof s.id).toBe('number');
        expect(typeof s.measurement).toBe('string');
        expect(Array.isArray(s.fields)).toBe(true);
        expect(typeof s.scopes).toBe('object');
        expect(typeof s.handler_shard).toBe('number');
        expect(typeof s.queue_depth).toBe('number');
        expect(typeof s.queue_capacity).toBe('number');
        expect(typeof s.dropped_points).toBe('number');
        expect(typeof s.events_sent).toBe('number');
      }
    });
  });

  describe('POST /subscribe validation', () => {
    test('rejects an unparseable query with 400', async () => {
      const res = await api.post('/subscribe', { query: 'not a valid query' });

      expect(res.status).toBe(400);
      expect(res.data.status).toBe('error');
      // NOTE: /subscribe still uses the NESTED error shape
      // {"error":{"code","message"}}, unlike the flat shape used by the
      // rest of the API since the jsonError() unification.
      expect(res.data.error.code).toBe('INVALID_QUERY');
      expect(typeof res.data.error.message).toBe('string');
    });

    test('rejects a body with neither query nor queries with 400', async () => {
      const res = await api.post('/subscribe', {});

      expect(res.status).toBe(400);
      expect(res.data.status).toBe('error');
      expect(res.data.error.code).toBe('INVALID_QUERY');
    });

    test('rejects both query and queries together with 400', async () => {
      const res = await api.post('/subscribe', {
        query: 'avg:whatever()',
        queries: [{ query: 'avg:whatever()', label: 'a' }],
      });

      expect(res.status).toBe(400);
      expect(res.data.status).toBe('error');
      expect(res.data.error.code).toBe('AMBIGUOUS_REQUEST');
    });
  });

  describe('SSE streaming flow', () => {
    test('streams data events for matching writes; subscription is listed and cleaned up', async () => {
      const meas = `e2e_sub_flow_${ts}`;

      // 1. Open the subscription
      const sub = await openSubscription({ query: `avg:${meas}()` }, { collectMs: 2000 });

      // 2. The subscription must appear in GET /subscriptions (registration
      //    happens before the stream body starts flowing)
      const listed = await pollSubscriptions(
        (subs) => subs.some((s) => s.measurement === meas),
        5000
      );
      expect(listed).not.toBeNull();
      const entry = listed.subscriptions.find((s) => s.measurement === meas);
      expect(entry.queue_capacity).toBeGreaterThan(0);

      // 3. Write matching points in waves until events arrive. Waves are
      //    needed because the server only flushes the SSE stream once
      //    several KB of events have accumulated (see file header note).
      const waveTimer = setInterval(() => writeWave(meas), 300);

      let dataEvent;
      try {
        dataEvent = await sub.waitForEvent(
          (e) => e.event === 'data' && e.data && e.data.includes(meas),
          10000
        );

        // 4. Assert the SSE event format and payload
        expect(dataEvent).not.toBeNull();
        expect(dataEvent.id).toBeDefined();
        expect(Number(dataEvent.id)).toBeGreaterThanOrEqual(0);

        const payload = JSON.parse(dataEvent.data);
        expect(Array.isArray(payload.series)).toBe(true);
        expect(payload.series.length).toBeGreaterThan(0);
        const series = payload.series.find((s) => s.measurement === meas);
        expect(series).toBeDefined();
        expect(series.tags).toBeDefined();
        expect(series.fields.value).toBeDefined();
        expect(Array.isArray(series.fields.value.timestamps)).toBe(true);
        expect(Array.isArray(series.fields.value.values)).toBe(true);
        expect(series.fields.value.timestamps.length).toBe(series.fields.value.values.length);
        expect(series.fields.value.timestamps.length).toBeGreaterThan(0);

        // Headers arrive with the first flushed bytes
        expect(sub.statusCode).toBe(200);
        expect(sub.headers['x-subscription-ids']).toBeDefined();
        expect(String(entry.id)).toBe(sub.headers['x-subscription-ids']);

        // First flushed block includes the SSE retry directive
        const retryEvent = sub.events.find((e) => e.retry);
        expect(retryEvent).toBeDefined();
        expect(retryEvent.retry).toBe('5000');

        // 5. Destroy the client connection; with writes still flowing the
        //    server detects the dead socket and unregisters the subscription
        sub.destroy();
        const removed = await pollSubscriptions(
          (subs) => !subs.some((s) => s.measurement === meas),
          8000
        );
        expect(removed).not.toBeNull();
      } finally {
        clearInterval(waveTimer);
        sub.destroy();
      }
    });

    test('multi-query subscription is listed with its label', async () => {
      const meas = `e2e_sub_multi_${ts}`;
      const sub = await openSubscription(
        { queries: [{ query: `avg:${meas}()`, label: 'labelled_q' }] },
        { collectMs: 1000 }
      );

      try {
        const listed = await pollSubscriptions(
          (subs) => subs.some((s) => s.measurement === meas),
          5000
        );
        expect(listed).not.toBeNull();
        const entry = listed.subscriptions.find((s) => s.measurement === meas);
        expect(entry.label).toBe('labelled_q');
      } finally {
        // Note: without event traffic the server does not notice the closed
        // socket, so this subscription may linger in /subscriptions. The
        // unique measurement name keeps other tests unaffected.
        sub.destroy();
      }
    });
  });
});
