/**
 * End-to-end API tests for the streaming endpoints:
 *   POST /subscribe     - SSE streaming subscription
 *   GET  /subscriptions - list active subscriptions
 *
 * SSE event format (observed from lib/http/http_stream_handler.cpp):
 *   retry: 5000\n\n                              (sent once at stream start)
 *   :heartbeat\n\n                               (comment, every heartbeat interval)
 *   id: <seq>\nevent: data\ndata: {"series":[{"measurement":...,"tags":{...},
 *       "fields":{"<f>":{"timestamps":[...],"values":[...]}}}]}\n\n
 *
 * The server flushes every SSE write (preamble, heartbeats, events) all the
 * way to the socket, so response headers and the "retry:" preamble arrive
 * immediately on connect and a single sparse write is delivered promptly.
 * Heartbeat write failures are also how the server detects disconnected
 * clients on quiet streams (subscription cleanup within ~2 heartbeat
 * intervals, 15s each by default).
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

// Write a single point (awaited, so the event can be timed from here).
async function writeOne(measurement, value = 42.5, tags = { host: 'host0' }) {
  const res = await api.post('/write', {
    measurement,
    tags,
    fields: { value },
    timestamp: Date.now() * 1000000,
  });
  expect(res.status).toBeLessThan(300);
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
      else if (line.startsWith(':')) ev.comment = line.slice(1);
    }
    events.push(ev);
  }
  return { events, rest };
}

// Open an SSE subscription. Resolves once response headers arrive (which the
// server now sends immediately) with a handle exposing the collected events,
// response headers/status, and a destroy() method.
function openSubscription(requestBody) {
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
            setTimeout(poll, 50);
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

    // Safety net: fail rather than hang if headers never arrive.
    setTimeout(() => {
      if (!resolved) {
        resolved = true;
        reject(new Error('SSE response headers did not arrive within 10s'));
      }
    }, 10000);
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
    await sleep(250);
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
    // /subscribe uses the flat jsonError() shape like the rest of the API:
    //   {"status":"error","error_code":"...","message":"...","error":"..."}
    test('rejects an unparseable query with 400', async () => {
      const res = await api.post('/subscribe', { query: 'not a valid query' });

      expect(res.status).toBe(400);
      expect(res.data.status).toBe('error');
      expect(res.data.error_code).toBe('INVALID_QUERY');
      expect(typeof res.data.error).toBe('string');
      expect(res.data.message).toBe(res.data.error);
    });

    test('rejects a body with neither query nor queries with 400', async () => {
      const res = await api.post('/subscribe', {});

      expect(res.status).toBe(400);
      expect(res.data.status).toBe('error');
      expect(res.data.error_code).toBe('INVALID_QUERY');
      expect(typeof res.data.error).toBe('string');
    });

    test('rejects both query and queries together with 400', async () => {
      const res = await api.post('/subscribe', {
        query: 'avg:whatever()',
        queries: [{ query: 'avg:whatever()', label: 'a' }],
      });

      expect(res.status).toBe(400);
      expect(res.data.status).toBe('error');
      expect(res.data.error_code).toBe('AMBIGUOUS_REQUEST');
      expect(typeof res.data.error).toBe('string');
    });
  });

  describe('SSE streaming flow', () => {
    test('headers, Content-Type and retry preamble arrive immediately on connect', async () => {
      const meas = `e2e_sub_preamble_${ts}`;
      const sub = await openSubscription({ query: `avg:${meas}()` });

      try {
        // Headers arrive without any event traffic
        expect(sub.statusCode).toBe(200);
        expect(sub.headers['content-type']).toMatch(/^text\/event-stream/);
        expect(sub.headers['x-subscription-ids']).toBeDefined();

        // The retry preamble flushes promptly on connect (no writes needed)
        const retryEvent = await sub.waitForEvent((e) => e.retry, 3000);
        expect(retryEvent).not.toBeNull();
        expect(retryEvent.retry).toBe('5000');
      } finally {
        sub.destroy();
      }
    });

    test('a single write is delivered as an SSE data event within 5s', async () => {
      const meas = `e2e_sub_single_${ts}`;

      // 1. Open the subscription and wait until it is registered
      const sub = await openSubscription({ query: `avg:${meas}()` });

      try {
        expect(sub.statusCode).toBe(200);
        const listed = await pollSubscriptions(
          (subs) => subs.some((s) => s.measurement === meas),
          5000
        );
        expect(listed).not.toBeNull();
        const entry = listed.subscriptions.find((s) => s.measurement === meas);
        expect(entry.queue_capacity).toBeGreaterThan(0);
        expect(String(entry.id)).toBe(sub.headers['x-subscription-ids']);

        // 2. ONE write must reach the client promptly (no volume workaround)
        await writeOne(meas, 82.5);
        const dataEvent = await sub.waitForEvent(
          (e) => e.event === 'data' && e.data && e.data.includes(meas),
          5000
        );
        expect(dataEvent).not.toBeNull();

        // 3. Assert the SSE event format and payload
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
        expect(series.fields.value.values).toContain(82.5);
      } finally {
        sub.destroy();
      }
    });

    test('multi-query subscription is listed with its label', async () => {
      const meas = `e2e_sub_multi_${ts}`;
      const sub = await openSubscription({
        queries: [{ query: `avg:${meas}()`, label: 'labelled_q' }],
      });

      try {
        expect(sub.statusCode).toBe(200);
        const listed = await pollSubscriptions(
          (subs) => subs.some((s) => s.measurement === meas),
          5000
        );
        expect(listed).not.toBeNull();
        const entry = listed.subscriptions.find((s) => s.measurement === meas);
        expect(entry.label).toBe('labelled_q');
      } finally {
        sub.destroy();
      }
    });

    test(
      'subscription is cleaned up after client disconnect with zero data writes',
      async () => {
        const meas = `e2e_sub_disconnect_${ts}`;

        // Open a subscription that will never receive matching writes
        const sub = await openSubscription({ query: `avg:${meas}()` });
        expect(sub.statusCode).toBe(200);

        const listed = await pollSubscriptions(
          (subs) => subs.some((s) => s.measurement === meas),
          5000
        );
        expect(listed).not.toBeNull();

        // Destroy the client socket. With NO event traffic the server must
        // still notice via failing heartbeat writes and unregister the
        // subscription within ~2 heartbeat intervals (15s each by default).
        sub.destroy();

        const removed = await pollSubscriptions(
          (subs) => !subs.some((s) => s.measurement === meas),
          35000
        );
        expect(removed).not.toBeNull();
      },
      45000
    );
  });
});
