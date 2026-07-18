/**
 * End-to-end tests for last-write-wins (LWW) overwrite semantics.
 *
 * Writing the same measurement + tags + field + timestamp again REPLACES the
 * earlier point. Queries must only ever observe the newest write: raw reads
 * return one point, and every aggregation method counts it exactly once.
 * (See docs/api-write.md "Duplicate points overwrite".)
 */

const axios = require('axios');

const HOST = process.env.TIMESTAR_HOST || 'localhost';
const PORT = process.env.TIMESTAR_PORT || 8086;
const BASE_URL = `http://${HOST}:${PORT}`;

jest.setTimeout(20000);

const http = axios.create({ baseURL: BASE_URL, validateStatus: () => true });

// Unique namespace so this suite is independent of all others
const runId = Date.now();
const MEAS = `e2e_lww_${runId}`;

// Fixed timestamps for deterministic assertions
const T0 = runId * 1000000; // ns
const RANGE = { startTime: T0 - 60_000_000_000, endTime: T0 + 60_000_000_000 };

async function write(fields, timestamp, tags = { host: 'a' }) {
  const res = await http.post('/write', { measurement: MEAS, tags, fields, timestamp });
  expect(res.status).toBe(200);
  expect(res.data.status).toBe('success');
}

async function query(q, extra = {}) {
  const res = await http.post('/query', { query: q, ...RANGE, ...extra });
  expect(res.status).toBe(200);
  expect(res.data.status).toBe('success');
  return res.data;
}

function field(data, name = 'v') {
  expect(data.series.length).toBe(1);
  return data.series[0].fields[name];
}

describe('Last-write-wins overwrite semantics', () => {
  test('rewriting a point replaces its value in raw reads', async () => {
    // NOTE: both values must be float-shaped on the wire (JS serializes 10.0
    // as "10", which the server type-detects as an INTEGER field — a
    // different typed series). Type consistency is the writer's job.
    await write({ v: 10.5 }, T0);
    await write({ v: 99.5 }, T0);

    const f = field(await query(`avg:${MEAS}(v){host:a}`));
    expect(f.timestamps).toEqual([T0]);
    expect(f.values).toEqual([99.5]);
  });

  test('aggregations count the rewritten point exactly once', async () => {
    const t = T0 + 1_000_000_000;
    await write({ c: 5.0 }, t);
    await write({ c: 7.0 }, t);
    await write({ c: 3.0 }, t);

    const count = field(await query(`count:${MEAS}(c){host:a}`, { aggregationInterval: '1h' }), 'c');
    expect(count.values).toEqual([1]);

    const sum = field(await query(`sum:${MEAS}(c){host:a}`, { aggregationInterval: '1h' }), 'c');
    expect(sum.values).toEqual([3.0]);

    // An overwritten larger value must not resurrect through MAX
    const max = field(await query(`max:${MEAS}(c){host:a}`, { aggregationInterval: '1h' }), 'c');
    expect(max.values).toEqual([3.0]);
  });

  test('latest and first observe only the newest write', async () => {
    const t = T0 + 2_000_000_000;
    await write({ lf: 1.0 }, t);
    await write({ lf: 2.0 }, t);

    const latest = field(await query(`latest:${MEAS}(lf){host:a}`), 'lf');
    expect(latest.timestamps).toEqual([t]);
    expect(latest.values).toEqual([2.0]);

    const first = field(await query(`first:${MEAS}(lf){host:a}`), 'lf');
    expect(first.timestamps).toEqual([t]);
    expect(first.values).toEqual([2.0]);
  });

  test('within one batch the last entry for a timestamp wins', async () => {
    const t = T0 + 3_000_000_000;
    const res = await http.post('/write', {
      writes: [
        { measurement: MEAS, tags: { host: 'b' }, fields: { v: 1.0 }, timestamp: t },
        { measurement: MEAS, tags: { host: 'b' }, fields: { v: 2.0 }, timestamp: t },
        { measurement: MEAS, tags: { host: 'b' }, fields: { v: 3.0 }, timestamp: t },
      ],
    });
    expect(res.status).toBe(200);

    const f = field(await query(`avg:${MEAS}(v){host:b}`));
    expect(f.timestamps).toEqual([t]);
    expect(f.values).toEqual([3.0]);
  });

  test('string fields overwrite too', async () => {
    const t = T0 + 4_000_000_000;
    await write({ s: 'old' }, t, { host: 'c' });
    await write({ s: 'new' }, t, { host: 'c' });

    const f = field(await query(`latest:${MEAS}(s){host:c}`), 's');
    expect(f.timestamps).toEqual([t]);
    expect(f.values).toEqual(['new']);
  });

  test('different timestamps still append normally', async () => {
    const t = T0 + 5_000_000_000;
    await write({ v: 1.0 }, t, { host: 'd' });
    await write({ v: 2.0 }, t + 1000, { host: 'd' });

    const f = field(await query(`avg:${MEAS}(v){host:d}`));
    expect(f.timestamps).toEqual([t, t + 1000]);
    expect(f.values).toEqual([1.0, 2.0]);
  });
});
