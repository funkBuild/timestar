/**
 * End-to-end API tests for GET /cardinality.
 *
 * Response shape (per docs/api-metadata.md):
 *   { "measurement": "...", "estimated_series_count": <double>,
 *     "tag_cardinalities": { "<tag_key>": <double>, ... } }
 *
 * estimated_series_count is an HLL estimate summed across shards, so
 * assertions allow slack. tag_cardinalities (measurement-level) are exact
 * distinct-tag-value counts from the schema cache.
 */

const axios = require('axios');

const HOST = process.env.TIMESTAR_HOST || 'localhost';
const PORT = process.env.TIMESTAR_PORT || 8086;
const BASE_URL = `http://${HOST}:${PORT}`;

jest.setTimeout(20000);

const http = axios.create({ baseURL: BASE_URL, validateStatus: () => true });

// Unique namespace so this suite is independent of all others
const ts = Date.now();
const MEAS_A = `e2e_card_a_${ts}`;
const MEAS_B = `e2e_card_b_${ts}`;

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function writeSeries(measurement, tagSets) {
  const base = Date.now() * 1000000;
  const writes = tagSets.map((tags, i) => ({
    measurement,
    tags,
    fields: { value: 10 + i },
    timestamp: base + i * 1000,
  }));
  const res = await http.post('/write', { writes });
  expect(res.status).toBe(200);
}

function expectFlatError(body) {
  expect(body.status).toBe('error');
  expect(typeof body.error).toBe('string');
  expect(body.error.length).toBeGreaterThan(0);
  expect(body.message).toBe(body.error);
}

describe('Cardinality API', () => {
  beforeAll(async () => {
    // Measurement A: 3 series (3 hosts x 1 region, single field)
    await writeSeries(MEAS_A, [
      { host: 'h1', region: 'r1' },
      { host: 'h2', region: 'r1' },
      { host: 'h3', region: 'r1' },
    ]);

    // Measurement B: 5 series (5 sensors, single field)
    await writeSeries(MEAS_B, [
      { sensor: 's1' },
      { sensor: 's2' },
      { sensor: 's3' },
      { sensor: 's4' },
      { sensor: 's5' },
    ]);

    // Give the schema broadcast a moment to reach all shards
    await sleep(500);
  });

  test('returns measurement-level cardinality with expected shape', async () => {
    const res = await http.get('/cardinality', { params: { measurement: MEAS_A } });

    expect(res.status).toBe(200);
    expect(res.data.measurement).toBe(MEAS_A);
    expect(typeof res.data.estimated_series_count).toBe('number');
    expect(typeof res.data.tag_cardinalities).toBe('object');

    // 3 series written; HLL estimate should be close (generous slack)
    expect(res.data.estimated_series_count).toBeGreaterThanOrEqual(2);
    expect(res.data.estimated_series_count).toBeLessThanOrEqual(4.5);

    // Per-tag-key distinct value counts are exact
    expect(res.data.tag_cardinalities.host).toBe(3);
    expect(res.data.tag_cardinalities.region).toBe(1);
  });

  test('measurements are counted independently', async () => {
    const res = await http.get('/cardinality', { params: { measurement: MEAS_B } });

    expect(res.status).toBe(200);
    expect(res.data.measurement).toBe(MEAS_B);
    expect(res.data.estimated_series_count).toBeGreaterThanOrEqual(4);
    expect(res.data.estimated_series_count).toBeLessThanOrEqual(6.5);
    expect(res.data.tag_cardinalities.sensor).toBe(5);
    // Tag keys from measurement A must not leak into B
    expect(res.data.tag_cardinalities.host).toBeUndefined();
  });

  test('estimate grows after adding new series (monotonic, HLL slack allowed)', async () => {
    const before = await http.get('/cardinality', { params: { measurement: MEAS_A } });
    expect(before.status).toBe(200);
    const beforeEstimate = before.data.estimated_series_count;

    // Add 5 brand-new series (new host tag values)
    await writeSeries(MEAS_A, [
      { host: 'h10', region: 'r2' },
      { host: 'h11', region: 'r2' },
      { host: 'h12', region: 'r2' },
      { host: 'h13', region: 'r2' },
      { host: 'h14', region: 'r2' },
    ]);
    await sleep(500);

    const after = await http.get('/cardinality', { params: { measurement: MEAS_A } });
    expect(after.status).toBe(200);
    const afterEstimate = after.data.estimated_series_count;

    // Must never shrink, and must clearly grow (5 added; allow HLL slack)
    expect(afterEstimate).toBeGreaterThanOrEqual(beforeEstimate);
    expect(afterEstimate).toBeGreaterThanOrEqual(beforeEstimate + 2);

    // Exact distinct tag value counts reflect the new tags
    expect(after.data.tag_cardinalities.host).toBe(8);
    expect(after.data.tag_cardinalities.region).toBe(2);
  });

  test('re-writing existing series does not inflate the estimate', async () => {
    const before = await http.get('/cardinality', { params: { measurement: MEAS_B } });
    expect(before.status).toBe(200);

    // Same series (same tags), new timestamps
    await writeSeries(MEAS_B, [{ sensor: 's1' }, { sensor: 's2' }, { sensor: 's3' }]);
    await sleep(500);

    const after = await http.get('/cardinality', { params: { measurement: MEAS_B } });
    expect(after.status).toBe(200);
    // Duplicate series must not grow the estimate beyond slack
    expect(after.data.estimated_series_count).toBeLessThanOrEqual(
      before.data.estimated_series_count + 0.5
    );
    expect(after.data.tag_cardinalities.sensor).toBe(5);
  });

  test('supports tag_key/tag_value pair cardinality', async () => {
    const res = await http.get('/cardinality', {
      params: { measurement: MEAS_A, tag_key: 'host', tag_value: 'h1' },
    });

    expect(res.status).toBe(200);
    expect(res.data.measurement).toBe(MEAS_A);
    // Exactly one series has host=h1
    expect(res.data.estimated_series_count).toBeGreaterThanOrEqual(0.5);
    expect(res.data.estimated_series_count).toBeLessThanOrEqual(1.5);
    // Pair queries key tag_cardinalities by "key:value"
    expect(res.data.tag_cardinalities['host:h1']).toBeDefined();
  });

  test('unknown measurement returns zero estimate', async () => {
    const res = await http.get('/cardinality', {
      params: { measurement: `e2e_card_never_written_${ts}` },
    });

    expect(res.status).toBe(200);
    expect(res.data.estimated_series_count).toBe(0);
    expect(res.data.tag_cardinalities).toEqual({});
  });

  test('missing measurement param returns 400 with flat error shape', async () => {
    const res = await http.get('/cardinality');

    expect(res.status).toBe(400);
    expectFlatError(res.data);
    expect(res.data.error_code).toBe('MISSING_PARAMETER');
    expect(res.data.error).toMatch(/measurement/);
  });

  test('tag_key without tag_value returns 400 with flat error shape', async () => {
    const res = await http.get('/cardinality', {
      params: { measurement: MEAS_A, tag_key: 'host' },
    });

    expect(res.status).toBe(400);
    expectFlatError(res.data);
    expect(res.data.error_code).toBe('INVALID_PARAMETER');
    expect(res.data.error).toMatch(/tag_key and tag_value/);
  });
});
