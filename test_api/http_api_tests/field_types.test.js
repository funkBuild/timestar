/**
 * End-to-end tests for explicit field typing on JSON writes.
 *
 * A write point may carry "field_types": {"<field>": "float"|"int"|"bool"|"string"}
 * to override JSON token-shape type detection. This matters for JS clients:
 * JSON.stringify(10.0) emits "10", which would otherwise register the field
 * as an INTEGER series. (See docs/api-write.md "Explicit field types".)
 */

const axios = require('axios');

const HOST = process.env.TIMESTAR_HOST || 'localhost';
const PORT = process.env.TIMESTAR_PORT || 8086;
const BASE_URL = `http://${HOST}:${PORT}`;

jest.setTimeout(20000);

const http = axios.create({ baseURL: BASE_URL, validateStatus: () => true });

// Unique namespace so this suite is independent of all others
const runId = Date.now();
const MEAS = `e2e_ftypes_${runId}`;

const T0 = runId * 1000000; // ns
const RANGE = { startTime: T0 - 60_000_000_000, endTime: T0 + 60_000_000_000 };

async function query(q, extra = {}) {
  const res = await http.post('/query', { query: q, ...RANGE, ...extra });
  expect(res.status).toBe(200);
  expect(res.data.status).toBe('success');
  return res.data;
}

async function fieldTypeOf(measurement, name) {
  const res = await http.get(`/fields?measurement=${measurement}`);
  expect(res.status).toBe(200);
  return res.data.fields[name]?.type;
}

describe('Explicit field types on JSON writes', () => {
  test('declared float pins a whole-number value as a float series', async () => {
    // JS serializes 10.0 as "10" — without the declaration this would
    // register an INTEGER field and a later 99.5 write would be orphaned.
    const meas = `${MEAS}_f`;
    let res = await http.post('/write', {
      measurement: meas, tags: { h: 'a' },
      fields: { v: 10.0 }, field_types: { v: 'float' }, timestamp: T0,
    });
    expect(res.status).toBe(200);
    expect(await fieldTypeOf(meas, 'v')).toBe('float');

    // Rewrite with a float-shaped value lands in the SAME series (LWW)
    res = await http.post('/write', {
      measurement: meas, tags: { h: 'a' }, fields: { v: 99.5 }, timestamp: T0,
    });
    expect(res.status).toBe(200);

    const f = (await query(`avg:${meas}(v){h:a}`)).series[0].fields.v;
    expect(f.timestamps).toEqual([T0]);
    expect(f.values).toEqual([99.5]);
  });

  test('declared int accepts integral values and rejects fractional ones', async () => {
    const meas = `${MEAS}_i`;
    let res = await http.post('/write', {
      measurement: meas, tags: { h: 'a' },
      fields: { n: 1000 }, field_types: { n: 'int' }, timestamp: T0,
    });
    expect(res.status).toBe(200);
    expect(await fieldTypeOf(meas, 'n')).toBe('integer');

    res = await http.post('/write', {
      measurement: meas, tags: { h: 'a' },
      fields: { n: 10.5 }, field_types: { n: 'int' }, timestamp: T0,
    });
    expect(res.status).toBe(400);
    expect(res.data.error).toMatch(/declared int/);
  });

  test('declared bool and string reject mismatched values', async () => {
    let res = await http.post('/write', {
      measurement: `${MEAS}_b`, tags: { h: 'a' },
      fields: { flag: 1 }, field_types: { flag: 'bool' }, timestamp: T0,
    });
    expect(res.status).toBe(400);
    expect(res.data.error).toMatch(/declared bool/);

    res = await http.post('/write', {
      measurement: `${MEAS}_s`, tags: { h: 'a' },
      fields: { s: 42 }, field_types: { s: 'string' }, timestamp: T0,
    });
    expect(res.status).toBe(400);
    expect(res.data.error).toMatch(/declared string/);
  });

  test('unknown type name is rejected', async () => {
    const res = await http.post('/write', {
      measurement: `${MEAS}_u`, tags: { h: 'a' },
      fields: { v: 1 }, field_types: { v: 'decimal' }, timestamp: T0,
    });
    expect(res.status).toBe(400);
    expect(res.data.error).toMatch(/Unknown type 'decimal'/);
  });

  test('field_types entry with no matching field is rejected', async () => {
    const res = await http.post('/write', {
      measurement: `${MEAS}_m`, tags: { h: 'a' },
      fields: { v: 1 }, field_types: { typo: 'float' }, timestamp: T0,
    });
    expect(res.status).toBe(400);
    expect(res.data.error).toMatch(/no matching field/);
  });

  test('batch writes honor per-entry field_types', async () => {
    const meas = `${MEAS}_batch`;
    const res = await http.post('/write', {
      writes: [
        { measurement: meas, tags: { h: 'a' }, fields: { v: 10 }, field_types: { v: 'float' }, timestamp: T0 },
        { measurement: meas, tags: { h: 'a' }, fields: { v: 20 }, field_types: { v: 'float' }, timestamp: T0 + 1000 },
      ],
    });
    expect(res.status).toBe(200);
    expect(res.data.points_written).toBe(2);
    expect(await fieldTypeOf(meas, 'v')).toBe('float');

    const f = (await query(`avg:${meas}(v){h:a}`)).series[0].fields.v;
    expect(f.values).toEqual([10, 20]);
  });

  test('batch entry with invalid field_types is counted as failed', async () => {
    const meas = `${MEAS}_bfail`;
    const res = await http.post('/write', {
      writes: [
        { measurement: meas, tags: { h: 'a' }, fields: { v: 1.5 }, timestamp: T0 },
        { measurement: meas, tags: { h: 'a' }, fields: { v: 2.5 }, field_types: { v: 'bogus' }, timestamp: T0 + 1000 },
      ],
    });
    expect(res.status).toBe(200);
    expect(res.data.status).toBe('partial');
    expect(res.data.points_written).toBe(1);
    expect(res.data.failed_writes).toBe(1);
  });

  test('array-form fields honor declared type', async () => {
    const meas = `${MEAS}_arr`;
    const res = await http.post('/write', {
      measurement: meas, tags: { h: 'a' },
      fields: { v: [1, 2, 3] }, field_types: { v: 'float' },
      timestamps: [T0, T0 + 1000, T0 + 2000],
    });
    expect(res.status).toBe(200);
    expect(res.data.points_written).toBe(3);
    expect(await fieldTypeOf(meas, 'v')).toBe('float');
  });

  test('type aliases are accepted', async () => {
    const meas = `${MEAS}_alias`;
    const res = await http.post('/write', {
      measurement: meas, tags: { h: 'a' },
      fields: { a: 1, b: 2, c: true, d: 'x' },
      field_types: { a: 'double', b: 'int64', c: 'boolean', d: 'string' },
      timestamp: T0,
    });
    expect(res.status).toBe(200);
    expect(await fieldTypeOf(meas, 'a')).toBe('float');
    expect(await fieldTypeOf(meas, 'b')).toBe('integer');
    expect(await fieldTypeOf(meas, 'c')).toBe('boolean');
  });
});
