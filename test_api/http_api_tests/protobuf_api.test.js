/**
 * End-to-end tests for the protobuf HTTP API (application/x-protobuf).
 *
 * Exercises real HTTP protobuf round-trips against a running server:
 *   - POST /write with a binary WriteRequest body (single point + mixed-type batch)
 *   - POST /query with a binary QueryRequest body (protobuf and JSON responses)
 *   - Malformed / empty protobuf bodies
 *
 * Schema is loaded at runtime from proto/timestar.proto (package timestar_pb).
 * protobufjs converts field names to camelCase: points_written -> pointsWritten,
 * double_values -> doubleValues, etc.
 *
 * Actual server behavior verified against the C++ handlers
 * (lib/http/http_write_handler.cpp handleProtobufWrite,
 *  lib/http/http_query_handler.cpp, lib/http/content_negotiation.cpp):
 *
 *   - Request format is selected by Content-Type: application/x-protobuf (or
 *     application/protobuf). Response format is selected by the Accept header;
 *     if Accept is absent it echoes the request format. NOTE: axios sends a
 *     default Accept header containing application/json, so protobuf responses
 *     must be requested explicitly with Accept: application/x-protobuf.
 *   - Protobuf responses carry Content-Type: application/x-protobuf.
 *     (JsonDefaultHandler in lib/http/http_routes.hpp defaults to JSON only
 *     when the endpoint set no explicit type; postProto() asserts this.)
 *   - points_written counts field-points: one WritePoint with 4 fields and
 *     1 timestamp counts as 4 points.
 *   - Protobuf /query SUCCESS responses always use the Approach B compressed
 *     fields (FieldData.compressed_timestamps = FFOR, DoubleArray.compressed_alp,
 *     Int64Array.compressed_ffor, BoolArray.compressed_rle,
 *     StringArray.compressed_zstd); the repeated packed `values` fields are left
 *     empty. Decoding those payloads requires the C++ FFOR/ALP/RLE/zstd decoders,
 *     so these tests assert structural validity (status, series, field names,
 *     non-empty compressed bytes, statistics) and verify exact values through
 *     the JSON /query API instead. Full compressed round-trip decoding is
 *     covered in C++ by test/unit/http/protobuf_integration_test.cpp and
 *     test/unit/http/proto_converter_test.cpp.
 *   - A garbage protobuf body fails parsing with std::runtime_error, which the
 *     write handler's generic catch maps to 500 (not 400). An empty body is
 *     rejected earlier with 400. Both use the flat JSON error shape
 *     {status, error, message} when the client accepts JSON, and a protobuf
 *     WriteResponse{status:"error", errors:[...]} when it accepts protobuf.
 *
 * COMPRESSED-FIELD WRITES (WritePoint.compressed_timestamps,
 * DoubleArray.compressed_alp, Int64Array.compressed_ffor, BoolArray.compressed_rle,
 * StringArray.compressed_zstd on the WRITE path) are intentionally NOT tested
 * here: producing valid FFOR/ALP/RLE/dictionary-zstd payloads requires the C++
 * encoders and cannot be done from JS. That path is covered by the C++ tests in
 * test/unit/http/protobuf_integration_test.cpp and
 * test/unit/http/proto_write_fast_path_test.cpp.
 */

const path = require('path');
const axios = require('axios');
const protobuf = require('protobufjs');

const HOST = process.env.TIMESTAR_HOST || 'localhost';
const PORT = process.env.TIMESTAR_PORT || 8086;
const BASE_URL = `http://${HOST}:${PORT}`;
const PROTO_PATH = path.join(__dirname, '..', '..', 'proto', 'timestar.proto');
const PB_CONTENT_TYPE = 'application/x-protobuf';

jest.setTimeout(30000);

const http = axios.create({ baseURL: BASE_URL, validateStatus: () => true });

// Base timestamp kept below 2^53 so values survive JS double round-trips
// exactly (JSON responses are parsed into doubles). Any uint64 nanosecond
// value is valid to the server.
const BASE_TS = 1700000000000000;
const STEP = 1000000000; // 1s in ns

// Unique namespace so this suite is independent of all others
const runId = Date.now();
const MEAS_SINGLE = `e2e_pb_single_${runId}`;
const MEAS_BATCH = `e2e_pb_batch_${runId}`;

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// Message types resolved from proto/timestar.proto in beforeAll
let WriteRequest;
let WriteResponse;
let QueryRequest;
let QueryResponse;

function encodeWriteRequest(obj) {
  const err = WriteRequest.verify(obj);
  if (err) throw new Error(`WriteRequest verify failed: ${err}`);
  return Buffer.from(WriteRequest.encode(WriteRequest.create(obj)).finish());
}

function encodeQueryRequest(obj) {
  const err = QueryRequest.verify(obj);
  if (err) throw new Error(`QueryRequest verify failed: ${err}`);
  return Buffer.from(QueryRequest.encode(QueryRequest.create(obj)).finish());
}

// POST a binary body asking for a protobuf response.
// Protobuf responses must carry a protobuf Content-Type (JsonDefaultHandler
// only defaults to application/json when the endpoint set no type).
async function postProto(url, buf) {
  const res = await http.post(url, buf, {
    headers: { 'Content-Type': PB_CONTENT_TYPE, Accept: PB_CONTENT_TYPE },
    responseType: 'arraybuffer',
  });
  expect(res.headers['content-type']).toContain('protobuf');
  return res;
}

function decodeWriteResponse(res) {
  return WriteResponse.toObject(WriteResponse.decode(Buffer.from(res.data)), {
    longs: Number,
    defaults: true,
  });
}

function decodeQueryResponse(res) {
  return QueryResponse.toObject(QueryResponse.decode(Buffer.from(res.data)), {
    longs: Number,
    bytes: Buffer,
    defaults: true,
  });
}

// JSON /query helper
async function jsonQuery(queryStr, startTime = 0, endTime = BASE_TS * 10) {
  const res = await http.post('/query', { query: queryStr, startTime, endTime });
  expect(res.status).toBe(200);
  expect(res.data.status).toBe('success');
  return res.data;
}

// The JSON query response may split fields of one logical series across
// multiple series entries (e.g. string fields separate from numeric ones).
// Merge all fields maps for a measurement into one object.
function collectFields(response) {
  const fields = {};
  for (const s of response.series) {
    Object.assign(fields, s.fields);
  }
  return fields;
}

function expectFlatJsonError(body) {
  expect(body.status).toBe('error');
  expect(typeof body.error).toBe('string');
  expect(body.error.length).toBeGreaterThan(0);
  expect(body.message).toBe(body.error);
}

beforeAll(async () => {
  const root = await protobuf.load(PROTO_PATH);
  WriteRequest = root.lookupType('timestar_pb.WriteRequest');
  WriteResponse = root.lookupType('timestar_pb.WriteResponse');
  QueryRequest = root.lookupType('timestar_pb.QueryRequest');
  QueryResponse = root.lookupType('timestar_pb.QueryResponse');
});

describe('Protobuf write API (POST /write, application/x-protobuf)', () => {
  test('single-point uncompressed write returns decodable WriteResponse and is queryable', async () => {
    const buf = encodeWriteRequest({
      writes: [
        {
          measurement: MEAS_SINGLE,
          tags: { host: 'pb-host-1' },
          fields: {
            value: { doubleValues: { values: [23.75] } },
          },
          timestamps: [BASE_TS],
        },
      ],
    });

    const res = await postProto('/write', buf);
    expect(res.status).toBe(200);

    const wr = decodeWriteResponse(res);
    expect(wr.status).toBe('success');
    expect(wr.pointsWritten).toBe(1);
    expect(wr.failedWrites).toBe(0);
    expect(wr.errors).toEqual([]);

    // Verify through the JSON query API: exact timestamp and value
    await sleep(300);
    const data = await jsonQuery(`avg:${MEAS_SINGLE}(value){host:pb-host-1}`);
    const fields = collectFields(data);
    expect(fields.value).toBeDefined();
    expect(fields.value.timestamps).toEqual([BASE_TS]);
    expect(fields.value.values).toEqual([23.75]);
  });

  test('3-point batch with tags and mixed field types (double/int/bool/string)', async () => {
    const temps = [20.5, 21.25, -3.5];
    const counts = [7, -42, 9000000];
    const actives = [true, false, true];
    const labels = ['alpha', 'beta', 'gamma ünicøde ✓'];

    const writes = [0, 1, 2].map((i) => ({
      measurement: MEAS_BATCH,
      tags: { host: 'pb-host-2', rack: 'r1' },
      fields: {
        temp: { doubleValues: { values: [temps[i]] } },
        count: { int64Values: { values: [counts[i]] } },
        active: { boolValues: { values: [actives[i]] } },
        label: { stringValues: { values: [labels[i]] } },
      },
      timestamps: [BASE_TS + i * STEP],
    }));

    const res = await postProto('/write', encodeWriteRequest({ writes }));
    expect(res.status).toBe(200);

    const wr = decodeWriteResponse(res);
    expect(wr.status).toBe('success');
    // points_written counts field-points: 3 WritePoints x 4 fields x 1 timestamp
    expect(wr.pointsWritten).toBe(12);
    expect(wr.failedWrites).toBe(0);
    expect(wr.errors).toEqual([]);

    // Verify all four fields via the JSON query API with exact values
    await sleep(300);
    const expectedTs = [BASE_TS, BASE_TS + STEP, BASE_TS + 2 * STEP];
    const data = await jsonQuery(`avg:${MEAS_BATCH}()`);
    const fields = collectFields(data);

    expect(Object.keys(fields).sort()).toEqual(['active', 'count', 'label', 'temp']);

    expect(fields.temp.timestamps).toEqual(expectedTs);
    expect(fields.temp.values).toEqual(temps);

    expect(fields.count.timestamps).toEqual(expectedTs);
    expect(fields.count.values).toEqual(counts);

    // Booleans are serialized as 1/0 in the JSON query response
    expect(fields.active.timestamps).toEqual(expectedTs);
    expect(fields.active.values).toEqual([1, 0, 1]);

    expect(fields.label.timestamps).toEqual(expectedTs);
    expect(fields.label.values).toEqual(labels);
  });
});

describe('Protobuf query API (POST /query, application/x-protobuf)', () => {
  // Data written by the batch test above (jest runs tests in a file in order).

  test('protobuf QueryRequest returns structurally valid protobuf QueryResponse with compressed fields', async () => {
    const buf = encodeQueryRequest({
      query: `avg:${MEAS_BATCH}(temp)`,
      startTime: 0,
      endTime: BASE_TS * 10,
    });

    const res = await postProto('/query', buf);
    expect(res.status).toBe(200);

    const qr = decodeQueryResponse(res);
    expect(qr.status).toBe('success');
    expect(qr.errorCode).toBe('');
    expect(qr.statistics.seriesCount).toBe(1);
    expect(qr.statistics.pointCount).toBe(3);
    expect(qr.statistics.executionTimeMs).toBeGreaterThanOrEqual(0);

    expect(qr.series).toHaveLength(1);
    const series = qr.series[0];
    expect(series.measurement).toBe(MEAS_BATCH);
    expect(Object.keys(series.fields)).toEqual(['temp']);

    // Approach B: values arrive in compressed form only. FFOR timestamps +
    // ALP doubles. Decoding these requires the C++ decoders; the C++ suite
    // (test/unit/http/protobuf_integration_test.cpp) covers value round-trips.
    const fd = series.fields.temp;
    expect(fd.compressedTimestamps.length).toBeGreaterThan(0);
    expect(fd.doubleValues).toBeDefined();
    expect(fd.doubleValues.compressedAlp.length).toBeGreaterThan(0);
    // The repeated packed fields must be empty when compressed data is present
    expect(fd.timestamps).toEqual([]);
    expect(fd.doubleValues.values).toEqual([]);
  });

  test('protobuf QueryRequest with Accept: application/json returns exact JSON values', async () => {
    // Interop path: binary request, JSON response — lets JS verify exact values
    // from a protobuf-initiated query without C++ decoders.
    const buf = encodeQueryRequest({
      query: `avg:${MEAS_BATCH}(temp){host:pb-host-2}`,
      startTime: 0,
      endTime: BASE_TS * 10,
    });

    const res = await http.post('/query', buf, {
      headers: { 'Content-Type': PB_CONTENT_TYPE, Accept: 'application/json' },
    });
    expect(res.status).toBe(200);
    expect(res.data.status).toBe('success');

    const fields = collectFields(res.data);
    expect(fields.temp.timestamps).toEqual([BASE_TS, BASE_TS + STEP, BASE_TS + 2 * STEP]);
    expect(fields.temp.values).toEqual([20.5, 21.25, -3.5]);
  });

  test('invalid query string in protobuf request returns protobuf error with INVALID_QUERY', async () => {
    const buf = encodeQueryRequest({
      query: 'not_a_valid_query_string',
      startTime: 0,
      endTime: 1,
    });

    const res = await postProto('/query', buf);
    expect(res.status).toBe(400);

    const qr = decodeQueryResponse(res);
    expect(qr.status).toBe('error');
    expect(qr.errorCode).toBe('INVALID_QUERY');
    expect(qr.errorMessage.length).toBeGreaterThan(0);
    expect(qr.series).toEqual([]);
  });
});

describe('Malformed protobuf bodies (POST /write)', () => {
  const garbage = Buffer.from([0xff, 0xde, 0xad, 0xbe, 0xef, 0x01]);

  test('garbage body with protobuf Content-Type and JSON Accept returns flat JSON error', async () => {
    // Current behavior: proto parse failure surfaces as std::runtime_error and
    // maps to 500 Internal Server Error (not 400) — asserted as-is. See
    // parseWriteRequestFast in lib/http/proto_write_fast_path.cpp.
    const res = await http.post('/write', garbage, {
      headers: { 'Content-Type': PB_CONTENT_TYPE, Accept: 'application/json' },
    });
    expect(res.status).toBe(500);
    expectFlatJsonError(res.data);
  });

  test('garbage body with protobuf Accept returns protobuf WriteResponse error', async () => {
    const res = await postProto('/write', garbage);
    expect(res.status).toBe(500);

    const wr = decodeWriteResponse(res);
    expect(wr.status).toBe('error');
    expect(wr.pointsWritten).toBe(0);
    expect(wr.errors.length).toBeGreaterThan(0);
  });

  test('empty body with protobuf Content-Type returns 400 flat JSON error', async () => {
    const res = await http.post('/write', Buffer.alloc(0), {
      headers: { 'Content-Type': PB_CONTENT_TYPE, Accept: 'application/json' },
    });
    expect(res.status).toBe(400);
    expectFlatJsonError(res.data);
    expect(res.data.error).toMatch(/empty/i);
  });
});
