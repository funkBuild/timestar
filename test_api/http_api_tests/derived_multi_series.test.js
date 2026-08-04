/**
 * End-to-end tests for MULTI-SERIES derived queries (POST /derived).
 *
 * The reported bug: a sub-query ("leg") that resolved to more than one series
 * was refused with HTTP 400, which made the natural per-device use of
 * forecast() / anomalies() -- one result group per device -- impossible.
 *
 * forecast() and anomalies() now fan out over groups unconditionally; the plain
 * arithmetic path does so behind the "multiSeries": true opt-in.
 *
 * These tests assert VALUES, not just group counts and labels.  The seed data
 * is chosen so every group holds a distinct, hand-computable arithmetic
 * progression, so a response that emitted group i's values against group j's
 * tags FAILS rather than passing on shape alone.
 *
 * Seeded data -- N points, one minute apart, i = 0 .. N-1:
 *
 *   dev=DEV-A   f1 = 10 + 1i     f2 = 20 + 2i     f3 = 30 + 3i
 *   dev=DEV-B   f1 = 100 + 10i   f2 = 200 + 20i   f3 = 300 + 30i
 *
 * Every series is exactly linear, so a linear forecast over it has
 * r_squared = 1 and residual_std_dev = 0 and its extrapolation is
 * `intercept + slope * k` at slot k -- exact, not approximate.  The
 * cross-series merge of f1 (case C) is the mean of the two devices,
 * (10 + i + 100 + 10i) / 2 = 55 + 5.5i.
 */

const http = require('http');

const HOST = process.env.TIMESTAR_HOST || 'localhost';
const PORT = process.env.TIMESTAR_PORT || 8086;
const BASE_URL = `http://${HOST}:${PORT}`;

function httpRequest(method, path, body = null) {
    return new Promise((resolve, reject) => {
        const url = new URL(path, BASE_URL);
        const options = {
            hostname: url.hostname,
            port: url.port,
            path: url.pathname,
            method: method,
            headers: {
                'Content-Type': 'application/json'
            }
        };

        const req = http.request(options, (res) => {
            let data = '';
            res.on('data', chunk => data += chunk);
            res.on('end', () => {
                try {
                    resolve({ status: res.statusCode, data: JSON.parse(data) });
                } catch (e) {
                    resolve({ status: res.statusCode, data: data });
                }
            });
        });

        req.on('error', reject);
        if (body) req.write(JSON.stringify(body));
        req.end();
    });
}

// ---------------------------------------------------------------- seed data

const MEASUREMENT = `derived_multi_series_${Date.now()}`;
const BASE_NS = 1704067200000000000;  // 2024-01-01T00:00:00Z
const STEP_NS = 60000000000;          // one minute
const N = 30;

const START_TIME = BASE_NS;
const END_TIME = BASE_NS + (N - 1) * STEP_NS;

// (intercept, slope) per device per field -- the whole of the expected answer.
const SERIES = {
    'DEV-A': { f1: [10, 1], f2: [20, 2], f3: [30, 3] },
    'DEV-B': { f1: [100, 10], f2: [200, 20], f3: [300, 30] }
};

// The cross-series merge of f1 with no `by` clause: avg over both devices.
const MERGED_F1 = [55, 5.5];

function ramp(intercept, slope, count = N) {
    const out = [];
    for (let i = 0; i < count; i++) out.push(intercept + slope * i);
    return out;
}

async function seed() {
    const writes = [];
    for (const dev of Object.keys(SERIES)) {
        for (let i = 0; i < N; i++) {
            const fields = {};
            for (const field of Object.keys(SERIES[dev])) {
                const [intercept, slope] = SERIES[dev][field];
                fields[field] = intercept + slope * i;
            }
            writes.push({
                measurement: MEASUREMENT,
                tags: { dev },
                fields,
                // JS serializes 10.0 as 10, which the type sniffer would read as
                // an integer; pin the type so every field is a float series.
                field_types: { f1: 'float', f2: 'float', f3: 'float' },
                timestamp: BASE_NS + i * STEP_NS
            });
        }
    }

    for (let i = 0; i < writes.length; i += 100) {
        const batch = writes.slice(i, i + 100);
        const response = await httpRequest('POST', '/write', { writes: batch });
        if (response.status !== 200 && response.status !== 204) {
            throw new Error(`Failed to seed: ${JSON.stringify(response.data)}`);
        }
    }
    await new Promise(resolve => setTimeout(resolve, 500));
}

// ------------------------------------------------------------------ helpers

async function derived(queries, formula, extra = {}) {
    return httpRequest('POST', '/derived', Object.assign({
        queries,
        formula,
        startTime: START_TIME,
        endTime: END_TIME
    }, extra));
}

/**
 * Collapse a forecast/anomaly response's flat piece list into one entry per
 * group, in EMISSION order, keeping each group's pieces keyed by piece name.
 * The order matters: it is part of the contract (tags ascending, then field),
 * and it is what makes a tag/value mispairing detectable.
 */
function groupsOf(response) {
    const groups = [];
    const seen = new Map();
    for (const piece of (response.data.series || [])) {
        const tags = piece.group_tags || [];
        const label = tags.join('|');
        if (!seen.has(label)) {
            seen.set(label, groups.length);
            groups.push({ tags, label, pieces: {} });
        }
        groups[seen.get(label)].pieces[piece.piece] = piece.values;
    }
    return groups;
}

function expectValuesClose(actual, expected, what) {
    expect(Array.isArray(actual)).toBe(true);
    expect(`${what}: length ${actual.length}`).toBe(`${what}: length ${expected.length}`);
    for (let i = 0; i < expected.length; i++) {
        if (expected[i] === null) {
            expect(actual[i]).toBeNull();
        } else {
            expect(typeof actual[i]).toBe('number');
            expect(actual[i]).toBeCloseTo(expected[i], 6);
        }
    }
}

/**
 * A forecast group over a perfectly linear ramp.  Historical slots hold the
 * stored values verbatim under `past`; the forecast slots hold
 * `intercept + slope * k` at slot k, which is exact because r_squared is 1.
 */
function expectLinearForecastGroup(group, forecastStartIndex, axisLength, intercept, slope, what) {
    expect(group.pieces.past).toBeDefined();
    expect(group.pieces.forecast).toBeDefined();

    // Historical half: the stored series, value for value.
    expectValuesClose(group.pieces.past.slice(0, forecastStartIndex),
                      ramp(intercept, slope, forecastStartIndex), `${what} past`);
    // ... and nothing fabricated into the forecast half of the `past` piece.
    for (let k = forecastStartIndex; k < axisLength; k++) {
        expect(group.pieces.past[k]).toBeNull();
    }

    // Forecast half: the same line continued.  The slot at forecastStartIndex-1
    // is the connector -- the forecast piece repeats the last historical value
    // there so the two halves join; everything before it is null.
    for (let k = 0; k < forecastStartIndex - 1; k++) {
        expect(group.pieces.forecast[k]).toBeNull();
        expect(group.pieces.upper[k]).toBeNull();
        expect(group.pieces.lower[k]).toBeNull();
    }
    for (let k = forecastStartIndex - 1; k < axisLength; k++) {
        expect(group.pieces.forecast[k]).toBeCloseTo(intercept + slope * k, 6);
    }
    for (let k = forecastStartIndex; k < axisLength; k++) {
        // Confidence band brackets the projection.
        expect(group.pieces.upper[k]).toBeGreaterThanOrEqual(group.pieces.forecast[k] - 1e-6);
        expect(group.pieces.lower[k]).toBeLessThanOrEqual(group.pieces.forecast[k] + 1e-6);
    }
}

function expectAnomalyRaw(group, intercept, slope, what) {
    expect(group.pieces.raw).toBeDefined();
    expectValuesClose(group.pieces.raw, ramp(intercept, slope), `${what} raw`);
}

// --------------------------------------------------------------------- tests

describe('Multi-series derived queries', () => {
    beforeAll(async () => {
        await seed();
    }, 60000);

    // A leg matching exactly one series -- the shape that always worked.  Pinned
    // so the fan-out cannot change it.
    describe('case A: one field, one device -- a single group', () => {
        const leg = `avg:${MEASUREMENT}(f1){dev:DEV-A}`;

        test('forecast() returns one group holding DEV-A f1', async () => {
            const response = await derived({ q: leg }, "forecast(q, 'linear', 2)");
            expect(response.status).toBe(200);
            expect(response.data.status).toBe('success');

            const groups = groupsOf(response);
            expect(groups.length).toBe(1);
            expect(groups[0].tags).toEqual([]);
            expect(response.data.statistics.series_count).toBe(1);
            expect(response.data.forecast_start_index).toBe(N);

            expectLinearForecastGroup(groups[0], N, response.data.times.length, 10, 1, 'A');
            expect(response.data.statistics.r_squared).toBeCloseTo(1, 9);
        });

        test('anomalies() returns one group holding DEV-A f1', async () => {
            const response = await derived({ q: leg }, "anomalies(q, 'basic', 2)");
            expect(response.status).toBe(200);
            expect(response.data.status).toBe('success');

            const groups = groupsOf(response);
            expect(groups.length).toBe(1);
            expect(groups[0].tags).toEqual([]);
            expect(response.data.statistics.total_points).toBe(N);
            expectAnomalyRaw(groups[0], 10, 1, 'A');
        });
    });

    // Three fields of one device.  The fan-out is on the FIELD axis, so each
    // group carries the synthetic "_field=" label and nothing else.
    describe('case B: three fields, one device -- three groups labelled by field', () => {
        const leg = `avg:${MEASUREMENT}(f1,f2,f3){dev:DEV-A}`;
        const expected = [
            { tags: ['_field=f1'], intercept: 10, slope: 1 },
            { tags: ['_field=f2'], intercept: 20, slope: 2 },
            { tags: ['_field=f3'], intercept: 30, slope: 3 }
        ];

        test('forecast() returns three field groups, each with its own values', async () => {
            const response = await derived({ q: leg }, "forecast(q, 'linear', 2)");
            expect(response.status).toBe(200);

            const groups = groupsOf(response);
            expect(groups.map(g => g.tags)).toEqual(expected.map(e => e.tags));
            expect(response.data.statistics.series_count).toBe(3);
            expect(response.data.forecast_start_index).toBe(N);

            expected.forEach((e, idx) => {
                // Assert against the group found BY LABEL, not by position, so a
                // response that emitted f2's numbers under _field=f1 fails here.
                const group = groups.find(g => g.label === e.tags.join('|'));
                expect(group).toBeDefined();
                expectLinearForecastGroup(group, N, response.data.times.length,
                                          e.intercept, e.slope, `B[${idx}]`);
            });
        });

        test('anomalies() returns three field groups, each with its own values', async () => {
            const response = await derived({ q: leg }, "anomalies(q, 'basic', 2)");
            expect(response.status).toBe(200);

            const groups = groupsOf(response);
            expect(groups.map(g => g.tags)).toEqual(expected.map(e => e.tags));
            expect(response.data.statistics.total_points).toBe(3 * N);

            expected.forEach((e, idx) => {
                const group = groups.find(g => g.label === e.tags.join('|'));
                expect(group).toBeDefined();
                expectAnomalyRaw(group, e.intercept, e.slope, `B[${idx}]`);
            });
        });
    });

    // No `by` clause: /query aggregates ACROSS series at equal timestamps, so
    // the two devices merge into one series before /derived ever sees them.
    describe('case C: one field, both devices, no group-by -- one merged group', () => {
        const leg = `avg:${MEASUREMENT}(f1){}`;

        test('forecast() forecasts the cross-series mean, not either device', async () => {
            const response = await derived({ q: leg }, "forecast(q, 'linear', 2)");
            expect(response.status).toBe(200);

            const groups = groupsOf(response);
            expect(groups.length).toBe(1);
            expect(groups[0].tags).toEqual([]);
            expect(response.data.statistics.series_count).toBe(1);
            expect(response.data.forecast_start_index).toBe(N);

            expectLinearForecastGroup(groups[0], N, response.data.times.length,
                                      MERGED_F1[0], MERGED_F1[1], 'C');
        });

        test('anomalies() scores the cross-series mean, not either device', async () => {
            const response = await derived({ q: leg }, "anomalies(q, 'basic', 2)");
            expect(response.status).toBe(200);

            const groups = groupsOf(response);
            expect(groups.length).toBe(1);
            expect(groups[0].tags).toEqual([]);
            expect(response.data.statistics.total_points).toBe(N);
            expectAnomalyRaw(groups[0], MERGED_F1[0], MERGED_F1[1], 'C');
        });
    });

    // THE REPORTED BUG: this used to be HTTP 400.
    describe('case D: one field, both devices, by {dev} -- one group per device', () => {
        const leg = `avg:${MEASUREMENT}(f1){} by {dev}`;
        const expected = [
            { tags: ['dev=DEV-A'], intercept: 10, slope: 1 },
            { tags: ['dev=DEV-B'], intercept: 100, slope: 10 }
        ];

        test('forecast() returns one group per device with that device values', async () => {
            const response = await derived({ q: leg }, "forecast(q, 'linear', 2)");
            expect(response.status).toBe(200);
            expect(response.data.status).toBe('success');

            const groups = groupsOf(response);
            // Order is a contract: tag sets ascending.
            expect(groups.map(g => g.tags)).toEqual([['dev=DEV-A'], ['dev=DEV-B']]);
            // A single field means NO synthetic "_field=" label.
            expect(groups.every(g => g.tags.every(t => !t.startsWith('_field=')))).toBe(true);
            expect(response.data.statistics.series_count).toBe(2);
            expect(response.data.statistics.declined_series_count).toBeUndefined();
            expect(response.data.forecast_start_index).toBe(N);

            expected.forEach((e, idx) => {
                const group = groups.find(g => g.label === e.tags.join('|'));
                expect(group).toBeDefined();
                expectLinearForecastGroup(group, N, response.data.times.length,
                                          e.intercept, e.slope, `D[${idx}]`);
            });

            // Belt and braces on the mispairing this suite exists to catch: the
            // two devices are an order of magnitude apart, so no group may hold
            // the other's opening value.
            const a = groups.find(g => g.label === 'dev=DEV-A');
            const b = groups.find(g => g.label === 'dev=DEV-B');
            expect(a.pieces.past[0]).toBeCloseTo(10, 9);
            expect(b.pieces.past[0]).toBeCloseTo(100, 9);
        });

        test('anomalies() returns one group per device with that device values', async () => {
            const response = await derived({ q: leg }, "anomalies(q, 'basic', 2)");
            expect(response.status).toBe(200);
            expect(response.data.status).toBe('success');

            const groups = groupsOf(response);
            expect(groups.map(g => g.tags)).toEqual([['dev=DEV-A'], ['dev=DEV-B']]);
            expect(response.data.statistics.total_points).toBe(2 * N);
            expect(response.data.statistics.declined_series_count).toBeUndefined();

            expected.forEach((e, idx) => {
                const group = groups.find(g => g.label === e.tags.join('|'));
                expect(group).toBeDefined();
                expectAnomalyRaw(group, e.intercept, e.slope, `D[${idx}]`);
            });
        });

        test('every device group shares the response time axis', async () => {
            const response = await derived({ q: leg }, "anomalies(q, 'basic', 2)");
            expect(response.status).toBe(200);
            expect(response.data.times.length).toBe(N);
            for (let i = 0; i < N; i++) {
                expect(response.data.times[i]).toBe(BASE_NS + i * STEP_NS);
            }
        });
    });

    // `field` is not a tag key any series carries, so nothing groups -- the same
    // answer an unknown by-key gets on /query.
    describe('case E: by {field} -- an unknown grouping key yields nothing', () => {
        const leg = `avg:${MEASUREMENT}(f1,f2,f3){} by {field}`;

        test('forecast() returns success with no groups', async () => {
            const response = await derived({ q: leg }, "forecast(q, 'linear', 2)");
            expect(response.status).toBe(200);
            expect(response.data.status).toBe('success');
            expect(response.data.series).toEqual([]);
            expect(response.data.times).toEqual([]);
            expect(response.data.statistics.series_count).toBe(0);
        });

        test('anomalies() returns success with no groups', async () => {
            const response = await derived({ q: leg }, "anomalies(q, 'basic', 2)");
            expect(response.status).toBe(200);
            expect(response.data.status).toBe('success');
            expect(response.data.series).toEqual([]);
            expect(response.data.times).toEqual([]);
            expect(response.data.statistics.total_points).toBe(0);
        });
    });

    // The arithmetic path is opt-in: same request, two answers.
    describe('multiSeries arithmetic opt-in', () => {
        const legs = {
            a: `avg:${MEASUREMENT}(f1){} by {dev}`,
            b: `avg:${MEASUREMENT}(f2){} by {dev}`
        };

        test('WITHOUT the flag a multi-series leg is still refused', async () => {
            const response = await derived(legs, 'a + b');
            expect(response.status).toBe(400);
            expect(response.data.status).toBe('error');
            expect(response.data.message).toMatch(/returned 2 series/);
            expect(response.data.message).toMatch(/exactly one series/);
            // No partial answer smuggled alongside the error.
            expect(response.data.series).toBeUndefined();
        });

        test('WITH the flag the formula is evaluated once per device', async () => {
            const response = await derived(legs, 'a + b', { multiSeries: true });
            expect(response.status).toBe(200);
            expect(response.data.status).toBe('success');

            const series = response.data.series;
            expect(Array.isArray(series)).toBe(true);
            expect(series.map(s => s.group_tags)).toEqual([['dev=DEV-A'], ['dev=DEV-B']]);
            expect(response.data.statistics.group_count).toBe(2);
            expect(response.data.statistics.point_count).toBe(2 * N);
            expect(response.data.formula).toBe('a + b');

            // The answer lives ONLY under `series`; the flat pair stays empty.
            expect(response.data.timestamps).toEqual([]);
            expect(response.data.values).toEqual([]);

            // DEV-A: (10 + i) + (20 + 2i) = 30 + 3i
            // DEV-B: (100 + 10i) + (200 + 20i) = 300 + 30i
            const expected = {
                'dev=DEV-A': ramp(30, 3),
                'dev=DEV-B': ramp(300, 30)
            };
            for (const group of series) {
                const label = group.group_tags.join('|');
                expect(expected[label]).toBeDefined();
                expectValuesClose(group.values, expected[label], label);
                expect(group.timestamps.length).toBe(N);
                for (let i = 0; i < N; i++) {
                    expect(group.timestamps[i]).toBe(BASE_NS + i * STEP_NS);
                }
            }
        });

        test('WITHOUT the flag a single-series formula is unaffected', async () => {
            const response = await derived({
                a: `avg:${MEASUREMENT}(f1){dev:DEV-A}`,
                b: `avg:${MEASUREMENT}(f2){dev:DEV-A}`
            }, 'a + b');

            expect(response.status).toBe(200);
            expect(response.data.status).toBe('success');
            // The pre-flag response shape: a flat pair, and no `series` key at
            // all -- absent means "this server has no such flag".
            expect(response.data.series).toBeUndefined();
            expect(response.data.statistics.group_count).toBeUndefined();
            expectValuesClose(response.data.values, ramp(30, 3), 'single-series a + b');
        });
    });
});
