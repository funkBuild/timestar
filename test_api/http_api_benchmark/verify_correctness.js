const axios = require("axios");
const BASE_URL = "http://localhost:8086";

let passed = 0;
let failed = 0;

function assert(condition, msg) {
  if (condition) { passed++; process.stdout.write("."); }
  else { failed++; console.error("\nFAIL: " + msg); }
}

function approxEq(a, b, tol = 0.001) {
  return Math.abs(a - b) < tol;
}

async function main() {
  console.log("=== CORRECTNESS VERIFICATION ===\n");

  const T0 = 1700000000000000000n; // base timestamp in ns
  const MIN = 60000000000n;        // 1 minute in ns

  // -------------------------------------------------------
  // Test 1: Insert known data, query raw values back
  // -------------------------------------------------------
  console.log("Test 1: Insert & retrieve known values");

  // Insert 5 points for host-01 with known values
  const writes1 = [];
  for (let i = 0; i < 5; i++) {
    writes1.push({
      measurement: "test_verify",
      tags: { host: "host-01", rack: "rack-1" },
      fields: { cpu: 10.0 * (i + 1), mem: 20.0 * (i + 1) },
      timestamp: Number(T0 + BigInt(i) * MIN)
    });
  }

  // Insert 5 points for host-02 with different values
  for (let i = 0; i < 5; i++) {
    writes1.push({
      measurement: "test_verify",
      tags: { host: "host-02", rack: "rack-1" },
      fields: { cpu: 100.0 + i, mem: 200.0 + i },
      timestamp: Number(T0 + BigInt(i) * MIN)
    });
  }

  // Insert 3 points for host-03, rack-2
  for (let i = 0; i < 3; i++) {
    writes1.push({
      measurement: "test_verify",
      tags: { host: "host-03", rack: "rack-2" },
      fields: { cpu: 50.0, mem: 75.0 },
      timestamp: Number(T0 + BigInt(i) * MIN)
    });
  }

  await axios.post(`${BASE_URL}/write`, { writes: writes1 });

  // -------------------------------------------------------
  // Test 2: Query single field — verify field filtering works
  // -------------------------------------------------------
  console.log("\nTest 2: Single field query (cpu only)");

  const res2 = await axios.post(`${BASE_URL}/query`, {
    query: "avg:test_verify(cpu){host:host-01}",
    startTime: Number(T0 - MIN),
    endTime: Number(T0 + 10n * MIN)
  });

  assert(res2.data.status === "success", "Query should succeed");
  const series2 = res2.data.series;
  assert(series2.length === 1, `Expected 1 series, got ${series2.length}`);
  assert(series2[0].fields.cpu !== undefined, "Should have cpu field");
  assert(series2[0].fields.mem === undefined, "Should NOT have mem field (field filtering)");

  const cpuVals = series2[0].fields.cpu.values;
  assert(cpuVals.length === 5, `Expected 5 cpu values, got ${cpuVals.length}`);
  // cpu values should be 10, 20, 30, 40, 50
  assert(approxEq(cpuVals[0], 10.0), `cpu[0] should be 10, got ${cpuVals[0]}`);
  assert(approxEq(cpuVals[1], 20.0), `cpu[1] should be 20, got ${cpuVals[1]}`);
  assert(approxEq(cpuVals[2], 30.0), `cpu[2] should be 30, got ${cpuVals[2]}`);
  assert(approxEq(cpuVals[3], 40.0), `cpu[3] should be 40, got ${cpuVals[3]}`);
  assert(approxEq(cpuVals[4], 50.0), `cpu[4] should be 50, got ${cpuVals[4]}`);

  // -------------------------------------------------------
  // Test 3: Query all fields — verify both fields returned
  // -------------------------------------------------------
  console.log("\nTest 3: All fields query");

  const res3 = await axios.post(`${BASE_URL}/query`, {
    query: "avg:test_verify(){host:host-01}",
    startTime: Number(T0 - MIN),
    endTime: Number(T0 + 10n * MIN)
  });

  const series3 = res3.data.series;
  // Should have 2 series entries (one per field) or fields merged
  let cpuFound = false, memFound = false;
  for (const s of series3) {
    if (s.fields.cpu) cpuFound = true;
    if (s.fields.mem) memFound = true;
  }
  assert(cpuFound, "All-fields query should include cpu");
  assert(memFound, "All-fields query should include mem");

  // -------------------------------------------------------
  // Test 4: Aggregation — avg of known values
  // -------------------------------------------------------
  console.log("\nTest 4: AVG aggregation correctness");

  // host-01 cpu values: 10, 20, 30, 40, 50 → avg = 30
  const res4 = await axios.post(`${BASE_URL}/query`, {
    query: "avg:test_verify(cpu){host:host-01}",
    startTime: Number(T0 - MIN),
    endTime: Number(T0 + 10n * MIN),
    aggregationInterval: "1h"
  });

  const agg4 = res4.data.series[0].fields.cpu.values;
  // All 5 points fall within a single 1h bucket → avg = 30
  assert(agg4.length === 1, `Expected 1 bucket, got ${agg4.length}`);
  assert(approxEq(agg4[0], 30.0), `AVG should be 30, got ${agg4[0]}`);

  // -------------------------------------------------------
  // Test 5: SUM aggregation
  // -------------------------------------------------------
  console.log("\nTest 5: SUM aggregation correctness");

  const res5 = await axios.post(`${BASE_URL}/query`, {
    query: "sum:test_verify(cpu){host:host-01}",
    startTime: Number(T0 - MIN),
    endTime: Number(T0 + 10n * MIN),
    aggregationInterval: "1h"
  });

  const sum5 = res5.data.series[0].fields.cpu.values;
  // 10+20+30+40+50 = 150
  assert(sum5.length === 1, `Expected 1 bucket, got ${sum5.length}`);
  assert(approxEq(sum5[0], 150.0), `SUM should be 150, got ${sum5[0]}`);

  // -------------------------------------------------------
  // Test 6: MIN/MAX aggregation
  // -------------------------------------------------------
  console.log("\nTest 6: MIN/MAX aggregation correctness");

  const res6min = await axios.post(`${BASE_URL}/query`, {
    query: "min:test_verify(cpu){host:host-01}",
    startTime: Number(T0 - MIN),
    endTime: Number(T0 + 10n * MIN),
    aggregationInterval: "1h"
  });
  assert(approxEq(res6min.data.series[0].fields.cpu.values[0], 10.0),
    `MIN should be 10, got ${res6min.data.series[0].fields.cpu.values[0]}`);

  const res6max = await axios.post(`${BASE_URL}/query`, {
    query: "max:test_verify(cpu){host:host-01}",
    startTime: Number(T0 - MIN),
    endTime: Number(T0 + 10n * MIN),
    aggregationInterval: "1h"
  });
  assert(approxEq(res6max.data.series[0].fields.cpu.values[0], 50.0),
    `MAX should be 50, got ${res6max.data.series[0].fields.cpu.values[0]}`);

  // -------------------------------------------------------
  // Test 7: Group-by correctness
  // -------------------------------------------------------
  console.log("\nTest 7: Group-by correctness");

  const res7 = await axios.post(`${BASE_URL}/query`, {
    query: "avg:test_verify(cpu){} by {rack}",
    startTime: Number(T0 - MIN),
    endTime: Number(T0 + 10n * MIN),
    aggregationInterval: "1h"
  });

  const series7 = res7.data.series;
  // Should have 2 rack groups
  assert(series7.length === 2, `Expected 2 rack groups, got ${series7.length}`);

  let rack1Avg = null, rack2Avg = null;
  for (const s of series7) {
    const gt = (s.groupTags || []);
    if (gt.includes("rack=rack-1")) rack1Avg = s.fields.cpu.values[0];
    if (gt.includes("rack=rack-2")) rack2Avg = s.fields.cpu.values[0];
  }

  // rack-1: host-01 avg=30, host-02 avg=102. Combined avg = (10+20+30+40+50+100+101+102+103+104)/10 = 66
  assert(rack1Avg !== null, "Should have rack-1 group");
  assert(approxEq(rack1Avg, 66.0), `rack-1 avg should be 66, got ${rack1Avg}`);

  // rack-2: host-03 all 50.0 → avg = 50
  assert(rack2Avg !== null, "Should have rack-2 group");
  assert(approxEq(rack2Avg, 50.0), `rack-2 avg should be 50, got ${rack2Avg}`);

  // -------------------------------------------------------
  // Test 8: Tag scope filtering
  // -------------------------------------------------------
  console.log("\nTest 8: Tag scope filtering");

  const res8 = await axios.post(`${BASE_URL}/query`, {
    query: "avg:test_verify(cpu){rack:rack-2}",
    startTime: Number(T0 - MIN),
    endTime: Number(T0 + 10n * MIN)
  });

  const series8 = res8.data.series;
  assert(series8.length === 1, `Expected 1 series for rack-2, got ${series8.length}`);
  const cpuVals8 = series8[0].fields.cpu.values;
  assert(cpuVals8.length === 3, `Expected 3 values for host-03, got ${cpuVals8.length}`);
  assert(approxEq(cpuVals8[0], 50.0), `All host-03 values should be 50, got ${cpuVals8[0]}`);

  // -------------------------------------------------------
  // Test 9: Time range filtering
  // -------------------------------------------------------
  console.log("\nTest 9: Time range filtering");

  // Query only first 3 minutes
  const res9 = await axios.post(`${BASE_URL}/query`, {
    query: "avg:test_verify(cpu){host:host-01}",
    startTime: Number(T0),
    endTime: Number(T0 + 2n * MIN + 1n) // T0, T0+1min, T0+2min
  });

  const vals9 = res9.data.series[0].fields.cpu.values;
  assert(vals9.length === 3, `Expected 3 values in time range, got ${vals9.length}`);
  assert(approxEq(vals9[0], 10.0), `First value should be 10, got ${vals9[0]}`);
  assert(approxEq(vals9[2], 30.0), `Third value should be 30, got ${vals9[2]}`);

  // -------------------------------------------------------
  // Test 10: Multi-field aggregation with group-by
  // -------------------------------------------------------
  console.log("\nTest 10: Multi-field aggregation with group-by");

  const res10 = await axios.post(`${BASE_URL}/query`, {
    query: "avg:test_verify(cpu,mem){} by {host}",
    startTime: Number(T0 - MIN),
    endTime: Number(T0 + 10n * MIN),
    aggregationInterval: "1h"
  });

  const series10 = res10.data.series;
  assert(series10.length === 3, `Expected 3 host groups, got ${series10.length}`);

  for (const s of series10) {
    const gt = (s.groupTags || []);
    const hostTag = gt.find(t => t.startsWith("host="));
    const hostName = hostTag ? hostTag.split("=")[1] : "unknown";
    assert(s.fields.cpu !== undefined, `${hostName} should have cpu field`);
    assert(s.fields.mem !== undefined, `${hostName} should have mem field`);

    if (hostName === "host-01") {
      assert(approxEq(s.fields.cpu.values[0], 30.0),
        `host-01 avg cpu should be 30, got ${s.fields.cpu.values[0]}`);
      // mem: 20,40,60,80,100 → avg=60
      assert(approxEq(s.fields.mem.values[0], 60.0),
        `host-01 avg mem should be 60, got ${s.fields.mem.values[0]}`);
    }
    if (hostName === "host-03") {
      assert(approxEq(s.fields.cpu.values[0], 50.0),
        `host-03 avg cpu should be 50, got ${s.fields.cpu.values[0]}`);
      assert(approxEq(s.fields.mem.values[0], 75.0),
        `host-03 avg mem should be 75, got ${s.fields.mem.values[0]}`);
    }
  }

  // -------------------------------------------------------
  // Summary
  // -------------------------------------------------------
  console.log("\n\n" + "=".repeat(50));
  console.log(` RESULTS: ${passed} passed, ${failed} failed`);
  console.log("=".repeat(50));

  if (failed > 0) process.exit(1);
}

main().catch(e => {
  console.error("\nERROR:", e.message);
  if (e.response) console.error("Response:", JSON.stringify(e.response.data, null, 2));
  process.exit(1);
});
