const axios = require("axios");
const { performance } = require("perf_hooks");

// Configuration
const HOST = process.env.TSDB_HOST || "localhost";
const PORT = process.env.TSDB_PORT || 8086;
const BASE_URL = `http://${HOST}:${PORT}`;

// Benchmark configuration
const MEASUREMENT = "server.metrics";
const MINUTE_IN_NS = 60 * 1000000000; // 60 seconds in nanoseconds
const MINUTES_PER_YEAR = 365 * 24 * 60; // 525,600 minutes

// Query-only benchmark
async function runSingleHostQuery() {
  console.log("=".repeat(80));
  console.log(" QUERY-ONLY BENCHMARK - Single Host Query");
  console.log("=".repeat(80));
  console.log(`Server: ${BASE_URL}`);
  console.log(`Measurement: ${MEASUREMENT}`);
  console.log("=".repeat(80));

  // Check server health
  try {
    await axios.get(`${BASE_URL}/health`);
    console.log("✓ Server is healthy\n");
  } catch (error) {
    console.error("✗ Server health check failed:", error.message);
    process.exit(1);
  }

  const now = Date.now() * 1000000;
  const yearAgo = now - MINUTES_PER_YEAR * MINUTE_IN_NS;

  // Single host query - host-01
  const hostName = "host-01";

  const query = {
    query: `avg:${MEASUREMENT}(cpu_usage,memory_usage){host:${hostName}}`,
    startTime: yearAgo,
    endTime: now,
    aggregationInterval: "12h", // 12-hour buckets for year of data
  };

  console.log("Query Details:");
  console.log(`  Host: ${hostName}`);
  console.log(`  Query: ${query.query}`);
  console.log(`  Time Range: ${new Date(yearAgo / 1000000).toISOString()} to ${new Date(now / 1000000).toISOString()}`);
  console.log(`  Aggregation Interval: ${query.aggregationInterval}`);
  console.log("\nExecuting query...\n");

  const start = performance.now();

  try {
    const response = await axios.post(`${BASE_URL}/query`, query);
    const end = performance.now();

    const totalTime = (end - start).toFixed(2);
    const stats = response.data.statistics;
    const series = response.data.series;

    console.log("=".repeat(80));
    console.log(" QUERY RESULTS");
    console.log("=".repeat(80));

    console.log(`\n📊 Statistics:`);
    console.log(`  Total Execution Time: ${totalTime} ms`);
    console.log(`  Series Count: ${stats?.series_count || 0}`);
    console.log(`  Points Retrieved: ${stats?.point_count || 0}`);
    console.log(`  Server Execution Time: ${stats?.execution_time_ms?.toFixed(2) || 'N/A'} ms`);

    if (stats?.execution_time_ms) {
      console.log(`\n📈 Performance Breakdown:`);
      console.log(`  Network + Client Overhead: ${(totalTime - stats.execution_time_ms).toFixed(2)} ms`);
      console.log(`  Server Processing: ${stats.execution_time_ms.toFixed(2)} ms`);
    }

    console.log(`\n📋 Series Details:`);
    if (series && series.length > 0) {
      series.forEach((s, idx) => {
        console.log(`\n  Series ${idx + 1}:`);
        console.log(`    Measurement: ${s.measurement}`);
        console.log(`    Tags: ${JSON.stringify(s.tags)}`);
        console.log(`    Fields: ${Object.keys(s.fields).join(', ')}`);

        Object.keys(s.fields).forEach(field => {
          const fieldData = s.fields[field];
          console.log(`\n    Field: ${field}`);
          console.log(`      Data points: ${fieldData.timestamps?.length || 0}`);
          if (fieldData.timestamps && fieldData.timestamps.length > 0) {
            console.log(`      First timestamp: ${new Date(fieldData.timestamps[0] / 1000000).toISOString()}`);
            console.log(`      Last timestamp: ${new Date(fieldData.timestamps[fieldData.timestamps.length - 1] / 1000000).toISOString()}`);
            console.log(`      First value: ${fieldData.values[0]}`);
            console.log(`      Last value: ${fieldData.values[fieldData.values.length - 1]}`);
          }
        });
      });
    }

    console.log("\n" + "=".repeat(80));

    // If we have detailed timing info from server, show it
    if (response.data.timing) {
      console.log("\n📊 Server-side Timing Breakdown:");
      Object.entries(response.data.timing).forEach(([key, value]) => {
        console.log(`  ${key}: ${value}`);
      });
      console.log("=".repeat(80));
    }

  } catch (error) {
    console.error("❌ Query failed:", error.message);
    if (error.response) {
      console.error("Response status:", error.response.status);
      console.error("Response data:", JSON.stringify(error.response.data, null, 2));
    }
    process.exit(1);
  }
}

// Run the query
runSingleHostQuery().catch(console.error);
