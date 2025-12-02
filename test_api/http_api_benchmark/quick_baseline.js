const axios = require("axios");
const { performance } = require("perf_hooks");

// Configuration
const HOST = process.env.TSDB_HOST || "localhost";
const PORT = process.env.TSDB_PORT || 8086;
const BASE_URL = `http://${HOST}:${PORT}`;

// Quick baseline configuration - enough data to measure aggregation performance
const MEASUREMENT = "server.metrics";
const NUM_HOSTS = 10;
const FIELDS = ["cpu_usage", "memory_usage", "disk_io_read", "network_in"];

// Reduced dataset: 30 days at 1-minute intervals = 43,200 points per host
const DAYS = 30;
const MINUTES_PER_DAY = 24 * 60;
const TOTAL_MINUTES = DAYS * MINUTES_PER_DAY;
const MINUTE_IN_NS = 60 * 1000000000;
const START_TIME = Date.now() * 1000000 - TOTAL_MINUTES * MINUTE_IN_NS;

console.log("=".repeat(80));
console.log(" QUICK BASELINE BENCHMARK");
console.log("=".repeat(80));
console.log(`Server: ${BASE_URL}`);
console.log(`Measurement: ${MEASUREMENT}`);
console.log(`Hosts: ${NUM_HOSTS}`);
console.log(`Fields: ${FIELDS.length}`);
console.log(`Time range: ${DAYS} days`);
console.log(`Total points to insert: ${NUM_HOSTS * TOTAL_MINUTES} (${NUM_HOSTS} hosts × ${TOTAL_MINUTES} minutes)`);
console.log(`Total data points: ${NUM_HOSTS * TOTAL_MINUTES * FIELDS.length} (${FIELDS.length} fields per point)`);
console.log("=".repeat(80));

async function insertData() {
    console.log("\nInserting data...");
    const BATCH_SIZE = 1000;
    let inserted = 0;
    const totalPoints = NUM_HOSTS * TOTAL_MINUTES;

    for (let hostId = 1; hostId <= NUM_HOSTS; hostId++) {
        for (let minuteOffset = 0; minuteOffset < TOTAL_MINUTES; minuteOffset += BATCH_SIZE) {
            const batch = [];
            const batchEnd = Math.min(minuteOffset + BATCH_SIZE, TOTAL_MINUTES);

            for (let m = minuteOffset; m < batchEnd; m++) {
                const timestamp = START_TIME + m * MINUTE_IN_NS;
                const fields = {};
                FIELDS.forEach(field => {
                    fields[field] = 20 + Math.random() * 60;
                });

                batch.push({
                    measurement: MEASUREMENT,
                    tags: { host: `host-${hostId.toString().padStart(2, "0")}` },
                    fields,
                    timestamp
                });
            }

            try {
                await axios.post(`${BASE_URL}/write`, { writes: batch });
                inserted += batch.length;
                if (inserted % 10000 === 0 || inserted === totalPoints) {
                    process.stdout.write(`\rInserted ${inserted}/${totalPoints} points (${(inserted / totalPoints * 100).toFixed(1)}%)`);
                }
            } catch (error) {
                console.error(`\n❌ Insert failed at ${inserted} points:`, error.message);
                process.exit(1);
            }
        }
    }
    console.log("\n✓ Data insertion complete\n");
}

async function runQuery() {
    const now = Date.now() * 1000000;
    const queryStart = START_TIME;

    const query = {
        query: `avg:${MEASUREMENT}(cpu_usage,memory_usage){} by {host}`,
        startTime: queryStart,
        endTime: now,
        aggregationInterval: "12h" // 12-hour buckets
    };

    console.log("Query Details:");
    console.log(`  Query: ${query.query}`);
    console.log(`  Time Range: ${new Date(queryStart / 1000000).toISOString()} to ${new Date(now / 1000000).toISOString()}`);
    console.log(`  Aggregation Interval: ${query.aggregationInterval}`);
    console.log("\nExecuting query...\n");

    const start = performance.now();

    try {
        const response = await axios.post(`${BASE_URL}/query`, query);
        const end = performance.now();

        const totalTime = (end - start).toFixed(2);
        const stats = response.data.statistics;

        console.log("=".repeat(80));
        console.log(" BASELINE QUERY RESULTS");
        console.log("=".repeat(80));
        console.log(`\n📊 Statistics:`);
        console.log(`  Total Execution Time: ${totalTime} ms`);
        console.log(`  Series Count: ${stats?.series_count || 0}`);
        console.log(`  Points Retrieved: ${stats?.point_count || 0}`);
        console.log(`  Server Execution Time: ${stats?.execution_time_ms?.toFixed(2) || 'N/A'} ms`);

        if (response.data.timing) {
            console.log("\n📈 Server-side Timing Breakdown:");
            const t = response.data.timing;
            console.log(`  Parse Request: ${t.parseRequestMs?.toFixed(2) || 'N/A'} ms`);
            console.log(`  Find Series: ${t.findSeriesMs?.toFixed(2) || 'N/A'} ms`);
            console.log(`  Shard Queries: ${t.shardQueriesMs?.toFixed(2) || 'N/A'} ms`);
            console.log(`  Result Merging: ${t.resultMergingMs?.toFixed(2) || 'N/A'} ms`);
            console.log(`  Aggregation: ${t.aggregationMs?.toFixed(2) || 'N/A'} ms`);
            console.log(`  Total: ${t.totalMs?.toFixed(2) || 'N/A'} ms`);
        }

        console.log("\n" + "=".repeat(80));
        console.log("✓ BASELINE ESTABLISHED");
        console.log("=".repeat(80));

        return response.data;
    } catch (error) {
        console.error("❌ Query failed:", error.message);
        if (error.response) {
            console.error("Response:", JSON.stringify(error.response.data, null, 2));
        }
        process.exit(1);
    }
}

async function main() {
    // Check server health
    try {
        await axios.get(`${BASE_URL}/health`);
        console.log("✓ Server is healthy\n");
    } catch (error) {
        console.error("✗ Server health check failed:", error.message);
        process.exit(1);
    }

    await insertData();
    await runQuery();
}

main().catch(console.error);
