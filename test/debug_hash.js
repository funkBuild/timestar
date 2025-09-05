// Test which shard the series should go to
const crypto = require('crypto');

function hashString(str) {
    // Simulating C++ std::hash<std::string> behavior
    let hash = 0;
    for (let i = 0; i < str.length; i++) {
        const char = str.charCodeAt(i);
        hash = ((hash << 5) - hash + char) & 0xffffffff;
    }
    return Math.abs(hash);
}

// Build series key like the C++ code does
const measurement = "lid_data";
const field = "pnf";
const tags = { meter_id: "33616" };

// Build series key: measurement,tag_key=tag_value,field
let seriesKey = measurement;
for (const [tagKey, tagValue] of Object.entries(tags)) {
    seriesKey += "," + tagKey + "=" + tagValue;
}
seriesKey += "," + field;

console.log('Series key:', seriesKey);

const hash = hashString(seriesKey);
const shardCount = 32;
const targetShard = hash % shardCount;

console.log('Hash:', hash);
console.log('Target shard:', targetShard);
console.log('');

// Test with both fields
const pnfKey = measurement + ",meter_id=33616,pnf";
const pnfStatusKey = measurement + ",meter_id=33616,pnf_status";

console.log('pnf series key:', pnfKey);
console.log('pnf target shard:', hashString(pnfKey) % shardCount);
console.log('');
console.log('pnf_status series key:', pnfStatusKey);
console.log('pnf_status target shard:', hashString(pnfStatusKey) % shardCount);