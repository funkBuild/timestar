const BASE = '/api';

const TYPE_DISPLAY = { boolean: 'bool', integer: 'int' };
function normalizeFieldType(t) { return TYPE_DISPLAY[t] || t; }

export async function fetchMeasurements() {
  const res = await fetch(`${BASE}/measurements`);
  if (!res.ok) throw new Error(`Failed to fetch measurements: ${res.status}`);
  const data = await res.json();
  return data.measurements || [];
}

export async function fetchFields(measurement) {
  const res = await fetch(`${BASE}/fields?measurement=${encodeURIComponent(measurement)}`);
  if (!res.ok) throw new Error(`Failed to fetch fields: ${res.status}`);
  const data = await res.json();
  const fields = data.fields || {};
  for (const key of Object.keys(fields)) {
    const f = fields[key];
    if (f && f.type) f.type = normalizeFieldType(f.type);
  }
  return fields;
}

export async function fetchTags(measurement) {
  const res = await fetch(`${BASE}/tags?measurement=${encodeURIComponent(measurement)}`);
  if (!res.ok) throw new Error(`Failed to fetch tags: ${res.status}`);
  const data = await res.json();
  return data.tags || {};
}

export async function runQuery({ query, startTime, endTime, aggregationInterval }) {
  const body = { query, startTime, endTime };
  if (aggregationInterval) body.aggregationInterval = aggregationInterval;
  const res = await fetch(`${BASE}/query`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({}));
    throw new Error(err.error?.message || err.message || `Query failed: ${res.status}`);
  }
  return res.json();
}

export async function runDerivedQuery({ queries, formula, startTime, endTime, aggregationInterval }) {
  const body = { queries, formula, startTime, endTime };
  if (aggregationInterval) body.aggregationInterval = aggregationInterval;
  const res = await fetch(`${BASE}/derived`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({}));
    throw new Error(err.error?.message || err.message || `Query failed: ${res.status}`);
  }
  return res.json();
}
