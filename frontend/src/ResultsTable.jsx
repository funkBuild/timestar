import './ResultsTable.css';

function formatTimestamp(ns) {
  return new Date(Number(ns) / 1_000_000).toISOString().replace('T', ' ').replace('Z', '');
}

function formatValue(v) {
  if (v === null || v === undefined) return '-';
  if (typeof v === 'number') {
    return Number.isInteger(v) ? String(v) : v.toPrecision(6);
  }
  return String(v);
}

function columnarToRows(fields) {
  const fieldNames = Object.keys(fields);
  if (fieldNames.length === 0) return { fieldNames: [], rows: [] };

  const tsSet = new Set();
  for (const name of fieldNames) {
    const timestamps = fields[name].timestamps || [];
    for (const ts of timestamps) tsSet.add(String(ts));
  }

  const sortedTs = Array.from(tsSet).sort((a, b) => {
    const diff = BigInt(a) - BigInt(b);
    return diff < 0n ? -1 : diff > 0n ? 1 : 0;
  });

  const tsIndex = {};
  for (const name of fieldNames) {
    const f = fields[name];
    const map = {};
    const timestamps = f.timestamps || [];
    const values = f.values || [];
    for (let i = 0; i < timestamps.length; i++) {
      map[String(timestamps[i])] = values[i];
    }
    tsIndex[name] = map;
  }

  const rows = sortedTs.map((ts) => {
    const row = { _ts: ts };
    for (const name of fieldNames) {
      row[name] = tsIndex[name][ts] ?? null;
    }
    return row;
  });

  return { fieldNames, rows };
}

function SeriesTable({ series }) {
  const { measurement, groupTags, tags, fields } = series;
  const { fieldNames, rows } = columnarToRows(fields || {});

  if (rows.length === 0) {
    return (
      <div className="series-block">
        <div className="series-header">
          <strong>{measurement}</strong>
          {renderTags(groupTags, tags)}
          <span style={{ marginLeft: 10 }}>No data points</span>
        </div>
      </div>
    );
  }

  return (
    <div className="series-block">
      <div className="series-header">
        <strong>{measurement}</strong>
        {renderTags(groupTags, tags)}
        <span style={{ marginLeft: 10 }}>{rows.length} points</span>
      </div>
      <div className="results-table-wrap">
        <table className="results-table">
          <thead>
            <tr>
              <th>Timestamp</th>
              {fieldNames.map((f) => (
                <th key={f}>{f}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr key={row._ts}>
                <td className="ts-col">{formatTimestamp(row._ts)}</td>
                {fieldNames.map((f) => (
                  <td key={f} className={row[f] === null ? 'missing' : ''}>
                    {row[f] === null ? '-' : formatValue(row[f])}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function renderTags(groupTags, tags) {
  const items = [];
  if (groupTags && groupTags.length > 0) {
    for (const gt of groupTags) items.push(gt);
  } else if (tags && typeof tags === 'object') {
    for (const [k, v] of Object.entries(tags)) items.push(`${k}=${v}`);
  }
  if (items.length === 0) return null;
  return (
    <span className="series-tags">
      {items.map((t) => (
        <span key={t} className="series-tag">{t}</span>
      ))}
    </span>
  );
}

export default function ResultsTable({ results, loading }) {
  if (loading) {
    return (
      <div className="loading-spinner">
        <div className="spinner-dot" />
        Running query...
      </div>
    );
  }

  if (!results) {
    return <div className="empty-state">Run a query to see results</div>;
  }

  const { series, statistics } = results;

  if (!series || series.length === 0) {
    return <div className="empty-state">Query returned no results</div>;
  }

  return (
    <div className="results-container">
      {statistics && (
        <div className="stats-bar">
          <div>Series: <span>{statistics.series_count}</span></div>
          <div>Points: <span>{statistics.point_count}</span></div>
          <div>Time: <span>{statistics.execution_time_ms?.toFixed(1)}ms</span></div>
        </div>
      )}
      {series.map((s, i) => (
        <SeriesTable key={i} series={s} />
      ))}
    </div>
  );
}
