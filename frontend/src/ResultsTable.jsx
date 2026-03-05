import { useState, useEffect, useMemo } from 'react';
import './ResultsTable.css';
import ChartToggle from './ChartToggle.jsx';
import UPlotChart from './UPlotChart.jsx';

// Color palette cycling for multiple series/fields
const STROKE_COLORS = [
  '#6c7ee0', '#44cc88', '#e05555', '#cc8844', '#4488cc',
  '#cc44aa', '#44cccc', '#aacc44', '#ee9944', '#aa44ee',
];

// Dark-theme uPlot axis config
const AXIS_X = {
  stroke: '#8888aa',
  grid: { stroke: '#3a3c5c', width: 1 },
  ticks: { stroke: '#3a3c5c', width: 1 },
};
const AXIS_Y = {
  stroke: '#8888aa',
  grid: { stroke: '#3a3c5c', width: 1 },
  ticks: { stroke: '#3a3c5c', width: 1 },
};

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

function buildSeriesChartData(fields) {
  const fieldNames = Object.keys(fields);
  if (fieldNames.length === 0) return null;

  // Collect all unique timestamps from the first field with timestamps
  // (assume all fields share the same timestamps for a given series)
  const firstField = fields[fieldNames[0]];
  const timestamps = firstField.timestamps || [];
  if (timestamps.length === 0) return null;

  // Convert ns timestamps to seconds for uPlot
  const xs = timestamps.map((ns) => Number(ns) / 1e9);

  // Build value arrays per field; align by index (same-length arrays)
  const ys = fieldNames.map((name) => {
    const vals = fields[name].values || [];
    return vals.map((v) => (v === null || v === undefined ? null : Number(v)));
  });

  return { xs, ys, fieldNames };
}

function SeriesChart({ series }) {
  const { measurement, groupTags, tags, fields } = series;

  const chartData = useMemo(() => buildSeriesChartData(fields || {}), [fields]);

  if (!chartData) {
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

  const { xs, ys, fieldNames } = chartData;

  const uplotData = [xs, ...ys];

  const seriesOpts = [
    {},
    ...fieldNames.map((name, idx) => ({
      label: name,
      stroke: STROKE_COLORS[idx % STROKE_COLORS.length],
      width: 1.5,
    })),
  ];

  const opts = {
    height: 260,
    series: seriesOpts,
    axes: [AXIS_X, AXIS_Y],
    cursor: { drag: { x: true, y: false } },
  };

  const label = [measurement, ...(groupTags || Object.entries(tags || {}).map(([k, v]) => `${k}=${v}`))].join(' ');

  return (
    <div className="series-block">
      <div className="series-header">
        <strong>{measurement}</strong>
        {renderTags(groupTags, tags)}
        <span style={{ marginLeft: 10 }}>{xs.length} points</span>
      </div>
      <UPlotChart
        data={uplotData}
        opts={opts}
        style={{ background: '#232540', borderRadius: 4, overflow: 'hidden' }}
      />
    </div>
  );
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

export default function ResultsTable({ results, loading }) {
  const [view, setView] = useState('table');

  // Reset to table view when new results arrive
  useEffect(() => {
    setView('table');
  }, [results]);

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
      <ChartToggle view={view} onChange={setView} />
      {view === 'table'
        ? series.map((s, i) => <SeriesTable key={i} series={s} />)
        : series.map((s, i) => <SeriesChart key={i} series={s} />)
      }
    </div>
  );
}
