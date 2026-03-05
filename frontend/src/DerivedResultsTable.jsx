import { useState, useEffect, useMemo } from 'react';
import './DerivedResultsTable.css';
import ChartToggle from './ChartToggle.jsx';
import UPlotChart from './UPlotChart.jsx';

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
  if (typeof v === 'number') return Number.isInteger(v) ? String(v) : v.toPrecision(6);
  return String(v);
}

export default function DerivedResultsTable({ results, loading }) {
  const [view, setView] = useState('table');

  useEffect(() => {
    setView('table');
  }, [results]);

  const { timestamps = [], values = [], formula, statistics } = results || {};

  const uplotData = useMemo(() => {
    if (!timestamps.length) return null;
    const xs = timestamps.map((ns) => Number(ns) / 1e9);
    const ys = values.map((v) => (v === null || v === undefined ? null : Number(v)));
    return [xs, ys];
  }, [timestamps, values]);

  if (loading) {
    return (
      <div className="loading-spinner">
        <div className="spinner-dot" />
        Running derived query...
      </div>
    );
  }

  if (!results) {
    return <div className="empty-state">Configure sub-queries and a formula, then run</div>;
  }

  if (results.status === 'error') {
    return (
      <div className="empty-state" style={{ color: 'var(--danger)' }}>
        {results.error?.message || 'Query failed'}
      </div>
    );
  }

  if (timestamps.length === 0) {
    return <div className="empty-state">Query returned no results</div>;
  }

  const chartLabel = formula ? (formula.length > 40 ? formula.slice(0, 40) + '...' : formula) : 'value';

  const uplotOpts = {
    height: 260,
    series: [
      {},
      { label: chartLabel, stroke: '#6c7ee0', width: 1.5 },
    ],
    axes: [AXIS_X, AXIS_Y],
    cursor: { drag: { x: true, y: false } },
  };

  return (
    <div className="results-container">
      {statistics && (
        <div className="stats-bar">
          <div>Points: <span>{statistics.point_count}</span></div>
          <div>Sub-queries: <span>{statistics.sub_queries_executed}</span></div>
          {statistics.points_dropped_due_to_alignment > 0 && (
            <div>Dropped: <span style={{ color: 'var(--badge-bool)' }}>{statistics.points_dropped_due_to_alignment}</span></div>
          )}
          <div>Time: <span>{statistics.execution_time_ms?.toFixed(1)}ms</span></div>
        </div>
      )}
      {formula && (
        <div className="derived-formula-bar">
          formula: <code>{formula}</code>
        </div>
      )}
      <ChartToggle view={view} onChange={setView} />
      {view === 'chart' ? (
        <UPlotChart
          data={uplotData}
          opts={uplotOpts}
          style={{ background: '#232540', borderRadius: 4, overflow: 'hidden' }}
        />
      ) : (
        <div className="series-block">
          <div className="results-table-wrap">
            <table className="results-table">
              <thead>
                <tr>
                  <th>Timestamp</th>
                  <th>Value</th>
                </tr>
              </thead>
              <tbody>
                {timestamps.map((ts, i) => (
                  <tr key={ts}>
                    <td className="ts-col">{formatTimestamp(ts)}</td>
                    <td>{formatValue(values[i])}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
}
