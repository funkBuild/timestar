import { useState, useEffect, useMemo } from 'react';
import './AnomalyResultsTable.css';
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
  if (v === null || v === undefined || (typeof v === 'number' && !isFinite(v))) return '-';
  if (typeof v === 'number') return v.toPrecision(6);
  return String(v);
}

function groupByTags(series) {
  const groups = new Map();
  for (const s of series) {
    const key = (s.group_tags || []).join(',');
    if (!groups.has(key)) groups.set(key, { group_tags: s.group_tags || [], pieces: {} });
    groups.get(key).pieces[s.piece] = s;
  }
  return Array.from(groups.values());
}

function toNullableNumbers(arr) {
  return (arr || []).map((v) =>
    v === null || v === undefined || (typeof v === 'number' && !isFinite(v)) ? null : Number(v)
  );
}

function AnomalyGroupChart({ times, group }) {
  const { group_tags, pieces } = group;
  const raw = pieces.raw?.values || [];
  const upper = pieces.upper?.values || [];
  const lower = pieces.lower?.values || [];
  const scores = pieces.scores?.values || [];
  const alertValue = pieces.scores?.alert_value ?? 1.0;

  const uplotData = useMemo(() => {
    const xs = times.map((ns) => Number(ns) / 1e9);
    return [
      xs,
      toNullableNumbers(raw),
      toNullableNumbers(upper),
      toNullableNumbers(lower),
      toNullableNumbers(scores),
    ];
  }, [times, raw, upper, lower, scores]);

  // Score series gets a dynamic stroke: red when anomalous, otherwise muted gold.
  // uPlot supports a stroke function for per-point coloring via paths, but the
  // simplest approach is a fixed color with the anomaly highlighted by the table.
  // Here we use a solid red line for score so anomalies are visually prominent.
  const opts = {
    height: 280,
    series: [
      {},
      { label: 'raw',   stroke: '#6c7ee0', width: 1.5 },
      { label: 'upper', stroke: '#44cc88', width: 1, dash: [4, 4] },
      { label: 'lower', stroke: '#cc8844', width: 1, dash: [4, 4] },
      { label: 'score', stroke: '#e05555', width: 1 },
    ],
    axes: [AXIS_X, AXIS_Y],
    cursor: { drag: { x: true, y: false } },
  };

  return (
    <div className="series-block">
      <div className="series-header">
        {group_tags.length > 0 && (
          <span className="series-tags">
            {group_tags.map((t) => <span key={t} className="series-tag">{t}</span>)}
          </span>
        )}
        <span style={{ marginLeft: group_tags.length > 0 ? 10 : 0 }}>
          {times.length} points · alert threshold {alertValue}
        </span>
      </div>
      <UPlotChart
        data={uplotData}
        opts={opts}
        style={{ background: '#232540', borderRadius: 4, overflow: 'hidden' }}
      />
    </div>
  );
}

function AnomalyGroupTable({ times, group }) {
  const { group_tags, pieces } = group;
  const raw = pieces.raw?.values || [];
  const upper = pieces.upper?.values || [];
  const lower = pieces.lower?.values || [];
  const scores = pieces.scores?.values || [];
  const alertValue = pieces.scores?.alert_value ?? 1.0;

  return (
    <div className="series-block">
      <div className="series-header">
        {group_tags.length > 0 && (
          <span className="series-tags">
            {group_tags.map((t) => <span key={t} className="series-tag">{t}</span>)}
          </span>
        )}
        <span style={{ marginLeft: group_tags.length > 0 ? 10 : 0 }}>{times.length} points</span>
      </div>
      <div className="results-table-wrap">
        <table className="results-table">
          <thead>
            <tr>
              <th>Timestamp</th>
              <th>Raw</th>
              <th>Upper</th>
              <th>Lower</th>
              <th>Score</th>
            </tr>
          </thead>
          <tbody>
            {times.map((ts, i) => {
              const score = scores[i];
              const isAnomaly = typeof score === 'number' && isFinite(score) && score > alertValue;
              return (
                <tr key={ts} className={isAnomaly ? 'anomaly-row' : ''}>
                  <td className="ts-col">{formatTimestamp(ts)}</td>
                  <td>{formatValue(raw[i])}</td>
                  <td>{formatValue(upper[i])}</td>
                  <td>{formatValue(lower[i])}</td>
                  <td className={isAnomaly ? 'anomaly-score' : ''}>{formatValue(score)}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}

export default function AnomalyResultsTable({ results, loading }) {
  const [view, setView] = useState('table');

  useEffect(() => {
    setView('table');
  }, [results]);

  if (loading) {
    return (
      <div className="loading-spinner">
        <div className="spinner-dot" />
        Detecting anomalies...
      </div>
    );
  }

  if (!results) {
    return <div className="empty-state">Configure the query and run anomaly detection</div>;
  }

  if (results.status === 'error') {
    return (
      <div className="empty-state" style={{ color: 'var(--danger)' }}>
        {results.error?.message || 'Anomaly detection failed'}
      </div>
    );
  }

  const { times = [], series = [], statistics } = results;

  if (times.length === 0 || series.length === 0) {
    return <div className="empty-state">Query returned no results</div>;
  }

  const groups = groupByTags(series);

  return (
    <div className="results-container">
      {statistics && (
        <div className="stats-bar">
          <div>Algorithm: <span>{statistics.algorithm}</span></div>
          <div>Bounds: <span>{statistics.bounds}σ</span></div>
          <div>Seasonality: <span>{statistics.seasonality}</span></div>
          <div>Anomalies: <span style={{ color: statistics.anomaly_count > 0 ? 'var(--danger)' : 'var(--success)' }}>{statistics.anomaly_count}</span></div>
          <div>Points: <span>{statistics.total_points}</span></div>
          <div>Time: <span>{statistics.execution_time_ms?.toFixed(1)}ms</span></div>
        </div>
      )}
      <ChartToggle view={view} onChange={setView} />
      {groups.map((group, i) =>
        view === 'chart'
          ? <AnomalyGroupChart key={i} times={times} group={group} />
          : <AnomalyGroupTable key={i} times={times} group={group} />
      )}
    </div>
  );
}
