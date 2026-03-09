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
const CHART_STYLE = { background: '#232540', borderRadius: 4, overflow: 'hidden' };

function formatTimestamp(ns) {
  return new Date(Number(ns) / 1_000_000).toISOString().replace('T', ' ').replace('Z', '');
}

function formatValue(v) {
  if (v === null || v === undefined || (typeof v === 'number' && !isFinite(v))) return '-';
  if (typeof v === 'number') return Number.isInteger(v) ? String(v) : v.toPrecision(6);
  return String(v);
}

function toNullableNumbers(arr) {
  return (arr || []).map((v) =>
    v === null || v === undefined || (typeof v === 'number' && !isFinite(v)) ? null : Number(v)
  );
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

function detectResultType(results) {
  if (!results) return 'derived';
  if ('forecast_start_index' in results) return 'forecast';
  if (results.times && results.series) return 'anomaly';
  return 'derived';
}

// --- Group header shared by anomaly and forecast ---

function GroupHeader({ group_tags, children }) {
  return (
    <div className="series-header">
      {group_tags.length > 0 && (
        <span className="series-tags">
          {group_tags.map((t) => <span key={t} className="series-tag">{t}</span>)}
        </span>
      )}
      <span style={{ marginLeft: group_tags.length > 0 ? 10 : 0 }}>{children}</span>
    </div>
  );
}

// --- Plain derived ---

function DerivedChart({ timestamps, values, formula }) {
  const uplotData = useMemo(() => {
    if (!timestamps.length) return null;
    const xs = timestamps.map((ns) => Number(ns) / 1e9);
    const ys = values.map((v) => (v === null || v === undefined ? null : Number(v)));
    return [xs, ys];
  }, [timestamps, values]);

  if (!uplotData) return null;

  const chartLabel = formula ? (formula.length > 40 ? formula.slice(0, 40) + '...' : formula) : 'value';
  const opts = {
    height: 260,
    series: [{}, { label: chartLabel, stroke: '#6c7ee0', width: 1.5 }],
    axes: [AXIS_X, AXIS_Y],
    cursor: { drag: { x: true, y: false } },
  };

  return <UPlotChart data={uplotData} opts={opts} style={CHART_STYLE} />;
}

function DerivedTable({ timestamps, values }) {
  return (
    <div className="series-block">
      <div className="results-table-wrap">
        <table className="results-table">
          <thead>
            <tr><th>Timestamp</th><th>Value</th></tr>
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
  );
}

function DerivedStats({ statistics }) {
  if (!statistics) return null;
  return (
    <div className="stats-bar">
      <div>Points: <span>{statistics.point_count}</span></div>
      <div>Sub-queries: <span>{statistics.sub_queries_executed}</span></div>
      {statistics.points_dropped_due_to_alignment > 0 && (
        <div>Dropped: <span style={{ color: 'var(--badge-bool)' }}>{statistics.points_dropped_due_to_alignment}</span></div>
      )}
      <div>Time: <span>{statistics.execution_time_ms?.toFixed(1)}ms</span></div>
    </div>
  );
}

// --- Anomaly ---

function AnomalyGroupChart({ times, group }) {
  const { pieces } = group;
  const uplotData = useMemo(() => {
    const xs = times.map((ns) => Number(ns) / 1e9);
    return [
      xs,
      toNullableNumbers(pieces.raw?.values),
      toNullableNumbers(pieces.upper?.values),
      toNullableNumbers(pieces.lower?.values),
      toNullableNumbers(pieces.scores?.values),
    ];
  }, [times, pieces]);

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

  const alertValue = pieces.scores?.alert_value ?? 1.0;

  return (
    <div className="series-block">
      <GroupHeader group_tags={group.group_tags}>
        {times.length} points · alert threshold {alertValue}
      </GroupHeader>
      <UPlotChart data={uplotData} opts={opts} style={CHART_STYLE} />
    </div>
  );
}

function AnomalyGroupTable({ times, group }) {
  const { pieces } = group;
  const raw = pieces.raw?.values || [];
  const upper = pieces.upper?.values || [];
  const lower = pieces.lower?.values || [];
  const scores = pieces.scores?.values || [];
  const alertValue = pieces.scores?.alert_value ?? 1.0;

  return (
    <div className="series-block">
      <GroupHeader group_tags={group.group_tags}>{times.length} points</GroupHeader>
      <div className="results-table-wrap">
        <table className="results-table">
          <thead>
            <tr><th>Timestamp</th><th>Raw</th><th>Upper</th><th>Lower</th><th>Score</th></tr>
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

function AnomalyStats({ statistics }) {
  if (!statistics) return null;
  return (
    <div className="stats-bar">
      <div>Algorithm: <span>{statistics.algorithm}</span></div>
      <div>Bounds: <span>{statistics.bounds}σ</span></div>
      <div>Seasonality: <span>{statistics.seasonality}</span></div>
      <div>Anomalies: <span style={{ color: statistics.anomaly_count > 0 ? 'var(--danger)' : 'var(--success)' }}>{statistics.anomaly_count}</span></div>
      <div>Points: <span>{statistics.total_points}</span></div>
      <div>Time: <span>{statistics.execution_time_ms?.toFixed(1)}ms</span></div>
    </div>
  );
}

// --- Forecast ---

function ForecastGroupChart({ times, forecastStartIndex, group }) {
  const { pieces } = group;

  const uplotData = useMemo(() => {
    const xs = times.map((ns) => Number(ns) / 1e9);
    const pastRaw = toNullableNumbers(pieces.past?.values);
    const forecastRaw = toNullableNumbers(pieces.forecast?.values);
    return [
      xs,
      pastRaw.map((v, i) => (i < forecastStartIndex ? v : null)),
      forecastRaw.map((v, i) => (i >= forecastStartIndex ? v : null)),
      toNullableNumbers(pieces.upper?.values),
      toNullableNumbers(pieces.lower?.values),
    ];
  }, [times, forecastStartIndex, pieces]);

  const forecastLinePlugin = useMemo(() => {
    if (times.length === 0 || forecastStartIndex <= 0 || forecastStartIndex >= times.length) return null;
    const boundarySecond = Number(times[forecastStartIndex]) / 1e9;
    return {
      hooks: {
        draw: [(u) => {
          const cx = u.valToPos(boundarySecond, 'x');
          if (cx == null || isNaN(cx)) return;
          const ctx = u.ctx;
          ctx.save();
          ctx.strokeStyle = '#8888aa';
          ctx.lineWidth = 1;
          ctx.setLineDash([5, 5]);
          ctx.beginPath();
          ctx.moveTo(cx + u.bbox.left, u.bbox.top);
          ctx.lineTo(cx + u.bbox.left, u.bbox.top + u.bbox.height);
          ctx.stroke();
          ctx.restore();
        }],
      },
    };
  }, [times, forecastStartIndex]);

  const opts = useMemo(() => ({
    height: 280,
    series: [
      {},
      { label: 'past',     stroke: '#6c7ee0', width: 1.5 },
      { label: 'forecast', stroke: '#44cc88', width: 1.5, dash: [6, 3] },
      { label: 'upper',    stroke: '#8888aa', width: 1,   dash: [3, 5] },
      { label: 'lower',    stroke: '#8888aa', width: 1,   dash: [3, 5] },
    ],
    axes: [AXIS_X, AXIS_Y],
    cursor: { drag: { x: true, y: false } },
    plugins: forecastLinePlugin ? [forecastLinePlugin] : [],
  }), [forecastLinePlugin]);

  return (
    <div className="series-block">
      <GroupHeader group_tags={group.group_tags}>
        {forecastStartIndex} historical · {times.length - forecastStartIndex} forecast
      </GroupHeader>
      <UPlotChart data={uplotData} opts={opts} style={CHART_STYLE} />
    </div>
  );
}

function ForecastGroupTable({ times, forecastStartIndex, group }) {
  const { pieces } = group;
  const past = pieces.past?.values || [];
  const forecast = pieces.forecast?.values || [];
  const upper = pieces.upper?.values || [];
  const lower = pieces.lower?.values || [];

  return (
    <div className="series-block">
      <GroupHeader group_tags={group.group_tags}>
        {forecastStartIndex} historical · {times.length - forecastStartIndex} forecast
      </GroupHeader>
      <div className="results-table-wrap">
        <table className="results-table">
          <thead>
            <tr><th>Timestamp</th><th>Past</th><th>Forecast</th><th>Upper</th><th>Lower</th></tr>
          </thead>
          <tbody>
            {times.map((ts, i) => {
              const isForecast = i >= forecastStartIndex;
              return (
                <tr key={ts} className={isForecast ? 'forecast-row' : ''}>
                  <td className="ts-col">{formatTimestamp(ts)}</td>
                  <td>{formatValue(past[i])}</td>
                  <td className={isForecast ? 'forecast-val' : ''}>{formatValue(forecast[i])}</td>
                  <td>{formatValue(upper[i])}</td>
                  <td>{formatValue(lower[i])}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function ForecastStats({ statistics }) {
  if (!statistics) return null;
  return (
    <div className="stats-bar">
      <div>Algorithm: <span>{statistics.algorithm}</span></div>
      {statistics.seasonality && <div>Seasonality: <span>{statistics.seasonality}</span></div>}
      <div>Historical: <span>{statistics.historical_points}</span></div>
      <div>Forecast: <span style={{ color: 'var(--accent)' }}>{statistics.forecast_points}</span></div>
      {statistics.r_squared != null && <div>R²: <span>{statistics.r_squared?.toFixed(4)}</span></div>}
      <div>Time: <span>{statistics.execution_time_ms?.toFixed(1)}ms</span></div>
    </div>
  );
}

// --- Unified component ---

export default function DerivedResultsTable({ results, loading }) {
  const [view, setView] = useState('table');

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
    return <div className="empty-state">Configure a query, then run</div>;
  }

  if (results.status === 'error') {
    return (
      <div className="empty-state" style={{ color: 'var(--danger)' }}>
        {results.error?.message || 'Query failed'}
      </div>
    );
  }

  const resultType = detectResultType(results);

  // --- Anomaly rendering ---
  if (resultType === 'anomaly') {
    const { times = [], series = [], statistics } = results;
    if (times.length === 0 || series.length === 0) {
      return <div className="empty-state">Query returned no results</div>;
    }
    const groups = groupByTags(series);
    return (
      <div className="results-container">
        <AnomalyStats statistics={statistics} />
        <ChartToggle view={view} onChange={setView} />
        {groups.map((group, i) =>
          view === 'chart'
            ? <AnomalyGroupChart key={i} times={times} group={group} />
            : <AnomalyGroupTable key={i} times={times} group={group} />
        )}
      </div>
    );
  }

  // --- Forecast rendering ---
  if (resultType === 'forecast') {
    const { times = [], forecast_start_index = 0, series = [], statistics } = results;
    if (times.length === 0 || series.length === 0) {
      return <div className="empty-state">Query returned no results</div>;
    }
    const groups = groupByTags(series);
    return (
      <div className="results-container">
        <ForecastStats statistics={statistics} />
        <ChartToggle view={view} onChange={setView} />
        {groups.map((group, i) =>
          view === 'chart'
            ? <ForecastGroupChart key={i} times={times} forecastStartIndex={forecast_start_index} group={group} />
            : <ForecastGroupTable key={i} times={times} forecastStartIndex={forecast_start_index} group={group} />
        )}
      </div>
    );
  }

  // --- Plain derived rendering ---
  const { timestamps = [], values = [], formula, statistics } = results;
  if (timestamps.length === 0) {
    return <div className="empty-state">Query returned no results</div>;
  }

  return (
    <div className="results-container">
      <DerivedStats statistics={statistics} />
      {formula && (
        <div className="derived-formula-bar">
          formula: <code>{formula}</code>
        </div>
      )}
      <ChartToggle view={view} onChange={setView} />
      {view === 'chart'
        ? <DerivedChart timestamps={timestamps} values={values} formula={formula} />
        : <DerivedTable timestamps={timestamps} values={values} />
      }
    </div>
  );
}
