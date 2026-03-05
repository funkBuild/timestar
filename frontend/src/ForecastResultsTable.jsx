import { useState, useEffect, useMemo } from 'react';
import './ForecastResultsTable.css';
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

// Build forecast uPlot data. The past series is null from forecastStartIndex onward
// and the forecast series is null before forecastStartIndex, so the two lines
// appear as clearly distinct segments on the same chart.
function buildForecastData(times, forecastStartIndex, pieces) {
  const xs = times.map((ns) => Number(ns) / 1e9);

  const pastRaw = toNullableNumbers(pieces.past?.values || []);
  const forecastRaw = toNullableNumbers(pieces.forecast?.values || []);
  const upperRaw = toNullableNumbers(pieces.upper?.values || []);
  const lowerRaw = toNullableNumbers(pieces.lower?.values || []);

  // Null out the portions that don't belong to each segment
  const pastSeries = pastRaw.map((v, i) => (i < forecastStartIndex ? v : null));
  const forecastSeries = forecastRaw.map((v, i) => (i >= forecastStartIndex ? v : null));

  return [xs, pastSeries, forecastSeries, upperRaw, lowerRaw];
}

function ForecastGroupChart({ times, forecastStartIndex, group }) {
  const { group_tags, pieces } = group;

  const uplotData = useMemo(
    () => buildForecastData(times, forecastStartIndex, pieces),
    [times, forecastStartIndex, pieces]
  );

  // Add a vertical line plugin to mark the forecast boundary
  const forecastLinePlugin = useMemo(() => {
    if (times.length === 0 || forecastStartIndex <= 0 || forecastStartIndex >= times.length) {
      return null;
    }
    const boundarySecond = Number(times[forecastStartIndex]) / 1e9;

    return {
      hooks: {
        draw: [
          (u) => {
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
          },
        ],
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
      <div className="series-header">
        {group_tags.length > 0 && (
          <span className="series-tags">
            {group_tags.map((t) => <span key={t} className="series-tag">{t}</span>)}
          </span>
        )}
        <span style={{ marginLeft: group_tags.length > 0 ? 10 : 0 }}>
          {forecastStartIndex} historical · {times.length - forecastStartIndex} forecast
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

function ForecastGroupTable({ times, forecastStartIndex, group }) {
  const { group_tags, pieces } = group;
  const past = pieces.past?.values || [];
  const forecast = pieces.forecast?.values || [];
  const upper = pieces.upper?.values || [];
  const lower = pieces.lower?.values || [];

  return (
    <div className="series-block">
      <div className="series-header">
        {group_tags.length > 0 && (
          <span className="series-tags">
            {group_tags.map((t) => <span key={t} className="series-tag">{t}</span>)}
          </span>
        )}
        <span style={{ marginLeft: group_tags.length > 0 ? 10 : 0 }}>
          {forecastStartIndex} historical · {times.length - forecastStartIndex} forecast
        </span>
      </div>
      <div className="results-table-wrap">
        <table className="results-table">
          <thead>
            <tr>
              <th>Timestamp</th>
              <th>Past</th>
              <th>Forecast</th>
              <th>Upper</th>
              <th>Lower</th>
            </tr>
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

export default function ForecastResultsTable({ results, loading }) {
  const [view, setView] = useState('table');

  useEffect(() => {
    setView('table');
  }, [results]);

  if (loading) {
    return (
      <div className="loading-spinner">
        <div className="spinner-dot" />
        Running forecast...
      </div>
    );
  }

  if (!results) {
    return <div className="empty-state">Configure the query and run a forecast</div>;
  }

  if (results.status === 'error') {
    return (
      <div className="empty-state" style={{ color: 'var(--danger)' }}>
        {results.error?.message || 'Forecast failed'}
      </div>
    );
  }

  const { times = [], forecast_start_index = 0, series = [], statistics } = results;

  if (times.length === 0 || series.length === 0) {
    return <div className="empty-state">Query returned no results</div>;
  }

  const groups = groupByTags(series);

  return (
    <div className="results-container">
      {statistics && (
        <div className="stats-bar">
          <div>Algorithm: <span>{statistics.algorithm}</span></div>
          {statistics.seasonality && <div>Seasonality: <span>{statistics.seasonality}</span></div>}
          <div>Historical: <span>{statistics.historical_points}</span></div>
          <div>Forecast: <span style={{ color: 'var(--accent)' }}>{statistics.forecast_points}</span></div>
          {statistics.r_squared != null && <div>R²: <span>{statistics.r_squared?.toFixed(4)}</span></div>}
          <div>Time: <span>{statistics.execution_time_ms?.toFixed(1)}ms</span></div>
        </div>
      )}
      <ChartToggle view={view} onChange={setView} />
      {groups.map((group, i) =>
        view === 'chart'
          ? <ForecastGroupChart key={i} times={times} forecastStartIndex={forecast_start_index} group={group} />
          : <ForecastGroupTable key={i} times={times} forecastStartIndex={forecast_start_index} group={group} />
      )}
    </div>
  );
}
