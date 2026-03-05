import { useState, useCallback } from 'react';
import useQueryBuilder from './useQueryBuilder';
import './QueryForm.css';

const AGGREGATIONS = ['avg', 'min', 'max', 'sum', 'latest'];
const INTERVAL_UNITS = [
  { value: 'ns', label: 'ns' },
  { value: 'us', label: 'us' },
  { value: 'ms', label: 'ms' },
  { value: 's', label: 's' },
  { value: 'm', label: 'min' },
  { value: 'h', label: 'hr' },
  { value: 'd', label: 'day' },
];

function nsToDatetimeLocal(ns) {
  if (!ns || ns === 0) return '';
  const ms = Number(ns) / 1_000_000;
  const d = new Date(ms);
  const pad = (n) => String(n).padStart(2, '0');
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
}

function datetimeLocalToNs(str) {
  if (!str) return 0;
  return new Date(str).getTime() * 1_000_000;
}

function defaultTimeRange() {
  const now = Date.now() * 1_000_000;
  return { start: now - 24 * 60 * 60 * 1_000_000_000, end: now };
}

function buildForecastFormula(algorithm, deviations, seasonality) {
  if (algorithm === 'seasonal') {
    return `forecast(a, 'seasonal', ${deviations}, seasonality='${seasonality}')`;
  }
  return `forecast(a, '${algorithm}', ${deviations})`;
}

export default function ForecastQueryForm({ measurements, onSubmit, loading }) {
  const qb = useQueryBuilder();
  const [algorithm, setAlgorithm] = useState('linear');
  const [deviations, setDeviations] = useState(2);
  const [seasonality, setSeasonality] = useState('daily');
  const [startTime, setStartTime] = useState(() => defaultTimeRange().start);
  const [endTime, setEndTime] = useState(() => defaultTimeRange().end);
  const [intervalValue, setIntervalValue] = useState('');
  const [intervalUnit, setIntervalUnit] = useState('m');

  const formula = buildForecastFormula(algorithm, deviations, seasonality);
  const canSubmit = !!qb.selectedMeasurement && !!qb.queryString;

  const handleSubmit = useCallback(() => {
    if (!canSubmit) return;
    let aggregationInterval = null;
    if (intervalValue && Number(intervalValue) > 0) {
      aggregationInterval = `${intervalValue}${intervalUnit}`;
    }
    onSubmit({ queries: { a: qb.queryString }, formula, startTime, endTime, aggregationInterval });
  }, [canSubmit, qb.queryString, formula, startTime, endTime, intervalValue, intervalUnit, onSubmit]);

  const fieldEntries = Object.entries(qb.fields);
  const scopeEntries = [];
  for (const [key, values] of Object.entries(qb.tags)) {
    for (const val of values) scopeEntries.push({ key, val, id: `${key}:${val}` });
  }

  return (
    <div className="query-form">
      <div className="form-section">
        <div className="form-section-label">Measurement</div>
        <select value={qb.selectedMeasurement} onChange={(e) => qb.setSelectedMeasurement(e.target.value)}>
          <option value="">Select measurement...</option>
          {measurements.map((m) => <option key={m} value={m}>{m}</option>)}
        </select>
      </div>

      <div className="form-section">
        <div className="form-section-label">Aggregation</div>
        <select value={qb.aggregation} onChange={(e) => qb.setAggregation(e.target.value)}>
          {AGGREGATIONS.map((a) => <option key={a} value={a}>{a}</option>)}
        </select>
      </div>

      <div className="form-section">
        <div className="form-section-label">
          Fields {fieldEntries.length > 0 && `(${qb.selectedFields.length}/${fieldEntries.length})`}
        </div>
        {fieldEntries.length === 0 ? (
          <div style={{ color: 'var(--text-muted)', fontSize: 12 }}>Select a measurement</div>
        ) : (
          <div className="checkbox-list">
            {fieldEntries.map(([name, info]) => (
              <label key={name}>
                <input type="checkbox" checked={qb.selectedFields.includes(name)} onChange={() => qb.toggleField(name)} />
                {name}
                <span className={`type-badge ${info.type || 'float'}`}>{info.type || '?'}</span>
              </label>
            ))}
          </div>
        )}
      </div>

      <div className="form-section">
        <div className="form-section-label">
          Scopes {scopeEntries.length > 0 && `(${qb.selectedScopes.length}/${scopeEntries.length})`}
        </div>
        {scopeEntries.length === 0 ? (
          <div style={{ color: 'var(--text-muted)', fontSize: 12 }}>No tags available</div>
        ) : (
          <div className="checkbox-list">
            {scopeEntries.map(({ key, val, id }) => (
              <label key={id}>
                <input type="checkbox" checked={qb.selectedScopes.includes(id)} onChange={() => qb.toggleScope(id)} />
                <span style={{ color: 'var(--text-muted)' }}>{key}:</span>{val}
              </label>
            ))}
          </div>
        )}
      </div>

      <div className="form-section">
        <div className="form-section-label">Group By</div>
        {qb.tagKeys.length === 0 ? (
          <div style={{ color: 'var(--text-muted)', fontSize: 12 }}>No tag keys available</div>
        ) : (
          <div className="checkbox-list">
            {qb.tagKeys.map((key) => (
              <label key={key}>
                <input type="checkbox" checked={qb.selectedGroupBy.includes(key)} onChange={() => qb.toggleGroupBy(key)} />
                {key}
              </label>
            ))}
          </div>
        )}
      </div>

      <div className="form-section">
        <div className="form-section-label">Algorithm</div>
        <select value={algorithm} onChange={(e) => setAlgorithm(e.target.value)}>
          <option value="linear">linear</option>
          <option value="seasonal">seasonal</option>
        </select>
      </div>

      <div className="form-section">
        <div className="form-section-label">Deviations (σ)</div>
        <input
          type="number" min="1" max="4" step="0.5"
          value={deviations}
          onChange={(e) => setDeviations(Number(e.target.value))}
        />
      </div>

      {algorithm === 'seasonal' && (
        <div className="form-section">
          <div className="form-section-label">Seasonality</div>
          <select value={seasonality} onChange={(e) => setSeasonality(e.target.value)}>
            <option value="none">none</option>
            <option value="hourly">hourly</option>
            <option value="daily">daily</option>
            <option value="weekly">weekly</option>
            <option value="auto">auto</option>
            <option value="multi">multi</option>
          </select>
        </div>
      )}

      <div className="form-section">
        <div className="form-section-label">Time Range</div>
        <div className="time-inputs">
          <input type="datetime-local" step="1" value={nsToDatetimeLocal(startTime)} onChange={(e) => setStartTime(datetimeLocalToNs(e.target.value))} />
          <input type="datetime-local" step="1" value={nsToDatetimeLocal(endTime)} onChange={(e) => setEndTime(datetimeLocalToNs(e.target.value))} />
          <button type="button" className="alltime-btn" onClick={() => { setStartTime(0); setEndTime(Number.MAX_SAFE_INTEGER); }}>All Time</button>
        </div>
      </div>

      <div className="form-section">
        <div className="form-section-label">
          Interval {algorithm === 'seasonal' && <span style={{ color: 'var(--danger)', marginLeft: 4 }}>(required)</span>}
        </div>
        <div className="interval-row">
          <input
            type="number" min="0" step="any" placeholder="e.g. 5"
            value={intervalValue}
            onChange={(e) => setIntervalValue(e.target.value)}
          />
          <select value={intervalUnit} onChange={(e) => setIntervalUnit(e.target.value)}>
            {INTERVAL_UNITS.map((u) => <option key={u.value} value={u.value}>{u.label}</option>)}
          </select>
        </div>
      </div>

      <div className="submit-section">
        <button className="submit-btn" disabled={!canSubmit || loading} onClick={handleSubmit}>
          {loading ? 'Running...' : 'Run Forecast'}
        </button>
        {qb.queryString && <div className="query-preview">{formula}</div>}
      </div>
    </div>
  );
}
