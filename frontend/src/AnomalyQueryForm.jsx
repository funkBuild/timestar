import { useState, useCallback } from 'react';
import useQueryBuilder from './useQueryBuilder';
import './QueryForm.css';

const AGGREGATIONS = ['avg', 'min', 'max', 'sum', 'latest'];

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

export default function AnomalyQueryForm({ measurements, onSubmit, loading }) {
  const qb = useQueryBuilder();
  const [algorithm, setAlgorithm] = useState('basic');
  const [bounds, setBounds] = useState(2);
  const [seasonality, setSeasonality] = useState('none');
  const [startTime, setStartTime] = useState(() => defaultTimeRange().start);
  const [endTime, setEndTime] = useState(() => defaultTimeRange().end);

  const formula = `anomalies(a, '${algorithm}', ${bounds}, '${seasonality}')`;
  const canSubmit = !!qb.selectedMeasurement && !!qb.queryString;

  const handleSubmit = useCallback(() => {
    if (!canSubmit) return;
    onSubmit({ queries: { a: qb.queryString }, formula, startTime, endTime, aggregationInterval: null });
  }, [canSubmit, qb.queryString, formula, startTime, endTime, onSubmit]);

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
          <option value="basic">basic</option>
          <option value="agile">agile</option>
          <option value="robust">robust</option>
        </select>
      </div>

      <div className="form-section">
        <div className="form-section-label">Bounds (σ)</div>
        <input
          type="number" min="1" max="4" step="0.5"
          value={bounds}
          onChange={(e) => setBounds(Number(e.target.value))}
        />
      </div>

      <div className="form-section">
        <div className="form-section-label">Seasonality</div>
        <select value={seasonality} onChange={(e) => setSeasonality(e.target.value)}>
          <option value="none">none</option>
          <option value="hourly">hourly</option>
          <option value="daily">daily</option>
          <option value="weekly">weekly</option>
        </select>
      </div>

      <div className="form-section">
        <div className="form-section-label">Time Range</div>
        <div className="time-inputs">
          <input type="datetime-local" step="1" value={nsToDatetimeLocal(startTime)} onChange={(e) => setStartTime(datetimeLocalToNs(e.target.value))} />
          <input type="datetime-local" step="1" value={nsToDatetimeLocal(endTime)} onChange={(e) => setEndTime(datetimeLocalToNs(e.target.value))} />
          <button type="button" className="alltime-btn" onClick={() => { setStartTime(0); setEndTime(Number.MAX_SAFE_INTEGER); }}>All Time</button>
        </div>
      </div>

      <div className="submit-section">
        <button className="submit-btn" disabled={!canSubmit || loading} onClick={handleSubmit}>
          {loading ? 'Running...' : 'Detect Anomalies'}
        </button>
        {qb.queryString && <div className="query-preview">{formula}</div>}
      </div>
    </div>
  );
}
