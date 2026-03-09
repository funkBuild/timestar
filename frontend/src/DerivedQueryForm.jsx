import { useState, useCallback, useEffect, useRef } from 'react';
import useQueryBuilder from './useQueryBuilder';
import './QueryForm.css';
import './DerivedQueryForm.css';

const AGGREGATIONS = ['avg', 'min', 'max', 'sum', 'latest', 'count', 'first', 'median', 'stddev', 'stdvar', 'spread'];
const INTERVAL_UNITS = [
  { value: 'ns', label: 'ns' },
  { value: 'us', label: 'us' },
  { value: 'ms', label: 'ms' },
  { value: 's', label: 's' },
  { value: 'm', label: 'min' },
  { value: 'h', label: 'hr' },
  { value: 'd', label: 'day' },
];
const ALL_LABELS = 'abcdefgh'.split('');

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

function SubQueryRow({ label, measurements, onQueryChange, onRemove, canRemove }) {
  const qb = useQueryBuilder();

  useEffect(() => {
    onQueryChange(label, qb.queryString);
  }, [label, qb.queryString, onQueryChange]);

  const fieldEntries = Object.entries(qb.fields);
  const scopeEntries = [];
  for (const [key, values] of Object.entries(qb.tags)) {
    for (const val of values) scopeEntries.push({ key, val, id: `${key}:${val}` });
  }

  return (
    <div className="sub-query-row">
      <div className="sub-query-header">
        <span className="sub-query-label">{label}</span>
        {canRemove && (
          <button type="button" className="sub-query-remove" onClick={() => onRemove(label)} title="Remove">×</button>
        )}
      </div>

      <div className="sub-query-body">
        <div className="sub-form-row">
          <select value={qb.selectedMeasurement} onChange={(e) => qb.setSelectedMeasurement(e.target.value)} style={{ flex: 1 }}>
            <option value="">Measurement...</option>
            {measurements.map((m) => <option key={m} value={m}>{m}</option>)}
          </select>
          <select value={qb.aggregation} onChange={(e) => qb.setAggregation(e.target.value)} style={{ width: 70 }}>
            {AGGREGATIONS.map((a) => <option key={a} value={a}>{a}</option>)}
          </select>
        </div>

        {fieldEntries.length > 0 && (
          <div className="sub-form-section-label">Fields</div>
        )}
        {fieldEntries.length > 0 && (
          <div className="checkbox-list sub-checkbox-list">
            {fieldEntries.map(([name, info]) => (
              <label key={name}>
                <input type="checkbox" checked={qb.selectedFields.includes(name)} onChange={() => qb.toggleField(name)} />
                {name}
                <span className={`type-badge ${info.type || 'float'}`}>{info.type || '?'}</span>
              </label>
            ))}
          </div>
        )}

        {scopeEntries.length > 0 && (
          <div className="sub-form-section-label">Scopes</div>
        )}
        {scopeEntries.length > 0 && (
          <div className="checkbox-list sub-checkbox-list">
            {scopeEntries.map(({ key, val, id }) => (
              <label key={id}>
                <input type="checkbox" checked={qb.selectedScopes.includes(id)} onChange={() => qb.toggleScope(id)} />
                <span style={{ color: 'var(--text-muted)' }}>{key}:</span>{val}
              </label>
            ))}
          </div>
        )}

        {qb.tagKeys.length > 0 && (
          <div className="sub-form-section-label">Group By</div>
        )}
        {qb.tagKeys.length > 0 && (
          <div className="checkbox-list sub-checkbox-list">
            {qb.tagKeys.map((key) => (
              <label key={key}>
                <input type="checkbox" checked={qb.selectedGroupBy.includes(key)} onChange={() => qb.toggleGroupBy(key)} />
                {key}
              </label>
            ))}
          </div>
        )}

        {qb.queryString && (
          <div className="sub-query-preview">{qb.queryString}</div>
        )}
      </div>
    </div>
  );
}

export default function DerivedQueryForm({ measurements, onSubmit, loading }) {
  const [subQueries, setSubQueries] = useState([{ label: 'a', key: 0 }, { label: 'b', key: 1 }]);
  const [subQueryStrings, setSubQueryStrings] = useState({ a: '', b: '' });
  const [formula, setFormula] = useState('(a + b) / 2');
  const [startTime, setStartTime] = useState(() => defaultTimeRange().start);
  const [endTime, setEndTime] = useState(() => defaultTimeRange().end);
  const [intervalValue, setIntervalValue] = useState('');
  const [intervalUnit, setIntervalUnit] = useState('m');
  const nextKey = useRef(2);

  const handleQueryChange = useCallback((label, queryStr) => {
    setSubQueryStrings((prev) => ({ ...prev, [label]: queryStr }));
  }, []);

  const addSubQuery = useCallback(() => {
    const usedLabels = subQueries.map((sq) => sq.label);
    const nextLabel = ALL_LABELS.find((l) => !usedLabels.includes(l));
    if (!nextLabel) return;
    const key = nextKey.current++;
    setSubQueries((prev) => [...prev, { label: nextLabel, key }]);
    setSubQueryStrings((prev) => ({ ...prev, [nextLabel]: '' }));
  }, [subQueries]);

  const removeSubQuery = useCallback((label) => {
    setSubQueries((prev) => prev.filter((sq) => sq.label !== label));
    setSubQueryStrings((prev) => {
      const next = { ...prev };
      delete next[label];
      return next;
    });
  }, []);

  const queries = Object.fromEntries(
    Object.entries(subQueryStrings).filter(([, v]) => v)
  );
  const canSubmit = Object.keys(queries).length > 0 && formula.trim();

  const handleSubmit = useCallback(() => {
    if (!canSubmit) return;
    let aggregationInterval = null;
    if (intervalValue && Number(intervalValue) > 0) {
      aggregationInterval = `${intervalValue}${intervalUnit}`;
    }
    onSubmit({ queries, formula: formula.trim(), startTime, endTime, aggregationInterval });
  }, [canSubmit, queries, formula, startTime, endTime, intervalValue, intervalUnit, onSubmit]);

  const previewBody = canSubmit
    ? JSON.stringify({ queries, formula: formula.trim() }, null, 2)
    : null;

  return (
    <div className="query-form">
      <div className="form-section">
        <div className="form-section-label">Sub-queries</div>
        <div className="sub-queries-list">
          {subQueries.map((sq) => (
            <SubQueryRow
              key={sq.key}
              label={sq.label}
              measurements={measurements}
              onQueryChange={handleQueryChange}
              onRemove={removeSubQuery}
              canRemove={subQueries.length > 1}
            />
          ))}
        </div>
        {subQueries.length < ALL_LABELS.length && (
          <button type="button" className="add-subquery-btn" onClick={addSubQuery}>
            + Add sub-query
          </button>
        )}
      </div>

      <div className="form-section">
        <div className="form-section-label">Formula</div>
        <input
          type="text"
          value={formula}
          onChange={(e) => setFormula(e.target.value)}
          placeholder="e.g. (a + b) / 2"
          style={{ width: '100%' }}
        />
        <div style={{ fontSize: 11, color: 'var(--text-muted)', marginTop: 4 }}>
          Available: {subQueries.map((sq) => sq.label).join(', ')}
        </div>
      </div>

      <div className="form-section">
        <div className="form-section-label">Time Range</div>
        <div className="time-inputs">
          <input type="datetime-local" step="1" value={nsToDatetimeLocal(startTime)} onChange={(e) => setStartTime(datetimeLocalToNs(e.target.value))} />
          <input type="datetime-local" step="1" value={nsToDatetimeLocal(endTime)} onChange={(e) => setEndTime(datetimeLocalToNs(e.target.value))} />
          <button type="button" className="alltime-btn" onClick={() => { setStartTime(0); setEndTime(Number.MAX_SAFE_INTEGER); }}>All Time</button>
        </div>
      </div>

      <div className="form-section">
        <div className="form-section-label">Interval (optional)</div>
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
          {loading ? 'Running...' : 'Run Derived Query'}
        </button>
        {previewBody && (
          <div className="query-preview" style={{ whiteSpace: 'pre', fontFamily: 'inherit' }}>
            {previewBody}
          </div>
        )}
      </div>
    </div>
  );
}
