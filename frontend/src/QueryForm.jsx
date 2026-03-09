import './QueryForm.css';

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

function nsToDatetimeLocal(ns) {
  if (!ns || ns === 0) return '';
  const ms = Number(ns) / 1_000_000;
  const d = new Date(ms);
  const pad = (n, len = 2) => String(n).padStart(len, '0');
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
}

function datetimeLocalToNs(str) {
  if (!str) return 0;
  return new Date(str).getTime() * 1_000_000;
}

export default function QueryForm({
  measurements, selectedMeasurement, onMeasurementChange,
  fields, selectedFields, onFieldToggle,
  tags, selectedScopes, onScopeToggle,
  tagKeys, selectedGroupBy, onGroupByToggle,
  aggregation, onAggregationChange,
  startTime, endTime, onStartTimeChange, onEndTimeChange,
  intervalValue, intervalUnit, onIntervalValueChange, onIntervalUnitChange,
  onSubmit, loading, queryString,
}) {
  const fieldEntries = Object.entries(fields);
  const scopeEntries = [];
  for (const [key, values] of Object.entries(tags)) {
    for (const val of values) {
      scopeEntries.push({ key, val, id: `${key}:${val}` });
    }
  }

  return (
    <div className="query-form">
      {/* Measurement */}
      <div className="form-section">
        <div className="form-section-label">Measurement</div>
        <select
          value={selectedMeasurement}
          onChange={(e) => onMeasurementChange(e.target.value)}
        >
          <option value="">Select measurement...</option>
          {measurements.map((m) => (
            <option key={m} value={m}>{m}</option>
          ))}
        </select>
      </div>

      {/* Aggregation */}
      <div className="form-section">
        <div className="form-section-label">Aggregation</div>
        <select value={aggregation} onChange={(e) => onAggregationChange(e.target.value)}>
          {AGGREGATIONS.map((a) => (
            <option key={a} value={a}>{a}</option>
          ))}
        </select>
      </div>

      {/* Fields */}
      <div className="form-section">
        <div className="form-section-label">Fields {fieldEntries.length > 0 && `(${selectedFields.length}/${fieldEntries.length})`}</div>
        {fieldEntries.length === 0 ? (
          <div style={{ color: 'var(--text-muted)', fontSize: 12 }}>Select a measurement</div>
        ) : (
          <div className="checkbox-list">
            {fieldEntries.map(([name, info]) => (
              <label key={name}>
                <input
                  type="checkbox"
                  checked={selectedFields.includes(name)}
                  onChange={() => onFieldToggle(name)}
                />
                {name}
                <span className={`type-badge ${info.type || 'float'}`}>{info.type || '?'}</span>
              </label>
            ))}
          </div>
        )}
      </div>

      {/* Scopes */}
      <div className="form-section">
        <div className="form-section-label">Scopes {scopeEntries.length > 0 && `(${selectedScopes.length}/${scopeEntries.length})`}</div>
        {scopeEntries.length === 0 ? (
          <div style={{ color: 'var(--text-muted)', fontSize: 12 }}>No tags available</div>
        ) : (
          <div className="checkbox-list">
            {scopeEntries.map(({ key, val, id }) => (
              <label key={id}>
                <input
                  type="checkbox"
                  checked={selectedScopes.includes(id)}
                  onChange={() => onScopeToggle(id)}
                />
                <span style={{ color: 'var(--text-muted)' }}>{key}:</span>{val}
              </label>
            ))}
          </div>
        )}
      </div>

      {/* Group By */}
      <div className="form-section">
        <div className="form-section-label">Group By</div>
        {tagKeys.length === 0 ? (
          <div style={{ color: 'var(--text-muted)', fontSize: 12 }}>No tag keys available</div>
        ) : (
          <div className="checkbox-list">
            {tagKeys.map((key) => (
              <label key={key}>
                <input
                  type="checkbox"
                  checked={selectedGroupBy.includes(key)}
                  onChange={() => onGroupByToggle(key)}
                />
                {key}
              </label>
            ))}
          </div>
        )}
      </div>

      {/* Time Range */}
      <div className="form-section">
        <div className="form-section-label">Time Range</div>
        <div className="time-inputs">
          <input
            type="datetime-local"
            value={nsToDatetimeLocal(startTime)}
            onChange={(e) => onStartTimeChange(datetimeLocalToNs(e.target.value))}
            step="1"
          />
          <input
            type="datetime-local"
            value={nsToDatetimeLocal(endTime)}
            onChange={(e) => onEndTimeChange(datetimeLocalToNs(e.target.value))}
            step="1"
          />
          <button
            type="button"
            className="alltime-btn"
            onClick={() => {
              onStartTimeChange(0);
              onEndTimeChange(Number.MAX_SAFE_INTEGER);
            }}
          >
            All Time
          </button>
        </div>
      </div>

      {/* Aggregation Interval */}
      <div className="form-section">
        <div className="form-section-label">Interval (optional)</div>
        <div className="interval-row">
          <input
            type="number"
            min="0"
            step="any"
            placeholder="e.g. 5"
            value={intervalValue}
            onChange={(e) => onIntervalValueChange(e.target.value)}
          />
          <select value={intervalUnit} onChange={(e) => onIntervalUnitChange(e.target.value)}>
            {INTERVAL_UNITS.map((u) => (
              <option key={u.value} value={u.value}>{u.label}</option>
            ))}
          </select>
        </div>
      </div>

      {/* Submit */}
      <div className="submit-section">
        <button
          className="submit-btn"
          disabled={!selectedMeasurement || loading}
          onClick={onSubmit}
        >
          {loading ? 'Running...' : 'Run Query'}
        </button>
        {queryString && (
          <div className="query-preview">{queryString}</div>
        )}
      </div>
    </div>
  );
}
