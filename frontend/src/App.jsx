import { useState, useEffect, useCallback } from 'react';
import { fetchMeasurements, fetchFields, fetchTags, runQuery, runDerivedQuery } from './api';
import QueryForm from './QueryForm';
import ResultsTable from './ResultsTable';
import DerivedQueryForm from './DerivedQueryForm';
import DerivedResultsTable from './DerivedResultsTable';
import AnomalyQueryForm from './AnomalyQueryForm';
import AnomalyResultsTable from './AnomalyResultsTable';
import ForecastQueryForm from './ForecastQueryForm';
import ForecastResultsTable from './ForecastResultsTable';
import './App.css';

const QUERY_MODES = ['standard', 'derived', 'anomaly', 'forecast'];

function defaultTimeRange() {
  const now = Date.now() * 1_000_000;
  const dayAgo = now - 24 * 60 * 60 * 1_000_000_000;
  return { start: dayAgo, end: now };
}

function buildQueryString(aggregation, measurement, selectedFields, selectedScopes, selectedGroupBy) {
  if (!measurement) return '';
  const fieldsPart = selectedFields.length > 0 ? selectedFields.join(',') : '';
  let q = `${aggregation}:${measurement}(${fieldsPart})`;
  if (selectedScopes.length > 0) {
    q += `{${selectedScopes.join(',')}}`;
  }
  if (selectedGroupBy.length > 0) {
    q += ` by {${selectedGroupBy.join(',')}}`;
  }
  return q;
}

export default function App() {
  const [queryMode, setQueryMode] = useState('standard');
  const [measurements, setMeasurements] = useState([]);
  const [selectedMeasurement, setSelectedMeasurement] = useState('');
  const [fields, setFields] = useState({});
  const [selectedFields, setSelectedFields] = useState([]);
  const [tags, setTags] = useState({});
  const [selectedScopes, setSelectedScopes] = useState([]);
  const [tagKeys, setTagKeys] = useState([]);
  const [selectedGroupBy, setSelectedGroupBy] = useState([]);
  const [aggregation, setAggregation] = useState('avg');
  const [startTime, setStartTime] = useState(() => defaultTimeRange().start);
  const [endTime, setEndTime] = useState(() => defaultTimeRange().end);
  const [intervalValue, setIntervalValue] = useState('');
  const [intervalUnit, setIntervalUnit] = useState('m');
  const [results, setResults] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  useEffect(() => {
    fetchMeasurements()
      .then(setMeasurements)
      .catch((e) => setError(e.message));
  }, []);

  useEffect(() => {
    if (!selectedMeasurement) {
      setFields({});
      setTags({});
      setTagKeys([]);
      setSelectedFields([]);
      setSelectedScopes([]);
      setSelectedGroupBy([]);
      return;
    }
    setError(null);
    Promise.all([
      fetchFields(selectedMeasurement),
      fetchTags(selectedMeasurement),
    ]).then(([f, t]) => {
      setFields(f);
      setTags(t);
      setTagKeys(Object.keys(t));
      setSelectedFields([]);
      setSelectedScopes([]);
      setSelectedGroupBy([]);
    }).catch((e) => setError(e.message));
  }, [selectedMeasurement]);

  const toggleField = useCallback((name) => {
    setSelectedFields((prev) =>
      prev.includes(name) ? prev.filter((f) => f !== name) : [...prev, name]
    );
  }, []);

  const toggleScope = useCallback((id) => {
    setSelectedScopes((prev) =>
      prev.includes(id) ? prev.filter((s) => s !== id) : [...prev, id]
    );
  }, []);

  const toggleGroupBy = useCallback((key) => {
    setSelectedGroupBy((prev) =>
      prev.includes(key) ? prev.filter((k) => k !== key) : [...prev, key]
    );
  }, []);

  const queryString = buildQueryString(aggregation, selectedMeasurement, selectedFields, selectedScopes, selectedGroupBy);

  const handleSubmit = useCallback(async () => {
    if (!selectedMeasurement) return;
    setLoading(true);
    setError(null);
    try {
      let aggregationInterval = null;
      if (intervalValue && Number(intervalValue) > 0) {
        aggregationInterval = `${intervalValue}${intervalUnit}`;
      }
      const data = await runQuery({ query: queryString, startTime, endTime, aggregationInterval });
      setResults(data);
    } catch (e) {
      setError(e.message);
      setResults(null);
    } finally {
      setLoading(false);
    }
  }, [selectedMeasurement, queryString, startTime, endTime, intervalValue, intervalUnit]);

  const handleDerivedSubmit = useCallback(async ({ queries, formula, startTime: st, endTime: et, aggregationInterval }) => {
    setLoading(true);
    setError(null);
    try {
      const data = await runDerivedQuery({ queries, formula, startTime: st, endTime: et, aggregationInterval });
      setResults(data);
    } catch (e) {
      setError(e.message);
      setResults(null);
    } finally {
      setLoading(false);
    }
  }, []);

  const handleModeChange = useCallback((mode) => {
    setQueryMode(mode);
    setResults(null);
    setError(null);
  }, []);

  return (
    <>
      <header className="app-header">
        <h1>TSDB Query</h1>
      </header>
      <div className="app-body">
        <div className="pane-left">
          <div className="query-tabs">
            {QUERY_MODES.map((mode) => (
              <button
                key={mode}
                className={`query-tab${queryMode === mode ? ' active' : ''}`}
                onClick={() => handleModeChange(mode)}
              >
                {mode.charAt(0).toUpperCase() + mode.slice(1)}
              </button>
            ))}
          </div>

          {queryMode === 'standard' && (
            <QueryForm
              measurements={measurements}
              selectedMeasurement={selectedMeasurement}
              onMeasurementChange={setSelectedMeasurement}
              fields={fields}
              selectedFields={selectedFields}
              onFieldToggle={toggleField}
              tags={tags}
              selectedScopes={selectedScopes}
              onScopeToggle={toggleScope}
              tagKeys={tagKeys}
              selectedGroupBy={selectedGroupBy}
              onGroupByToggle={toggleGroupBy}
              aggregation={aggregation}
              onAggregationChange={setAggregation}
              startTime={startTime}
              endTime={endTime}
              onStartTimeChange={setStartTime}
              onEndTimeChange={setEndTime}
              intervalValue={intervalValue}
              intervalUnit={intervalUnit}
              onIntervalValueChange={setIntervalValue}
              onIntervalUnitChange={setIntervalUnit}
              onSubmit={handleSubmit}
              loading={loading}
              queryString={queryString}
            />
          )}
          {queryMode === 'derived' && (
            <DerivedQueryForm measurements={measurements} onSubmit={handleDerivedSubmit} loading={loading} />
          )}
          {queryMode === 'anomaly' && (
            <AnomalyQueryForm measurements={measurements} onSubmit={handleDerivedSubmit} loading={loading} />
          )}
          {queryMode === 'forecast' && (
            <ForecastQueryForm measurements={measurements} onSubmit={handleDerivedSubmit} loading={loading} />
          )}
        </div>
        <div className="pane-right">
          {error && <div className="error-banner">{error}</div>}
          {queryMode === 'standard' && <ResultsTable results={results} loading={loading} />}
          {queryMode === 'derived' && <DerivedResultsTable results={results} loading={loading} />}
          {queryMode === 'anomaly' && <AnomalyResultsTable results={results} loading={loading} />}
          {queryMode === 'forecast' && <ForecastResultsTable results={results} loading={loading} />}
        </div>
      </div>
    </>
  );
}
