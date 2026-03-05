import { useState, useEffect, useCallback } from 'react';
import { fetchFields, fetchTags } from './api';

export function buildQueryString(aggregation, measurement, selectedFields, selectedScopes, selectedGroupBy) {
  if (!measurement) return '';
  const fieldsPart = selectedFields.join(',');
  let q = `${aggregation}:${measurement}(${fieldsPart})`;
  if (selectedScopes.length > 0) q += `{${selectedScopes.join(',')}}`;
  if (selectedGroupBy.length > 0) q += ` by {${selectedGroupBy.join(',')}}`;
  return q;
}

export default function useQueryBuilder() {
  const [selectedMeasurement, setSelectedMeasurement] = useState('');
  const [fields, setFields] = useState({});
  const [tags, setTags] = useState({});
  const [tagKeys, setTagKeys] = useState([]);
  const [selectedFields, setSelectedFields] = useState([]);
  const [selectedScopes, setSelectedScopes] = useState([]);
  const [selectedGroupBy, setSelectedGroupBy] = useState([]);
  const [aggregation, setAggregation] = useState('avg');

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
    }).catch(() => {});
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

  return {
    selectedMeasurement,
    setSelectedMeasurement,
    fields,
    tags,
    tagKeys,
    selectedFields,
    toggleField,
    selectedScopes,
    toggleScope,
    selectedGroupBy,
    toggleGroupBy,
    aggregation,
    setAggregation,
    queryString,
  };
}
