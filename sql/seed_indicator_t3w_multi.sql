INSERT INTO meta.indicator (system_code, indicator_code, indicator_name, direction, unit, methodology)
SELECT
  'TRANSPORT',
  'T3W_MULTI_SCHED_MEDIAN_HEADWAY_MIN',
  'Median scheduled headway (07:00–10:00), multi-weekday typical',
  'UP_IS_BAD',
  'minutes',
  'Median of daily route-median headways across several Tue–Thu non-holidays; per-stop headways → route median → day network median; GTFS hours ≥24; Attica filter.'
WHERE NOT EXISTS (
  SELECT 1 FROM meta.indicator WHERE indicator_code='T3W_MULTI_SCHED_MEDIAN_HEADWAY_MIN'
);
