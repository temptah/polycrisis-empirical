INSERT INTO meta.indicator (system_code, indicator_code, indicator_name, direction, unit, methodology)
SELECT
  'TRANSPORT',
  'T3W_SCHED_MEDIAN_HEADWAY_MIN',
  'Median scheduled headway (07:00–10:00), weekday-only',
  'UP_IS_BAD',
  'minutes',
  'Same as T3 but with service filtering by calendar/calendar_dates for a chosen weekday.'
WHERE NOT EXISTS (
  SELECT 1 FROM meta.indicator WHERE indicator_code='T3W_SCHED_MEDIAN_HEADWAY_MIN'
);
