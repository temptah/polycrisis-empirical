INSERT INTO meta.indicator (system_code, indicator_code, indicator_name, direction, unit, methodology)
SELECT
  'TRANSPORT',
  'T3_SCHED_MEDIAN_HEADWAY_MIN',
  'Median scheduled headway (07:00–10:00)',
  'UP_IS_BAD',
  'minutes',
  'Within 07:00–10:00 local: compute headways per stop on each route from GTFS stop_times; take median per route, then median across routes. Normalize as min(headway/30,1). Times handled as GTFS strings allowing >24h; geographic filter = Attica (EL30).'
WHERE NOT EXISTS (
  SELECT 1 FROM meta.indicator WHERE indicator_code = 'T3_SCHED_MEDIAN_HEADWAY_MIN'
);
