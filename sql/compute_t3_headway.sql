WITH params AS (
  SELECT 7*3600 AS sec_start, 10*3600 AS sec_end           -- 07:00–10:00
), rgn AS (
  SELECT geom FROM meta.region WHERE iso_code = 'EL30'
), stops_attica AS (
  SELECT s.stop_id, s.mode
  FROM raw.gtfs_stops_geom_all s, rgn
  WHERE ST_Intersects(s.geom, rgn.geom)
), stop_arrivals AS (
  SELECT
    st.mode,
    t.route_id,
    st.stop_id,
    CASE
      -- robust parse allowing HH>=24 per GTFS spec
      WHEN st.arrival_time ~ '^\d{1,2}:\d{2}:\d{2}$'
        OR st.arrival_time ~ '^\d{2,}:\d{2}:\d{2}$'
      THEN (split_part(st.arrival_time,':',1)::int * 3600
          +  split_part(st.arrival_time,':',2)::int * 60
          +  split_part(st.arrival_time,':',3)::int)
      ELSE NULL
    END AS sec
  FROM raw.gtfs_stop_times_all st
  JOIN raw.gtfs_trips_all t
    ON t.trip_id = st.trip_id AND t.mode = st.mode
  JOIN stops_attica sa
    ON sa.stop_id = st.stop_id AND sa.mode = st.mode
), win AS (
  -- keep arrivals whose local clock-time modulo 24h is in the window
  SELECT a.mode, a.route_id, a.stop_id, a.sec
  FROM stop_arrivals a, params p
  WHERE a.sec IS NOT NULL
    AND (a.sec % 86400) >= p.sec_start
    AND (a.sec % 86400) <  p.sec_end
), headways AS (
  SELECT
    mode, route_id, stop_id,
    sec - lag(sec) OVER (PARTITION BY mode, route_id, stop_id ORDER BY sec) AS dh
  FROM win
), headways_pos AS (
  -- discard zero/negatives and extreme gaps > 60 min for peak analysis
  SELECT mode, route_id, stop_id, dh
  FROM headways
  WHERE dh IS NOT NULL AND dh > 0 AND dh <= 3600
), route_median AS (
  SELECT mode, route_id,
         percentile_cont(0.5) WITHIN GROUP (ORDER BY dh::double precision) AS med_sec
  FROM headways_pos
  GROUP BY mode, route_id
), overall AS (
  SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY med_sec) AS med_sec
  FROM route_median
), ids AS (
  SELECT
    (SELECT region_id FROM meta.region WHERE iso_code='EL30') AS region_id,
    (SELECT indicator_id FROM meta.indicator WHERE indicator_code='T3_SCHED_MEDIAN_HEADWAY_MIN') AS indicator_id
)
INSERT INTO feat.indicator_value
  (region_id, indicator_id, time_start,  time_end,    value_raw,                  value_norm,                          source)
SELECT
  ids.region_id,
  ids.indicator_id,
  DATE '2024-01-01', DATE '2024-12-31',
  (overall.med_sec/60.0)                                     AS value_raw,       -- minutes
  LEAST( (overall.med_sec/60.0) / 30.0, 1.0 )                AS value_norm,      -- min(h/30, 1)
  'GTFS static (bus∪fixed). Headways per stop in 07:00–10:00 → route median → network median. Normalize min(h/30,1). Attica only.'
FROM overall, ids
ON CONFLICT (region_id, indicator_id, time_start, time_end)
DO UPDATE SET value_raw=EXCLUDED.value_raw, value_norm=EXCLUDED.value_norm, source=EXCLUDED.source;
