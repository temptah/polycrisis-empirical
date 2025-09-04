\set ON_ERROR_STOP on
\pset format unaligned
\pset tuples_only on
\o /tmp/attica_headways_2024-11-20.geojson

WITH target AS (
  SELECT DATE '2024-11-20' AS d, to_char(DATE '2024-11-20','YYYYMMDD') AS dstr,
         EXTRACT(DOW FROM DATE '2024-11-20')::int AS dow
),
base_bus AS (
  SELECT 'bus'::text AS mode, c.service_id::text AS service_id
  FROM raw.gtfs_calendar_bus c, target t
  WHERE to_date(c.start_date,'YYYYMMDD')<=t.d AND to_date(c.end_date,'YYYYMMDD')>=t.d
    AND ((t.dow=1 AND c.monday=1) OR (t.dow=2 AND c.tuesday=1) OR (t.dow=3 AND c.wednesday=1)
      OR (t.dow=4 AND c.thursday=1) OR (t.dow=5 AND c.friday=1) OR (t.dow=6 AND c.saturday=1)
      OR (t.dow=0 AND c.sunday=1))
),
incl_bus AS (
  SELECT 'bus'::text AS mode, cd.service_id::text AS service_id
  FROM raw.gtfs_calendar_dates_bus cd, target t
  WHERE cd.date=t.dstr AND cd.exception_type=1
),
excl_bus AS (
  SELECT 'bus'::text AS mode, cd.service_id::text AS service_id
  FROM raw.gtfs_calendar_dates_bus cd, target t
  WHERE cd.date=t.dstr AND cd.exception_type=2
),
services_bus AS (
  SELECT DISTINCT * FROM (SELECT * FROM base_bus UNION ALL SELECT * FROM incl_bus) u
  EXCEPT SELECT * FROM excl_bus
),
incl_fixed AS (
  SELECT 'fixed'::text AS mode, cd.service_id::text AS service_id
  FROM raw.gtfs_calendar_dates_fixed cd, target t
  WHERE cd.date=t.dstr AND cd.exception_type=1
),
excl_fixed AS (
  SELECT 'fixed'::text AS mode, cd.service_id::text AS service_id
  FROM raw.gtfs_calendar_dates_fixed cd, target t
  WHERE cd.date=t.dstr AND cd.exception_type=2
),
services_fixed AS (SELECT DISTINCT * FROM incl_fixed EXCEPT SELECT * FROM excl_fixed),
services_on_date AS (SELECT * FROM services_bus UNION ALL SELECT * FROM services_fixed),
rgn AS (SELECT geom FROM meta.region WHERE iso_code='EL30'),
stops_attica AS (
  SELECT s.stop_id, s.mode, s.geom
  FROM raw.gtfs_stops_geom_all s, rgn
  WHERE ST_Intersects(s.geom, rgn.geom)
),
arr AS (
  SELECT st.stop_id, st.mode,
         CASE
           WHEN st.arrival_time ~ '^[0-9]{1,2}:[0-9]{2}:[0-9]{2}$'
             OR st.arrival_time ~ '^[0-9]{2,}:[0-9]{2}:[0-9]{2}$'
           THEN (split_part(st.arrival_time,':',1)::int*3600
               +  split_part(st.arrival_time,':',2)::int*60
               +  split_part(st.arrival_time,':',3)::int)
           ELSE NULL
         END AS sec
  FROM services_on_date s
  JOIN raw.gtfs_trips_all t ON t.service_id=s.service_id AND t.mode=s.mode
  JOIN raw.gtfs_stop_times_all st ON st.trip_id=t.trip_id AND st.mode=t.mode
  JOIN stops_attica sa ON sa.stop_id=st.stop_id AND sa.mode=st.mode
),
win AS (
  SELECT a.stop_id, a.mode, a.sec
  FROM arr a, (SELECT 7*3600 AS sec_start, 10*3600 AS sec_end) p
  WHERE a.sec IS NOT NULL AND (a.sec % 86400) BETWEEN p.sec_start AND p.sec_end-1
),
hw AS (
  SELECT stop_id, mode, sec - lag(sec) OVER (PARTITION BY stop_id, mode ORDER BY sec) AS dh
  FROM win
),
hw_pos AS (
  SELECT stop_id, mode, dh FROM hw WHERE dh IS NOT NULL AND dh > 0 AND dh <= 3600
),
stop_median AS (
  SELECT stop_id, mode, percentile_cont(0.5) WITHIN GROUP (ORDER BY dh) AS med_sec
  FROM hw_pos
  GROUP BY stop_id, mode
)
SELECT jsonb_build_object(
  'type','FeatureCollection',
  'features', jsonb_agg(
     jsonb_build_object(
       'type','Feature',
       'geometry', ST_AsGeoJSON(sa.geom)::jsonb,
       'properties', jsonb_build_object(
         'stop_id', sa.stop_id,
         'mode', sa.mode,
         'med_min', (sm.med_sec/60.0)
       )
     )
  )
)::text
FROM stop_median sm
JOIN stops_attica sa ON sa.stop_id=sm.stop_id AND sa.mode=sm.mode;

\o
