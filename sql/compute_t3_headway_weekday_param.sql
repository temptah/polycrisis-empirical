-- compute_t3_headway_weekday_param.sql
-- Usage: psql -v ON_ERROR_STOP=1 -v wdate=2024-10-15 -f this.sql

WITH target AS (
  SELECT to_date(:'wdate','YYYY-MM-DD') AS d,
         to_char(to_date(:'wdate','YYYY-MM-DD'),'YYYYMMDD') AS dstr,
         EXTRACT(DOW FROM to_date(:'wdate','YYYY-MM-DD'))::int AS dow
), base_bus AS (
  SELECT DISTINCT 'bus'::text AS mode, c.service_id::text AS service_id
  FROM raw.gtfs_calendar_bus c, target t
  WHERE to_date(c.start_date,'YYYYMMDD') <= t.d
    AND to_date(c.end_date,  'YYYYMMDD') >= t.d
    AND ((t.dow=1 AND c.monday=1) OR (t.dow=2 AND c.tuesday=1) OR (t.dow=3 AND c.wednesday=1)
      OR (t.dow=4 AND c.thursday=1) OR (t.dow=5 AND c.friday=1) OR (t.dow=6 AND c.saturday=1)
      OR (t.dow=0 AND c.sunday=1))
), incl_bus AS (
  SELECT 'bus'::text AS mode, cd.service_id::text AS service_id
  FROM raw.gtfs_calendar_dates_bus cd, target t
  WHERE cd.date = t.dstr AND cd.exception_type = 1
), excl_bus AS (
  SELECT 'bus'::text AS mode, cd.service_id::text AS service_id
  FROM raw.gtfs_calendar_dates_bus cd, target t
  WHERE cd.date = t.dstr AND cd.exception_type = 2
), services_bus AS (
  SELECT DISTINCT * FROM (SELECT * FROM base_bus UNION ALL SELECT * FROM incl_bus) u
  EXCEPT SELECT * FROM excl_bus
), incl_fixed AS (
  SELECT 'fixed'::text AS mode, cd.service_id::text AS service_id
  FROM raw.gtfs_calendar_dates_fixed cd, target t
  WHERE cd.date = t.dstr AND cd.exception_type = 1
), excl_fixed AS (
  SELECT 'fixed'::text AS mode, cd.service_id::text AS service_id
  FROM raw.gtfs_calendar_dates_fixed cd, target t
  WHERE cd.date = t.dstr AND cd.exception_type = 2
), services_fixed AS (
  SELECT DISTINCT * FROM incl_fixed
  EXCEPT SELECT * FROM excl_fixed
), services_on_date AS (
  SELECT * FROM services_bus
  UNION ALL
  SELECT * FROM services_fixed
), rgn AS (
  SELECT geom FROM meta.region WHERE iso_code='EL30'
), stops_attica AS (
  SELECT s.stop_id, s.mode
  FROM raw.gtfs_stops_geom_all s, rgn
  WHERE ST_Intersects(s.geom, rgn.geom)
), stop_arrivals AS (
  SELECT st.mode, t.route_id, st.stop_id,
         CASE
           WHEN st.arrival_time ~ '^[0-9]{1,2}:[0-9]{2}:[0-9]{2}$'
             OR st.arrival_time ~ '^[0-9]{2,}:[0-9]{2}:[0-9]{2}$'
           THEN (split_part(st.arrival_time,':',1)::int*3600
               +  split_part(st.arrival_time,':',2)::int*60
               +  split_part(st.arrival_time,':',3)::int)
           ELSE NULL
         END AS sec
  FROM raw.gtfs_stop_times_all st
  JOIN raw.gtfs_trips_all t ON t.trip_id=st.trip_id AND t.mode=st.mode
  JOIN services_on_date s ON s.service_id=t.service_id AND s.mode=t.mode
  JOIN stops_attica sa ON sa.stop_id=st.stop_id AND sa.mode=st.mode
), params AS (
  SELECT 7*3600 AS sec_start, 10*3600 AS sec_end
), win AS (
  SELECT a.mode, a.route_id, a.stop_id, a.sec
  FROM stop_arrivals a, params p
  WHERE a.sec IS NOT NULL
    AND (a.sec % 86400) BETWEEN p.sec_start AND p.sec_end-1
), headways AS (
  SELECT mode, route_id, stop_id,
         sec - lag(sec) OVER (PARTITION BY mode, route_id, stop_id ORDER BY sec) AS dh
  FROM win
), headways_pos AS (
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
    (SELECT indicator_id FROM meta.indicator WHERE indicator_code='T3W_SCHED_MEDIAN_HEADWAY_MIN') AS indicator_id
)
INSERT INTO feat.indicator_value
  (region_id, indicator_id, time_start, time_end, value_raw, value_norm, source)
SELECT
  ids.region_id,
  ids.indicator_id,
  DATE '2024-01-01', DATE '2024-12-31',
  (overall.med_sec/60.0) AS value_raw,
  LEAST((overall.med_sec/60.0)/30.0, 1.0) AS value_norm,
  'GTFS static WEEKDAY ('||to_char(to_date(:'wdate','YYYY-MM-DD'),'YYYY-MM-DD')||'). Headways 07:00–10:00; per-stop → route median → network median. Attica; HH≥24; services filtered by calendar/calendar_dates.'
FROM overall, ids
ON CONFLICT (region_id, indicator_id, time_start, time_end)
DO UPDATE SET value_raw=EXCLUDED.value_raw, value_norm=EXCLUDED.value_norm, source=EXCLUDED.source;
