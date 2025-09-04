-- sql/export_event_transport_summary_steps.sql
\timing on
\pset format aligned
\echo 'START export_event_transport_summary_steps.sql'

SET statement_timeout = '45min';
SET jit = off;
SET work_mem = 262144;  -- 256 MB in kB

BEGIN;

\echo 'S0: Build region stops (stops_attica)'
CREATE TEMP TABLE stops_attica AS
SELECT s.stop_id, s.mode
FROM raw.gtfs_stops_geom_all s
JOIN meta.region r ON r.iso_code = :'iso'
WHERE ST_Intersects(s.geom, r.geom);
SELECT COUNT(*) AS n_stops FROM stops_attica;

-- ----------------------- T3_all (AM/PM) -----------------------
\echo 'S1: T3_all route medians (AM/PM) -> net_med_all'
CREATE TEMP TABLE net_med_all AS
WITH stop_arrivals_all AS (
  SELECT st.mode, t.route_id, st.stop_id,
         CASE
           WHEN st.arrival_time ~ $$^[0-9]{1,2}:[0-9]{2}:[0-9]{2}$$
             OR st.arrival_time ~ $$^[0-9]{2,}:[0-9]{2}:[0-9]{2}$$
           THEN (split_part(st.arrival_time,':',1)::int*3600
               +  split_part(st.arrival_time,':',2)::int*60
               +  split_part(st.arrival_time,':',3)::int)
           ELSE NULL
         END AS sec
  FROM raw.gtfs_stop_times_all st
  JOIN raw.gtfs_trips_all     t  ON t.trip_id=st.trip_id AND t.mode=st.mode
  JOIN stops_attica           sa ON sa.stop_id=st.stop_id AND sa.mode=st.mode
),
hw_all AS (
  SELECT 'AM' AS win, mode, route_id, stop_id,
         sec - lag(sec) OVER (PARTITION BY mode, route_id, stop_id ORDER BY sec) AS dh
  FROM stop_arrivals_all
  WHERE sec IS NOT NULL AND (sec % 86400) BETWEEN 7*3600 AND 10*3600-1
  UNION ALL
  SELECT 'PM', mode, route_id, stop_id,
         sec - lag(sec) OVER (PARTITION BY mode, route_id, stop_id ORDER BY sec) AS dh
  FROM stop_arrivals_all
  WHERE sec IS NOT NULL AND (sec % 86400) BETWEEN 16*3600 AND 19*3600-1
),
route_med_all AS (
  SELECT win, mode, route_id,
         percentile_cont(0.5) WITHIN GROUP (ORDER BY dh::double precision) AS med_sec
  FROM hw_all
  WHERE dh IS NOT NULL AND dh>0 AND dh<=3600
  GROUP BY win, mode, route_id
)
SELECT win,
       ROUND(((percentile_cont(0.5) WITHIN GROUP (ORDER BY med_sec))/60.0)::numeric, 2) AS minutes
FROM route_med_all
GROUP BY win;

\echo 'S1: net_med_all (AM, PM)'
TABLE net_med_all;

-- ----------- T3W_MULTI fallback: six Tue–Thu dates (Nov 19–28, 2024) -----------
\echo 'S2: Build fallback services for six Tue–Thu dates (bus + fixed)'
CREATE TEMP TABLE services_fb AS
WITH fb_dates(d) AS (
  VALUES (DATE '2024-11-19'),(DATE '2024-11-20'),(DATE '2024-11-21'),
         (DATE '2024-11-26'),(DATE '2024-11-27'),(DATE '2024-11-28')
),
t AS (
  SELECT d, to_char(d,'YYYYMMDD') AS dstr, EXTRACT(DOW FROM d)::int AS dow FROM fb_dates
),
-- bus
base_bus_fb AS (
  SELECT 'bus'::text AS mode, c.service_id::text AS service_id, t.d
  FROM raw.gtfs_calendar_bus c
  JOIN t ON t.d BETWEEN to_date(c.start_date,'YYYYMMDD') AND to_date(c.end_date,'YYYYMMDD')
   AND ((t.dow=1 AND c.monday=1) OR (t.dow=2 AND c.tuesday=1) OR (t.dow=3 AND c.wednesday=1)
     OR (t.dow=4 AND c.thursday=1) OR (t.dow=5 AND c.friday=1) OR (t.dow=6 AND c.saturday=1)
     OR (t.dow=0 AND c.sunday=1))
),
incl_bus_fb AS (SELECT 'bus', cd.service_id::text, t.d FROM raw.gtfs_calendar_dates_bus cd JOIN t ON cd.date=t.dstr AND cd.exception_type=1),
excl_bus_fb AS (SELECT 'bus', cd.service_id::text, t.d FROM raw.gtfs_calendar_dates_bus cd JOIN t ON cd.date=t.dstr AND cd.exception_type=2),
services_bus_fb AS (
  SELECT DISTINCT * FROM (SELECT * FROM base_bus_fb UNION ALL SELECT * FROM incl_bus_fb) u
  EXCEPT SELECT * FROM excl_bus_fb
),
-- fixed (as in your earlier queries: dates table only)
incl_fixed_fb AS (SELECT 'fixed'::text AS mode, cd.service_id::text AS service_id, t.d FROM raw.gtfs_calendar_dates_fixed cd JOIN t ON cd.date=t.dstr AND cd.exception_type=1),
excl_fixed_fb AS (SELECT 'fixed'::text AS mode, cd.service_id::text AS service_id, t.d FROM raw.gtfs_calendar_dates_fixed cd JOIN t ON cd.date=t.dstr AND cd.exception_type=2),
services_fixed_fb AS (SELECT DISTINCT * FROM incl_fixed_fb EXCEPT SELECT * FROM excl_fixed_fb)
SELECT * FROM services_bus_fb
UNION ALL
SELECT * FROM services_fixed_fb;

SELECT COUNT(*) AS n_service_pairs FROM services_fb;

\echo 'S3: Arrivals for fallback services, pruned to AM/PM windows (string prefilter)'
CREATE TEMP TABLE arr_fb AS
SELECT s.d, st.mode, tr.route_id, st.stop_id,
       CASE
         WHEN st.arrival_time ~ $$^[0-9]{1,2}:[0-9]{2}:[0-9]{2}$$
           OR st.arrival_time ~ $$^[0-9]{2,}:[0-9]{2}:[0-9]{2}$$
         THEN (split_part(st.arrival_time,':',1)::int*3600
             +  split_part(st.arrival_time,':',2)::int*60
             +  split_part(st.arrival_time,':',3)::int)
         ELSE NULL
       END AS sec
FROM services_fb s
JOIN raw.gtfs_trips_all       tr ON tr.service_id=s.service_id AND tr.mode=s.mode
JOIN raw.gtfs_stop_times_all  st ON st.trip_id=tr.trip_id     AND st.mode=tr.mode
JOIN stops_attica             sa ON sa.stop_id=st.stop_id      AND sa.mode=st.mode
WHERE
  (st.arrival_time >= '07:00:00' AND st.arrival_time < '10:00:00')
  OR (st.arrival_time >= '16:00:00' AND st.arrival_time < '19:00:00')
  OR st.arrival_time ~ $$^[0-9]{2,}:[0-9]{2}:[0-9]{2}$$;

SELECT COUNT(*) AS n_arrivals FROM arr_fb;
ANALYZE arr_fb;

\echo 'S4: Headways -> route/day medians -> day medians -> net medians'
CREATE TEMP TABLE win_fb AS
SELECT CASE WHEN (sec % 86400) BETWEEN 7*3600 AND 10*3600-1 THEN 'AM' ELSE 'PM' END AS win,
       d, mode, route_id, stop_id, sec
FROM arr_fb
WHERE sec IS NOT NULL;

SELECT win, COUNT(*) AS n_rows FROM win_fb GROUP BY win ORDER BY win;

CREATE TEMP TABLE hw_fb AS
SELECT win, d, mode, route_id, stop_id,
       sec - lag(sec) OVER (PARTITION BY win, d, mode, route_id, stop_id ORDER BY sec) AS dh
FROM win_fb;

SELECT win, COUNT(*) AS n_headways FROM hw_fb WHERE dh IS NOT NULL AND dh>0 AND dh<=3600 GROUP BY win ORDER BY win;

CREATE TEMP TABLE route_med_fb AS
SELECT win, d, mode, route_id,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY dh::double precision) AS med_sec
FROM hw_fb
WHERE dh IS NOT NULL AND dh>0 AND dh<=3600
GROUP BY win, d, mode, route_id;

SELECT win, d, COUNT(*) AS n_routes FROM route_med_fb GROUP BY win, d ORDER BY win, d;

CREATE TEMP TABLE day_med_fb AS
SELECT win, d,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY med_sec) AS day_med_sec
FROM route_med_fb
GROUP BY win, d;

TABLE day_med_fb;

CREATE TEMP TABLE net_med_t3w_fb AS
SELECT win,
       ROUND(((percentile_cont(0.5) WITHIN GROUP (ORDER BY day_med_sec))/60.0)::numeric, 2) AS minutes
FROM day_med_fb
GROUP BY win;

\echo 'S4: net_med_t3w_fb (AM, PM)'
TABLE net_med_t3w_fb;

\echo 'S5: Final export -> /tmp/event_transport_summary.csv'
\copy (SELECT 'AM' AS window, 'T3_all' AS variant, (SELECT minutes FROM net_med_all WHERE win='AM') AS minutes UNION ALL SELECT 'PM','T3_all',(SELECT minutes FROM net_med_all WHERE win='PM') UNION ALL SELECT 'AM','T3W_MULTI_event',(SELECT minutes FROM net_med_t3w_fb WHERE win='AM') UNION ALL SELECT 'PM','T3W_MULTI_event',(SELECT minutes FROM net_med_t3w_fb WHERE win='PM') ORDER BY 1,2) TO '/tmp/event_transport_summary.csv' WITH (FORMAT CSV, HEADER TRUE);

COMMIT;

\echo 'DONE export_event_transport_summary_steps.sql'
