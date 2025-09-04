-- sql/export_event_transport_summary.sql
-- Params: :iso, :code
\pset format unaligned
\pset tuples_only on
\o /tmp/event_transport_summary.csv

WITH evt AS (
  SELECT e.time_start::date AS d0, e.time_end::date AS d1, r.region_id
  FROM meta.event e
  JOIN meta.region r ON r.region_id = e.region_id
  WHERE e.event_code = :'code' AND r.iso_code = :'iso'
),
daylist AS (
  SELECT g::date AS d, EXTRACT(DOW FROM g)::int AS dow
  FROM evt, generate_series((SELECT d0 FROM evt), (SELECT d1 FROM evt), interval '1 day') g
  WHERE EXTRACT(DOW FROM g)::int IN (2,3,4) -- Tue/Wed/Thu
),
rgn AS (SELECT geom FROM meta.region WHERE iso_code = :'iso'),
stops_attica AS (
  SELECT s.stop_id, s.mode
  FROM raw.gtfs_stops_geom_all s, rgn
  WHERE ST_Intersects(s.geom, rgn.geom)
),

-- ----------- T3 (all services, AM/PM) -----------
stop_arrivals_all AS (
  SELECT st.mode, t.route_id, st.stop_id,
         CASE
           WHEN st.arrival_time ~ '^[0-9]{1,2}:[0-9]{2}:[0-9]{2}$'
            OR st.arrival_time ~ '^[0-9]{2,}:[0-9]{2}:[0-9]{2}$'
           THEN (split_part(st.arrival_time,':',1)::int*3600
               + split_part(st.arrival_time,':',2)::int*60
               + split_part(st.arrival_time,':',3)::int)
           ELSE NULL
         END AS sec
  FROM raw.gtfs_stop_times_all st
  JOIN raw.gtfs_trips_all t ON t.trip_id=st.trip_id AND t.mode=st.mode
  JOIN stops_attica sa ON sa.stop_id=st.stop_id AND sa.mode=st.mode
),
hw_all AS (
  SELECT 'AM' AS win, mode, route_id, stop_id,
         sec - lag(sec) OVER (PARTITION BY mode, route_id, stop_id ORDER BY sec) AS dh
  FROM stop_arrivals_all
  WHERE sec IS NOT NULL AND (sec % 86400) BETWEEN 7*3600 AND 10*3600-1
  UNION ALL
  SELECT 'PM' AS win, mode, route_id, stop_id,
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
),
net_med_all AS (
  SELECT win,
         ROUND( ((percentile_cont(0.5) WITHIN GROUP (ORDER BY med_sec)) / 60.0)::numeric, 2 ) AS minutes
  FROM route_med_all
  GROUP BY win
),

-- ----------- T3W_MULTI within event Tue–Thu, AM/PM -----------
base_bus AS (
  SELECT 'bus'::text AS mode, c.service_id::text AS service_id, dl.d
  FROM raw.gtfs_calendar_bus c JOIN daylist dl
    ON dl.d BETWEEN to_date(c.start_date,'YYYYMMDD') AND to_date(c.end_date,'YYYYMMDD')
   AND ( (dl.dow=1 AND c.monday=1) OR (dl.dow=2 AND c.tuesday=1) OR (dl.dow=3 AND c.wednesday=1)
      OR (dl.dow=4 AND c.thursday=1) OR (dl.dow=5 AND c.friday=1) OR (dl.dow=6 AND c.saturday=1)
      OR (dl.dow=0 AND c.sunday=1) )
),
incl_bus AS (
  SELECT 'bus', cd.service_id::text, dl.d
  FROM raw.gtfs_calendar_dates_bus cd JOIN daylist dl
    ON cd.date=to_char(dl.d,'YYYYMMDD') AND cd.exception_type=1
),
excl_bus AS (
  SELECT 'bus', cd.service_id::text, dl.d
  FROM raw.gtfs_calendar_dates_bus cd JOIN daylist dl
    ON cd.date=to_char(dl.d,'YYYYMMDD') AND cd.exception_type=2
),
services_bus AS (
  SELECT DISTINCT * FROM (SELECT * FROM base_bus UNION ALL SELECT * FROM incl_bus) u
  EXCEPT SELECT * FROM excl_bus
),
incl_fixed AS (
  SELECT 'fixed'::text AS mode, cd.service_id::text AS service_id, dl.d
  FROM raw.gtfs_calendar_dates_fixed cd JOIN daylist dl
    ON cd.date=to_char(dl.d,'YYYYMMDD') AND cd.exception_type=1
),
excl_fixed AS (
  SELECT 'fixed'::text AS mode, cd.service_id::text AS service_id, dl.d
  FROM raw.gtfs_calendar_dates_fixed cd JOIN daylist dl
    ON cd.date=to_char(dl.d,'YYYYMMDD') AND cd.exception_type=2
),
services_fixed AS (SELECT DISTINCT * FROM incl_fixed EXCEPT SELECT * FROM excl_fixed),
services_on_date AS (SELECT * FROM services_bus UNION ALL SELECT * FROM services_fixed),
arr_dates AS (
  SELECT sod.d, st.mode, tr.route_id, st.stop_id,
         CASE
           WHEN st.arrival_time ~ '^[0-9]{1,2}:[0-9]{2}:[0-9]{2}$'
            OR st.arrival_time ~ '^[0-9]{2,}:[0-9]{2}:[0-9]{2}$'
           THEN (split_part(st.arrival_time,':',1)::int*3600
               + split_part(st.arrival_time,':',2)::int*60
               + split_part(st.arrival_time,':',3)::int)
           ELSE NULL
         END AS sec
  FROM services_on_date sod
  JOIN raw.gtfs_trips_all tr ON tr.service_id=sod.service_id AND tr.mode=sod.mode
  JOIN raw.gtfs_stop_times_all st ON st.trip_id=tr.trip_id AND st.mode=tr.mode
  JOIN stops_attica sa ON sa.stop_id=st.stop_id AND sa.mode=st.mode
),
win_dates AS (
  SELECT 'AM' AS win, d, mode, route_id, stop_id, sec
  FROM arr_dates WHERE sec IS NOT NULL AND (sec % 86400) BETWEEN 7*3600 AND 10*3600-1
  UNION ALL
  SELECT 'PM' AS win, d, mode, route_id, stop_id, sec
  FROM arr_dates WHERE sec IS NOT NULL AND (sec % 86400) BETWEEN 16*3600 AND 19*3600-1
),
hw_dates AS (
  SELECT win, d, mode, route_id, stop_id,
         sec - lag(sec) OVER (PARTITION BY win, d, mode, route_id, stop_id ORDER BY sec) AS dh
  FROM win_dates
),
route_med_dates AS (
  SELECT win, d, mode, route_id,
         percentile_cont(0.5) WITHIN GROUP (ORDER BY dh::double precision) AS med_sec
  FROM hw_dates
  WHERE dh IS NOT NULL AND dh>0 AND dh<=3600
  GROUP BY win, d, mode, route_id
),
day_med AS (
  SELECT win, d, percentile_cont(0.5) WITHIN GROUP (ORDER BY med_sec) AS day_med_sec
  FROM route_med_dates
  GROUP BY win, d
),
net_med_t3w AS (
  SELECT win,
         ROUND( ((percentile_cont(0.5) WITHIN GROUP (ORDER BY day_med_sec)) / 60.0)::numeric, 2 ) AS minutes
  FROM day_med
  GROUP BY win
)

SELECT 'AM' AS window, 'T3_all' AS variant, (SELECT minutes FROM net_med_all WHERE win='AM') AS minutes
UNION ALL
SELECT 'PM', 'T3_all', (SELECT minutes FROM net_med_all WHERE win='PM')
UNION ALL
SELECT 'AM', 'T3W_MULTI_event', (SELECT minutes FROM net_med_t3w WHERE win='AM')
UNION ALL
SELECT 'PM', 'T3W_MULTI_event', (SELECT minutes FROM net_med_t3w WHERE win='PM');

\o
