WITH dates(d) AS (
  VALUES
    (DATE '2024-11-19'),
    (DATE '2024-11-20'),
    (DATE '2024-11-21'),
    (DATE '2024-11-26'),
    (DATE '2024-11-27'),
    (DATE '2024-11-28')
),
target AS (
  SELECT d, to_char(d,'YYYYMMDD') AS dstr, EXTRACT(DOW FROM d)::int AS dow FROM dates
),
base_bus AS (
  SELECT 'bus'::text AS mode, c.service_id::text AS service_id, t.d
  FROM raw.gtfs_calendar_bus c
  JOIN target t
    ON t.d BETWEEN to_date(c.start_date,'YYYYMMDD') AND to_date(c.end_date,'YYYYMMDD')
   AND ((t.dow=1 AND c.monday=1) OR (t.dow=2 AND c.tuesday=1) OR (t.dow=3 AND c.wednesday=1)
     OR (t.dow=4 AND c.thursday=1) OR (t.dow=5 AND c.friday=1) OR (t.dow=6 AND c.saturday=1)
     OR (t.dow=0 AND c.sunday=1))
),
incl_bus AS (
  SELECT 'bus'::text AS mode, cd.service_id::text AS service_id, t.d
  FROM raw.gtfs_calendar_dates_bus cd JOIN target t ON cd.date=t.dstr AND cd.exception_type=1
),
excl_bus AS (
  SELECT 'bus'::text AS mode, cd.service_id::text AS service_id, t.d
  FROM raw.gtfs_calendar_dates_bus cd JOIN target t ON cd.date=t.dstr AND cd.exception_type=2
),
services_bus AS (
  SELECT DISTINCT * FROM (SELECT * FROM base_bus UNION ALL SELECT * FROM incl_bus) u
  EXCEPT SELECT * FROM excl_bus
),
incl_fixed AS (
  SELECT 'fixed'::text AS mode, cd.service_id::text AS service_id, t.d
  FROM raw.gtfs_calendar_dates_fixed cd JOIN target t ON cd.date=t.dstr AND cd.exception_type=1
),
excl_fixed AS (
  SELECT 'fixed'::text AS mode, cd.service_id::text AS service_id, t.d
  FROM raw.gtfs_calendar_dates_fixed cd JOIN target t ON cd.date=t.dstr AND cd.exception_type=2
),
services_fixed AS (SELECT DISTINCT * FROM incl_fixed EXCEPT SELECT * FROM excl_fixed),
services_on_date AS (SELECT * FROM services_bus UNION ALL SELECT * FROM services_fixed),
rgn AS (SELECT geom FROM meta.region WHERE iso_code='EL30'),
stops_attica AS (
  SELECT s.stop_id, s.mode FROM raw.gtfs_stops_geom_all s, rgn
  WHERE ST_Intersects(s.geom, rgn.geom)
),
stop_arrivals AS (
  SELECT sod.d, st.mode, tr.route_id, st.stop_id,
         CASE
           WHEN st.arrival_time ~ '^[0-9]{1,2}:[0-9]{2}:[0-9]{2}$'
             OR st.arrival_time ~ '^[0-9]{2,}:[0-9]{2}:[0-9]{2}$'
           THEN (split_part(st.arrival_time,':',1)::int*3600
               +  split_part(st.arrival_time,':',2)::int*60
               +  split_part(st.arrival_time,':',3)::int)
           ELSE NULL
         END AS sec
  FROM services_on_date sod
  JOIN raw.gtfs_trips_all tr ON tr.service_id=sod.service_id AND tr.mode=sod.mode
  JOIN raw.gtfs_stop_times_all st ON st.trip_id=tr.trip_id AND st.mode=tr.mode
  JOIN stops_attica sa ON sa.stop_id=st.stop_id AND sa.mode=st.mode
),
params AS (SELECT 7*3600 AS sec_start, 10*3600 AS sec_end),
win AS (
  SELECT a.d, a.mode, a.route_id, a.stop_id, a.sec
  FROM stop_arrivals a, params p
  WHERE a.sec IS NOT NULL AND (a.sec % 86400) BETWEEN p.sec_start AND p.sec_end-1
),
headways AS (
  SELECT d, mode, route_id, stop_id,
         sec - lag(sec) OVER (PARTITION BY d, mode, route_id, stop_id ORDER BY sec) AS dh
  FROM win
),
headways_pos AS (
  SELECT d, mode, route_id, stop_id, dh
  FROM headways WHERE dh IS NOT NULL AND dh > 0 AND dh <= 3600
),
route_median AS (
  SELECT d, mode, route_id,
         percentile_cont(0.5) WITHIN GROUP (ORDER BY dh::double precision) AS med_sec
  FROM headways_pos
  GROUP BY d, mode, route_id
)
SELECT to_char(d,'YYYY-MM-DD') AS day,
       COUNT(*) AS routes,
       ROUND((percentile_cont(0.25) WITHIN GROUP (ORDER BY med_sec)/60.0)::numeric,2) AS p25_min,
       ROUND((percentile_cont(0.50) WITHIN GROUP (ORDER BY med_sec)/60.0)::numeric,2) AS p50_min,
       ROUND((percentile_cont(0.75) WITHIN GROUP (ORDER BY med_sec)/60.0)::numeric,2) AS p75_min
FROM route_median
GROUP BY d
ORDER BY d;
