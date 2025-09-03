-- Set the target weekday here:
WITH target AS (
  SELECT DATE '2024-10-15' AS d,
         to_char(DATE '2024-10-15','YYYYMMDD') AS dstr,
         EXTRACT(DOW FROM DATE '2024-10-15')::int AS dow
), base_bus AS (
  -- calendar.txt (bus) within date range and matching day-of-week
  SELECT DISTINCT 'bus'::text AS mode, c.service_id::text AS service_id
  FROM raw.gtfs_calendar_bus c, target t
  WHERE to_date(c.start_date,'YYYYMMDD') <= t.d
    AND to_date(c.end_date,  'YYYYMMDD') >= t.d
    AND (
      (t.dow=1 AND c.monday=1) OR
      (t.dow=2 AND c.tuesday=1) OR
      (t.dow=3 AND c.wednesday=1) OR
      (t.dow=4 AND c.thursday=1) OR
      (t.dow=5 AND c.friday=1) OR
      (t.dow=6 AND c.saturday=1) OR
      (t.dow=0 AND c.sunday=1)
    )
), incl_bus AS (
  SELECT 'bus'::text AS mode, cd.service_id::text AS service_id
  FROM raw.gtfs_calendar_dates_bus cd, target t
  WHERE cd.date = t.dstr AND cd.exception_type = 1
), excl_bus AS (
  SELECT 'bus'::text AS mode, cd.service_id::text AS service_id
  FROM raw.gtfs_calendar_dates_bus cd, target t
  WHERE cd.date = t.dstr AND cd.exception_type = 2
), services_bus AS (
  -- (base ∪ include) \ exclude
  SELECT DISTINCT * FROM (
    SELECT * FROM base_bus
    UNION ALL
    SELECT * FROM incl_bus
  ) u
  EXCEPT
  SELECT * FROM excl_bus
), incl_fixed AS (
  -- fixed has only calendar_dates in this feed
  SELECT 'fixed'::text AS mode, cd.service_id::text AS service_id
  FROM raw.gtfs_calendar_dates_fixed cd, target t
  WHERE cd.date = t.dstr AND cd.exception_type = 1
), excl_fixed AS (
  SELECT 'fixed'::text AS mode, cd.service_id::text AS service_id
  FROM raw.gtfs_calendar_dates_fixed cd, target t
  WHERE cd.date = t.dstr AND cd.exception_type = 2
), services_fixed AS (
  SELECT DISTINCT * FROM incl_fixed
  EXCEPT
  SELECT * FROM excl_fixed
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
           -- numeric-class regex avoids backslash issues
           WHEN st.arrival_time ~ '^[0-9]{1,2}:[0-9]{2}:[0-9]{2}$'
             OR st.arrival_time ~ '^[0-9]{2,}:[0-9]{2}:[0-9]{2}$'
           THEN (split_part(st.arrival_time,':',1)::int*3600
               +  split_part(st.arrival_time,':',2)::int*60
               +  split_part(st.arrival_time,':',3)::int)
           ELSE NULL
         END AS sec
  FROM raw.gtfs_stop_times_all st
  JOIN raw.gtfs_trips_all t
    ON t.trip_id=st.trip_id AND t.mode=st.mode
  JOIN services_on_date s
    ON s.service_id=t.service_id AND s.mode=t.mode
  JOIN stops_attica sa
    ON sa.stop_id=st.stop_id AND sa.mode=st.mode
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
)
SELECT ROUND((percentile_cont(0.5) WITHIN GROUP (ORDER BY med_sec)/60.0)::numeric, 2)
  AS weekday_network_median_minutes
FROM route_median;
