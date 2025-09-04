\set ON_ERROR_STOP on
\copy (
WITH rgn AS (SELECT geom FROM meta.region WHERE iso_code='EL30'),
stops_attica AS (
  SELECT s.stop_id, s.mode FROM raw.gtfs_stops_geom_all s, rgn
  WHERE ST_Intersects(s.geom, rgn.geom)
),
stop_arrivals AS (
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
  JOIN stops_attica sa ON sa.stop_id=st.stop_id AND sa.mode=st.mode
),
params AS (SELECT 16*3600 AS sec_start, 19*3600 AS sec_end),
win AS (
  SELECT a.mode, a.route_id, a.stop_id, a.sec
  FROM stop_arrivals a, params p
  WHERE a.sec IS NOT NULL AND (a.sec % 86400) BETWEEN p.sec_start AND p.sec_end-1
),
headways AS (
  SELECT mode, route_id, stop_id,
         sec - lag(sec) OVER (PARTITION BY mode, route_id, stop_id ORDER BY sec) AS dh
  FROM win
),
headways_pos AS (
  SELECT mode, route_id, stop_id, dh
  FROM headways WHERE dh IS NOT NULL AND dh > 0 AND dh <= 3600
),
route_median AS (
  SELECT mode, route_id,
         percentile_cont(0.5) WITHIN GROUP (ORDER BY dh::double precision) AS med_sec
  FROM headways_pos
  GROUP BY mode, route_id
)
SELECT rm.mode, rm.route_id,
       coalesce(r.route_short_name,'') AS route_short_name,
       coalesce(r.route_long_name,'')  AS route_long_name,
       ROUND((rm.med_sec/60.0)::numeric,2) AS med_minutes
FROM route_median rm
LEFT JOIN raw.gtfs_routes_all r ON r.route_id=rm.route_id AND r.mode=rm.mode
ORDER BY rm.mode, med_minutes DESC
) TO '/tmp/t3_route_medians_16_19.csv' CSV HEADER
