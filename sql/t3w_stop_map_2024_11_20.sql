-- t3w_stop_map_2024_11_20.sql → writes a GeoPackage from Postgres
\set ON_ERROR_STOP on

-- Compute per-stop median headway for the selected weekday
WITH target AS (
  SELECT DATE ''2024-11-20'' AS d, to_char(DATE ''2024-11-20'',''YYYYMMDD'') AS dstr
),
services AS (
  SELECT * FROM (
    SELECT ''bus''::text AS mode, c.service_id::text AS service_id FROM raw.gtfs_calendar_bus c, target t
    WHERE to_date(c.start_date,''YYYYMMDD'')<=t.d AND to_date(c.end_date,''YYYYMMDD'')>=t.d
      AND EXTRACT(DOW FROM t.d)=EXTRACT(DOW FROM t.d)  -- keep day-of-week from calendar
    UNION ALL
    SELECT ''bus'', cd.service_id::text FROM raw.gtfs_calendar_dates_bus cd, target t WHERE cd.date=t.dstr AND cd.exception_type=1
  ) u
  EXCEPT
  SELECT ''bus'', cd.service_id::text FROM raw.gtfs_calendar_dates_bus cd, target t WHERE cd.date=t.dstr AND cd.exception_type=2
),
rgn AS (SELECT geom FROM meta.region WHERE iso_code=''EL30''),
stops_attica AS (
  SELECT s.stop_id, s.mode, s.geom
  FROM raw.gtfs_stops_geom_all s, rgn
  WHERE ST_Intersects(s.geom, rgn.geom)
),
arr AS (
  SELECT st.stop_id, st.mode,
         CASE
           WHEN st.arrival_time ~ ''^[0-9]{1,2}:[0-9]{2}:[0-9]{2}$''
             OR st.arrival_time ~ ''^[0-9]{2,}:[0-9]{2}:[0-9]{2}$''
           THEN (split_part(st.arrival_time,'':'',1)::int*3600
               +  split_part(st.arrival_time,'':'',2)::int*60
               +  split_part(st.arrival_time,'':'',3)::int)
           ELSE NULL
         END AS sec
  FROM services s
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
  SELECT stop_id, mode,
         sec - lag(sec) OVER (PARTITION BY stop_id, mode ORDER BY sec) AS dh
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
SELECT sa.stop_id, sa.mode,
       (sm.med_sec/60.0) AS med_min,
       sa.geom
INTO TEMP t3w_stop_map
FROM stop_median sm
JOIN stops_attica sa ON sa.stop_id=sm.stop_id AND sa.mode=sm.mode;

-- Export to GeoPackage on the container FS
\copy (SELECT * FROM t3w_stop_map) TO PROGRAM 'ogr2ogr -f GPKG /tmp/attica_headways_2024-11-20.gpkg /vsistdin/ -nln headways -nlt POINT -skipfailures' WITH (FORMAT binary)
