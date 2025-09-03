-- list_candidate_weekdays_2024_octnov.sql
WITH daylist AS (
  SELECT d::date AS d, EXTRACT(DOW FROM d)::int AS dow
  FROM generate_series(DATE '2024-10-01', DATE '2024-11-30', INTERVAL '1 day') g(d)
  WHERE EXTRACT(DOW FROM d)::int IN (2,3,4)  -- Tue/Wed/Thu
),
bus_base AS (
  SELECT dl.d, COUNT(DISTINCT c.service_id) AS base_cnt
  FROM daylist dl
  JOIN raw.gtfs_calendar_bus c
    ON dl.d BETWEEN to_date(c.start_date,'YYYYMMDD') AND to_date(c.end_date,'YYYYMMDD')
   AND ((dl.dow=1 AND c.monday=1) OR (dl.dow=2 AND c.tuesday=1) OR (dl.dow=3 AND c.wednesday=1)
     OR (dl.dow=4 AND c.thursday=1) OR (dl.dow=5 AND c.friday=1) OR (dl.dow=6 AND c.saturday=1)
     OR (dl.dow=0 AND c.sunday=1))
  GROUP BY dl.d
),
bus_incl AS (
  SELECT to_date(date,'YYYYMMDD') AS d, COUNT(DISTINCT service_id) AS incl_cnt
  FROM raw.gtfs_calendar_dates_bus cd
  JOIN daylist dl ON dl.d = to_date(cd.date,'YYYYMMDD')
  WHERE cd.exception_type=1
  GROUP BY 1
),
bus_excl AS (
  SELECT to_date(date,'YYYYMMDD') AS d, COUNT(DISTINCT service_id) AS excl_cnt
  FROM raw.gtfs_calendar_dates_bus cd
  JOIN daylist dl ON dl.d = to_date(cd.date,'YYYYMMDD')
  WHERE cd.exception_type=2
  GROUP BY 1
),
fixed_incl AS (
  SELECT to_date(date,'YYYYMMDD') AS d, COUNT(DISTINCT service_id) AS incl_cnt
  FROM raw.gtfs_calendar_dates_fixed cd
  JOIN daylist dl ON dl.d = to_date(cd.date,'YYYYMMDD')
  WHERE cd.exception_type=1
  GROUP BY 1
),
fixed_excl AS (
  SELECT to_date(date,'YYYYMMDD') AS d, COUNT(DISTINCT service_id) AS excl_cnt
  FROM raw.gtfs_calendar_dates_fixed cd
  JOIN daylist dl ON dl.d = to_date(cd.date,'YYYYMMDD')
  WHERE cd.exception_type=2
  GROUP BY 1
)
SELECT dl.d,
       CASE dl.dow WHEN 0 THEN 'Sun' WHEN 1 THEN 'Mon' WHEN 2 THEN 'Tue'
                   WHEN 3 THEN 'Wed' WHEN 4 THEN 'Thu' WHEN 5 THEN 'Fri' ELSE 'Sat' END AS dow,
       COALESCE(bb.base_cnt,0)+COALESCE(bi.incl_cnt,0)-COALESCE(be.excl_cnt,0) AS bus_services,
       COALESCE(fi.incl_cnt,0)-COALESCE(fe.excl_cnt,0)                           AS fixed_services,
       (COALESCE(bb.base_cnt,0)+COALESCE(bi.incl_cnt,0)-COALESCE(be.excl_cnt,0)
        + COALESCE(fi.incl_cnt,0)-COALESCE(fe.excl_cnt,0))                        AS total_services
FROM daylist dl
LEFT JOIN bus_base  bb ON bb.d = dl.d
LEFT JOIN bus_incl  bi ON bi.d = dl.d
LEFT JOIN bus_excl  be ON be.d = dl.d
LEFT JOIN fixed_incl fi ON fi.d = dl.d
LEFT JOIN fixed_excl fe ON fe.d = dl.d
ORDER BY total_services DESC, bus_services DESC, fixed_services DESC, dl.d
LIMIT 12;
