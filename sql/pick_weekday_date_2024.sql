WITH bus_range AS (
  SELECT GREATEST(MIN(to_date(start_date,'YYYYMMDD')), DATE '2024-01-01') AS start_d,
         LEAST(MAX(to_date(end_date,'YYYYMMDD')),   DATE '2024-12-31') AS end_d
  FROM raw.gtfs_calendar_bus
),
cand AS (
  SELECT d::date AS d
  FROM bus_range, LATERAL generate_series(start_d, end_d, interval '1 day') g(d)
  WHERE EXTRACT(DOW FROM d)=2  -- pick a Tuesday; change to 1..6/0 as you like
),
fixed_add AS (
  SELECT DISTINCT to_date(date,'YYYYMMDD') AS d
  FROM raw.gtfs_calendar_dates_fixed
  WHERE exception_type=1
),
pick AS (
  SELECT c.d
  FROM cand c
  JOIN fixed_add f ON f.d=c.d
  ORDER BY c.d DESC
  LIMIT 1
)
SELECT d FROM pick;
