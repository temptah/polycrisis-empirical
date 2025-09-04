-- sql/seed_event_param.sql
-- Params: :iso, :code, :start, :end
WITH rid AS (
  SELECT region_id FROM meta.region WHERE iso_code = :'iso'
)
INSERT INTO meta.event (event_code, event_name, time_start, time_end, region_id)
SELECT :'code',
       'Heatwave 2024 ('||:'iso'||')',
       :'start'::date,
       :'end'::date,
       (SELECT region_id FROM rid)
WHERE NOT EXISTS (SELECT 1 FROM meta.event WHERE event_code = :'code');
