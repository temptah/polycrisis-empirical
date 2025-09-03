WITH e AS (
  INSERT INTO raw.event (region_id, event_type, t_start, t_end, description)
  SELECT region_id,
         'heatwave',
         TIMESTAMP '2024-07-08 00:00:00',
         TIMESTAMP '2024-07-23 23:59:59',
         'Athens/Attica heatwave ~16 days (Jul 8–23, 2024). Placeholder impact=hazard_days.'
  FROM meta.region
  WHERE iso_code = 'EL30'
  RETURNING event_id
)
INSERT INTO raw.impact (event_id, metric, value)
SELECT event_id, 'hazard_days', 16 FROM e;

-- Verify
SELECT e.event_id, e.event_type, e.t_start, e.t_end, i.metric, i.value
FROM raw.event e
LEFT JOIN raw.impact i USING (event_id)
WHERE e.event_type='heatwave' AND e.t_start::date='2024-07-08';
