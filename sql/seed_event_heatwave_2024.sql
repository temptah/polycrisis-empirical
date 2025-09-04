CREATE TABLE IF NOT EXISTS feat.event_window (
  event_code text NOT NULL,
  region_id integer NOT NULL,
  hazard text NOT NULL,
  time_start date NOT NULL,
  time_end date NOT NULL,
  description text,
  source text,
  PRIMARY KEY (event_code, region_id, time_start, time_end)
);

WITH ids AS (
  SELECT (SELECT region_id FROM meta.region WHERE iso_code='EL30') AS region_id
)
INSERT INTO feat.event_window (event_code, region_id, hazard, time_start, time_end, description, source)
VALUES (
  'HEATWAVE_2024',
  (SELECT region_id FROM ids),
  'heatwave',
  DATE '2024-06-11',
  DATE '2024-06-15',
  'Placeholder heatwave window for Attica; to be validated against ERA5 temperatures and EuroMOMO.',
  'Draft seed; will update after ERA5/EuroMOMO validation.'
)
ON CONFLICT (event_code, region_id, time_start, time_end)
DO UPDATE SET description=EXCLUDED.description, source=EXCLUDED.source;
