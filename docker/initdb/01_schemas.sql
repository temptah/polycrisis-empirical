-- Schemas
CREATE SCHEMA IF NOT EXISTS meta;
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS feat;
CREATE SCHEMA IF NOT EXISTS model;
CREATE SCHEMA IF NOT EXISTS outputs;

-- Dimensions
CREATE TABLE IF NOT EXISTS meta.region (
  region_id SERIAL PRIMARY KEY,
  region_name TEXT UNIQUE,
  iso_code TEXT,
  geom GEOMETRY(MULTIPOLYGON, 4326)
);

CREATE TABLE IF NOT EXISTS meta.system (
  system_id SERIAL PRIMARY KEY,
  system_code TEXT UNIQUE,   -- WATER, ENERGY, TRANSPORT, HEALTH, DIGITAL
  system_name TEXT
);

CREATE TABLE IF NOT EXISTS meta.indicator (
  indicator_id SERIAL PRIMARY KEY,
  system_code TEXT,
  indicator_code TEXT UNIQUE, -- e.g., T1_EXPOSURE_FLOODPRONE_KM
  indicator_name TEXT,
  direction TEXT,             -- 'UP_IS_BAD' or 'DOWN_IS_BAD'
  unit TEXT,
  methodology TEXT
);

-- Fact: indicator values (normalized & raw)
CREATE TABLE IF NOT EXISTS feat.indicator_value (
  region_id INT REFERENCES meta.region(region_id),
  indicator_id INT REFERENCES meta.indicator(indicator_id),
  time_start DATE,
  time_end   DATE,
  value_raw DOUBLE PRECISION,
  value_norm DOUBLE PRECISION,
  source TEXT,
  PRIMARY KEY (region_id, indicator_id, time_start, time_end)
);

-- IFI scores per system
CREATE TABLE IF NOT EXISTS model.ifi_score (
  region_id INT REFERENCES meta.region(region_id),
  system_code TEXT,
  time_start DATE,
  time_end DATE,
  ifi DOUBLE PRECISION,        -- 0..1
  ifi_ci_low DOUBLE PRECISION,
  ifi_ci_high DOUBLE PRECISION,
  PRIMARY KEY (region_id, system_code, time_start, time_end)
);

-- Events & impacts (for validation)
CREATE TABLE IF NOT EXISTS raw.event (
  event_id SERIAL PRIMARY KEY,
  region_id INT REFERENCES meta.region(region_id),
  event_type TEXT,             -- heatwave, storm, blackout, etc.
  t_start TIMESTAMP,
  t_end   TIMESTAMP,
  description TEXT
);

CREATE TABLE IF NOT EXISTS raw.impact (
  event_id INT REFERENCES raw.event(event_id),
  metric TEXT,                 -- downtime_hours, persons_affected, excess_mortality
  value DOUBLE PRECISION
);
