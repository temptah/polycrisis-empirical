DROP TABLE IF EXISTS raw.osm_water;
CREATE TABLE raw.osm_water AS
SELECT geom FROM raw.osm_water_stage;

DO $do$
BEGIN
  IF to_regclass('raw.osm_coastline_stage') IS NOT NULL THEN
    EXECUTE ''INSERT INTO raw.osm_water(geom) SELECT geom FROM raw.osm_coastline_stage'';
  END IF;
END
$do$;

CREATE INDEX IF NOT EXISTS idx_osm_water_geom ON raw.osm_water USING GIST (geom);
