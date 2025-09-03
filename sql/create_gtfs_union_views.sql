-- ROUTES (agency_id is missing in BUS feed; fill with NULL there)
CREATE OR REPLACE VIEW raw.gtfs_routes_all AS
SELECT
  route_id::text                           AS route_id,
  NULL::text                               AS agency_id,
  route_short_name                         AS route_short_name,
  route_long_name                          AS route_long_name,
  route_type::bigint                       AS route_type,
  'bus'::text                              AS mode
FROM raw.gtfs_routes_bus
UNION ALL
SELECT
  route_id::text                           AS route_id,
  agency_id::text                          AS agency_id,
  route_short_name                         AS route_short_name,
  route_long_name                          AS route_long_name,
  route_type::bigint                       AS route_type,
  'fixed'::text                            AS mode
FROM raw.gtfs_routes_fixed;

-- TRIPS (cast integers to BIGINT to match loader defaults)
CREATE OR REPLACE VIEW raw.gtfs_trips_all AS
SELECT
  route_id::text                           AS route_id,
  service_id::text                         AS service_id,
  trip_id::text                            AS trip_id,
  direction_id::bigint                     AS direction_id,
  shape_id::text                           AS shape_id,
  'bus'::text                              AS mode
FROM raw.gtfs_trips_bus
UNION ALL
SELECT
  route_id::text                           AS route_id,
  service_id::text                         AS service_id,
  trip_id::text                            AS trip_id,
  direction_id::bigint                     AS direction_id,
  shape_id::text                           AS shape_id,
  'fixed'::text                            AS mode
FROM raw.gtfs_trips_fixed;

-- STOP_TIMES (canonical 7 columns on both sides; BIGINT for integer codes)
CREATE OR REPLACE VIEW raw.gtfs_stop_times_all AS
SELECT
  trip_id::text                            AS trip_id,
  arrival_time                             AS arrival_time,
  departure_time                           AS departure_time,
  stop_id::text                            AS stop_id,
  stop_sequence::bigint                    AS stop_sequence,
  pickup_type::bigint                      AS pickup_type,
  drop_off_type::bigint                    AS drop_off_type,
  'bus'::text                              AS mode
FROM raw.gtfs_stop_times_bus
UNION ALL
SELECT
  trip_id::text                            AS trip_id,
  arrival_time                             AS arrival_time,
  departure_time                           AS departure_time,
  stop_id::text                            AS stop_id,
  stop_sequence::bigint                    AS stop_sequence,
  pickup_type::bigint                      AS pickup_type,
  drop_off_type::bigint                    AS drop_off_type,
  'fixed'::text                            AS mode
FROM raw.gtfs_stop_times_fixed;

-- STOPS (BUS feed lacks location_type/parent_station → fill with NULLs)
CREATE OR REPLACE VIEW raw.gtfs_stops_all AS
SELECT
  stop_id::text                            AS stop_id,
  stop_code                                AS stop_code,
  stop_name                                AS stop_name,
  stop_lat::double precision               AS stop_lat,
  stop_lon::double precision               AS stop_lon,
  NULL::bigint                             AS location_type,
  NULL::text                               AS parent_station,
  'bus'::text                              AS mode
FROM raw.gtfs_stops_bus
UNION ALL
SELECT
  stop_id::text                            AS stop_id,
  stop_code                                AS stop_code,
  stop_name                                AS stop_name,
  stop_lat::double precision               AS stop_lat,
  stop_lon::double precision               AS stop_lon,
  location_type::bigint                    AS location_type,
  parent_station::text                     AS parent_station,
  'fixed'::text                            AS mode
FROM raw.gtfs_stops_fixed;

-- STOPS GEOMETRY (explicit list to ensure both sides match 1:1)
CREATE OR REPLACE VIEW raw.gtfs_stops_geom_all AS
SELECT
  stop_id::text                            AS stop_id,
  stop_code                                AS stop_code,
  stop_name                                AS stop_name,
  stop_lat::double precision               AS stop_lat,
  stop_lon::double precision               AS stop_lon,
  geom                                     AS geom,
  'bus'::text                              AS mode
FROM raw.gtfs_stops_geom_bus
UNION ALL
SELECT
  stop_id::text                            AS stop_id,
  stop_code                                AS stop_code,
  stop_name                                AS stop_name,
  stop_lat::double precision               AS stop_lat,
  stop_lon::double precision               AS stop_lon,
  geom                                     AS geom,
  'fixed'::text                            AS mode
FROM raw.gtfs_stops_geom_fixed;
