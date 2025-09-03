-- Indexes that make the T3 headway query fast and repeatable
CREATE INDEX IF NOT EXISTS idx_gtfs_stop_times_stop_bus     ON raw.gtfs_stop_times_bus(stop_id);
CREATE INDEX IF NOT EXISTS idx_gtfs_stop_times_stop_fixed   ON raw.gtfs_stop_times_fixed(stop_id);

-- Already had trip_id indexes from the loader; include IF NOT EXISTS for idempotency
CREATE INDEX IF NOT EXISTS idx_gtfs_stop_times_trip_bus     ON raw.gtfs_stop_times_bus(trip_id);
CREATE INDEX IF NOT EXISTS idx_gtfs_stop_times_trip_fixed   ON raw.gtfs_stop_times_fixed(trip_id);

-- Helpful when grouping on route_id or filtering by service_id (weekday variant)
CREATE INDEX IF NOT EXISTS idx_gtfs_trips_trip_bus          ON raw.gtfs_trips_bus(trip_id);
CREATE INDEX IF NOT EXISTS idx_gtfs_trips_trip_fixed        ON raw.gtfs_trips_fixed(trip_id);
CREATE INDEX IF NOT EXISTS idx_gtfs_trips_route_bus         ON raw.gtfs_trips_bus(route_id);
CREATE INDEX IF NOT EXISTS idx_gtfs_trips_route_fixed       ON raw.gtfs_trips_fixed(route_id);
CREATE INDEX IF NOT EXISTS idx_gtfs_trips_service_bus       ON raw.gtfs_trips_bus(service_id);
CREATE INDEX IF NOT EXISTS idx_gtfs_trips_service_fixed     ON raw.gtfs_trips_fixed(service_id);

ANALYZE raw.gtfs_stop_times_bus;
ANALYZE raw.gtfs_stop_times_fixed;
ANALYZE raw.gtfs_trips_bus;
ANALYZE raw.gtfs_trips_fixed;
ANALYZE raw.gtfs_stops_geom_bus;
ANALYZE raw.gtfs_stops_geom_fixed;
