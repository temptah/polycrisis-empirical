# Data Dictionary (Living Document)
- meta.region: administrative region geometries (EPSG:4326)
- meta.system: five infrastructure systems (WATER, ENERGY, TRANSPORT, HEALTH, DIGITAL)
- meta.indicator: catalog of indicators (code, unit, direction, methodology)
- feat.indicator_value: raw & normalized values per indicator, region, period
- model.ifi_score: IFI per system & period
- raw.event / raw.impact: events and impacts used for validation

Conventions:
- Time windows: [time_start, time_end]
- Normalization: min-max to 0..1; direction 'UP_IS_BAD' or 'DOWN_IS_BAD'

Region in DB: Attica (EL30, NUTS-2) from GISCO NUTS 2021 GeoJSON (EPSG:4326).

raw.osm_roads: OSM (Geofabrik Greece PBF), clipped to Attica (EL30) via ogr2ogr; SRID=4326.

raw.impact (ERA5): heatwave metrics added via cdsapi (tmax_mean_c, tmax_area_max_c, days_* thresholds).
