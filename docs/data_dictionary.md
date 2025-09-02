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
