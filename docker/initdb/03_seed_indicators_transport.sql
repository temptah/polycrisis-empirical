INSERT INTO meta.indicator (system_code, indicator_code, indicator_name, direction, unit, methodology) VALUES
 ('TRANSPORT','T1_EXPOSURE_FLOODPRONE_KM','Flood-prone network share','UP_IS_BAD','%', 'OSM network ∩ hazard zones; km at risk / total km'),
 ('TRANSPORT','T2_VULN_CENTRAL_EDGES_SHARE','Topologically critical edges share','UP_IS_BAD','%', 'Share of edges ≥ P90 edge betweenness'),
 ('TRANSPORT','T3_RECOVERY_HEADWAY_GAP','Headway gap during disruptions','UP_IS_BAD','minutes','Median (observed - scheduled)_+ over event window');
