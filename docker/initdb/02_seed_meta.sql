INSERT INTO meta.system (system_code, system_name) VALUES
 ('WATER','Water Supply'), ('ENERGY','Electric Power'), ('TRANSPORT','Transport'),
 ('HEALTH','Health System'), ('DIGITAL','Digital & ICT')
ON CONFLICT DO NOTHING;

-- Temporary placeholder region polygon (replace on Day-2 with Attica EL30)
INSERT INTO meta.region (region_name, iso_code, geom)
VALUES ('PilotRegion', 'GR-AT',
        ST_Multi(
          ST_GeomFromText(
            'POLYGON((23.5 37.8, 24.2 37.8, 24.2 38.2, 23.5 38.2, 23.5 37.8))', 4326
          )
        )
) ON CONFLICT DO NOTHING;
