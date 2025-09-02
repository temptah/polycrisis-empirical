# Polycrisis Empirical Stack

This repo contains the empirical pipeline for the Infrastructure Fragility Index (IFI):
- Dockerized Postgres+PostGIS
- Schemas for indicators, IFI, events/impacts
- Python utilities for DB connectivity

## Quick start
1) docker compose up -d
2) Verify DB: python src/utils/db_test.py
