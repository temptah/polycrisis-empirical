import os
from math import isfinite
from statistics import quantiles
import numpy as np
import pandas as pd
import sqlalchemy as sa
import networkx as nx
from dotenv import load_dotenv

"""
T2_VULN_CENTRAL_EDGES_SHARE
- Build a graph from major OSM roads in Attica (EL30)
- Compute approximate edge betweenness centrality (k-sampled)
- T2 = share (%) of edges with centrality >= 90th percentile
- Store (raw) in feat.indicator_value for 2024
"""

MAJOR_HIGHWAYS = (
    "motorway","motorway_link","trunk","trunk_link",
    "primary","primary_link","secondary","secondary_link",
    "tertiary","tertiary_link"
)

TIME_START = "2024-01-01"
TIME_END   = "2024-12-31"
REGION_ISO = "EL30"
IND_CODE   = "T2_VULN_CENTRAL_EDGES_SHARE"
SOURCE_STR = "OSM roads (major classes) in Attica; NetworkX edge betweenness (k-sampled); rounded nodes @1e-5 deg"

def node_key(x, y, ndp=5):
    # Round coordinates to a small grid so identical junctions snap to same node
    return (round(float(x), ndp), round(float(y), ndp))

def main():
    load_dotenv()
    url = sa.URL.create(
        "postgresql+psycopg2",
        username=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "postgres"),
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "5432")),
        database=os.getenv("DB_NAME", "postgres"),
    )
    eng = sa.create_engine(url, future=True)

    with eng.connect() as con:
        # Region + indicator ids
        region_id = con.exec_driver_sql(
            "SELECT region_id FROM meta.region WHERE iso_code = %s;", (REGION_ISO,)
        ).scalar_one()
        indicator_id = con.exec_driver_sql(
            "SELECT indicator_id FROM meta.indicator WHERE indicator_code = %s;", (IND_CODE,)
        ).scalar_one()

        # Pull major roads; create endpoints in SQL for speed
        df = pd.read_sql(
            sa.text(f"""
                SELECT
                  row_number() OVER () AS edge_id,
                  highway,
                  ST_X(ST_StartPoint(geom)) AS x1,
                  ST_Y(ST_StartPoint(geom)) AS y1,
                  ST_X(ST_EndPoint(geom))   AS x2,
                  ST_Y(ST_EndPoint(geom))   AS y2
                FROM raw.osm_roads
                WHERE highway = ANY(:major)
            """),
            con,
            params={"major": list(MAJOR_HIGHWAYS)}
        )

    if df.empty:
        raise SystemExit("No major road edges found in raw.osm_roads — check import/filters.")

    # Build an undirected graph with rounded endpoints
    G = nx.Graph()
    edges = []
    for _, r in df.iterrows():
        u = node_key(r.x1, r.y1)
        v = node_key(r.x2, r.y2)
        if u != v:
            edges.append((u, v))
    G.add_edges_from(edges)

    # Keep it tractable: sample k nodes for approximate centrality
    # Choose k relative to graph size (up to 1500)
    k = min(1500, max(200, int(0.02 * G.number_of_nodes())))
    ec = nx.edge_betweenness_centrality(G, k=k, seed=42)
    vals = np.fromiter(ec.values(), dtype=float)
    vals = vals[np.isfinite(vals)]

    if vals.size == 0:
        raise SystemExit("Centrality returned no values—graph may be empty after filtering.")

    p90 = float(np.quantile(vals, 0.90))
    share = float((vals >= p90).mean() * 100.0)

    print(f"Graph nodes: {G.number_of_nodes():,}, edges: {G.number_of_edges():,}")
    print(f"Edge betweenness p90: {p90:.6g}")
    print(f"T2 share >= p90: {share:.3f}%")

    # Write to DB (raw value; value_norm is NULL for now)
    with eng.begin() as con:
        con.exec_driver_sql(
            """
            INSERT INTO feat.indicator_value
                (region_id, indicator_id, time_start, time_end, value_raw, value_norm, source)
            VALUES
                (%s, %s, %s, %s, %s, NULL, %s)
            ON CONFLICT (region_id, indicator_id, time_start, time_end)
            DO UPDATE SET value_raw=EXCLUDED.value_raw, source=EXCLUDED.source;
            """,
            (region_id, indicator_id, TIME_START, TIME_END, share, SOURCE_STR)
        )

if __name__ == "__main__":
    main()
