import os, numpy as np, pandas as pd, sqlalchemy as sa, networkx as nx
from dotenv import load_dotenv

MAJOR=("motorway","motorway_link","trunk","trunk_link","primary","primary_link",
       "secondary","secondary_link","tertiary","tertiary_link")
REGION_ISO="EL30"; IND_CODE="T2_VULN_CENTRAL_EDGES_SHARE"
TIME_START="2024-01-01"; TIME_END="2024-12-31"

def eng():
    load_dotenv()
    return sa.create_engine(sa.URL.create("postgresql+psycopg2",
        username=os.getenv("DB_USER","postgres"),
        password=os.getenv("DB_PASSWORD","postgres"),
        host=os.getenv("DB_HOST","localhost"),
        port=int(os.getenv("DB_PORT","5432")),
        database=os.getenv("DB_NAME","postgres")), future=True)

def node_key(x,y,n=5): return (round(float(x),n), round(float(y),n))

def main():
    e=eng()
    with e.connect() as c:
        region_id=c.exec_driver_sql("SELECT region_id FROM meta.region WHERE iso_code=%s;",(REGION_ISO,)).scalar_one()
        ind_id   =c.exec_driver_sql("SELECT indicator_id FROM meta.indicator WHERE indicator_code=%s;",(IND_CODE,)).scalar_one()
        q=sa.text("""
          WITH ln AS (
            SELECT highway,(ST_Dump(ST_LineMerge(geom))).geom AS geom
            FROM raw.osm_roads
            WHERE highway=ANY(:major) AND NOT ST_IsEmpty(geom)
          )
          SELECT ST_X(ST_StartPoint(geom)) AS x1, ST_Y(ST_StartPoint(geom)) AS y1,
                 ST_X(ST_EndPoint(geom))   AS x2, ST_Y(ST_EndPoint(geom))   AS y2,
                 ST_Length(geom::geography)/1000.0 AS km
          FROM ln WHERE ST_NPoints(geom)>=2;""")
        df=pd.read_sql(q,c,params={"major":list(MAJOR)})
    df=df.dropna(subset=["x1","y1","x2","y2","km"])
    if df.empty: raise SystemExit("No major segments found.")
    G=nx.Graph()
    for r in df.itertuples(index=False):
        u=node_key(r.x1,r.y1); v=node_key(r.x2,r.y2)
        if u!=v: G.add_edge(u,v,weight=float(r.km), km=float(r.km))
    k=min(1500,max(200,int(0.02*G.number_of_nodes())))
    ec=nx.edge_betweenness_centrality(G,k=k,weight="weight",seed=42)
    rows=[(c,G[u][v].get("km",0.0)) for (u,v),c in ec.items()]
    if not rows: raise SystemExit("No centrality values.")
    arr=np.array(rows,float); cvals, lens=arr[:,0], arr[:,1]
    p90=float(np.quantile(cvals,0.90))
    tot=float(lens.sum()); hi=float(lens[cvals>=p90].sum())
    share=100.0*hi/tot if tot>0 else None
    print(f"nodes={G.number_of_nodes():,} edges={G.number_of_edges():,} p90={p90:.6g} share_len={share:.3f}%")
    with eng().begin() as c:
        c.exec_driver_sql("""
          INSERT INTO feat.indicator_value
          (region_id,indicator_id,time_start,time_end,value_raw,value_norm,source)
          VALUES (%s,%s,%s,%s,%s,NULL,%s)
          ON CONFLICT (region_id,indicator_id,time_start,time_end)
          DO UPDATE SET value_raw=EXCLUDED.value_raw, source=EXCLUDED.source;""",
          (region_id,ind_id,TIME_START,TIME_END,share,
           "Length-weighted edge betweenness (major roads); value_raw=% of km >=p90"))
if __name__=="__main__": main()
