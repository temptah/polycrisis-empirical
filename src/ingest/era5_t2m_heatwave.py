import os, datetime as dt
import numpy as np
import pandas as pd
import xarray as xr
import cdsapi
import sqlalchemy as sa
from dotenv import load_dotenv
from src.config import make_engine

REGION_ISO = "EL30"
EVENT_DATE = "2024-07-08"   # event start date we inserted
START = dt.date(2024,7,8); END = dt.date(2024,7,23)

def get_area_from_db():
    eng = make_engine()
    with eng.connect() as con:
        row = con.exec_driver_sql("""
            SELECT ST_YMax(ext), ST_XMin(ext), ST_YMin(ext), ST_XMax(ext)
            FROM (SELECT ST_Extent(geom) AS ext FROM meta.region WHERE iso_code=%s) s;
        """, (REGION_ISO,)).first()
    north, west, south, east = map(float, row)
    pad = 0.1
    return [north+pad, west-pad, south-pad, east+pad]  # [N, W, S, E] for CDS

def download_nc(area, out_nc):
    c = cdsapi.Client()
    days = [d.strftime("%d") for d in pd.date_range(START, END, freq="D")]
    times = [f"{h:02d}:00" for h in range(24)]
    c.retrieve(
        "reanalysis-era5-single-levels",
        {
            "product_type": "reanalysis",
            "variable": ["2m_temperature"],
            "year": "2024",
            "month": "07",
            "day": days,
            "time": times,
            "area": area,   # [N, W, S, E]
            "format": "netcdf",
        },
        out_nc,
    )

def process_and_insert(out_nc):
    ds = xr.open_dataset(out_nc, engine="h5netcdf")
    t2m_c = ds["t2m"] - 273.15
    daily_max = t2m_c.resample(time="1D").max()
    area_mean = daily_max.mean(dim=("latitude","longitude"))
    metrics = {
        "tmax_mean_c": float(area_mean.mean().values),
        "tmax_max_c":  float(area_mean.max().values),
        "days_tmax_ge_37c": int((area_mean >= 37.0).sum().values),
        "days_tmax_ge_40c": int((area_mean >= 40.0).sum().values),
    }

    eng = make_engine()
    with eng.begin() as con:
        event_id = con.exec_driver_sql("""
            SELECT event_id
            FROM raw.event
            WHERE region_id=(SELECT region_id FROM meta.region WHERE iso_code=%s)
              AND event_type='heatwave' AND DATE(t_start)=%s;
        """, (REGION_ISO, EVENT_DATE)).scalar_one()

        # Upsert by deleting the metric if present, then inserting
        for metric, value in metrics.items():
            con.exec_driver_sql(
                "DELETE FROM raw.impact WHERE event_id=%s AND metric=%s;",
                (event_id, metric),
            )
            con.exec_driver_sql(
                "INSERT INTO raw.impact (event_id, metric, value) VALUES (%s,%s,%s);",
                (event_id, metric, float(value)),
            )

    print("Inserted metrics:", metrics)

def main():
    load_dotenv()
    os.makedirs("data/external/era5", exist_ok=True)
    out_nc = "data/external/era5/era5_t2m_attica_20240708_20240723.nc"
    area = get_area_from_db()
    print("CDS area [N,W,S,E]:", area)
    if not os.path.exists(out_nc):
        download_nc(area, out_nc)
    process_and_insert(out_nc)

if __name__ == "__main__":
    main()
