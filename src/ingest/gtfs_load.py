import os, zipfile, argparse
import pandas as pd
from dotenv import load_dotenv
import sqlalchemy as sa
from src.config import make_engine

# Minimal dtypes to keep memory and types sane
DTYPES = {
    "routes": {
        "route_id": "string", "agency_id": "string", "route_short_name": "string",
        "route_long_name": "string", "route_type": "Int64", "route_color": "string",
    },
    "trips": {
        "route_id": "string", "service_id": "string", "trip_id": "string",
        "shape_id": "string", "direction_id": "Int64"
    },
    "stop_times": {
        "trip_id": "string", "arrival_time": "string", "departure_time": "string",
        "stop_id": "string", "stop_sequence": "Int64", "pickup_type": "Int64", "drop_off_type": "Int64"
    },
    "stops": {
        "stop_id": "string", "stop_code": "string", "stop_name": "string",
        "stop_lat": "float64", "stop_lon": "float64", "location_type": "Int64", "parent_station": "string"
    },
    "calendar": {
        "service_id": "string", "monday": "Int64", "tuesday": "Int64", "wednesday": "Int64",
        "thursday": "Int64", "friday": "Int64", "saturday": "Int64", "sunday": "Int64",
        "start_date": "string", "end_date": "string"
    },
    "calendar_dates": {
        "service_id": "string", "date": "string", "exception_type": "Int64"
    },
    "shapes": {
        "shape_id": "string", "shape_pt_lat": "float64", "shape_pt_lon": "float64", "shape_pt_sequence": "Int64"
    },
}

FILES = [
    ("routes",         "routes.txt"),
    ("trips",          "trips.txt"),
    ("stop_times",     "stop_times.txt"),  # handled chunked
    ("stops",          "stops.txt"),
    ("calendar",       "calendar.txt"),
    ("calendar_dates", "calendar_dates.txt"),
    ("shapes",         "shapes.txt"),
]

def read_member(zf: zipfile.ZipFile, member: str, dtypes: dict | None):
    """Try typical encodings (Greek feeds sometimes ship with BOM/CP1253)."""
    if member not in zf.namelist():
        return None
    last_err = None
    for enc in ("utf-8", "utf-8-sig", "cp1253"):
        try:
            with zf.open(member) as f:
                return pd.read_csv(f, dtype=dtypes, low_memory=False, encoding=enc, engine="python", on_bad_lines="skip")
        except Exception as e:
            last_err = e
            continue
    raise RuntimeError(f"Failed to read {member}: {last_err}")

def load_zip(zip_path: str, suffix: str = ""):
    suffix = (suffix or "").strip()
    if suffix and not suffix.startswith("_"):
        suffix = "_" + suffix

    eng = make_engine()

    with zipfile.ZipFile(zip_path, "r") as zf:
        # Light tables first (not stop_times)
        for name, member in FILES:
            if name == "stop_times":
                continue
            df = read_member(zf, member, DTYPES.get(name))
            if df is None:
                print(f"[skip] {member} not in ZIP")
                continue
            table = f"gtfs_{name}{suffix}"
            df.to_sql(table, eng, schema="raw", if_exists="replace", index=False, method="multi", chunksize=50000)
            print(f"[ok] raw.{table}: {len(df):,} rows")

        # Chunked load for stop_times
        member = "stop_times.txt"
        if member in zf.namelist():
            first = True
            total = 0
            with zf.open(member) as f:
                for chunk in pd.read_csv(f, dtype=DTYPES["stop_times"], chunksize=250_000, low_memory=False, encoding="utf-8", engine="python", on_bad_lines="skip"):
                    table = f"gtfs_stop_times{suffix}"
                    chunk.to_sql(table, eng, schema="raw",
                                 if_exists=("replace" if first else "append"),
                                 index=False, method="multi")
                    total += len(chunk)
                    print(f"[ok] raw.{table} += {len(chunk):,} (total {total:,})")
                    first = False
        else:
            print(f"[skip] {member} not in ZIP")

    # Build geometry table + indexes
    with eng.begin() as con:
        con.exec_driver_sql(f"""
            DROP TABLE IF EXISTS raw.gtfs_stops_geom{suffix};
            CREATE TABLE raw.gtfs_stops_geom{suffix} AS
            SELECT s.*, ST_SetSRID(ST_MakePoint(s.stop_lon, s.stop_lat), 4326) AS geom
            FROM raw.gtfs_stops{suffix} s
            WHERE s.stop_lon IS NOT NULL AND s.stop_lat IS NOT NULL;

            CREATE INDEX IF NOT EXISTS idx_gtfs_stops_geom{suffix}      ON raw.gtfs_stops_geom{suffix} USING GIST (geom);
            CREATE INDEX IF NOT EXISTS idx_gtfs_trips_route{suffix}     ON raw.gtfs_trips{suffix}(route_id);
            CREATE INDEX IF NOT EXISTS idx_gtfs_stop_times_trip{suffix} ON raw.gtfs_stop_times{suffix}(trip_id);
            CREATE INDEX IF NOT EXISTS idx_gtfs_stops_id{suffix}        ON raw.gtfs_stops{suffix}(stop_id);
        """)
    print(f"GTFS load complete > {zip_path}  (suffix: '{suffix or ''}')")

def main():
    ap = argparse.ArgumentParser(description="Load a GTFS zip into Postgres (raw schema) with optional suffix.")
    ap.add_argument("--zip",     dest="zip_path", required=True, help="Path to GTFS zip")
    ap.add_argument("--suffix",  default="", help="Suffix for table names (e.g., bus, fixed)")
    args = ap.parse_args()

    load_dotenv()
    if not os.path.exists(args.zip_path):
        raise SystemExit(f"ZIP not found: {args.zip_path}")
    load_zip(args.zip_path, args.suffix)

if __name__ == "__main__":
    main()
