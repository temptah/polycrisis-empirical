import os
import io
import csv
import zipfile
import argparse
import pandas as pd
from dotenv import load_dotenv
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
    ("stop_times",     "stop_times.txt"),  # handled with robust chunker
    ("stops",          "stops.txt"),
    ("calendar",       "calendar.txt"),
    ("calendar_dates", "calendar_dates.txt"),
    ("shapes",         "shapes.txt"),
]


def read_member(zf: zipfile.ZipFile, member: str, dtypes: dict | None):
    """
    Light reader for non-stop_times members.
    We let pandas choose the engine (fast C-engine when possible).
    """
    if member not in zf.namelist():
        return None
    with zf.open(member) as f:
        return pd.read_csv(f, dtype=dtypes)


def _normalize_stop_times_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize header to GTFS names; add missing cols as NA; keep canonical order.
    Also strips quotes/BOM/whitespace from header names.
    """
    expected = [
        "trip_id", "arrival_time", "departure_time",
        "stop_id", "stop_sequence", "pickup_type", "drop_off_type"
    ]

    def norm(s: str) -> str:
        s = str(s)
        # Remove BOM, surrounding quotes and whitespace
        s = s.lstrip("\ufeff").strip().strip('"').strip("'")
        # Normalize separators and case
        s = s.replace(" ", "_").replace("-", "_").lower()
        return s

    synonyms = {
        "tripid": "trip_id",
        "stopid": "stop_id",
        "stopsequence": "stop_sequence",
        "pickuptype": "pickup_type",
        "dropoff_type": "drop_off_type",
        "drop_offtype": "drop_off_type",
        "drop-off_type": "drop_off_type",
        # very rare typos seen in the wild
        "arrivaltime": "arrival_time",
        "departuretime": "departure_time",
    }

    cols = []
    for c in df.columns:
        n = norm(c)
        n = synonyms.get(n, n)
        cols.append(n)

    df = df.copy()
    df.columns = cols

    for col in expected:
        if col not in df.columns:
            df[col] = pd.NA

    return df[expected]


def _normalize_newlines(s: str) -> str:
    return s.replace("\r\n", "\n").replace("\r", "\n")


def _balance_and_drop_tail(text: str) -> tuple[str, int]:
    """
    Attempt to join lines across accidental newlines inside quoted fields.
    If the file ends still 'in quotes', drop the trailing unbalanced fragment (rows lost are negligible).
    Returns (fixed_text, n_dropped_lines_from_tail_buffer).
    """
    lines = text.split("\n")
    out_lines = []
    buf: list[str] = []
    in_quotes = False

    for ln in lines:
        # discount doubled quotes ("")
        doubled = ln.count('""')
        q = ln.count('"') - 2 * doubled

        buf.append(ln)
        if q % 2 != 0:
            in_quotes = not in_quotes

        if not in_quotes:
            out_lines.append("\n".join(buf))
            buf = []

    # If file ends while still "in quotes", drop the remainder entirely
    dropped = len(buf) if buf else 0

    fixed = "\n".join(out_lines)
    if not fixed.endswith("\n"):
        fixed += "\n"
    return fixed, dropped


def iter_stop_times_chunks(zf: zipfile.ZipFile, member: str):
    """
    Yield normalized stop_times chunks robustly:
    1) normal chunked parse (fast)
    2) python engine with on_bad_lines="skip"
    3) pre-clean in memory by balancing quotes and dropping any trailing unbalanced fragment
    4) last-resort: python engine with QUOTE_NONE (then normalize header)
    """
    # 1) normal
    try:
        with zf.open(member) as f:
            for chunk in pd.read_csv(f, dtype=DTYPES["stop_times"], chunksize=200_000):
                yield _normalize_stop_times_df(chunk)
        return
    except Exception as e:
        print(f"[warn] stop_times: normal parse failed ({e}); trying python engine with on_bad_lines=skip…")

    # 2) tolerant python
    try:
        with zf.open(member) as f:
            for chunk in pd.read_csv(
                f, dtype=DTYPES["stop_times"], chunksize=200_000,
                engine="python", on_bad_lines="skip"
            ):
                yield _normalize_stop_times_df(chunk)
        return
    except Exception as e:
        print(f"[warn] stop_times: python/skip failed ({e}); trying pre-clean with quote balancing…")

    # 3) pre-clean text
    raw = zf.read(member)
    text = raw.decode("utf-8", errors="ignore")
    text = _normalize_newlines(text)
    fixed, dropped = _balance_and_drop_tail(text)
    if dropped:
        print(f"[fix] stop_times: dropped {dropped} trailing unbalanced line(s) after quote-balancing.")
    with io.StringIO(fixed) as f:
        for chunk in pd.read_csv(
            f, dtype=DTYPES["stop_times"], chunksize=200_000, engine="python"
        ):
            yield _normalize_stop_times_df(chunk)
    return

    # 4) LAST resort (usually not needed now)
    # (kept here for completeness; unreachable because of the early return above)
    # raw = zf.read(member)
    # with io.BytesIO(raw) as f:
    #     for chunk in pd.read_csv(
    #         f, dtype=DTYPES["stop_times"], chunksize=200_000,
    #         engine="python", on_bad_lines="skip",
    #         quoting=csv.QUOTE_NONE, escapechar="\\"
    #     ):
    #         yield _normalize_stop_times_df(chunk)


def load_zip(zip_path: str, suffix: str = ""):
    suffix = (suffix or "").strip()
    if suffix and not suffix.startswith("_"):
        suffix = "_" + suffix

    eng = make_engine()

    with zipfile.ZipFile(zip_path, "r") as zf:
        # Light tables (everything except stop_times)
        for name, member in FILES:
            if name == "stop_times":
                continue
            df = read_member(zf, member, DTYPES.get(name))
            if df is None:
                print(f"[skip] {member} not in ZIP")
                continue
            table = f"gtfs_{name}{suffix}"
            df.to_sql(table, eng, schema="raw", if_exists="replace", index=False, method="multi", chunksize=50_000)
            print(f"[ok] raw.{table}: {len(df):,} rows")

        # stop_times (robust, chunked)
        member = "stop_times.txt"
        if member in zf.namelist():
            first = True
            total = 0
            for chunk in iter_stop_times_chunks(zf, member):
                table = f"gtfs_stop_times{suffix}"
                chunk.to_sql(
                    table, eng, schema="raw",
                    if_exists=("replace" if first else "append"),
                    index=False, method="multi"
                )
                total += len(chunk)
                print(f"[ok] raw.{table} += {len(chunk):,} (total {total:,})")
                first = False
        else:
            print(f"[skip] {member} not in ZIP")

    # Geometry + indexes
    with eng.begin() as con:
        con.exec_driver_sql(f"""
            DROP TABLE IF EXISTS raw.gtfs_stops_geom{suffix};
            CREATE TABLE raw.gtfs_stops_geom{suffix} AS
            SELECT s.*, ST_SetSRID(ST_MakePoint(s.stop_lon, s.stop_lat), 4326) AS geom
            FROM raw.gtfs_stops{suffix} s
            WHERE s.stop_lon IS NOT NULL AND s.stop_lat IS NOT NULL;

            CREATE INDEX IF NOT EXISTS idx_gtfs_stops_geom{suffix}
                ON raw.gtfs_stops_geom{suffix} USING GIST (geom);
            CREATE INDEX IF NOT EXISTS idx_gtfs_trips_route{suffix}
                ON raw.gtfs_trips{suffix}(route_id);
            CREATE INDEX IF NOT EXISTS idx_gtfs_stop_times_trip{suffix}
                ON raw.gtfs_stop_times{suffix}(trip_id);
            CREATE INDEX IF NOT EXISTS idx_gtfs_stops_id{suffix}
                ON raw.gtfs_stops{suffix}(stop_id);
        """)
    print(f"GTFS load complete → {zip_path}  (suffix: '{suffix or ''}')")


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
