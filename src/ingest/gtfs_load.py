import os, zipfile, io, csv
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
    ("stop_times",     "stop_times.txt"),  # handled via robust loader
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
                # Use python engine + on_bad_lines=skip to be forgiving
                return pd.read_csv(f, dtype=dtypes, encoding=enc, engine="python", on_bad_lines="skip")
        except Exception as e:
            last_err = e
            continue
    raise RuntimeError(f"Failed to read {member}: {last_err}")

def _clean_stop_times_bytes(b: bytes) -> bytes:
    """
    Decode forgivingly, normalize newlines, and merge lines when quotes span lines.
    This helps when stop_times.txt has rogue quotes or CR/LF issues.
    """
    s = b.decode("utf-8", errors="ignore").replace("\r\n", "\n").replace("\r", "\n")
    lines = s.split("\n")

    out_lines = []
    buf = []
    in_quotes = False

    for ln in lines:
        doubled = ln.count('""')
        q = ln.count('"') - 2 * doubled
        buf.append(ln)
        if q % 2 != 0:
            in_quotes = not in_quotes
        if not in_quotes:
            out_lines.append("\n".join(buf))
            buf = []

    if buf:
        # file ended while still open quotes -> stitch remainder
        out_lines.append(" ".join(buf))
    fixed = "\n".join(out_lines)
    if not fixed.endswith("\n"):
        fixed += "\n"
    return fixed.encode("utf-8")

def iter_stop_times_chunks(zf: zipfile.ZipFile, member: str):
    """
    Yield DataFrame chunks for stop_times with layered fallbacks:
    (1) normal tolerant parse
    (2) quoting=QUOTE_NONE + escapechar
    (3) pre-clean + tolerant parse (QUOTE_NONE)
    """
    if member not in zf.namelist():
        return

    # (1) Normal tolerant parse
    try:
        with zf.open(member) as f:
            for chunk in pd.read_csv(
                f,
                dtype=DTYPES["stop_times"],
                chunksize=200_000,
                encoding="utf-8",
                engine="python",
                on_bad_lines="skip",
            ):
                yield chunk
        return
    except Exception as e1:
        print(f"[warn] stop_times: tolerant parse failed ({e1}); trying QUOTE_NONE…")

    # (2) Ignore quotes completely (helps when quotes are unbalanced)
    try:
        with zf.open(member) as f:
            for chunk in pd.read_csv(
                f,
                dtype=DTYPES["stop_times"],
                chunksize=200_000,
                encoding="utf-8",
                engine="python",
                on_bad_lines="skip",
                quoting=csv.QUOTE_NONE,
                escapechar="\\",
            ):
                yield chunk
        return
    except Exception as e2:
        print(f"[warn] stop_times: QUOTE_NONE parse failed ({e2}); pre-cleaning bytes…")

    # (3) Pre-clean the bytes, then parse
    raw = zf.read(member)
    cleaned = _clean_stop_times_bytes(raw)
    bio = io.BytesIO(cleaned)
    try:
        for chunk in pd.read_csv(
            bio,
            dtype=DTYPES["stop_times"],
            chunksize=200_000,
            encoding="utf-8",
            engine="python",
            on_bad_lines="skip",
            quoting=csv.QUOTE_NONE,
            escapechar="\\",
        ):
            yield chunk
    except Exception as e3:
        raise RuntimeError(f"stop_times parse failed even after cleaning: {e3}") from e3

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

        # stop_times: chunked + robust fallbacks
        member = "stop_times.txt"
        if member in zf.namelist():
            table = f"gtfs_stop_times{suffix}"
            first = True
            total = 0
            for chunk in iter_stop_times_chunks(zf, member):
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
    print(f"GTFS load complete → {zip_path}  (suffix: '{suffix or ''}')")

def main():
    import argparse
    load_dotenv()
    ap = argparse.ArgumentParser(description="Load a GTFS zip into Postgres (raw schema) with optional suffix.")
    ap.add_argument("--zip",     dest="zip_path", required=True, help="Path to GTFS zip")
    ap.add_argument("--suffix",  default="", help="Suffix for table names (e.g., bus, fixed)")
    args = ap.parse_args()

    if not os.path.exists(args.zip_path):
        raise SystemExit(f"ZIP not found: {args.zip_path}")
    load_zip(args.zip_path, args.suffix)

if __name__ == "__main__":
    main()
