import os, io, csv, zipfile, argparse
from typing import List, Tuple
import pandas as pd

FILES = [
    "agency.txt",
    "routes.txt",
    "trips.txt",
    "stops.txt",
    "calendar.txt",
    "calendar_dates.txt",
    "shapes.txt",
    "stop_times.txt",
]

def detect_encoding(b: bytes) -> str:
    for enc in ("utf-8", "utf-8-sig", "cp1253"):
        try:
            b.decode(enc)
            return enc
        except Exception:
            continue
    return "utf-8"

def normalize_newlines(s: str) -> str:
    return s.replace("\r\n","\n").replace("\r","\n")

def unbalanced_quote_lines(lines: List[str]) -> List[int]:
    bad = []
    in_quotes = False
    for i, ln in enumerate(lines, start=1):
        # discount doubled quotes ("")
        doubled = ln.count('""')
        q = ln.count('"') - 2*doubled
        if q % 2 != 0:
            in_quotes = not in_quotes
            bad.append(i)
    # if we end "inside quotes", mark the last line
    return bad

def sample_reader_fields(sample_text: str) -> Tuple[List[str], int]:
    # Try csv.Sniffer first; fall back to simple split
    try:
        sniffer = csv.Sniffer()
        dialect = sniffer.sniff(sample_text, delimiters=",;\t")
        reader = csv.reader(sample_text.splitlines(), dialect=dialect)
        row0 = next(reader, [])
        return row0, len(row0)
    except Exception:
        header = sample_text.splitlines()[0]
        cols = header.split(",")
        return cols, len(cols)

def preview_member(zf: zipfile.ZipFile, name: str, max_bytes: int = 200_000):
    if name not in zf.namelist():
        print(f"[missing] {name}")
        return
    raw = zf.read(name)
    enc = detect_encoding(raw)
    text = normalize_newlines(raw.decode(enc, errors="ignore"))
    lines = text.split("\n")
    header = lines[0] if lines else ""
    preview = "\n".join(lines[:5])

    print(f"\n== {name} ==")
    print(f"- encoding guess: {enc}")
    print(f"- total bytes: {len(raw):,}")
    print(f"- header (first line): {header}")
    cols, ncols = sample_reader_fields("\n".join(lines[:50]))
    print(f"- parsed header columns ({ncols}): {cols}")

    if name == "stop_times.txt":
        # look for unbalanced quotes
        bad = unbalanced_quote_lines(lines[1:])  # exclude header in count
        print(f"- stop_times: total lines (incl header): {len(lines):,}")
        print(f"- stop_times: unbalanced-quote line count: {len(bad):,}")
        if bad:
            print(f"- first 5 suspect line numbers (1-based, excluding header): {bad[:5]}")
            # write a tiny sample with those lines for inspection
            outdir = "data/external/gtfs/_diagnostics"
            os.makedirs(outdir, exist_ok=True)
            sample_path = os.path.join(outdir, "stop_times_suspect_rows.txt")
            with open(sample_path, "w", encoding="utf-8") as f:
                f.write(header + "\n")
                for ln_idx in bad[:25]:
                    # +1 to map back to raw split (we removed header)
                    f.write(lines[ln_idx] + "\n")
            print(f"- wrote sample suspect rows → {sample_path}")

    # show tiny preview
    print("- preview (first 5 lines):")
    for ln in preview.split("\n"):
        print("  " + ln)

def try_pandas_parses(zf: zipfile.ZipFile, name: str):
    if name not in zf.namelist():
        return
    print(f"\n-- pandas quick-parse attempts for {name} --")
    # limit to a few rows to illustrate the header and shapes
    with zf.open(name) as f:
        try:
            df = pd.read_csv(f, nrows=20)
            print(f"[engine=c] OK: shape={df.shape} | cols={list(df.columns)}")
        except Exception as e:
            print(f"[engine=c] FAIL: {e}")

    with zf.open(name) as f:
        try:
            df = pd.read_csv(f, nrows=20, engine="python", on_bad_lines="skip")
            print(f"[engine=python] OK: shape={df.shape} | cols={list(df.columns)}")
        except Exception as e:
            print(f"[engine=python] FAIL: {e}")

    with zf.open(name) as f:
        try:
            df = pd.read_csv(
                f, nrows=20, engine="python", on_bad_lines="skip",
                quoting=csv.QUOTE_NONE, escapechar="\\"
            )
            print(f"[engine=python, QUOTE_NONE] OK: shape={df.shape} | cols={list(df.columns)}")
        except Exception as e:
            print(f"[engine=python, QUOTE_NONE] FAIL: {e}")

def main():
    ap = argparse.ArgumentParser(description="Diagnose GTFS ZIP structure and CSV quirks")
    ap.add_argument("--zip", required=True, help="Path to GTFS zip")
    args = ap.parse_args()

    if not os.path.exists(args.zip):
        raise SystemExit(f"ZIP not found: {args.zip}")

    with zipfile.ZipFile(args.zip, "r") as zf:
        print(f"ZIP: {args.zip}")
        print("Members:")
        for n in zf.namelist():
            print(" -", n)

        for name in FILES:
            preview_member(zf, name)

        # extra pandas probes for stop_times
        try_pandas_parses(zf, "stop_times.txt")

if __name__ == "__main__":
    main()
