import os, zipfile

def _clean_stop_times_bytes(b: bytes) -> bytes:
    # Decode forgivingly and normalize newlines
    s = b.decode("utf-8", errors="ignore").replace("\r\n","\n").replace("\r","\n")
    lines = s.split("\n")

    out_lines = []
    buf = []
    in_quotes = False

    for ln in lines:
        # Count quotes, discounting escaped/doubled quotes ("")
        # This rough heuristic works well for GTFS files with occasional rogue quotes.
        doubled = ln.count('""')
        q = ln.count('"') - 2*doubled

        buf.append(ln)
        if q % 2 != 0:
            in_quotes = not in_quotes

        # When we are *not* inside a quoted field anymore, flush the buffered lines as a single logical row
        if not in_quotes:
            out_lines.append("\n".join(buf))
            buf = []

    # If file ends while still "in quotes", flush remainder as a single line
    if buf:
        out_lines.append(" ".join(buf))

    fixed = "\n".join(out_lines)
    if not fixed.endswith("\n"):
        fixed += "\n"

    return fixed.encode("utf-8")

def clean_zip(in_zip: str, out_zip: str):
    with zipfile.ZipFile(in_zip, "r") as zin, zipfile.ZipFile(out_zip, "w", compression=zipfile.ZIP_DEFLATED) as zout:
        for name in zin.namelist():
            data = zin.read(name)
            if name.lower() == "stop_times.txt":
                data = _clean_stop_times_bytes(data)
            zout.writestr(name, data)

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="Clean GTFS ZIP: fix malformed stop_times.txt quoting/newlines")
    ap.add_argument("--in",  dest="in_zip",  required=True, help="Path to original GTFS ZIP")
    ap.add_argument("--out", dest="out_zip", required=True, help="Path to cleaned GTFS ZIP")
    args = ap.parse_args()

    if not os.path.exists(args.in_zip):
        raise SystemExit(f"Not found: {args.in_zip}")
    os.makedirs(os.path.dirname(args.out_zip), exist_ok=True)
    clean_zip(args.in_zip, args.out_zip)
    print(f"Cleaned ZIP written → {args.out_zip}")
