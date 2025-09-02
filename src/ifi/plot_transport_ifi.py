import pandas as pd
import sqlalchemy as sa
from src.config import make_engine
import matplotlib.pyplot as plt

def main():
    eng = make_engine()
    with eng.connect() as con:
        df = pd.read_sql(sa.text("""
            SELECT r.region_name, i.system_code, i.time_start, i.time_end, i.ifi
            FROM model.ifi_score i
            JOIN meta.region r USING (region_id)
            WHERE r.iso_code='EL30' AND i.system_code='TRANSPORT'
            ORDER BY time_start
        """), con)

    print(df)

    if not df.empty:
        # simple bar chart (one bar if only 2024 exists)
        ax = df.plot(kind="bar", x="time_start", y="ifi", legend=False)
        ax.set_title("Transport IFI — Attica (EL30)")
        ax.set_ylabel("IFI (0..1, higher=worse)")
        ax.set_xlabel("Time window start")
        plt.tight_layout()
        out = "docs/transport_ifi_attica.png"
        plt.savefig(out, dpi=150)
        print(f"Saved {out}")

if __name__ == "__main__":
    main()
