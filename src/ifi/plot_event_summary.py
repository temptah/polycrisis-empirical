# src/ifi/plot_event_summary.py
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

csv = Path("docs/event_transport_summary.csv")
df = pd.read_csv(csv)

# Ensure expected order
order = [("AM","T3_all"),("AM","T3W_MULTI_event"),("PM","T3_all"),("PM","T3W_MULTI_event")]
df["order"] = df.apply(lambda r: order.index((r["window"], r["variant"])), axis=1)
df = df.sort_values("order")

# Compute effects
am_all   = df.loc[(df.window=="AM") & (df.variant=="T3_all"), "minutes"].iloc[0]
am_wk    = df.loc[(df.window=="AM") & (df.variant=="T3W_MULTI_event"), "minutes"].iloc[0]
pm_all   = df.loc[(df.window=="PM") & (df.variant=="T3_all"), "minutes"].iloc[0]
pm_wk    = df.loc[(df.window=="PM") & (df.variant=="T3W_MULTI_event"), "minutes"].iloc[0]

am_delta = am_wk - am_all
pm_delta = pm_wk - pm_all
am_ratio = am_wk / am_all
pm_ratio = pm_wk / pm_all

# Figure
fig, ax = plt.subplots(figsize=(6.4,3.4))
labels = ["AM – T3_all","AM – T3W_MULTI","PM – T3_all","PM – T3W_MULTI"]
ax.bar(labels, df["minutes"].values)
ax.set_ylabel("Network median headway (min)")
ax.set_title("Attica (EL30), weekday‑filtered schedule headways — AM vs PM")
for i,v in enumerate(df["minutes"].values):
    ax.text(i, v + 0.2, f"{v:.2f}", ha="center", va="bottom", fontsize=9)
fig.tight_layout()

Path("docs").mkdir(exist_ok=True, parents=True)
out_png = Path("docs/event_transport_summary_heatwave2024.png")
fig.savefig(out_png, dpi=200)

# Notes for the thesis text and SI
note = Path("docs/event_transport_summary_notes.txt")
note.write_text(
    "Attica EL30, weekday‑filtered schedule medians\n"
    f"AM: T3_all={am_all:.2f} min, T3W_MULTI={am_wk:.2f} min, Δ={am_delta:.2f} min, ratio={am_ratio:.3f}x, +{(am_ratio-1)*100:.1f}%\n"
    f"PM: T3_all={pm_all:.2f} min, T3W_MULTI={pm_wk:.2f} min, Δ={pm_delta:.2f} min, ratio={pm_ratio:.3f}x, +{(pm_ratio-1)*100:.1f}%\n"
)

print("Wrote:", out_png, "and", note)
