import pandas as pd
import matplotlib.pyplot as plt

am = pd.read_csv("docs/t3_vs_t3w_multi_deltas_07_10_summary.csv")
pm = pd.read_csv("docs/t3_vs_t3w_multi_deltas_16_19_summary.csv")

fig, ax = plt.subplots(figsize=(6,3))
labels = ["Morning (07–10)", "Evening (16–19)"]
values = [am["delta_min_median"].iloc[0], pm["delta_min_median"].iloc[0]]
ax.barh(labels, values)
for i, v in enumerate(values):
    ax.text(float(v) + 0.01, i, f"{float(v):.2f} min")
ax.set_xlabel("Δ median headway = T3W_MULTI − T3 (min)")
ax.set_xlim(min(0,min(values))-0.1, max(values)+0.5)
ax.set_title("Weekday‑filtering effect on route medians — Attica 2024")
fig.tight_layout()
fig.savefig("docs/t3w_multi_vs_t3_delta_am_pm.png", dpi=200)
