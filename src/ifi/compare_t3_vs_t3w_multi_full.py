import pandas as pd
from pathlib import Path

t3  = pd.read_csv("docs/t3_route_medians_07_10.csv")
t3w = pd.read_csv("docs/t3w_multi_route_medians_07_10.csv")

t3  = t3[["mode","route_id","route_short_name","route_long_name","med_minutes"]] \
        .rename(columns={"med_minutes":"med_T3"})
t3w = t3w[["mode","route_id","med_minutes"]] \
        .rename(columns={"med_minutes":"med_T3W_MULTI"})

df = (t3.merge(t3w, on=["mode","route_id"], how="inner")
        .assign(delta_min=lambda d: d["med_T3W_MULTI"] - d["med_T3"])
        .sort_values(["mode","delta_min"], ascending=[True, False]))

out_full = Path("docs/t3_vs_t3w_multi_route_deltas_07_10.csv")
df.to_csv(out_full, index=False)

df.nlargest(30, "delta_min").to_csv("docs/t3_vs_t3w_multi_top_increases_07_10.csv", index=False)
df.nsmallest(30, "delta_min").to_csv("docs/t3_vs_t3w_multi_top_decreases_07_10.csv", index=False)

summary = pd.DataFrame({
    "n_routes":[len(df)],
    "delta_min_mean":[df["delta_min"].mean()],
    "delta_min_median":[df["delta_min"].median()],
    "p25":[df["delta_min"].quantile(0.25)],
    "p75":[df["delta_min"].quantile(0.75)],
    "min":[df["delta_min"].min()],
    "max":[df["delta_min"].max()],
})
summary.to_csv("docs/t3_vs_t3w_multi_deltas_07_10_summary.csv", index=False)
print("Saved 4 CSVs under docs/")
