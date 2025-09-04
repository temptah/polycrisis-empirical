import pandas as pd

t3  = pd.read_csv("docs/t3_route_medians_07_10.csv")
t3w = pd.read_csv("docs/t3w_multi_route_medians_07_10.csv")

t3  = t3[["mode","route_id","route_short_name","route_long_name","med_minutes"]].rename(columns={"med_minutes":"med_T3"})
t3w = t3w[["mode","route_id","route_short_name","route_long_name","med_minutes"]].rename(columns={"med_minutes":"med_T3W_MULTI"})

df = pd.merge(t3, t3w, on=["mode","route_id"], how="inner", suffixes=("_T3","_T3W"))
df["delta_min"] = df["med_T3W_MULTI"] - df["med_T3"]

df.sort_values("delta_min", ascending=False).head(20).to_csv("docs/t3_vs_t3w_multi_top_increases.csv", index=False)
df.sort_values("delta_min").head(20).to_csv("docs/t3_vs_t3w_multi_top_decreases.csv", index=False)
print("Saved: top_increases & top_decreases under docs/")
