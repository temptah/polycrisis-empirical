import pandas as pd
import matplotlib.pyplot as plt

t3   = pd.read_csv("docs/t3_route_medians_07_10.csv")
t3w  = pd.read_csv("docs/t3w_multi_route_medians_07_10.csv")

def plot_hist(df, title, out):
    fig, ax = plt.subplots(figsize=(6,3))
    ax.hist(df["med_minutes"].values, bins=30)
    ax.set_xlabel("Route median headway (min, 07–10)")
    ax.set_ylabel("Routes")
    ax.set_title(title)
    fig.tight_layout()
    fig.savefig(out, dpi=200)

plot_hist(t3,  "Route medians — T3 (all services)",    "docs/t3_route_medians_hist.png")
plot_hist(t3w, "Route medians — T3W_MULTI (6 weekdays)","docs/t3w_multi_route_medians_hist.png")
