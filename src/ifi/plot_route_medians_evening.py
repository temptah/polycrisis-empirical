import pandas as pd
import matplotlib.pyplot as plt

def plot_hist(csv_path, title, out):
    df = pd.read_csv(csv_path)
    fig, ax = plt.subplots(figsize=(6,3))
    ax.hist(df["med_minutes"].values, bins=30)
    ax.set_xlabel("Route median headway (min, 16–19)")
    ax.set_ylabel("Routes")
    ax.set_title(title)
    fig.tight_layout()
    fig.savefig(out, dpi=200)

plot_hist("docs/t3_route_medians_16_19.csv",  "Route medians — T3 (evening)",        "docs/t3_route_medians_16_19_hist.png")
plot_hist("docs/t3w_multi_route_medians_16_19.csv", "Route medians — T3W_MULTI (evening)", "docs/t3w_multi_route_medians_16_19_hist.png")
