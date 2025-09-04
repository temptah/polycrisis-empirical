import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv("docs/transport_ifi_attica_scenarios.csv")
df["label"] = df["system_code"].map({"TRANSPORT":"Official (T3)", "TRANSPORT_T3W_MULTI":"Scenario (T3W_MULTI)"})
fig, ax = plt.subplots(figsize=(6,3))
ax.barh(df["label"], df["ifi"])
for i, v in enumerate(df["ifi"]):
    ax.text(v + 0.01, i, f"{v:.3f}")
ax.set_xlim(0, 1)
ax.set_xlabel("IFI (0=best, 1=worst)")
ax.set_title("Transport IFI — Attica 2024")
fig.tight_layout()
fig.savefig("docs/transport_ifi_attica_scenarios.png", dpi=200)
