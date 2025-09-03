# Indicator Catalog (to be filled as indicators are implemented)

## TRANSPORT
- T1_EXPOSURE_FLOODPRONE_KM
  - Direction: UP_IS_BAD
  - Unit: %
  - Source: (TBD)
  - Methodology: (TBD)
- T2_VULN_CENTRAL_EDGES_SHARE
  - Direction: UP_IS_BAD
  - Unit: %
  - Source: (TBD)
  - Methodology: (TBD)
- T3_RECOVERY_HEADWAY_GAP
  - Direction: UP_IS_BAD
  - Unit: minutes (or %)
  - Source: (TBD)
  - Methodology: (TBD)

Seeded Transport indicators: T1_EXPOSURE_FLOODPRONE_KM, T2_VULN_CENTRAL_EDGES_SHARE, T3_RECOVERY_HEADWAY_GAP.

T1 (2024, Attica): prox to waterways within 200 m; value_raw=% road-km in buffer; value_norm=share (0–1).
### Transport frequency indicators (Attica, 2024)

**T3_SCHED_MEDIAN_HEADWAY_MIN (official)** — Median scheduled headway in 07:00–10:00, computed from **all services** (unfiltered by date). Attica spatial filter; GTFS hours ≥ 24 handled. Pipeline: per‑stop headways → per‑route median → network median. **Value:** 7.93 min, **normalized:** 0.2643.

**T3W_SCHED_MEDIAN_HEADWAY_MIN (diagnostic)** — Weekday‑filtered using service_ids active on **2024‑11‑20** (Tue). Same pipeline and filters. **Value:** 20.00 min, **normalized:** 0.6667. Diagnostic only; **not** included in the official IFI.

**T3W_MULTI_SCHED_MEDIAN_HEADWAY_MIN (diagnostic/scenario)** — Median of **day‑level network medians** over six Tue–Thu dates (**Nov 19–28, 2024**). Same pipeline and filters. **Value:** 20.00 min, **normalized:** 0.6667. Used to compute scenario IFI `TRANSPORT_T3W_MULTI` by **replacing T3** and excluding the single‑day T3W.
