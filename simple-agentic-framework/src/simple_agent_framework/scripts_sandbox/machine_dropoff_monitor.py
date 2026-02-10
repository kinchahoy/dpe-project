# machine_dropoff_monitor.py
# Detect machine-level sales/revenue drops vs recent observed mean.
# Triggers: CHECK_MACHINE

MIN_BASELINE_DAYS = 3
MIN_TODAY_UNITS = 8.0
MIN_PREDICTED_1D_UNITS = 8.0
UNITS_DROP_PCT_TRIGGER = -0.30
REVENUE_DROP_PCT_TRIGGER = -0.25
MIN_TOP_PRODUCT_BASELINE_MEAN_UNITS = 2.0

as_of = ctx["meta"]["as_of_date"]
run_id = ctx["meta"].get("run_id")
days = ctx.get("days", []) or []
obs = [d for d in days if d.get("kind") == "observed"]
pred = [d for d in days if d.get("kind") == "predicted"]
today = [d for d in obs if d.get("date") == as_of]
baseline = [d for d in obs if d.get("date") != as_of and d.get("totals")]

projected_units_1d = 0.0
for d in pred:
    if int(d.get("offset_days") or 0) == 1:
        projected_units_1d = sum(
            float(r.get("units") or 0.0) for r in d.get("by_product", []) or []
        )
        break

result = []
if today and len(baseline) >= MIN_BASELINE_DAYS:
    today_totals = today[0].get("totals") or {}
    today_units = float(today_totals.get("units") or 0.0)
    today_revenue = float(today_totals.get("revenue") or 0.0)
    if today_units >= MIN_TODAY_UNITS and (
        run_id is None or projected_units_1d >= MIN_PREDICTED_1D_UNITS
    ):
        baseline_days = len(baseline)
        bl_units_mean = mean([float(d["totals"].get("units") or 0.0) for d in baseline])
        bl_rev_mean = mean([float(d["totals"].get("revenue") or 0.0) for d in baseline])
        units_drop_frac = (
            (today_units - bl_units_mean) / bl_units_mean if bl_units_mean > 0 else 0.0
        )
        rev_drop_frac = (
            (today_revenue - bl_rev_mean) / bl_rev_mean if bl_rev_mean > 0 else 0.0
        )
        if (
            units_drop_frac <= UNITS_DROP_PCT_TRIGGER
            or rev_drop_frac <= REVENUE_DROP_PCT_TRIGGER
        ):
            severity = (
                "HIGH"
                if (
                    units_drop_frac <= UNITS_DROP_PCT_TRIGGER
                    and rev_drop_frac <= REVENUE_DROP_PCT_TRIGGER
                )
                else "MEDIUM"
            )
            today_units_by_pid = {
                int(r.get("product_id") or 0): float(r.get("units") or 0.0)
                for r in (today[0].get("by_product", []) or [])
                if isinstance(r, dict)
            }
            bl_sum = {}
            bl_name = {}
            for d in baseline:
                for r in d.get("by_product", []) or []:
                    if not isinstance(r, dict):
                        continue
                    pid = int(r.get("product_id") or 0)
                    bl_sum[pid] = bl_sum.get(pid, 0.0) + float(r.get("units") or 0.0)
                    bl_name.setdefault(pid, r.get("product_name"))
            top_drop = None
            if bl_sum:
                best_frac = 0.0
                for pid, s in bl_sum.items():
                    m = (float(s) / baseline_days) if baseline_days > 0 else 0.0
                    if m < MIN_TOP_PRODUCT_BASELINE_MEAN_UNITS:
                        continue
                    tu = float(today_units_by_pid.get(pid, 0.0))
                    frac = (tu - m) / m if m > 0 else 0.0
                    if top_drop is None or frac < best_frac:
                        best_frac = frac
                        top_drop = {
                            "product_id": pid,
                            "product_name": bl_name.get(pid),
                            "today_units": round(tu, 2),
                            "baseline_units_mean": round(m, 2),
                            "units_drop_pct": round(frac * 100, 1),
                        }
            summary = (
                "Units and/or revenue fell materially vs the recent observed mean."
            )
            if isinstance(top_drop, dict) and top_drop.get("product_id"):
                summary += (
                    f" Largest product drop: {top_drop.get('product_name')} "
                    f"({top_drop.get('today_units')} vs {top_drop.get('baseline_units_mean')})."
                )
            result = [
                alert(
                    "machine_dropoff",
                    severity,
                    "Machine-level dropoff vs recent mean",
                    summary,
                    {
                        "today_units": round(today_units, 2),
                        "today_revenue": round(today_revenue, 2),
                        "baseline_days": baseline_days,
                        "baseline_units_mean": round(bl_units_mean, 2),
                        "baseline_revenue_mean": round(bl_rev_mean, 2),
                        "units_drop_pct": round(units_drop_frac * 100, 1),
                        "revenue_drop_pct": round(rev_drop_frac * 100, 1),
                        "projected_units_1d": round(projected_units_1d, 2),
                        "top_drop_product": top_drop,
                    },
                    [
                        (
                            "CHECK_MACHINE",
                            {
                                "machine_id": ctx["ids"]["machine_id"],
                                "what_to_check": "uptime logs, hopper jams, site foot traffic changes",
                            },
                        )
                    ],
                )
            ]
