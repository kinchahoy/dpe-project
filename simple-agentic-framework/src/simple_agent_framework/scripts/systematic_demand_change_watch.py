# systematic_demand_change_watch.py
# Detect when machine demand spikes above same-weekday baseline.
# Triggers: RESTOCK_MACHINE, ADJUST_PRICE

today = [d for d in daily_totals if d["date"] == as_of_date]
if not today:
    result = []
else:
    today_units = today[0]["units_total"]
    today_revenue = today[0]["revenue_total"]
    bl_units = [d["units_total"] for d in daily_totals if d["date"] in baseline_dates]

    units_z = z_score(today_units, bl_units)
    units_pct = percentile(today_units, bl_units)

    if units_z < 1.8 and units_pct < 0.9:
        result = []
    else:
        bl_revenue = [d["revenue_total"] for d in daily_totals if d["date"] in baseline_dates]
        revenue_z = z_score(today_revenue, bl_revenue)

        actions = [("RESTOCK_MACHINE", {
            "machine_id": machine_id,
            "restock_level_by_ingredient": {"espresso_beans": "top_up"},
            "deadline": date_add(as_of_date, 1),
        })]
        if revenue_z < units_z - 1.0:
            actions.append(("ADJUST_PRICE", {
                "location_id": location_id,
                "duration": "3d",
                "rationale": "Demand rose faster than revenue; evaluate underpricing during peak windows.",
            }))

        result = [alert(
            "systematic_demand_change", "MEDIUM",
            "Demand moved above baseline",
            "Machine demand spiked relative to prior same-weekday baseline.",
            {"units_z": round(units_z, 2), "units_percentile": round(units_pct, 2)},
            actions,
        )]
