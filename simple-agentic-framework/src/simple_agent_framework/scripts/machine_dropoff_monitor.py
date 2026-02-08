# machine_dropoff_monitor.py
# Detect machine-level sales/revenue drops vs same-weekday baseline.
# Triggers: CHECK_MACHINE

today = [d for d in daily_totals if d["date"] == as_of_date]
if not today:
    result = []
else:
    today_units = today[0]["units_total"]
    today_revenue = today[0]["revenue_total"]
    bl_units = [d["units_total"] for d in daily_totals if d["date"] in baseline_dates]
    bl_revenue = [d["revenue_total"] for d in daily_totals if d["date"] in baseline_dates]

    units_z = z_score(today_units, bl_units)
    revenue_z = z_score(today_revenue, bl_revenue)

    if units_z > -2.0 and revenue_z > -1.5:
        result = []
    else:
        result = [alert(
            "machine_dropoff", "HIGH",
            "Machine-level dropoff vs baseline",
            "Sales and/or revenue fell materially vs same-weekday baseline.",
            {"units_z": round(units_z, 2), "revenue_z": round(revenue_z, 2)},
            [("CHECK_MACHINE", {"machine_id": machine_id,
              "what_to_check": "uptime logs, hopper jams, site foot traffic changes"})]
        )]
