# pricing_sanity_check.py
# Detect peak-hour demand concentration suggesting pricing experiment opportunity.
# Triggers: ADJUST_PRICE

today = [d for d in daily_totals if d["date"] == as_of_date]
if not today:
    result = []
else:
    today_units = today[0]["units_total"]
    today_revenue = today[0]["revenue_total"]
    bl_units = [d["units_total"] for d in daily_totals if d["date"] in baseline_dates]
    bl_revenue = [d["revenue_total"] for d in daily_totals if d["date"] in baseline_dates]

    units_z = z_score(today_units, bl_units)
    if units_z < 1.0:
        result = []
    else:
        peak_units = 0.0
        offpeak_units = 0.0
        for row in hourly_product_sales:
            hour = row["hour_of_day"]
            units = row["units_sold"]
            if 7 <= hour <= 10:
                peak_units = peak_units + units
            elif 13 <= hour <= 16:
                offpeak_units = offpeak_units + units

        if peak_units <= offpeak_units:
            result = []
        else:
            revenue_z = z_score(today_revenue, bl_revenue)
            recommendation = "Test +3% peak pricing for 3 days"
            if revenue_z < units_z:
                recommendation = "Demand grew faster than revenue; test +5% during peak only"

            result = [alert(
                "pricing_sanity", "MEDIUM",
                "Pricing opportunity detected",
                "Peak-hour demand concentration suggests room for a limited pricing experiment.",
                {"peak_units": round(peak_units, 2),
                 "offpeak_units": round(offpeak_units, 2),
                 "units_z": round(units_z, 2),
                 "revenue_z": round(revenue_z, 2)},
                [("ADJUST_PRICE", {
                    "location_id": location_id,
                    "duration": "3d",
                    "rationale": recommendation})]
            )]
