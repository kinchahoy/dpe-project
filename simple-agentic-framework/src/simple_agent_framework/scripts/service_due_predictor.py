# service_due_predictor.py
# Predict when preventive service should be scheduled based on hours and elapsed time.
# Triggers: SCHEDULE_SERVICE

last_serviced_at_raw = machine.get("last_serviced_at") if machine else None
if not last_serviced_at_raw:
    result = []
else:
    current_hours = machine.get("current_hours") or 0
    days_since_service = days_between(str(last_serviced_at_raw)[:10], as_of_date)

    today = [d for d in daily_totals if d["date"] == as_of_date]
    bl_units = [d["units_total"] for d in daily_totals if d["date"] in baseline_dates]
    today_units = today[0]["units_total"] if today else 0
    units_z = z_score(today_units, bl_units)

    due_by_hours = current_hours >= 1000
    due_by_time = days_since_service >= 60
    drift_signal = units_z <= -1.5
    if not (due_by_hours or due_by_time or drift_signal):
        result = []
    else:
        priority = "HIGH" if (due_by_hours or drift_signal) else "MEDIUM"
        suggested_date = date_add(as_of_date, 2)
        result = [alert(
            "service_due", priority,
            "Service window likely due",
            "Usage and elapsed time suggest preventive service should be scheduled.",
            {"current_hours": current_hours,
             "days_since_service": days_since_service,
             "units_z": round(units_z, 2)},
            [("SCHEDULE_SERVICE", {
                "machine_id": machine_id,
                "priority": priority,
                "reason": "hours/time/performance threshold reached",
                "suggested_date": suggested_date})]
        )]
