# service_due_predictor.py
# Alerts when a machine is within its preventive-service window (14 days
# before the scheduled interval). Severity is HIGH if due within 7 days.
# Triggers: SCHEDULE_SERVICE

SERVICE_INTERVAL_DAYS = 110
SERVICE_WINDOW_DAYS = 14

as_of = ctx["meta"]["as_of_date"]
last_serviced_raw = (ctx.get("entities", {}).get("machine", {}) or {}).get("last_serviced_at")

if not as_of or not last_serviced_raw:
    result = []
else:
    last_service_date = str(last_serviced_raw)[:10]
    days_since = max(0, days_between(last_service_date, as_of))
    days_until_due = SERVICE_INTERVAL_DAYS - days_since

    if days_until_due > SERVICE_WINDOW_DAYS:
        result = []
    else:
        severity = "HIGH" if days_until_due <= 7 else "MEDIUM"
        result = [
            alert(
                "service_due",
                severity,
                "Service window likely due",
                f"Preventive service is due in {days_until_due} day(s).",
                {
                    "days_since_service": days_since,
                    "days_until_due": days_until_due,
                    "last_service_date": last_service_date,
                    "service_interval_days": SERVICE_INTERVAL_DAYS,
                    "service_due_date": date_add(last_service_date, SERVICE_INTERVAL_DAYS),
                    "overdue_days": max(0, -days_until_due),
                },
                [
                    (
                        "SCHEDULE_SERVICE",
                        {
                            "machine_id": ctx["ids"]["machine_id"],
                            "priority": severity,
                            "reason": "time since last service",
                            "suggested_date": date_add(as_of, 2),
                        },
                    )
                ],
            )
        ]
