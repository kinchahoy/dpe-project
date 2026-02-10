# service_due_predictor.py
# Predict when preventive service should be scheduled based on elapsed time.
# Triggers: SCHEDULE_SERVICE

SERVICE_INTERVAL_DAYS = 110
SERVICE_WINDOW_DAYS = 14

as_of = ctx["meta"]["as_of_date"]
machine = ctx.get("entities", {}).get("machine", {}) or {}
last_serviced_at_raw = machine.get("last_serviced_at")

if not as_of or not last_serviced_at_raw:
    result = []
else:
    last_service_date = str(last_serviced_at_raw)[:10]
    days_since_service = days_between(last_service_date, as_of)
    days_since_service = max(0, int(days_since_service))

    days_until_due = SERVICE_INTERVAL_DAYS - days_since_service
    overdue_days = max(0, -days_until_due)
    in_service_window = days_until_due <= SERVICE_WINDOW_DAYS

    if not in_service_window:
        result = []
    else:
        priority = "HIGH" if days_until_due <= 7 else "MEDIUM"
        service_due_date = date_add(last_service_date, SERVICE_INTERVAL_DAYS)

        result = [
            alert(
                "service_due",
                priority,
                "Service window likely due",
                f"Preventive service is due in {days_until_due} day(s).",
                {
                    "days_since_service": days_since_service,
                    "days_until_due": days_until_due,
                    "last_service_date": last_service_date,
                    "service_interval_days": SERVICE_INTERVAL_DAYS,
                    "service_due_date": service_due_date,
                    "overdue_days": overdue_days,
                },
                [
                    (
                        "SCHEDULE_SERVICE",
                        {
                            "machine_id": ctx["ids"]["machine_id"],
                            "priority": priority,
                            "reason": "time since last service",
                            "suggested_date": date_add(as_of, 2),
                        },
                    )
                ],
            )
        ]
