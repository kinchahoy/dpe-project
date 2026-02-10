# systematic_demand_change_watch.py
# Fires when observed sales over the past 7 days exceed the model's
# predicted demand by more than 50%, suggesting a price-increase test.
# Triggers: ADJUST_PRICE (increase)

WEEK_DAYS = 7
LIFT_TRIGGER_PCT = 0.50
MIN_PREDICTED_WEEK_UNITS = 50.0

as_of = ctx["meta"]["as_of_date"]
run_id = ctx["meta"].get("run_id")
days = ctx.get("days", [])

if run_id is None:
    result = []
else:
    week_offsets = [i for i in range(-(WEEK_DAYS - 1), 1)]

    observed_units = 0.0
    for d in days:
        if d.get("kind") == "observed" and int(d.get("offset_days") or 0) in week_offsets:
            observed_units += float((d.get("totals") or {}).get("units") or 0.0)

    predicted_units = 0.0
    for d in days:
        if d.get("kind") == "predicted" and int(d.get("offset_days") or 0) in week_offsets:
            predicted_units += sum(
                float(r.get("units") or 0.0) for r in d.get("by_product", [])
            )

    if predicted_units < MIN_PREDICTED_WEEK_UNITS:
        result = []
    else:
        lift = (observed_units - predicted_units) / predicted_units if predicted_units > 0 else 0.0
        if lift <= LIFT_TRIGGER_PCT:
            result = []
        else:
            result = [
                alert(
                    "systematic_demand_change",
                    "MEDIUM",
                    "Sustained demand above forecast (7d)",
                    "Observed demand over the last 7 days exceeded the model forecast by more than 50%. Consider a price increase test.",
                    {
                        "as_of_date": as_of,
                        "observed_week_units": round(observed_units, 2),
                        "predicted_week_units": round(predicted_units, 2),
                        "lift_pct": round(lift * 100, 1),
                        "lift_trigger_pct": round(LIFT_TRIGGER_PCT * 100, 1),
                    },
                    [
                        (
                            "ADJUST_PRICE",
                            {
                                "location_id": ctx["ids"]["location_id"],
                                "machine_id": ctx["ids"]["machine_id"],
                                "direction": "increase",
                                "duration": "7d",
                                "rationale": "Observed 7d demand ran >50% above forecast; test +3% to +5% pricing on top sellers.",
                            },
                        )
                    ],
                )
            ]
