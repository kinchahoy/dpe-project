# systematic_demand_change_watch.py
# Detect when machine demand exceeds predicted demand over a full week.
# Triggers: ADJUST_PRICE (increase)

WEEK_DAYS = 7
LIFT_TRIGGER_PCT = 0.50
MIN_PREDICTED_WEEK_UNITS = 50.0

as_of = ctx["meta"]["as_of_date"]
run_id = ctx["meta"].get("run_id")
days = ctx.get("days", [])
obs = [d for d in days if d.get("kind") == "observed"]
pred = [d for d in days if d.get("kind") == "predicted"]

week_offsets = [i for i in range(-(WEEK_DAYS - 1), 1)]
obs_week = [d for d in obs if int(d.get("offset_days") or 0) in week_offsets]
pred_week = [d for d in pred if int(d.get("offset_days") or 0) in week_offsets]

if run_id is None:
    result = []
else:
    observed_week_units = 0.0
    for d in obs_week:
        totals = d.get("totals") or {}
        observed_week_units += float(totals.get("units") or 0.0)

    predicted_week_units = 0.0
    for d in pred_week:
        predicted_week_units += sum(
            float(r.get("units") or 0.0) for r in d.get("by_product", [])
        )

    if predicted_week_units < MIN_PREDICTED_WEEK_UNITS:
        result = []
    else:
        lift_pct = (
            (observed_week_units - predicted_week_units) / predicted_week_units
            if predicted_week_units > 0
            else 0.0
        )
        if lift_pct <= LIFT_TRIGGER_PCT:
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
                        "observed_week_units": round(observed_week_units, 2),
                        "predicted_week_units": round(predicted_week_units, 2),
                        "lift_pct": round(lift_pct * 100, 1),
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
