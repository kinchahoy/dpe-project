# pricing_anomaly.py
# Detect charged price drift vs expected_price (per product).
# Triggers: CHECK_MACHINE

UNDERCHARGE_PCT_TRIGGER = -0.05
MIN_UNDERCHARGE_COUNT = 2

anomalies = ctx.get("price_anomalies", []) or []
worst = None
for row in anomalies:
    count = int(row.get("undercharge_count") or 0)
    if worst is None or count > int(worst.get("undercharge_count") or 0):
        worst = row

as_of = ctx["meta"]["as_of_date"]
currency = ctx["meta"].get("currency")
days = ctx.get("days", []) or []
window_start = None
for d in days:
    if d.get("kind") != "observed":
        continue
    dt = d.get("date")
    if dt:
        window_start = str(dt)
        break

if worst is None or int(worst.get("undercharge_count") or 0) < MIN_UNDERCHARGE_COUNT:
    result = []
else:
    product_name = worst.get("product_name") or "Unknown product"
    undercharge_count = int(worst.get("undercharge_count") or 0)
    examples_raw = worst.get("examples") or []
    examples = []
    worst_ex = None
    for ex in examples_raw:
        if not isinstance(ex, dict):
            continue
        amount = float(ex.get("amount") or 0.0)
        expected = float(ex.get("expected_price") or 0.0)
        delta_pct = float(ex.get("delta_pct") or 0.0)
        undercharge_amount = (expected - amount) if expected > 0 else None
        row = {
            "date": ex.get("date"),
            "amount": round(amount, 4),
            "expected_price": round(expected, 4),
            "undercharge_amount": round(undercharge_amount, 4)
            if undercharge_amount is not None
            else None,
            "delta_pct": round(delta_pct, 2),
            "currency": ex.get("currency") or currency,
        }
        examples.append(row)
        if worst_ex is None or float(row.get("delta_pct") or 0.0) < float(
            worst_ex.get("delta_pct") or 0.0
        ):
            worst_ex = row

    if worst_ex is None:
        worst_ex = {
            "date": None,
            "amount": None,
            "expected_price": None,
            "undercharge_amount": None,
            "delta_pct": None,
            "currency": currency,
        }

    result = [
        alert(
            "pricing_anomaly",
            "HIGH",
            f"Undercharge detected: {product_name}",
            (
                f"On {str(worst_ex.get('date') or 'unknown time')}, {product_name} was charged "
                f"{worst_ex.get('amount')} vs expected {worst_ex.get('expected_price')} "
                f"({worst_ex.get('delta_pct')}%); {undercharge_count} undercharge events in window "
                f"({window_start} to {as_of})."
            ),
            {
                "as_of_date": as_of,
                "window_start": window_start,
                "window_end": as_of,
                "product_id": worst.get("product_id"),
                "product_name": worst.get("product_name"),
                "undercharge_count": undercharge_count,
                "past_undercharge_event_count": undercharge_count,
                "undercharge_trigger_pct": round(UNDERCHARGE_PCT_TRIGGER * 100, 1),
                "min_undercharge_count": MIN_UNDERCHARGE_COUNT,
                "worst_example": worst_ex,
                "undercharged_at": worst_ex.get("date")
                if isinstance(worst_ex, dict)
                else None,
                "charged_amount": worst_ex.get("amount")
                if isinstance(worst_ex, dict)
                else None,
                "target_expected_price": worst_ex.get("expected_price")
                if isinstance(worst_ex, dict)
                else None,
                "undercharge_amount": worst_ex.get("undercharge_amount")
                if isinstance(worst_ex, dict)
                else None,
                "undercharge_delta_pct": worst_ex.get("delta_pct")
                if isinstance(worst_ex, dict)
                else None,
                "examples": examples,
            },
            [
                (
                    "CHECK_MACHINE",
                    {
                        "location_id": ctx["ids"]["location_id"],
                        "machine_id": ctx["ids"]["machine_id"],
                        "what_to_check": "price config vs expected_price source, firmware price tables, cash/card price application, unintended discounts/promotions",
                    },
                )
            ],
        )
    ]
