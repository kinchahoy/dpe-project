# pricing_anomaly.py
# Detects products being charged less than their expected price. Fires when
# a product has 2+ undercharge transactions (>5% below expected) in the
# observation window, highlighting the single worst offender.
# Triggers: CHECK_MACHINE

MIN_UNDERCHARGE_COUNT = 2

anomalies = ctx.get("price_anomalies", []) or []
as_of = ctx["meta"]["as_of_date"]
currency = ctx["meta"].get("currency")

# Find product with the most undercharge events.
worst = None
for row in anomalies:
    count = int(row.get("undercharge_count") or 0)
    if worst is None or count > int(worst.get("undercharge_count") or 0):
        worst = row

if worst is None or int(worst.get("undercharge_count") or 0) < MIN_UNDERCHARGE_COUNT:
    result = []
else:
    product_name = worst.get("product_name") or "Unknown product"
    undercharge_count = int(worst.get("undercharge_count") or 0)

    # Find the observation window start date.
    window_start = None
    for d in ctx.get("days", []) or []:
        if d.get("kind") == "observed" and d.get("date"):
            window_start = str(d.get("date"))
            break

    # Find worst single example.
    worst_ex = None
    examples = []
    for ex in worst.get("examples") or []:
        if not isinstance(ex, dict):
            continue
        amount = float(ex.get("amount") or 0.0)
        expected = float(ex.get("expected_price") or 0.0)
        delta_pct = float(ex.get("delta_pct") or 0.0)
        entry = {
            "date": ex.get("date"),
            "amount": round(amount, 4),
            "expected_price": round(expected, 4),
            "undercharge_amount": round(expected - amount, 4) if expected > 0 else None,
            "delta_pct": round(delta_pct, 2),
            "currency": ex.get("currency") or currency,
        }
        examples.append(entry)
        if worst_ex is None or delta_pct < float(worst_ex.get("delta_pct") or 0.0):
            worst_ex = entry

    if worst_ex is None:
        worst_ex = {"date": None, "amount": None, "expected_price": None, "undercharge_amount": None, "delta_pct": None, "currency": currency}

    result = [
        alert(
            "pricing_anomaly",
            "HIGH",
            f"Undercharge detected: {product_name}",
            f"On {str(worst_ex.get('date') or 'unknown')}, {product_name} was charged {worst_ex.get('amount')} vs expected {worst_ex.get('expected_price')} ({worst_ex.get('delta_pct')}%); {undercharge_count} undercharge events ({window_start} to {as_of}).",
            {
                "as_of_date": as_of,
                "window_start": window_start,
                "window_end": as_of,
                "product_id": worst.get("product_id"),
                "product_name": product_name,
                "undercharge_count": undercharge_count,
                "worst_example": worst_ex,
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
