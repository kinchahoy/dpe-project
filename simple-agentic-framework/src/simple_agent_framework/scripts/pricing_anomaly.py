# pricing_anomaly.py
# Detect charge amount drift vs expected prices.
# Triggers: CHECK_MACHINE

today = [d for d in daily_totals if d["date"] == as_of_date]
today_units = today[0]["units_total"] if today else 0

discrepancy_count = len([d for d in price_discrepancies if d["date"] == as_of_date])
discrepancy_rate = discrepancy_count / today_units if today_units > 0 else 0.0

if discrepancy_rate < 0.08 and len(price_discrepancies) < 5:
    result = []
else:
    result = [alert(
        "pricing_anomaly", "HIGH",
        "Charge amount drift detected",
        "Observed charges deviate from expected price often enough to warrant machine checks.",
        {"discrepancy_rate": round(discrepancy_rate, 3),
         "recent_discrepancies": min(len(price_discrepancies), 20)},
        [("CHECK_MACHINE", {
            "location_id": location_id,
            "machine_id": machine_id,
            "transaction_discrepancies": min(len(price_discrepancies), 20),
            "what_to_check": "price configuration, cash/card handlers, firmware pricing table"})]
    )]
