# payment_outage_hint.py
# Detect card payment share drops indicating terminal or gateway issues.
# Triggers: CHECK_MACHINE

today = [d for d in daily_totals if d["date"] == as_of_date]
if not today:
    result = []
else:
    card_share = today[0]["card_share"]
    bl_card = [d["card_share"] for d in daily_totals if d["date"] in baseline_dates]
    card_z = z_score(card_share, bl_card)

    if card_share > 0.15 and card_z > -2.0:
        result = []
    else:
        result = [alert(
            "payment_outage_hint", "HIGH",
            "Card payment behavior dropped",
            "Card share is far below baseline, indicating a potential payment outage or degraded terminal.",
            {"card_share": round(card_share, 3),
             "card_share_z": round(card_z, 2)},
            [("CHECK_MACHINE", {
                "location_id": location_id,
                "machine_id": machine_id,
                "what_to_check": "card reader connectivity, payment gateway status, terminal errors"})]
        )]
