# slow_mover_cleanup.py
# Identify persistent low-share products that tie up ingredient stock.
# Triggers: PROPOSE_DISCONTINUE

units_by_product = {}
for row in daily_product_sales:
    pid = row["product_id"]
    units_by_product[pid] = units_by_product.get(pid, 0.0) + row["units_sold"]

if len(units_by_product) < 3:
    result = []
else:
    total_units = 0.0
    for v in units_by_product.values():
        total_units = total_units + v

    if total_units <= 0:
        result = []
    else:
        slow_product_id = None
        slow_units = None
        for pid, u in units_by_product.items():
            if slow_units is None or u < slow_units:
                slow_product_id = pid
                slow_units = u

        share = slow_units / total_units
        if share > 0.04:
            result = []
        else:
            result = [alert(
                "slow_mover_cleanup", "LOW",
                "Product appears to be a persistent slow mover",
                "A low-share product may be tying up ingredient stock without meaningful demand.",
                {"product_id": slow_product_id,
                 "units_over_history": round(slow_units, 2),
                 "unit_share": round(share, 4)},
                [("PROPOSE_DISCONTINUE", {
                    "location_id": location_id,
                    "machine_id": machine_id})]
            )]
