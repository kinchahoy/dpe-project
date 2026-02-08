# restock_predictor.py
# Flag high projected 3-day ingredient consumption that risks stockout.
# Triggers: RESTOCK_MACHINE, ORDER_INGREDIENTS

forecast_by_ingredient = {}
for row in ingredient_forecasts:
    fd = row["forecast_date"]
    cutoff = date_add(as_of_date, 3)
    if fd <= cutoff:
        iid = row["ingredient_id"]
        forecast_by_ingredient[iid] = forecast_by_ingredient.get(iid, 0.0) + row["forecast_quantity"]

if not forecast_by_ingredient:
    result = []
else:
    top_ingredient = None
    top_qty = 0.0
    for iid, qty in forecast_by_ingredient.items():
        if qty > top_qty:
            top_ingredient = iid
            top_qty = qty

    if top_qty < 180:
        result = []
    else:
        deadline_restock = date_add(as_of_date, 1)
        deadline_order = date_add(as_of_date, 2)
        result = [alert(
            "restock_risk", "HIGH",
            "Restock soon for projected ingredient draw",
            "Projected 3-day ingredient consumption is high enough to risk stockout.",
            {"top_ingredient_id": top_ingredient,
             "projected_3d_qty": round(top_qty, 2)},
            [("RESTOCK_MACHINE", {"machine_id": machine_id,
              "restock_level_by_ingredient": {str(top_ingredient): round(top_qty * 1.25, 2)},
              "deadline": deadline_restock}),
             ("ORDER_INGREDIENTS", {"location_id": location_id,
              "ingredient_id": top_ingredient,
              "quantity": round(top_qty * 1.35, 2),
              "deadline": deadline_order})]
        )]
