# restock_predictor.py
# Checks each ingredient's fill level and days-of-cover against projected
# 3-day consumption. Alerts on the single riskiest ingredient when stock
# is below 35% capacity or covers less than 1.5 days of demand.
# Triggers: RESTOCK_MACHINE, ORDER_INGREDIENTS

FILL_PCT_TRIGGER = 0.35
DAYS_COVER_TRIGGER = 1.5
CRITICAL_FILL_PCT = 0.20
MIN_PROJECTED_3D_QTY = 40.0
MAX_SNAPSHOT_AGE_DAYS = 2

as_of = ctx["meta"]["as_of_date"]
inv = ctx.get("inventory", {}) or {}
snapshot_date = inv.get("snapshot_date")
inventory_rows = inv.get("by_ingredient", []) or []

# Bail out if inventory data is stale or missing.
if not inventory_rows or not snapshot_date or days_between(str(snapshot_date)[:10], as_of) > MAX_SNAPSHOT_AGE_DAYS:
    result = []
else:
    # Sum projected consumption over the next 3 days per ingredient.
    proj3 = {}
    for d in ctx.get("days", []):
        if d.get("kind") != "predicted":
            continue
        off = int(d.get("offset_days") or 0)
        if 1 <= off <= 3:
            for r in d.get("by_ingredient", []):
                iid = int(r.get("ingredient_id") or 0)
                proj3[iid] = proj3.get(iid, 0.0) + float(r.get("qty") or 0.0)

    # Score each ingredient and pick the riskiest one.
    best = None
    for row in inventory_rows:
        iid = int(row.get("ingredient_id") or 0)
        p3 = float(proj3.get(iid, 0.0))
        if p3 < MIN_PROJECTED_3D_QTY:
            continue

        qty = float(row.get("qty_on_hand") or 0.0)
        cap = row.get("capacity")
        cap = float(cap) if cap is not None else None
        unit = row.get("unit")

        fill = (qty / cap) if cap and cap > 0 and (row.get("capacity_unit") is None or row.get("capacity_unit") == unit) else None
        burn = p3 / 3.0
        cover = (qty / burn) if burn > 0 else None

        # Composite risk: higher is worse.
        risk = p3 / 2000.0
        if fill is not None:
            risk += max(0.0, FILL_PCT_TRIGGER - fill) * 3.0
        if cover is not None:
            risk += max(0.0, DAYS_COVER_TRIGGER - cover) * 2.0

        if best is None or risk > best[0]:
            best = (risk, iid, row.get("ingredient_name"), qty, unit, cap, fill, cover, p3)

    if best is None:
        result = []
    else:
        _, iid, name, qty, unit, cap, fill, cover, p3 = best
        triggered = (fill is not None and fill <= FILL_PCT_TRIGGER) or (cover is not None and cover <= DAYS_COVER_TRIGGER)

        if not triggered:
            result = []
        else:
            sev = "CRITICAL" if (fill is not None and fill <= CRITICAL_FILL_PCT) else "HIGH"
            result = [
                alert(
                    "restock_risk",
                    sev,
                    "Restock soon (inventory risk)",
                    "Current stock and projected ingredient draw indicate a restock risk.",
                    {
                        "ingredient_id": iid,
                        "ingredient_name": name,
                        "qty_on_hand": round(qty, 2),
                        "capacity": cap,
                        "fill_pct": round(fill, 3) if fill is not None else None,
                        "days_cover": round(cover, 2) if cover is not None else None,
                        "projected_3d_qty": round(p3, 2),
                        "inventory_snapshot_date": snapshot_date,
                    },
                    [
                        ("RESTOCK_MACHINE", {"machine_id": ctx["ids"]["machine_id"], "restock_level_by_ingredient": {str(iid): "top_up"}, "deadline": date_add(as_of, 1)}),
                        ("ORDER_INGREDIENTS", {"location_id": ctx["ids"]["location_id"], "ingredient_id": iid, "quantity": round(max(p3, qty * 0.5), 2), "unit": unit, "deadline": date_add(as_of, 2)}),
                    ],
                )
            ]
