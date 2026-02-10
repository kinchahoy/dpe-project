from __future__ import annotations

from collections.abc import Mapping
from datetime import date, timedelta
import json
from pathlib import Path
from typing import Any

from .db import VendingDbPaths, query_all, query_one
from .types import ActionType


SCRIPT_CONTEXT_INPUT_NAMES: list[str] = ["ctx"]


def normalize_script_context(context: Mapping[str, Any]) -> dict[str, Any]:
    """Return the canonical sandbox context surface with stable keys/defaults.

    This version intentionally exposes only one input name (`ctx`) to scripts.
    """
    raw_ctx: Any = context.get("ctx") if isinstance(context, Mapping) else None
    if not isinstance(raw_ctx, Mapping):
        raw_ctx = {}

    meta = raw_ctx.get("meta") if isinstance(raw_ctx.get("meta"), Mapping) else {}
    ids = raw_ctx.get("ids") if isinstance(raw_ctx.get("ids"), Mapping) else {}
    entities = (
        raw_ctx.get("entities") if isinstance(raw_ctx.get("entities"), Mapping) else {}
    )
    inventory = (
        raw_ctx.get("inventory")
        if isinstance(raw_ctx.get("inventory"), Mapping)
        else {}
    )
    days = raw_ctx.get("days") if isinstance(raw_ctx.get("days"), list) else []
    price_anomalies = (
        raw_ctx.get("price_anomalies")
        if isinstance(raw_ctx.get("price_anomalies"), list)
        else []
    )

    normalized_ctx = {
        "meta": {
            "as_of_date": str(meta.get("as_of_date") or ""),
            "currency": str(meta.get("currency") or "USD"),
            "run_id": meta.get("run_id"),
        },
        "ids": {
            "location_id": int(ids.get("location_id") or 0),
            "machine_id": int(ids.get("machine_id") or 0),
        },
        "entities": {
            "location": entities.get("location")
            if isinstance(entities.get("location"), Mapping)
            else {},
            "machine": entities.get("machine")
            if isinstance(entities.get("machine"), Mapping)
            else {},
        },
        "days": [d for d in days if isinstance(d, Mapping)],
        "inventory": {
            "snapshot_date": inventory.get("snapshot_date"),
            "by_ingredient": inventory.get("by_ingredient")
            if isinstance(inventory.get("by_ingredient"), list)
            else [],
        },
        "price_anomalies": [d for d in price_anomalies if isinstance(d, Mapping)],
    }

    return {"ctx": normalized_ctx}


def _resolve_projection_anchor_date(
    *,
    dbs: VendingDbPaths,
    run_id: str | None,
    machine_id: int,
    as_of_date: date,
) -> str | None:
    if run_id is None:
        return None

    row = query_one(
        dbs.analysis_db,
        """
        SELECT COALESCE(
            (
                SELECT MAX(projection_date)
                FROM sim_daily_projection
                WHERE run_id = ? AND machine_id = ? AND projection_date <= ?
            ),
            (
                SELECT MAX(projection_date)
                FROM sim_daily_projection
                WHERE run_id = ? AND machine_id = ?
            )
        ) AS projection_date
        """,
        (run_id, machine_id, as_of_date.isoformat(), run_id, machine_id),
        readonly=True,
    )
    if row is None or row.get("projection_date") is None:
        return None
    return str(row["projection_date"])


def _to_float_or_none(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def _normalize_delta_pct(params: Mapping[str, Any]) -> float | None:
    raw_delta = None
    for key in (
        "delta_pct",
        "pct",
        "percent",
        "adjustment_pct",
        "price_change_pct",
        "percent_change",
    ):
        if key in params:
            raw_delta = params.get(key)
            break
    delta = _to_float_or_none(raw_delta)
    direction = str(params.get("direction") or "").strip().lower()
    if delta is None:
        if direction == "increase":
            return 0.04
        if direction == "decrease":
            return -0.04
        return None
    if abs(delta) > 1.0:
        delta = delta / 100.0
    if direction == "increase":
        delta = abs(delta)
    elif direction == "decrease":
        delta = -abs(delta)
    return delta


def _load_price_adjustments(
    *,
    state_db: Path | str | None,
    machine_id: int,
    as_of_date: date,
) -> list[dict[str, Any]]:
    if state_db is None:
        return []

    rows = query_all(
        state_db,
        """
        SELECT effective_date, details_json
        FROM manager_action
        WHERE machine_id = ?
          AND action_type = ?
          AND effective_date <= ?
        ORDER BY effective_date, created_at
        """,
        (machine_id, str(ActionType.ADJUST_PRICE), as_of_date.isoformat()),
    )

    out: list[dict[str, Any]] = []
    for row in rows:
        effective_date = str(row.get("effective_date") or "")
        if not effective_date:
            continue
        details_raw = row.get("details_json")
        details = {}
        if isinstance(details_raw, str):
            try:
                details = json.loads(details_raw)
            except Exception:
                details = {}
        if not isinstance(details, dict):
            details = {}

        multiplier = _to_float_or_none(details.get("multiplier"))
        if multiplier is None:
            multiplier = _to_float_or_none(details.get("factor"))

        new_expected_price = _to_float_or_none(details.get("new_expected_price"))
        delta_pct = _normalize_delta_pct(details)
        product_id_raw = details.get("product_id")
        product_id: int | None = None
        try:
            if product_id_raw is not None:
                product_id = int(product_id_raw)
        except Exception:
            product_id = None

        out.append(
            {
                "effective_date": effective_date,
                "product_id": product_id,
                "multiplier": multiplier,
                "delta_pct": delta_pct,
                "new_expected_price": new_expected_price,
            }
        )
    return out


def _apply_price_adjustments(
    *,
    base_expected: float,
    tx_date: str,
    product_id: int,
    adjustments: list[dict[str, Any]],
) -> float:
    adjusted = base_expected
    for action in adjustments:
        if tx_date < str(action.get("effective_date") or ""):
            continue
        action_product_id = action.get("product_id")
        if action_product_id is not None and int(action_product_id) != product_id:
            continue

        new_expected_price = action.get("new_expected_price")
        if isinstance(new_expected_price, float) and new_expected_price > 0:
            adjusted = new_expected_price
            continue

        multiplier = action.get("multiplier")
        if isinstance(multiplier, float) and multiplier > 0:
            adjusted = adjusted * multiplier
            continue

        delta_pct = action.get("delta_pct")
        if isinstance(delta_pct, float):
            adjusted = adjusted * (1.0 + delta_pct)

    return round(max(0.0, adjusted), 4)


def build_script_context(
    *,
    dbs: VendingDbPaths,
    as_of_date: date,
    location_id: int,
    machine_id: int,
    currency: str,
    state_db: Path | str | None = None,
    history_days: int = 7,
    forecast_days: int = 7,
    inventory_override: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build the sandbox script context.

    The sandbox receives exactly one input name: `ctx`.
    """
    if history_days <= 0:
        raise ValueError("history_days must be > 0")
    if forecast_days < 0:
        raise ValueError("forecast_days must be >= 0")

    history_start = as_of_date - timedelta(days=history_days - 1)
    forecast_end = as_of_date + timedelta(days=forecast_days)
    predicted_start = history_start
    predicted_end = forecast_end

    machine = query_one(
        dbs.facts_db,
        """
        SELECT id, name, serial_number, model, installed_at, last_serviced_at,
               current_hours, location_id
        FROM machine
        WHERE id = ? AND location_id = ?
        """,
        (machine_id, location_id),
        readonly=True,
    )
    if machine is None:
        raise ValueError(f"Machine {machine_id} not found for location {location_id}")

    location = query_one(
        dbs.facts_db,
        "SELECT id, name, timezone, region, external_id FROM location WHERE id = ?",
        (location_id,),
        readonly=True,
    )
    if location is None:
        raise ValueError(f"Location {location_id} not found")

    history_daily = query_all(
        dbs.observed_db,
        """
        SELECT date,
               COUNT(*) AS units_total,
               SUM(amount) AS revenue_total,
               AVG(CASE WHEN cash_type = 'card' THEN 1.0 ELSE 0.0 END) AS card_share
        FROM "transaction"
        WHERE location_id = ? AND machine_id = ?
          AND date BETWEEN ? AND ?
        GROUP BY date
        ORDER BY date
        """,
        (location_id, machine_id, history_start.isoformat(), as_of_date.isoformat()),
        readonly=True,
    )

    history_by_product = query_all(
        dbs.observed_db,
        """
        SELECT s.date AS date,
               s.product_id AS product_id,
               p.name AS product_name,
               s.currency AS currency,
               SUM(s.units_sold) AS units,
               SUM(s.revenue) AS revenue
        FROM daily_product_sales s
        JOIN facts.product p ON p.id = s.product_id
        WHERE s.location_id = ? AND s.machine_id = ?
          AND s.date BETWEEN ? AND ?
        GROUP BY s.date, s.product_id, p.name, s.currency
        ORDER BY s.date, s.product_id
        """,
        (location_id, machine_id, history_start.isoformat(), as_of_date.isoformat()),
        attachments={"facts": dbs.facts_db},
        readonly=True,
    )

    history_by_ingredient = query_all(
        dbs.observed_db,
        """
        SELECT c.date AS date,
               c.ingredient_id AS ingredient_id,
               ing.name AS ingredient_name,
               c.total_quantity AS qty,
               c.unit AS unit
        FROM daily_ingredient_consumption c
        JOIN facts.ingredient ing ON ing.id = c.ingredient_id
        WHERE c.machine_id = ? AND c.date BETWEEN ? AND ?
        ORDER BY c.date, c.ingredient_id
        """,
        (machine_id, history_start.isoformat(), as_of_date.isoformat()),
        attachments={"facts": dbs.facts_db},
        readonly=True,
    )

    run_row = query_one(
        dbs.analysis_db,
        "SELECT id FROM sim_run ORDER BY created_at DESC LIMIT 1",
        readonly=True,
    )
    run_id = str(run_row["id"]) if run_row and run_row.get("id") else None

    if run_id:
        seed_row = query_one(
            dbs.analysis_db,
            "SELECT seed_start_date FROM sim_run WHERE id = ?",
            (run_id,),
            readonly=True,
        )
        seed_start_date_raw = seed_row.get("seed_start_date") if seed_row else None
        seed_start_date = (
            date.fromisoformat(str(seed_start_date_raw)[:10])
            if seed_start_date_raw
            else None
        )

        if seed_start_date:
            last_serviced_at_raw = machine.get("last_serviced_at")
            last_serviced_at = None
            if last_serviced_at_raw:
                try:
                    last_serviced_at = date.fromisoformat(
                        str(last_serviced_at_raw)[:10]
                    )
                except ValueError:
                    last_serviced_at = None

            if last_serviced_at is None or last_serviced_at > seed_start_date:
                service_interval_days = 110
                default_days_remaining = 100
                near_due_days_remaining = 9

                if location_id == 2 and machine_id == 3:
                    days_since_service = service_interval_days - near_due_days_remaining
                else:
                    days_since_service = service_interval_days - default_days_remaining

                machine["last_serviced_at"] = (
                    seed_start_date - timedelta(days=days_since_service)
                ).isoformat()

    projection_anchor_date = _resolve_projection_anchor_date(
        dbs=dbs, run_id=run_id, machine_id=machine_id, as_of_date=as_of_date
    )

    inventory_current: list[dict[str, Any]] = []
    predicted_ingredient_daily: list[dict[str, Any]] = []
    predicted_product_daily: list[dict[str, Any]] = []

    if isinstance(inventory_override, dict):
        inventory_current = (
            inventory_override.get("rows")
            if isinstance(inventory_override.get("rows"), list)
            else []
        )

    if run_id and projection_anchor_date:
        predicted_ingredient_daily = query_all(
            dbs.analysis_db,
            """
            SELECT p.forecast_date AS date,
                   p.ingredient_id AS ingredient_id,
                   ing.name AS ingredient_name,
                   p.forecast_quantity AS qty,
                   p.unit AS unit
            FROM sim_daily_ingredient_projection p
            JOIN facts.ingredient ing ON ing.id = p.ingredient_id
            WHERE p.run_id = ?
              AND p.machine_id = ?
              AND p.projection_date = ?
              AND p.forecast_date BETWEEN ? AND ?
            ORDER BY p.forecast_date, p.ingredient_id
            """,
            (
                run_id,
                machine_id,
                projection_anchor_date,
                predicted_start.isoformat(),
                predicted_end.isoformat(),
            ),
            attachments={"facts": dbs.facts_db},
            readonly=True,
        )

        predicted_product_daily = query_all(
            dbs.analysis_db,
            """
            SELECT p.forecast_date AS date,
                   p.product_id AS product_id,
                   prod.name AS product_name,
                   SUM(p.forecast_units) AS units
            FROM sim_daily_projection p
            JOIN facts.product prod ON prod.id = p.product_id
            WHERE p.run_id = ?
              AND p.machine_id = ?
              AND p.projection_date = ?
              AND p.forecast_date BETWEEN ? AND ?
              AND p.product_id IS NOT NULL
            GROUP BY p.forecast_date, p.product_id, prod.name
            ORDER BY p.forecast_date, p.product_id
            """,
            (
                run_id,
                machine_id,
                projection_anchor_date,
                predicted_start.isoformat(),
                predicted_end.isoformat(),
            ),
            attachments={"facts": dbs.facts_db},
            readonly=True,
        )

    inventory_snapshot_date = (
        str(inventory_override.get("snapshot_date") or "")
        if isinstance(inventory_override, dict)
        else (
            str(inventory_current[0].get("snapshot_date"))
            if inventory_current
            and inventory_current[0].get("snapshot_date") is not None
            else None
        )
    )

    totals_by_date: dict[str, dict[str, Any]] = {}
    for row in history_daily:
        dt = str(row.get("date") or "")
        if not dt:
            continue
        totals_by_date[dt] = {
            "units": float(row.get("units_total") or 0.0),
            "revenue": float(row.get("revenue_total") or 0.0),
            "card_share": (
                float(row["card_share"]) if row.get("card_share") is not None else None
            ),
        }

    observed_products_by_date: dict[str, list[dict[str, Any]]] = {}
    for row in history_by_product:
        dt = str(row.get("date") or "")
        if not dt:
            continue
        observed_products_by_date.setdefault(dt, []).append(
            {
                "product_id": int(row["product_id"]),
                "product_name": row.get("product_name"),
                "units": float(row.get("units") or 0.0),
                "revenue": float(row.get("revenue") or 0.0),
            }
        )

    observed_ingredients_by_date: dict[str, list[dict[str, Any]]] = {}
    for row in history_by_ingredient:
        dt = str(row.get("date") or "")
        if not dt:
            continue
        observed_ingredients_by_date.setdefault(dt, []).append(
            {
                "ingredient_id": int(row["ingredient_id"]),
                "ingredient_name": row.get("ingredient_name"),
                "qty": float(row.get("qty") or 0.0),
                "unit": row.get("unit"),
            }
        )

    predicted_products_by_date: dict[str, list[dict[str, Any]]] = {}
    for row in predicted_product_daily:
        dt = str(row.get("date") or "")
        if not dt:
            continue
        predicted_products_by_date.setdefault(dt, []).append(
            {
                "product_id": int(row["product_id"]),
                "product_name": row.get("product_name"),
                "units": float(row.get("units") or 0.0),
            }
        )

    predicted_ingredients_by_date: dict[str, list[dict[str, Any]]] = {}
    for row in predicted_ingredient_daily:
        dt = str(row.get("date") or "")
        if not dt:
            continue
        predicted_ingredients_by_date.setdefault(dt, []).append(
            {
                "ingredient_id": int(row["ingredient_id"]),
                "ingredient_name": row.get("ingredient_name"),
                "qty": float(row.get("qty") or 0.0),
                "unit": row.get("unit"),
            }
        )

    days: list[dict[str, Any]] = []
    for offset in range(-(history_days - 1), 1):
        dt = as_of_date + timedelta(days=offset)
        dt_str = dt.isoformat()
        days.append(
            {
                "kind": "observed",
                "date": dt_str,
                "offset_days": offset,
                "totals": totals_by_date.get(
                    dt_str, {"units": 0.0, "revenue": 0.0, "card_share": None}
                ),
                "by_product": observed_products_by_date.get(dt_str, []),
                "by_ingredient": observed_ingredients_by_date.get(dt_str, []),
            }
        )

    for offset in range(-(history_days - 1), forecast_days + 1):
        dt = as_of_date + timedelta(days=offset)
        dt_str = dt.isoformat()
        days.append(
            {
                "kind": "predicted",
                "date": dt_str,
                "offset_days": offset,
                "by_product": predicted_products_by_date.get(dt_str, []),
                "by_ingredient": predicted_ingredients_by_date.get(dt_str, []),
            }
        )

    inventory_rows: list[dict[str, Any]] = []
    for row in inventory_current:
        capacity_raw = row.get("capacity")
        inventory_rows.append(
            {
                "ingredient_id": int(row["ingredient_id"]),
                "ingredient_name": row.get("ingredient_name"),
                "qty_on_hand": float(row.get("quantity_on_hand") or 0.0),
                "unit": row.get("unit"),
                "capacity": float(capacity_raw) if capacity_raw is not None else None,
                "capacity_unit": row.get("capacity_unit"),
            }
        )

    price_anomalies: list[dict[str, Any]] = []
    if run_id:
        price_adjustments = _load_price_adjustments(
            state_db=state_db,
            machine_id=machine_id,
            as_of_date=as_of_date,
        )
        price_rows = query_all(
            dbs.analysis_db,
            """
            SELECT t.date AS date,
                   t.product_id AS product_id,
                   p.name AS product_name,
                   t.amount AS amount,
                   t.expected_price AS expected_price,
                   t.currency AS currency
            FROM sim_transaction_expanded t
            JOIN facts.product p ON p.id = t.product_id
            WHERE t.run_id = ?
              AND t.location_id = ?
              AND t.machine_id = ?
              AND t.date BETWEEN ? AND ?
              AND t.expected_price IS NOT NULL
              AND t.expected_price > 0
            ORDER BY t.date, t.product_id, t.id
            """,
            (
                run_id,
                location_id,
                machine_id,
                history_start.isoformat(),
                as_of_date.isoformat(),
            ),
            attachments={"facts": dbs.facts_db},
            readonly=True,
        )

        by_product: dict[int, dict[str, Any]] = {}
        for row in price_rows:
            product_id = int(row["product_id"])
            base_expected = float(row.get("expected_price") or 0.0)
            expected = _apply_price_adjustments(
                base_expected=base_expected,
                tx_date=str(row.get("date") or ""),
                product_id=product_id,
                adjustments=price_adjustments,
            )
            amount = float(row.get("amount") or 0.0)
            if expected <= 0:
                continue
            delta_pct = (amount - expected) / expected
            if delta_pct > -0.05:
                continue

            pid = product_id
            slot = by_product.setdefault(
                pid,
                {
                    "product_id": pid,
                    "product_name": row.get("product_name"),
                    "undercharge_count": 0,
                    "examples": [],
                },
            )
            slot["undercharge_count"] += 1
            examples = slot["examples"]
            if isinstance(examples, list) and len(examples) < 3:
                examples.append(
                    {
                        "date": str(row.get("date") or ""),
                        "amount": round(amount, 4),
                        "expected_price": round(expected, 4),
                        "delta_pct": round(delta_pct * 100, 2),
                        "currency": row.get("currency"),
                    }
                )

        price_anomalies = sorted(
            (
                v
                for v in by_product.values()
                if int(v.get("undercharge_count") or 0) > 0
            ),
            key=lambda item: int(item.get("undercharge_count") or 0),
            reverse=True,
        )

    ctx = {
        "meta": {
            "as_of_date": as_of_date.isoformat(),
            "currency": currency,
            "run_id": run_id,
        },
        "ids": {"location_id": location_id, "machine_id": machine_id},
        "entities": {"location": location, "machine": machine},
        "days": days,
        "inventory": {
            "snapshot_date": inventory_snapshot_date,
            "by_ingredient": inventory_rows,
        },
        "price_anomalies": price_anomalies,
    }
    return normalize_script_context({"ctx": ctx})
