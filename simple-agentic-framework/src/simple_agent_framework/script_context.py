from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from typing import Any

from .db import fetch_all, fetch_one, pick_existing_relation


def build_script_context(
    *,
    data_db: Path | str,
    as_of_date: date,
    location_id: int,
    machine_id: int,
    currency: str,
    baseline_weeks: int = 8,
    history_days: int = 35,
    discrepancy_epsilon: float = 0.05,
) -> dict[str, Any]:
    """Build a flat dict of inputs for sandboxed alert scripts.

    Every value is a plain Python primitive, list-of-dicts, or dict —
    no nested 'frames' wrapper, no Polars objects, no custom types.
    """
    history_start = as_of_date - timedelta(days=history_days - 1)
    week_start = as_of_date - timedelta(days=6)
    forecast_end = as_of_date + timedelta(days=10)

    discrepancy_relation = pick_existing_relation(
        data_db,
        ["enhanced_transactions", "transaction_expanded", "enhancedtransaction"],
    )

    machine = fetch_one(
        data_db,
        """
        SELECT id, name, serial_number, model, installed_at,
               last_serviced_at, current_hours, location_id
        FROM machines
        WHERE id = ? AND location_id = ?
        """,
        (machine_id, location_id),
    )
    if machine is None:
        raise ValueError(f"Machine {machine_id} not found for location {location_id}")

    location = fetch_one(
        data_db,
        "SELECT id, name, timezone, region, external_id FROM locations WHERE id = ?",
        (location_id,),
    )
    if location is None:
        raise ValueError(f"Location {location_id} not found")

    baseline_dates = [
        (as_of_date - timedelta(days=7 * i)).isoformat()
        for i in range(1, baseline_weeks + 1)
    ]

    daily_product_sales = fetch_all(
        data_db,
        """
        SELECT date, location_id, machine_id, product_id, currency,
               cash_type, units_sold
        FROM daily_product_sales
        WHERE location_id = ? AND machine_id = ?
          AND date BETWEEN ? AND ?
        ORDER BY date, product_id
        """,
        (location_id, machine_id, history_start.isoformat(), as_of_date.isoformat()),
    )

    revenue = fetch_all(
        data_db,
        """
        SELECT date, product_id, cash_type, currency,
               COUNT(*) AS tx_count, SUM(amount) AS revenue
        FROM transactions
        WHERE location_id = ? AND machine_id = ?
          AND date BETWEEN ? AND ?
        GROUP BY date, product_id, cash_type, currency
        ORDER BY date, product_id
        """,
        (location_id, machine_id, history_start.isoformat(), as_of_date.isoformat()),
    )

    ingredient_use = fetch_all(
        data_db,
        """
        SELECT date, machine_id, ingredient_id,
               total_quantity AS quantity_consumed, unit
        FROM daily_ingredient_consumption
        WHERE machine_id = ? AND date BETWEEN ? AND ?
        ORDER BY date, ingredient_id
        """,
        (machine_id, history_start.isoformat(), as_of_date.isoformat()),
    )

    price_discrepancies = fetch_all(
        data_db,
        f"""
        SELECT date, product_id, cash_type, amount, expected_price,
               ABS(amount - expected_price) AS delta
        FROM {discrepancy_relation}
        WHERE location_id = ? AND machine_id = ?
          AND date BETWEEN ? AND ?
          AND expected_price IS NOT NULL
          AND ABS(amount - expected_price) > ?
        ORDER BY date
        """,
        (
            location_id, machine_id,
            history_start.isoformat(), as_of_date.isoformat(),
            discrepancy_epsilon,
        ),
    )

    product_forecasts = fetch_all(
        data_db,
        """
        SELECT forecast_date, product_id, forecast_units, model_name
        FROM daily_projections
        WHERE location_id = ? AND machine_id = ?
          AND forecast_date > ? AND forecast_date <= ?
        ORDER BY forecast_date, product_id
        """,
        (location_id, machine_id, as_of_date.isoformat(), forecast_end.isoformat()),
    )

    ingredient_forecasts = fetch_all(
        data_db,
        """
        SELECT forecast_date, ingredient_id, forecast_quantity, unit, model_name
        FROM daily_ingredient_projections
        WHERE location_id = ? AND machine_id = ?
          AND forecast_date > ? AND forecast_date <= ?
        ORDER BY forecast_date, ingredient_id
        """,
        (location_id, machine_id, as_of_date.isoformat(), forecast_end.isoformat()),
    )

    hourly_product_sales = fetch_all(
        data_db,
        """
        SELECT date,
               CAST(strftime('%H', occurred_at) AS INTEGER) AS hour_of_day,
               product_id, COUNT(*) AS units_sold
        FROM transactions
        WHERE location_id = ? AND machine_id = ?
          AND date BETWEEN ? AND ?
        GROUP BY date, hour_of_day, product_id
        ORDER BY date, hour_of_day, product_id
        """,
        (location_id, machine_id, week_start.isoformat(), as_of_date.isoformat()),
    )

    cash_mix = fetch_all(
        data_db,
        """
        SELECT date, cash_type, COUNT(*) AS units, SUM(amount) AS revenue
        FROM transactions
        WHERE location_id = ? AND machine_id = ?
          AND date BETWEEN ? AND ?
        GROUP BY date, cash_type
        ORDER BY date, cash_type
        """,
        (location_id, machine_id, history_start.isoformat(), as_of_date.isoformat()),
    )

    # daily_totals: one row per day covering the full history window.
    # Scripts filter by baseline_dates or as_of_date themselves.
    daily_totals = fetch_all(
        data_db,
        """
        SELECT date,
               COUNT(*) AS units_total,
               SUM(amount) AS revenue_total,
               AVG(CASE WHEN cash_type = 'card' THEN 1.0 ELSE 0.0 END) AS card_share
        FROM transactions
        WHERE location_id = ? AND machine_id = ?
          AND date BETWEEN ? AND ?
        GROUP BY date
        ORDER BY date
        """,
        (location_id, machine_id, history_start.isoformat(), as_of_date.isoformat()),
    )

    return {
        "as_of_date": as_of_date.isoformat(),
        "location_id": location_id,
        "machine_id": machine_id,
        "currency": currency,
        "machine": machine,
        "location": location,
        "baseline_dates": baseline_dates,
        "daily_product_sales": daily_product_sales,
        "revenue": revenue,
        "ingredient_use": ingredient_use,
        "price_discrepancies": price_discrepancies,
        "product_forecasts": product_forecasts,
        "ingredient_forecasts": ingredient_forecasts,
        "hourly_product_sales": hourly_product_sales,
        "cash_mix": cash_mix,
        "daily_totals": daily_totals,
    }
