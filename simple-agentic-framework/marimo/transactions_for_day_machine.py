from __future__ import annotations

import os
from pathlib import Path

import marimo as mo
import polars as pl

from simple_agent_framework.db import pick_existing_relation, read_frame

app = mo.App(width="full")


@app.cell
def _():
    db_path = Path(os.environ.get("COFFEE_DB_PATH", "coffee.db"))
    db_exists = db_path.exists()
    return db_exists, db_path


@app.cell
def _(db_exists: bool, db_path: Path):
    if not db_exists:
        return {}, None, None, {}

    relations = {
        "transactions": pick_existing_relation(
            db_path, ["transactions", "transaction"]
        ),
        "machines": pick_existing_relation(db_path, ["machines", "machine"]),
        "products": pick_existing_relation(db_path, ["products", "product"]),
        "locations": pick_existing_relation(db_path, ["locations", "location"]),
        "inventory_snapshots": pick_existing_relation(db_path, ["inventory_snapshots"]),
        "ingredients": pick_existing_relation(db_path, ["ingredients", "ingredient"]),
        "daily_ingredient_consumption": pick_existing_relation(
            db_path, ["daily_ingredient_consumption", "dailyingredientconsumption"]
        ),
    }

    machines = read_frame(
        db_path,
        f"""
        SELECT id, name, serial_number
        FROM "{relations["machines"]}"
        ORDER BY name, id
        """.strip(),
    )
    if machines.is_empty():
        return {}, None, None, relations

    machine_options: dict[str, int] = {}
    for row in machines.iter_rows(named=True):
        label = f"{row['name']} (id={row['id']}, sn={row['serial_number']})"
        machine_options[label] = int(row["id"])

    min_max = read_frame(
        db_path,
        f"""
        SELECT MIN(date) AS min_date, MAX(date) AS max_date
        FROM "{relations["transactions"]}"
        """.strip(),
    )
    min_date = min_max["min_date"][0]
    max_date = min_max["max_date"][0]
    return machine_options, min_date, max_date, relations


@app.cell
def _(
    db_exists: bool,
    db_path: Path,
    machine_options: dict[str, int],
    min_date,
    max_date,
    relations: dict[str, str],
):
    if not db_exists:
        mo.md(f"DB not found: `{db_path}`")
        return None, None

    if not machine_options:
        mo.md(f"No machines found in `{db_path}`")
        return None, None

    day_picker = mo.ui.date(
        start=min_date,
        stop=max_date,
        value=max_date,
        label="Day",
    )
    machine_picker = mo.ui.dropdown(
        options=machine_options,
        value=next(iter(machine_options.keys())),
        searchable=True,
        label="Machine",
        full_width=True,
    )

    mo.vstack(
        [
            mo.md(f"DB: `{db_path}` (set `COFFEE_DB_PATH` to override)"),
            mo.hstack([day_picker, machine_picker], justify="start", gap=1),
        ]
    )
    return day_picker, machine_picker


@app.cell
def _(
    db_exists: bool,
    db_path: Path,
    day_picker,
    machine_picker,
    relations: dict[str, str],
):
    if (not db_exists) or day_picker is None or machine_picker is None:
        return pl.DataFrame()

    selected_day = day_picker.value
    selected_machine_id = machine_picker.value

    if selected_day is None or selected_machine_id is None:
        return pl.DataFrame()

    frame = read_frame(
        db_path,
        f"""
        SELECT
            t.id,
            t.occurred_at,
            t.date,
            t.amount,
            t.currency,
            t.cash_type,
            t.card_token,
            p.name AS product,
            m.name AS machine,
            l.name AS location,
            t.source_file,
            t.source_row
        FROM "{relations["transactions"]}" AS t
        JOIN "{relations["products"]}" AS p ON p.id = t.product_id
        JOIN "{relations["machines"]}" AS m ON m.id = t.machine_id
        JOIN "{relations["locations"]}" AS l ON l.id = t.location_id
        WHERE t.date = ? AND t.machine_id = ?
        ORDER BY t.occurred_at, t.id
        """.strip(),
        (selected_day.isoformat(), int(selected_machine_id)),
    )

    if frame.is_empty():
        return frame

    return frame.with_columns(
        pl.col("occurred_at").cast(pl.Utf8).str.strptime(pl.Datetime, strict=False)
    )


@app.cell
def _(frame: pl.DataFrame):
    if frame.is_empty():
        mo.md("No transactions for that day/machine.")
        return

    summary_frame = frame.select(
        pl.len().alias("transactions"),
        pl.col("amount").sum().alias("total_amount"),
        pl.col("currency").n_unique().alias("currencies"),
    )

    by_product_frame = (
        frame.group_by("product")
        .agg(
            pl.len().alias("transactions"),
            pl.col("amount").sum().alias("total_amount"),
        )
        .sort(["transactions", "total_amount"], descending=True)
    )

    mo.vstack(
        [
            mo.md("### Summary"),
            summary_frame,
            mo.md("### By product"),
            by_product_frame,
            mo.md("### Transactions"),
            frame,
        ]
    )


@app.cell
def _(
    db_exists: bool,
    db_path: Path,
    day_picker,
    machine_picker,
    relations: dict[str, str],
):
    if (not db_exists) or day_picker is None or machine_picker is None:
        return pl.DataFrame()

    selected_day = day_picker.value
    selected_machine_id = machine_picker.value
    if selected_day is None or selected_machine_id is None:
        return pl.DataFrame()

    inventory_frame = read_frame(
        db_path,
        f"""
        SELECT
            s.snapshot_date AS date,
            s.machine_id,
            s.ingredient_id,
            i.name AS ingredient,
            s.quantity_on_hand AS start_quantity,
            s.unit AS unit,
            COALESCE(c.total_quantity, 0) AS consumed_quantity,
            (s.quantity_on_hand - COALESCE(c.total_quantity, 0)) AS remaining_quantity
        FROM "{relations["inventory_snapshots"]}" AS s
        JOIN "{relations["ingredients"]}" AS i ON i.id = s.ingredient_id
        LEFT JOIN "{relations["daily_ingredient_consumption"]}" AS c
            ON c.date = s.snapshot_date
            AND c.machine_id = s.machine_id
            AND c.ingredient_id = s.ingredient_id
        WHERE s.snapshot_date = ? AND s.machine_id = ?
        ORDER BY remaining_quantity ASC, ingredient ASC
        """.strip(),
        (selected_day.isoformat(), int(selected_machine_id)),
    )
    return inventory_frame


@app.cell
def _(inventory_frame: pl.DataFrame):
    if inventory_frame.is_empty():
        mo.md("### Inventory\nNo inventory snapshot rows for that day/machine.")
        return

    inventory_summary = (
        inventory_frame.group_by("unit")
        .agg(
            pl.len().alias("ingredients"),
            pl.col("start_quantity").sum().alias("start_total"),
            pl.col("consumed_quantity").sum().alias("consumed_total"),
            pl.col("remaining_quantity").sum().alias("remaining_total"),
            (pl.col("remaining_quantity") < 0).sum().alias("negative_remaining"),
        )
        .sort("unit")
    )

    mo.vstack(
        [
            mo.md("### Inventory (start-of-day, with same-day consumption)"),
            inventory_summary,
            inventory_frame,
        ]
    )


if __name__ == "__main__":
    app.run()
