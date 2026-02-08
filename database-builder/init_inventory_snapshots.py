from __future__ import annotations

from collections import defaultdict

from loguru import logger
from sqlalchemy import delete, text
from sqlmodel import Session, select

from db import Ingredient, InventorySnapshot, Machine, Transaction, create_db, engine

DAYS_OF_CAPACITY = 5


def rebuild_inventory_snapshots() -> None:
    """
    Seed start-of-day on-hand inventory snapshots.

    Assumptions (simple + intended for demo/alert prototyping):
    - Each machine is "fully restocked" at the start of every day.
    - "Capacity" for each (machine, ingredient) is ~N days of typical use, where typical
      use is inferred from historical daily ingredient consumption.
    """
    create_db()
    with Session(engine) as session:
        machine_ids = list(session.exec(select(Machine.id)).all())
        ingredient_rows = list(
            session.exec(select(Ingredient.id, Ingredient.unit)).all()
        )
        ingredient_unit_by_id = {
            ingredient_id: unit for ingredient_id, unit in ingredient_rows
        }

        txn_dates = list(session.exec(select(Transaction.date)).all())
        snapshot_dates = sorted(set(txn_dates))
        if not snapshot_dates:
            logger.warning(
                "No transactions found. Skipping inventory snapshot rebuild."
            )
            return

        consumption_rows = session.exec(
            text(
                """
                SELECT
                    t.date AS date,
                    t.machine_id AS machine_id,
                    pi.ingredient_id AS ingredient_id,
                    SUM(pi.quantity) AS total_quantity
                FROM "transaction" AS t
                JOIN "productingredient" AS pi
                    ON pi.product_id = t.product_id
                GROUP BY
                    t.date,
                    t.machine_id,
                    pi.ingredient_id
                """
            )
        ).all()

        total_qty_by_machine_ing: dict[tuple[int, int], float] = defaultdict(float)
        day_count_by_machine_ing: dict[tuple[int, int], int] = defaultdict(int)
        total_qty_by_ing: dict[int, float] = defaultdict(float)
        day_count_by_ing: dict[int, int] = defaultdict(int)

        for _date_value, machine_id, ingredient_id, total_quantity in consumption_rows:
            key = (int(machine_id), int(ingredient_id))
            qty = float(total_quantity or 0.0)
            total_qty_by_machine_ing[key] += qty
            day_count_by_machine_ing[key] += 1
            total_qty_by_ing[int(ingredient_id)] += qty
            day_count_by_ing[int(ingredient_id)] += 1

        capacity_by_machine_ing: dict[tuple[int, int], float] = {}
        for machine_id in machine_ids:
            for ingredient_id in ingredient_unit_by_id.keys():
                key = (int(machine_id), int(ingredient_id))
                days = day_count_by_machine_ing.get(key, 0)
                if days > 0:
                    avg_daily = total_qty_by_machine_ing[key] / days
                else:
                    ing_days = day_count_by_ing.get(int(ingredient_id), 0)
                    avg_daily = (
                        (total_qty_by_ing[int(ingredient_id)] / ing_days)
                        if ing_days > 0
                        else 0.0
                    )
                capacity_by_machine_ing[key] = max(
                    0.0, float(avg_daily) * DAYS_OF_CAPACITY
                )

        session.exec(delete(InventorySnapshot))
        session.commit()

        rows: list[InventorySnapshot] = []
        for snapshot_date in snapshot_dates:
            for machine_id in machine_ids:
                for ingredient_id, unit in ingredient_unit_by_id.items():
                    capacity = capacity_by_machine_ing[
                        (int(machine_id), int(ingredient_id))
                    ]
                    rows.append(
                        InventorySnapshot(
                            snapshot_date=snapshot_date,
                            machine_id=int(machine_id),
                            ingredient_id=int(ingredient_id),
                            quantity_on_hand=capacity,
                            unit=unit,
                        )
                    )

        session.add_all(rows)
        session.commit()
        logger.info(
            "Rebuilt inventory snapshots: {dates} dates, {machines} machines, {ingredients} ingredients => {rows} rows.",
            dates=len(snapshot_dates),
            machines=len(machine_ids),
            ingredients=len(ingredient_unit_by_id),
            rows=len(rows),
        )


if __name__ == "__main__":
    rebuild_inventory_snapshots()
