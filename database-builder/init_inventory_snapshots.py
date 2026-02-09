from __future__ import annotations

from collections import defaultdict
from datetime import datetime, time

from loguru import logger
from sqlalchemy import delete
from sqlmodel import Session, select

from db import (
    DailyIngredientConsumption,
    Machine,
    MachineIngredientCapacity,
    SimInventoryDayStart,
    SimRefillEvent,
    SimRun,
    create_facts_db,
    create_observed_db,
    create_sim_db,
    facts_engine,
    observed_engine,
    sim_engine,
)

REFILL_THRESHOLD_FRACTION = 0.25


def _seed_window(*, run_id: str):
    with Session(sim_engine) as session:
        run = session.get(SimRun, run_id)
        if run is None:
            raise ValueError(f"Unknown run_id: {run_id}")
        return (run.seed_start_date, run.seed_end_date)


def rebuild_inventory_snapshots(*, run_id: str) -> None:
    """
    Seed `sim_inventory_day_start` and `sim_refill_event` for a sim run.

    Design
    - Capacity is a hard fact from `artifacts/vending_machine_facts.db` (`machine_ingredient_capacity`).
    - Daily drawdown is taken from `artifacts/vending_sales_observed.db` (`daily_ingredient_consumption`).
    - Refill events are synthesized with a simple policy:
      if end-of-day on-hand < REFILL_THRESHOLD_FRACTION * capacity, refill to capacity.
    """
    create_facts_db()
    create_observed_db()
    create_sim_db()

    seed_start, seed_end = _seed_window(run_id=run_id)

    with Session(facts_engine) as facts_session:
        machines = facts_session.exec(select(Machine.id, Machine.model)).all()
        capacities = facts_session.exec(select(MachineIngredientCapacity)).all()

    capacity_by_model_ing: dict[tuple[str, int], MachineIngredientCapacity] = {
        (row.machine_model, row.ingredient_id): row for row in capacities
    }

    machine_model_by_id = {int(machine_id): model for machine_id, model in machines}
    if not machine_model_by_id:
        logger.warning("No machines found in facts DB; skipping inventory seed.")
        return

    with Session(observed_engine) as obs_session:
        cons_rows = obs_session.exec(
            select(DailyIngredientConsumption).where(
                (DailyIngredientConsumption.date >= seed_start)
                & (DailyIngredientConsumption.date <= seed_end)
            )
        ).all()

    consumption_by_date_machine_ing: dict[tuple, float] = defaultdict(float)
    unit_by_ing: dict[int, str] = {}
    dates: set = set()
    for row in cons_rows:
        dates.add(row.date)
        consumption_by_date_machine_ing[
            (row.date, row.machine_id, row.ingredient_id)
        ] += float(row.total_quantity)
        unit_by_ing[int(row.ingredient_id)] = row.unit

    if not dates:
        logger.warning(
            "No daily ingredient consumption rows found in observed DB for run_id={run_id}; skipping inventory seed.",
            run_id=run_id,
        )
        return

    all_dates = sorted(dates)

    inventory_rows: list[SimInventoryDayStart] = []
    refill_rows: list[SimRefillEvent] = []

    for machine_id, machine_model in machine_model_by_id.items():
        relevant_caps = [
            cap
            for (model, _ingredient_id), cap in capacity_by_model_ing.items()
            if model == machine_model
        ]
        if not relevant_caps:
            continue

        on_hand_by_ing: dict[int, float] = {}
        for cap in relevant_caps:
            on_hand_by_ing[int(cap.ingredient_id)] = float(cap.capacity)

        for d in all_dates:
            for cap in relevant_caps:
                ing_id = int(cap.ingredient_id)
                unit = cap.unit or unit_by_ing.get(ing_id, "")
                inventory_rows.append(
                    SimInventoryDayStart(
                        run_id=run_id,
                        date=d,
                        machine_id=machine_id,
                        ingredient_id=ing_id,
                        quantity_on_hand=float(
                            on_hand_by_ing.get(ing_id, cap.capacity)
                        ),
                        unit=unit,
                    )
                )

            for cap in relevant_caps:
                ing_id = int(cap.ingredient_id)
                used = float(
                    consumption_by_date_machine_ing.get((d, machine_id, ing_id), 0.0)
                )
                on_hand_by_ing[ing_id] = (
                    float(on_hand_by_ing.get(ing_id, cap.capacity)) - used
                )

            for cap in relevant_caps:
                ing_id = int(cap.ingredient_id)
                capacity = float(cap.capacity)
                threshold = REFILL_THRESHOLD_FRACTION * capacity
                current = float(on_hand_by_ing.get(ing_id, capacity))
                if current >= threshold:
                    continue
                amount_added = max(0.0, capacity - max(0.0, current))
                if amount_added <= 0.0:
                    on_hand_by_ing[ing_id] = capacity
                    continue
                refill_rows.append(
                    SimRefillEvent(
                        run_id=run_id,
                        occurred_at=datetime.combine(d, time(23, 59, 0)),
                        date=d,
                        machine_id=machine_id,
                        ingredient_id=ing_id,
                        quantity_added=amount_added,
                        unit=cap.unit,
                        reason=f"auto_refill_below_{REFILL_THRESHOLD_FRACTION:.2f}x_capacity",
                    )
                )
                on_hand_by_ing[ing_id] = capacity

    with Session(sim_engine) as sim_session:
        sim_session.exec(
            delete(SimInventoryDayStart).where(SimInventoryDayStart.run_id == run_id)
        )
        sim_session.exec(delete(SimRefillEvent).where(SimRefillEvent.run_id == run_id))
        sim_session.commit()
        sim_session.add_all(inventory_rows)
        sim_session.add_all(refill_rows)
        sim_session.commit()

    logger.info(
        "Seeded sim inventory: {inv} start-of-day rows, {refills} refill events for run_id={run_id}.",
        inv=len(inventory_rows),
        refills=len(refill_rows),
        run_id=run_id,
    )


if __name__ == "__main__":
    raise SystemExit(
        "Run via init_db.py (needs a sim run id). Example: python init_db.py"
    )
