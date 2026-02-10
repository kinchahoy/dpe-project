from __future__ import annotations

import csv
import re
from datetime import date, datetime, time, timedelta, timezone
from glob import glob
from pathlib import Path
from typing import Dict
from uuid import uuid4

from loguru import logger
from sqlalchemy import delete
from sqlmodel import Session, col, select

from db import (
    Ingredient,
    Location,
    Machine,
    MachineIngredientCapacity,
    Product,
    ProductIngredient,
    SimRun,
    Transaction,
    FACTS_DB_FILE,
    OBSERVED_DB_FILE,
    SIM_DB_FILE,
    create_facts_db,
    create_observed_db,
    create_sim_db,
    facts_engine,
    observed_engine,
    sim_engine,
)
from init_daily_aggregates import rebuild_daily_aggregates
from init_daily_ingredient_projections import rebuild_daily_ingredient_projections
from init_daily_projections import rebuild_daily_projections
from init_price_schedule import init_price_schedule, update_expected_prices
from product_catalog import canonicalize_product_name
from product_ing import (
    PRODUCT_INGREDIENTS,
    ingredient_units,
    write_product_list_from_csvs,
)

# --- PLACEHOLDER DATA (keep all placeholders together) ---
UAH_TO_USD = 0.027

SERVICE_INTERVAL_DAYS = 110
DEFAULT_SERVICE_DAYS_REMAINING_AT_SIM_START = 100
NEAR_DUE_SERVICE_DAYS_REMAINING_AT_SIM_START = 9

PLACEHOLDER_UA_LOCATION = {
    "name": "Lviv, Ukraine",
    "external_id": "UA-LVIV-001",
    "timezone": "Europe/Kyiv",
    "region": "Lviv Oblast",
    "address": "Unknown address, Lviv, Ukraine",
}
PLACEHOLDER_SF_LOCATION = {
    "name": "San Francisco, CA",
    "external_id": "US-SF-001",
    "timezone": "America/Los_Angeles",
    "region": "California",
    "address": "Unknown address, San Francisco, CA, USA",
}
PLACEHOLDER_UA_MACHINE_1 = {
    "name": "UA-Cafe-1",
    "serial_number": "UA-VM-0001",
    "model": "CoffeeVend-X1",
    "installed_at": datetime(2024, 1, 15, 9, 0, 0),
    "last_serviced_at": datetime(2025, 1, 15, 9, 0, 0),
    "current_hours": 1250,
}
PLACEHOLDER_UA_MACHINE_2 = {
    "name": "UA-Cafe-2",
    "serial_number": "UA-VM-0002",
    "model": "CoffeeVend-X2",
    "installed_at": datetime(2024, 2, 10, 9, 0, 0),
    "last_serviced_at": datetime(2025, 2, 10, 9, 0, 0),
    "current_hours": 1180,
}
PLACEHOLDER_SF_MACHINE_3 = {
    "name": "SF-Cafe-3",
    "serial_number": "US-SF-0003",
    "model": "CoffeeVend-X1",
    "installed_at": datetime(2025, 1, 20, 9, 0, 0),
    "last_serviced_at": datetime(2025, 12, 20, 9, 0, 0),
    "current_hours": 900,
}
PLACEHOLDER_SF_MACHINE_4 = {
    "name": "SF-Cafe-4",
    "serial_number": "US-SF-0004",
    "model": "CoffeeVend-X2",
    "installed_at": datetime(2025, 3, 5, 9, 0, 0),
    "last_serviced_at": datetime(2026, 1, 5, 9, 0, 0),
    "current_hours": 720,
}
# --- END PLACEHOLDER DATA ---


def _parse_datetime(value: str) -> datetime:
    return datetime.fromisoformat(value)


def _reset_db_files() -> None:
    for path in (FACTS_DB_FILE, OBSERVED_DB_FILE, SIM_DB_FILE):
        if path.exists():
            path.unlink()


def _get_or_create_location(session: Session, data: dict) -> Location:
    existing = session.exec(
        select(Location).where(Location.external_id == data["external_id"])
    ).first()
    if existing is not None:
        return existing
    location = Location(**data)
    session.add(location)
    session.flush()
    return location


def _get_or_create_machine(
    session: Session, *, location_id: int, data: dict
) -> Machine:
    existing = session.exec(
        select(Machine).where(Machine.serial_number == data["serial_number"])
    ).first()
    if existing is not None:
        return existing
    machine = Machine(location_id=location_id, **data)
    session.add(machine)
    session.flush()
    return machine


def _seed_ingredients(session: Session) -> None:
    ingredient_cache: Dict[str, int] = {}
    for name, unit in ingredient_units().items():
        existing = session.exec(
            select(Ingredient).where(Ingredient.name == name)
        ).first()
        if existing is None:
            existing = Ingredient(name=name, unit=unit)
            session.add(existing)
            session.flush()
        ingredient_cache[name] = existing.id

    for product_name, specs in PRODUCT_INGREDIENTS.items():
        canonical_product_name = canonicalize_product_name(product_name)
        product = session.exec(
            select(Product).where(Product.name == canonical_product_name)
        ).first()
        if product is None:
            logger.warning(
                "No product row for ingredient mapping: {product}",
                product=canonical_product_name,
            )
            continue
        for spec in specs:
            ingredient_id = ingredient_cache[spec.name]
            existing = session.exec(
                select(ProductIngredient).where(
                    ProductIngredient.product_id == product.id,
                    ProductIngredient.ingredient_id == ingredient_id,
                )
            ).first()
            if existing is None:
                session.add(
                    ProductIngredient(
                        product_id=product.id,
                        ingredient_id=ingredient_id,
                        quantity=spec.quantity,
                    )
                )


def _canonicalize_ingredient_key(label: str) -> str:
    key = label.strip().lower()
    key = re.sub(r"[^a-z0-9]+", "_", key)
    key = re.sub(r"_+", "_", key).strip("_")
    return key


def _seed_machine_capacities(session: Session, capacities_md: Path) -> None:
    """
    Seed `machine_ingredient_capacity` from `machine_capacities.md`.

    The markdown uses human-readable ingredient names; we normalize them to the
    canonical ingredient keys used in `product_ing.py` (e.g. "Espresso Shot" -> "espresso_shot").
    """
    if not capacities_md.exists():
        logger.warning("No machine capacities file found at {path}", path=capacities_md)
        return

    ingredient_id_by_name = {
        ing.name: ing.id for ing in session.exec(select(Ingredient)).all()
    }

    current_model: str | None = None
    rows: list[MachineIngredientCapacity] = []
    for raw_line in capacities_md.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if line.startswith("## "):
            current_model = line.removeprefix("## ").split("(", 1)[0].strip()
            continue
        if current_model is None:
            continue
        if not line.startswith("|"):
            continue
        if "Ingredient" in line and "Capacity" in line:
            continue
        if set(line.replace("|", "").strip()) == {"-"}:
            continue
        parts = [part.strip() for part in line.strip("|").split("|")]
        if len(parts) < 4:
            continue
        ingredient_label, _avg_daily, cap_value, unit = parts[:4]
        notes = parts[4] if len(parts) >= 5 else ""

        ingredient_name = _canonicalize_ingredient_key(ingredient_label)
        ingredient_id = ingredient_id_by_name.get(ingredient_name)
        if ingredient_id is None:
            logger.warning(
                "Capacity references unknown ingredient '{ingredient}' (normalized='{norm}') for model '{model}'",
                ingredient=ingredient_label,
                norm=ingredient_name,
                model=current_model,
            )
            continue

        cap_clean = cap_value.replace(",", "").strip()
        try:
            capacity = float(cap_clean)
        except ValueError:
            logger.warning(
                "Could not parse capacity '{cap}' for ingredient '{ingredient}' model '{model}'",
                cap=cap_value,
                ingredient=ingredient_label,
                model=current_model,
            )
            continue

        rows.append(
            MachineIngredientCapacity(
                machine_model=current_model,
                ingredient_id=int(ingredient_id),
                capacity=capacity,
                unit=unit.strip(),
                notes=notes.strip(),
            )
        )

    session.exec(delete(MachineIngredientCapacity))
    session.add_all(rows)
    session.commit()
    logger.info("Seeded machine ingredient capacities: {count} rows.", count=len(rows))


def _create_sim_run_from_observed(*, seed_days: int = 30) -> tuple[str, date, date]:
    create_sim_db()
    with Session(observed_engine) as obs:
        max_date = obs.exec(
            select(col(Transaction.date)).order_by(col(Transaction.date).desc())
        ).first()
        min_date = obs.exec(
            select(col(Transaction.date)).order_by(col(Transaction.date).asc())
        ).first()

    if max_date is None or min_date is None:
        raise RuntimeError("No observed transactions; cannot create a sim run.")

    seed_end = max_date
    requested_start = seed_end - timedelta(days=seed_days - 1)
    seed_start = max(min_date, requested_start)

    run_id = str(uuid4())
    with Session(sim_engine) as sim:
        sim.add(
            SimRun(
                id=run_id,
                created_at=datetime.now(tz=timezone.utc),
                seed_start_date=seed_start,
                seed_end_date=seed_end,
                notes=f"seed_window={seed_days}d (observed {seed_start}..{seed_end})",
            )
        )
        sim.commit()
    logger.info(
        "Created sim run: {run_id} seed={start}..{end}",
        run_id=run_id,
        start=seed_start,
        end=seed_end,
    )
    return run_id, seed_start, seed_end


def _align_machine_service_facts_to_seed_window(
    *,
    seed_start: date,
    sf_location_id: int,
    sf_machine_3_id: int,
) -> None:
    """Overwrite placeholder machine facts so they are coherent at sim start.

    Targets:
    - installed_at <= last_serviced_at <= seed_start_date
    - last_serviced_at implies "days to go" until next service:
      - all machines: 100 days remaining (default)
      - SF machine #3: 9 days remaining (near due)
    """

    installed_at = datetime.combine((seed_start - timedelta(days=365)), time(9, 0))

    default_days_since_service = (
        SERVICE_INTERVAL_DAYS - DEFAULT_SERVICE_DAYS_REMAINING_AT_SIM_START
    )
    near_due_days_since_service = (
        SERVICE_INTERVAL_DAYS - NEAR_DUE_SERVICE_DAYS_REMAINING_AT_SIM_START
    )

    with Session(facts_engine) as facts:
        machines = facts.exec(select(Machine)).all()
        for machine in machines:
            machine.installed_at = installed_at
            if int(machine.location_id) == int(sf_location_id) and int(
                machine.id or 0
            ) == int(sf_machine_3_id):
                last_serviced_at = datetime.combine(
                    (seed_start - timedelta(days=near_due_days_since_service)),
                    time(9, 0),
                )
            else:
                last_serviced_at = datetime.combine(
                    (seed_start - timedelta(days=default_days_since_service)),
                    time(9, 0),
                )

            machine.last_serviced_at = last_serviced_at
            machine.current_hours = 0
            facts.add(machine)
        facts.commit()


def load_csvs() -> None:
    _reset_db_files()
    create_facts_db()
    create_observed_db()
    create_sim_db()

    product_names = write_product_list_from_csvs()
    files = sorted(glob("index_*.csv"))
    if not files:
        logger.warning("No index_*.csv files found")
        return

    with Session(facts_engine) as facts:
        ua_location = _get_or_create_location(facts, PLACEHOLDER_UA_LOCATION)
        sf_location = _get_or_create_location(facts, PLACEHOLDER_SF_LOCATION)
        assert ua_location.id is not None
        assert sf_location.id is not None
        ua_location_id = int(ua_location.id)
        sf_location_id = int(sf_location.id)

        ua_machine_1 = _get_or_create_machine(
            facts, location_id=ua_location_id, data=PLACEHOLDER_UA_MACHINE_1
        )
        ua_machine_2 = _get_or_create_machine(
            facts, location_id=ua_location_id, data=PLACEHOLDER_UA_MACHINE_2
        )
        sf_machine_3 = _get_or_create_machine(
            facts, location_id=sf_location_id, data=PLACEHOLDER_SF_MACHINE_3
        )
        sf_machine_4 = _get_or_create_machine(
            facts, location_id=sf_location_id, data=PLACEHOLDER_SF_MACHINE_4
        )
        facts.commit()
        assert ua_machine_1.id is not None
        assert ua_machine_2.id is not None
        assert sf_machine_3.id is not None
        assert sf_machine_4.id is not None
        ua_machine_1_id = int(ua_machine_1.id)
        ua_machine_2_id = int(ua_machine_2.id)
        sf_machine_3_id = int(sf_machine_3.id)
        sf_machine_4_id = int(sf_machine_4.id)

        # Seed products (from CSV scan) before loading transactions.
        product_cache: Dict[str, int] = {}
        for name in product_names:
            canonical_name = canonicalize_product_name(name)
            existing = facts.exec(
                select(Product).where(Product.name == canonical_name)
            ).first()
            if existing is None:
                existing = Product(name=canonical_name)
                facts.add(existing)
                facts.flush()
            product_cache[canonical_name] = int(existing.id)
        facts.commit()

        # Seed ingredients + recipes + capacities (facts).
        _seed_ingredients(facts)
        facts.commit()
        _seed_machine_capacities(facts, Path("machine_capacities.md"))

        machine_map = {
            "index_1.csv": (ua_machine_1_id, sf_machine_3_id),
            "index_2.csv": (ua_machine_2_id, sf_machine_4_id),
        }

    with Session(observed_engine) as obs:
        obs.exec(delete(Transaction))
        obs.commit()

        for f in files:
            path = Path(f)
            machine_pair = machine_map.get(path.name)
            if machine_pair is None:
                logger.warning("No machine mapping for {file}", file=path.name)
                continue
            ua_machine_id, sf_machine_id = machine_pair
            with path.open(newline="", encoding="utf-8") as fh:
                reader = csv.DictReader(fh)
                for row_idx, row in enumerate(reader, start=1):
                    name = canonicalize_product_name(row["coffee_name"])
                    product_id = product_cache.get(name)
                    if product_id is None:
                        logger.warning(
                            "Unknown product '{name}' in {file}:{row}; skipping row",
                            name=name,
                            file=path.name,
                            row=row_idx,
                        )
                        continue

                    card = row.get("card")
                    base_amount = float(row["money"])
                    occurred_at = _parse_datetime(row["datetime"])
                    base_date = datetime.fromisoformat(row["date"]).date()

                    ua_txn = Transaction(
                        product_id=product_id,
                        location_id=ua_location_id,
                        machine_id=ua_machine_id,
                        date=base_date,
                        occurred_at=occurred_at,
                        cash_type=row["cash_type"],
                        card_token=card if card else None,
                        amount=base_amount,
                        currency="UAH",
                        source_file=path.name,
                        source_row=row_idx,
                    )
                    sf_txn = Transaction(
                        product_id=product_id,
                        location_id=sf_location_id,
                        machine_id=sf_machine_id,
                        date=base_date,
                        occurred_at=occurred_at,
                        cash_type=row["cash_type"],
                        card_token=card if card else None,
                        amount=round(base_amount * UAH_TO_USD, 2),
                        currency="USD",
                        source_file=path.name,
                        source_row=row_idx,
                    )
                    obs.add(ua_txn)
                    obs.add(sf_txn)
            obs.commit()
            logger.info("Loaded {file}", file=path.name)

    if product_names:
        missing = [
            name
            for name in product_names
            if canonicalize_product_name(name)
            not in {canonicalize_product_name(k) for k in PRODUCT_INGREDIENTS}
        ]
        if missing:
            logger.warning(
                "Missing ingredient mappings for: {names}",
                names=", ".join(missing),
            )

    rebuild_daily_aggregates()
    run_id, seed_start, _seed_end = _create_sim_run_from_observed(seed_days=30)

    _align_machine_service_facts_to_seed_window(
        seed_start=seed_start,
        sf_location_id=sf_location_id,
        sf_machine_3_id=sf_machine_3_id,
    )

    init_price_schedule(run_id=run_id)
    update_expected_prices(run_id=run_id)
    rebuild_daily_projections(run_id=run_id)
    rebuild_daily_ingredient_projections(run_id=run_id)


if __name__ == "__main__":
    load_csvs()
