from __future__ import annotations

import csv
from datetime import datetime
from glob import glob
from pathlib import Path
from typing import Dict

from loguru import logger
from sqlmodel import Session, select

from db import (
    Ingredient,
    Location,
    Machine,
    Product,
    ProductIngredient,
    Transaction,
    create_db,
    engine,
)
from init_daily_ingredient_projections import rebuild_daily_ingredient_projections
from init_daily_projections import rebuild_daily_projections
from init_inventory_snapshots import rebuild_inventory_snapshots
from init_price_schedule import init_price_schedule, update_expected_prices
from product_catalog import canonicalize_product_name
from product_ing import (
    PRODUCT_INGREDIENTS,
    ingredient_units,
    write_product_list_from_csvs,
)

# --- PLACEHOLDER DATA (keep all placeholders together) ---
UAH_TO_USD = 0.027

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
    "model": "CoffeeVend-X1",
    "installed_at": datetime(2024, 2, 10, 9, 0, 0),
    "last_serviced_at": datetime(2025, 2, 10, 9, 0, 0),
    "current_hours": 1180,
}
PLACEHOLDER_SF_MACHINE_3 = {
    "name": "SF-Cafe-3",
    "serial_number": "US-SF-0003",
    "model": "CoffeeVend-X2",
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


def load_csvs() -> None:
    product_names = write_product_list_from_csvs()
    create_db()
    files = sorted(glob("index_*.csv"))
    if not files:
        logger.warning("No index_*.csv files found")
        return

    with Session(engine) as session:
        ua_location = _get_or_create_location(session, PLACEHOLDER_UA_LOCATION)
        sf_location = _get_or_create_location(session, PLACEHOLDER_SF_LOCATION)
        ua_machine_1 = _get_or_create_machine(
            session, location_id=ua_location.id, data=PLACEHOLDER_UA_MACHINE_1
        )
        ua_machine_2 = _get_or_create_machine(
            session, location_id=ua_location.id, data=PLACEHOLDER_UA_MACHINE_2
        )
        sf_machine_3 = _get_or_create_machine(
            session, location_id=sf_location.id, data=PLACEHOLDER_SF_MACHINE_3
        )
        sf_machine_4 = _get_or_create_machine(
            session, location_id=sf_location.id, data=PLACEHOLDER_SF_MACHINE_4
        )
        machine_map = {
            "index_1.csv": (ua_machine_1, sf_machine_3),
            "index_2.csv": (ua_machine_2, sf_machine_4),
        }
        product_cache: Dict[str, int] = {}
        for f in files:
            path = Path(f)
            machine_pair = machine_map.get(path.name)
            if machine_pair is None:
                logger.warning("No machine mapping for {file}", file=path.name)
                continue
            ua_machine, sf_machine = machine_pair
            with path.open(newline="", encoding="utf-8") as fh:
                reader = csv.DictReader(fh)
                for row_idx, row in enumerate(reader, start=1):
                    name = canonicalize_product_name(row["coffee_name"])
                    product_id = product_cache.get(name)
                    if product_id is None:
                        existing = session.exec(
                            select(Product).where(Product.name == name)
                        ).first()
                        if existing is None:
                            existing = Product(name=name)
                            session.add(existing)
                            session.flush()
                        product_cache[name] = existing.id
                        product_id = existing.id

                    card = row.get("card")
                    base_amount = float(row["money"])
                    occurred_at = _parse_datetime(row["datetime"])
                    base_date = datetime.fromisoformat(row["date"]).date()

                    ua_txn = Transaction(
                        product_id=product_id,
                        location_id=ua_location.id,
                        machine_id=ua_machine.id,
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
                        location_id=sf_location.id,
                        machine_id=sf_machine.id,
                        date=base_date,
                        occurred_at=occurred_at,
                        cash_type=row["cash_type"],
                        card_token=card if card else None,
                        amount=round(base_amount * UAH_TO_USD, 2),
                        currency="USD",
                        source_file=path.name,
                        source_row=row_idx,
                    )
                    session.add(ua_txn)
                    session.add(sf_txn)
            session.commit()
            logger.info("Loaded {file}", file=path.name)

        _seed_ingredients(session)
        session.commit()

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

    init_price_schedule()
    update_expected_prices()
    rebuild_inventory_snapshots()
    rebuild_daily_projections()
    rebuild_daily_ingredient_projections()


if __name__ == "__main__":
    load_csvs()
