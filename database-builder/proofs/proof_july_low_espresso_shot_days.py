from __future__ import annotations

import sys
from datetime import date as Date
from pathlib import Path

from loguru import logger
from sqlmodel import Session, col, select

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from db import (
    Ingredient,
    Product,
    ProductIngredient,
    Transaction,
    create_facts_db,
    create_observed_db,
    facts_engine,
    observed_engine,
)


def _configure_logging() -> Path:
    log_dir = Path("proofs/logs")
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / "proof_july_low_espresso_shot_days.log"

    logger.remove()
    logger.add(sys.stdout, level="INFO")
    logger.add(log_path, level="INFO", mode="w")
    return log_path


def run_proof() -> int:
    log_path = _configure_logging()
    logger.info("PROOF START: Days in July with <= 2 espresso shots consumed")

    logger.info("Step 1: Resolve ingredient id for espresso_shot")
    create_facts_db()
    create_observed_db()
    with Session(facts_engine) as session:
        products = session.exec(select(Product)).all()
        product_name_by_id = {row.id: row.name for row in products}

        ingredient = session.exec(
            select(Ingredient).where(Ingredient.name == "espresso_shot")
        ).first()
        if ingredient is None:
            logger.error("FAIL: ingredient 'espresso_shot' was not found")
            logger.info("Proof log file: {}", log_path)
            return 1

        logger.info(
            "Ingredient resolved: id={} name={} unit={}",
            ingredient.id,
            ingredient.name,
            ingredient.unit,
        )

        logger.info("Step 2: Load espresso-shot quantity per product recipe")
        recipe_rows = session.exec(
            select(ProductIngredient).where(
                ProductIngredient.ingredient_id == ingredient.id
            )
        ).all()
        shot_qty_by_product = {
            row.product_id: row.quantity for row in recipe_rows if row.quantity > 0
        }
        if not shot_qty_by_product:
            logger.error("FAIL: No product recipe uses ingredient 'espresso_shot'")
            logger.info("Proof log file: {}", log_path)
            return 1
        logger.info(
            "Loaded {} product recipes that consume espresso shots",
            len(shot_qty_by_product),
        )

        logger.info(
            "Step 3: Aggregate July espresso-shot consumption from raw transactions"
        )

    with Session(observed_engine) as session:
        txns = session.exec(
            select(Transaction.date, Transaction.machine_id, Transaction.product_id)
            .where(col(Transaction.date).is_not(None))
            .order_by(Transaction.date, Transaction.machine_id)
        ).all()
    machine_day_shots: dict[tuple, float] = {}
    for date_value, machine_id, product_id in txns:
        if date_value.month != 7:
            continue
        shot_qty = shot_qty_by_product.get(product_id, 0.0)
        if shot_qty <= 0:
            continue
        key = (date_value, machine_id)
        machine_day_shots[key] = machine_day_shots.get(key, 0.0) + shot_qty

    qualifying = sorted(
        [
            (date_value, machine_id, total_shots)
            for (date_value, machine_id), total_shots in machine_day_shots.items()
            if total_shots <= 2
        ],
        key=lambda row: (row[0], row[1]),
    )
    distinct_days = sorted({row[0] for row in qualifying})

    logger.info("Step 4: Output qualifying rows")
    if not qualifying:
        logger.error(
            "FAIL: No July machine-days found with <= 2 espresso shots from raw transactions"
        )
        logger.info("Proof log file: {}", log_path)
        return 1

    logger.info(
        "Found {} qualifying machine-day rows across {} distinct July days",
        len(qualifying),
        len(distinct_days),
    )
    for date_value, machine_id, total_shots in qualifying:
        logger.info(
            "evidence: date={} machine_id={} espresso_shots={}",
            date_value.isoformat(),
            machine_id,
            total_shots,
        )

    target_key = (Date(2024, 7, 12), 1)
    if target_key not in {(row[0], row[1]) for row in qualifying}:
        target_key = (qualifying[0][0], qualifying[0][1])

    logger.info(
        "Step 5: Transaction-level audit for date={} machine_id={}",
        target_key[0].isoformat(),
        target_key[1],
    )
    with Session(observed_engine) as session:
        day_txns = session.exec(
            select(Transaction)
            .where(Transaction.date == target_key[0])
            .where(Transaction.machine_id == target_key[1])
            .order_by(Transaction.occurred_at)
        ).all()

    if not day_txns:
        logger.error(
            "FAIL: No transactions found for audit date={} machine_id={}",
            target_key[0].isoformat(),
            target_key[1],
        )
        logger.info("Proof log file: {}", log_path)
        return 1

    espresso_txn_count = 0
    espresso_total = 0.0
    logger.info("Audit transactions count={}", len(day_txns))
    for txn in day_txns:
        shot_qty = shot_qty_by_product.get(txn.product_id, 0.0)
        if shot_qty > 0:
            espresso_txn_count += 1
            espresso_total += shot_qty
        logger.info(
            "txn: id={} occurred_at={} product={} amount={} {} espresso_shots_used={}",
            txn.id,
            txn.occurred_at.isoformat(),
            product_name_by_id.get(txn.product_id, f"product#{txn.product_id}"),
            txn.amount,
            txn.currency,
            shot_qty,
        )

    logger.info(
        "Audit summary: espresso_transactions={} total_espresso_shots={}",
        espresso_txn_count,
        espresso_total,
    )

    logger.info("PASS: There are July days with <= 2 espresso shots consumed")
    logger.info("Proof log file: {}", log_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(run_proof())
