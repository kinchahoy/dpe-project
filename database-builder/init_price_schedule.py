from __future__ import annotations

from bisect import bisect_right
from dataclasses import dataclass
from datetime import date
from typing import Iterable, Optional

import polars as pl
from loguru import logger
from sqlalchemy import delete
from sqlmodel import Session, select

from db import PriceChange, Product, Transaction, TransactionExpanded, create_db, engine
from product_catalog import canonical_product

MIN_SEASON_DAYS = 5


@dataclass(frozen=True)
class Season:
    price: float
    start: date
    end: date
    days: int


def _build_seasons(daily: list[dict]) -> list[Season]:
    if not daily:
        return []

    eras: list[Season] = []
    cur = Season(
        price=daily[0]["modal_price"],
        start=daily[0]["date"],
        end=daily[0]["date"],
        days=1,
    )
    for row in daily[1:]:
        if row["modal_price"] == cur.price:
            cur = Season(
                price=cur.price,
                start=cur.start,
                end=row["date"],
                days=cur.days + 1,
            )
        else:
            eras.append(cur)
            cur = Season(
                price=row["modal_price"],
                start=row["date"],
                end=row["date"],
                days=1,
            )
    eras.append(cur)

    seasons: list[Season] = []
    for era in eras:
        if era.days >= MIN_SEASON_DAYS or not seasons:
            seasons.append(era)
            continue
        # Blip — extend previous season's end date through it.
        prev = seasons[-1]
        seasons[-1] = Season(
            price=prev.price,
            start=prev.start,
            end=era.end,
            days=prev.days + era.days,
        )
    return seasons


def _season_changes(seasons: Iterable[Season]) -> list[dict]:
    changes: list[dict] = []
    prev_price: Optional[float] = None
    for season in seasons:
        changes.append(
            {
                "change_date": season.start,
                "old_price": prev_price,
                "new_price": season.price,
            }
        )
        prev_price = season.price
    return changes


def _load_transactions() -> pl.DataFrame:
    with Session(engine) as session:
        rows = session.exec(
            select(
                Transaction.product_id,
                Transaction.location_id,
                Transaction.date,
                Transaction.amount,
                Transaction.currency,
            )
        ).all()

    if not rows:
        return pl.DataFrame(
            schema={
                "product_id": pl.Int64,
                "location_id": pl.Int64,
                "date": pl.Date,
                "amount": pl.Float64,
                "currency": pl.Utf8,
            }
        )

    data = [
        {
            "product_id": row[0],
            "location_id": row[1],
            "date": row[2],
            "amount": row[3],
            "currency": row[4],
        }
        for row in rows
    ]
    return pl.DataFrame(data).sort(["product_id", "location_id", "currency", "date"])


def init_price_schedule() -> None:
    create_db()
    df = _load_transactions()
    if df.is_empty():
        logger.warning("No transactions found. Skipping price schedule init.")
        return
    null_locations = df.get_column("location_id").null_count()
    if null_locations:
        logger.warning(
            "Found {count} transactions with null location_id. Skipping them for pricing.",
            count=null_locations,
        )
        df = df.filter(pl.col("location_id").is_not_null())

    with Session(engine) as session:
        session.exec(delete(PriceChange))
        session.commit()

        groups = df.group_by(["product_id", "location_id", "currency"])
        for (product_id, location_id, currency), group_df in groups:
            daily = (
                group_df.group_by("date")
                .agg(
                    pl.col("amount").mode().first().alias("modal_price"),
                    pl.len().alias("n"),
                )
                .sort("date")
                .to_dicts()
            )
            seasons = _build_seasons(daily)
            if not seasons:
                logger.warning(
                    "No seasons inferred for product={product} location={location}",
                    product=product_id,
                    location=location_id,
                )
                continue

            for change in _season_changes(seasons):
                session.add(
                    PriceChange(
                        product_id=product_id,
                        location_id=location_id,
                        currency=currency,
                        change_date=change["change_date"],
                        old_price=change["old_price"],
                        new_price=change["new_price"],
                        tod_delta=0.0,
                    )
                )
        session.commit()
    logger.info("Price schedule initialized.")


def _load_price_changes(
    session: Session,
) -> dict[tuple[int, int, str], list[PriceChange]]:
    changes = session.exec(
        select(PriceChange).order_by(
            PriceChange.product_id,
            PriceChange.location_id,
            PriceChange.currency,
            PriceChange.change_date,
        )
    ).all()
    schedule: dict[tuple[int, int, str], list[PriceChange]] = {}
    for change in changes:
        key = (change.product_id, change.location_id, change.currency)
        schedule.setdefault(key, []).append(change)
    return schedule


def _match_time_window(change: PriceChange, txn_time) -> bool:
    if change.tod_start_time is None or change.tod_end_time is None:
        return True
    start = change.tod_start_time
    end = change.tod_end_time
    if start <= end:
        return start <= txn_time < end
    # Overnight window (e.g., 22:00 -> 06:00)
    return txn_time >= start or txn_time < end


def _expected_price_for(
    txn: Transaction, changes: list[PriceChange]
) -> Optional[float]:
    dates = [change.change_date for change in changes]
    idx = bisect_right(dates, txn.date) - 1
    if idx < 0:
        return None
    change = changes[idx]
    price = change.new_price
    if _match_time_window(change, txn.occurred_at.time()):
        price += change.tod_delta
    return price


def update_expected_prices() -> None:
    create_db()
    with Session(engine) as session:
        schedule = _load_price_changes(session)
        if not schedule:
            logger.warning("No price schedule rows found. Skipping expected prices.")
            return

        session.exec(delete(TransactionExpanded))
        session.commit()

        product_names = session.exec(select(Product.id, Product.name)).all()
        product_name_by_id = {row[0]: row[1] for row in product_names}

        txns = session.exec(select(Transaction)).all()
        inserted = 0
        for txn in txns:
            if txn.location_id is None:
                logger.warning(
                    "Skipping transaction {txn_id} with null location_id.",
                    txn_id=txn.id,
                )
                continue
            key = (txn.product_id, txn.location_id, txn.currency)
            changes = schedule.get(key)
            if not changes:
                continue
            expected = _expected_price_for(txn, changes)
            if expected is None:
                continue
            session.add(
                TransactionExpanded(
                    transaction_id=txn.id,
                    product_id=txn.product_id,
                    location_id=txn.location_id,
                    machine_id=txn.machine_id,
                    date=txn.date,
                    occurred_at=txn.occurred_at,
                    cash_type=txn.cash_type,
                    card_token=txn.card_token,
                    amount=txn.amount,
                    expected_price=expected,
                    product_group=canonical_product(
                        product_name_by_id.get(txn.product_id, "")
                    ).group,
                    currency=txn.currency,
                    source_file=txn.source_file,
                    source_row=txn.source_row,
                )
            )
            inserted += 1
        session.commit()
    logger.info("Inserted expanded transactions for {count} rows.", count=inserted)


if __name__ == "__main__":
    init_price_schedule()
