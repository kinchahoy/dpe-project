from __future__ import annotations

from datetime import timedelta

import polars as pl
from loguru import logger
from sqlalchemy import delete
from sqlmodel import Session, select

from db import (
    DailyIngredientProjection,
    DailyProjection,
    Ingredient,
    ProductIngredient,
    Transaction,
    create_db,
    engine,
)

MIX_SHORT_WINDOW_DAYS = 56
MIX_BLEND_WEIGHT_SHORT = 0.7


def _load_total_projection_rows() -> pl.DataFrame:
    with Session(engine) as session:
        rows = session.exec(
            select(DailyProjection).where(DailyProjection.product_id.is_(None))
        ).all()

    if not rows:
        return pl.DataFrame(
            schema={
                "projection_date": pl.Date,
                "forecast_date": pl.Date,
                "training_start": pl.Date,
                "training_end": pl.Date,
                "location_id": pl.Int64,
                "machine_id": pl.Int64,
                "forecast_units": pl.Float64,
                "model_name": pl.Utf8,
            }
        )

    return pl.DataFrame(
        [
            {
                "projection_date": row.projection_date,
                "forecast_date": row.forecast_date,
                "training_start": row.training_start,
                "training_end": row.training_end,
                "location_id": row.location_id,
                "machine_id": row.machine_id,
                "forecast_units": row.forecast_units,
                "model_name": row.model_name,
            }
            for row in rows
        ]
    ).sort(["location_id", "machine_id", "projection_date", "forecast_date"])


def _load_transactions() -> pl.DataFrame:
    with Session(engine) as session:
        rows = session.exec(
            select(
                Transaction.date,
                Transaction.location_id,
                Transaction.machine_id,
                Transaction.product_id,
            )
        ).all()
    if not rows:
        return pl.DataFrame(
            schema={
                "date": pl.Date,
                "location_id": pl.Int64,
                "machine_id": pl.Int64,
                "product_id": pl.Int64,
            }
        )
    return pl.DataFrame(
        [
            {
                "date": row[0],
                "location_id": row[1],
                "machine_id": row[2],
                "product_id": row[3],
            }
            for row in rows
        ]
    )


def _load_product_ingredients() -> pl.DataFrame:
    with Session(engine) as session:
        rows = session.exec(
            select(
                ProductIngredient.product_id,
                ProductIngredient.ingredient_id,
                ProductIngredient.quantity,
            )
        ).all()
    if not rows:
        return pl.DataFrame(
            schema={
                "product_id": pl.Int64,
                "ingredient_id": pl.Int64,
                "quantity": pl.Float64,
            }
        )
    return pl.DataFrame(
        [
            {"product_id": row[0], "ingredient_id": row[1], "quantity": row[2]}
            for row in rows
        ]
    )


def _load_ingredient_units() -> dict[int, str]:
    with Session(engine) as session:
        rows = session.exec(select(Ingredient.id, Ingredient.unit)).all()
    return {row[0]: row[1] for row in rows}


def _ingredient_rates(
    training_txn_df: pl.DataFrame, recipe_df: pl.DataFrame, training_end
) -> list[dict]:
    total_sales_long = float(training_txn_df.height)
    if total_sales_long <= 0:
        return []

    long_qty = (
        training_txn_df.join(recipe_df, on="product_id", how="inner")
        .group_by("ingredient_id")
        .agg(pl.col("quantity").sum().alias("qty_long"))
    )
    if long_qty.is_empty():
        return []

    short_start = training_end - timedelta(days=MIX_SHORT_WINDOW_DAYS - 1)
    short_txn_df = training_txn_df.filter(pl.col("date") >= short_start)
    total_sales_short = float(short_txn_df.height)
    short_qty = (
        short_txn_df.join(recipe_df, on="product_id", how="inner")
        .group_by("ingredient_id")
        .agg(pl.col("quantity").sum().alias("qty_short"))
    )

    rates = (
        long_qty.join(short_qty, on="ingredient_id", how="left")
        .with_columns(pl.col("qty_short").fill_null(0.0))
        .with_columns((pl.col("qty_long") / total_sales_long).alias("rate_long"))
        .with_columns(
            pl.when(total_sales_short > 0)
            .then(pl.col("qty_short") / total_sales_short)
            .otherwise(pl.col("rate_long"))
            .alias("rate_short")
        )
        .with_columns(
            (
                MIX_BLEND_WEIGHT_SHORT * pl.col("rate_short")
                + (1.0 - MIX_BLEND_WEIGHT_SHORT) * pl.col("rate_long")
            ).alias("rate")
        )
        .sort("ingredient_id")
    )
    return rates.select("ingredient_id", "rate").to_dicts()


def rebuild_daily_ingredient_projections() -> None:
    create_db()
    total_proj_df = _load_total_projection_rows()
    if total_proj_df.is_empty():
        logger.warning(
            "No total daily projections found. Skipping ingredient forecasts."
        )
        return

    txns_df = _load_transactions()
    recipe_df = _load_product_ingredients()
    ingredient_units = _load_ingredient_units()
    if txns_df.is_empty() or recipe_df.is_empty() or not ingredient_units:
        logger.warning(
            "Missing txns/recipes/ingredient units. Skipping ingredient forecasts."
        )
        return

    rows: list[DailyIngredientProjection] = []
    scenario_rows = (
        total_proj_df.select(
            "projection_date",
            "training_start",
            "training_end",
            "location_id",
            "machine_id",
            "model_name",
        )
        .unique()
        .sort(["location_id", "machine_id", "projection_date"])
        .to_dicts()
    )

    for scenario in scenario_rows:
        training_txn_df = txns_df.filter(
            (pl.col("location_id") == scenario["location_id"])
            & (pl.col("machine_id") == scenario["machine_id"])
            & (pl.col("date") >= scenario["training_start"])
            & (pl.col("date") <= scenario["training_end"])
        )
        if training_txn_df.is_empty():
            continue

        rates = _ingredient_rates(
            training_txn_df, recipe_df, training_end=scenario["training_end"]
        )
        if not rates:
            continue

        forecast_rows = total_proj_df.filter(
            (pl.col("location_id") == scenario["location_id"])
            & (pl.col("machine_id") == scenario["machine_id"])
            & (pl.col("projection_date") == scenario["projection_date"])
        ).to_dicts()

        for forecast_row in forecast_rows:
            forecast_units = float(forecast_row["forecast_units"])
            for item in rates:
                ingredient_id = int(item["ingredient_id"])
                rate = float(item["rate"])
                rows.append(
                    DailyIngredientProjection(
                        projection_date=forecast_row["projection_date"],
                        forecast_date=forecast_row["forecast_date"],
                        training_start=forecast_row["training_start"],
                        training_end=forecast_row["training_end"],
                        location_id=forecast_row["location_id"],
                        machine_id=forecast_row["machine_id"],
                        ingredient_id=ingredient_id,
                        forecast_quantity=max(0.0, forecast_units * rate),
                        unit=ingredient_units.get(ingredient_id, ""),
                        model_name=f"{scenario['model_name']}_ingredient_rate_mix",
                    )
                )

    with Session(engine) as session:
        session.exec(delete(DailyIngredientProjection))
        session.commit()
        session.add_all(rows)
        session.commit()

    logger.info("Rebuilt daily ingredient projections: {count} rows.", count=len(rows))


if __name__ == "__main__":
    rebuild_daily_ingredient_projections()
