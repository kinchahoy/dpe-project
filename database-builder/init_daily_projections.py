from __future__ import annotations

from datetime import date as Date
from datetime import timedelta
from typing import cast

import pandas as pd
import polars as pl
from loguru import logger
from sqlalchemy import delete
from sqlmodel import Session, select
from statsmodels.tsa.holtwinters import ExponentialSmoothing, SimpleExpSmoothing

from db import (
    SimDailyProjection,
    SimRun,
    Transaction,
    create_observed_db,
    create_sim_db,
    observed_engine,
    sim_engine,
)

FORECAST_DAYS = 10
TRAINING_WINDOW_DAYS = 365
PROJECTION_DATES_BACK = 60
MIN_SIMPLE_HISTORY_DAYS = 7
MIN_SEASONAL_HISTORY_DAYS = 21
SEASONAL_PERIODS = 7
MIX_SHORT_WINDOW_DAYS = 56
MIX_BLEND_WEIGHT_SHORT = 0.7


def _seed_window(*, run_id: str) -> tuple[Date, Date]:
    with Session(sim_engine) as session:
        run = session.get(SimRun, run_id)
        if run is None:
            raise ValueError(f"Unknown run_id: {run_id}")
        return (run.seed_start_date, run.seed_end_date)


def _load_transactions(*, seed_start: Date, seed_end: Date) -> pl.DataFrame:
    with Session(observed_engine) as session:
        rows = session.exec(
            select(
                Transaction.date,
                Transaction.location_id,
                Transaction.machine_id,
                Transaction.product_id,
            ).where((Transaction.date >= seed_start) & (Transaction.date <= seed_end))
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


def _calendar(start_date: Date, end_date: Date) -> pl.DataFrame:
    return pl.DataFrame(
        {"date": pl.date_range(start_date, end_date, interval="1d", eager=True)}
    )


def _total_units_series(
    training_df: pl.DataFrame, calendar_df: pl.DataFrame
) -> list[float]:
    units_by_day = training_df.group_by("date").agg(pl.len().alias("units"))
    joined = (
        calendar_df.join(units_by_day, on="date", how="left")
        .with_columns(pl.col("units").fill_null(0).cast(pl.Float64))
        .sort("date")
    )
    return joined.get_column("units").to_list()


def _forecast_total(units: list[float], horizon: int) -> tuple[list[float], str]:
    if not units:
        return [0.0] * horizon, "naive_zero"

    series = pd.Series(units, dtype="float64")
    if len(series) < MIN_SIMPLE_HISTORY_DAYS:
        return [max(0.0, float(series.iloc[-1]))] * horizon, "naive_last"

    if series.nunique() <= 1:
        return [max(0.0, float(series.iloc[-1]))] * horizon, "naive_constant"

    if len(series) >= MIN_SEASONAL_HISTORY_DAYS:
        seasonal = "add"
        if float(series.min()) > 0.0:
            seasonal = "mul"
        try:
            fit = ExponentialSmoothing(
                series,
                trend="add",
                damped_trend=True,
                seasonal=seasonal,
                seasonal_periods=SEASONAL_PERIODS,
                initialization_method="estimated",
            ).fit(optimized=True, use_brute=False, remove_bias=True)
            forecast = fit.forecast(horizon).astype(float).tolist()
            return [max(0.0, value) for value in forecast], (
                f"holt_winters_{seasonal}_damped"
            )
        except Exception as exc:
            logger.debug("Holt-Winters seasonal failed: {error}", error=exc)

    try:
        fit = ExponentialSmoothing(
            series,
            trend="add",
            damped_trend=True,
            seasonal=None,
            initialization_method="estimated",
        ).fit(optimized=True, use_brute=False, remove_bias=True)
        forecast = fit.forecast(horizon).astype(float).tolist()
        return [max(0.0, value) for value in forecast], "holt_linear_damped"
    except Exception as exc:
        logger.debug("Holt linear failed: {error}", error=exc)

    try:
        fit = SimpleExpSmoothing(series, initialization_method="estimated").fit(
            optimized=True
        )
        forecast = fit.forecast(horizon).astype(float).tolist()
        return [max(0.0, value) for value in forecast], "simple_exp_smoothing"
    except Exception as exc:
        logger.debug("SimpleExpSmoothing failed: {error}", error=exc)

    rolling_mean = float(series.tail(min(10, len(series))).mean())
    return [max(0.0, rolling_mean)] * horizon, "rolling_mean_10d"


def _product_mix(training_df: pl.DataFrame, train_end: Date) -> list[dict]:
    long_totals = (
        training_df.group_by("product_id")
        .agg(pl.len().alias("units_long"))
        .sort("units_long", descending=True)
    )
    if long_totals.is_empty():
        return []

    short_start = train_end - timedelta(days=MIX_SHORT_WINDOW_DAYS - 1)
    short_df = training_df.filter(pl.col("date") >= short_start)
    short_totals = (
        short_df.group_by("product_id")
        .agg(pl.len().alias("units_short"))
        .sort("units_short", descending=True)
    )

    mix = (
        long_totals.join(short_totals, on="product_id", how="left")
        .with_columns(pl.col("units_short").fill_null(0))
        .with_columns(
            (pl.col("units_long") / pl.col("units_long").sum()).alias("share_long"),
            pl.when(pl.col("units_short").sum() > 0)
            .then(pl.col("units_short") / pl.col("units_short").sum())
            .otherwise(pl.col("units_long") / pl.col("units_long").sum())
            .alias("share_short"),
        )
        .with_columns(
            (
                MIX_BLEND_WEIGHT_SHORT * pl.col("share_short")
                + (1.0 - MIX_BLEND_WEIGHT_SHORT) * pl.col("share_long")
            ).alias("share_raw")
        )
        .with_columns((pl.col("share_raw") / pl.col("share_raw").sum()).alias("share"))
        .sort("units_long", descending=True)
    )
    return mix.to_dicts()


def rebuild_daily_projections(*, run_id: str) -> None:
    create_observed_db()
    create_sim_db()
    seed_start, seed_end = _seed_window(run_id=run_id)
    txns = _load_transactions(seed_start=seed_start, seed_end=seed_end)
    if txns.is_empty():
        logger.warning(
            "No seed-window transactions found for run_id={run_id}. Skipping daily projections.",
            run_id=run_id,
        )
        return

    rows: list[SimDailyProjection] = []
    pair_rows = (
        txns.select("location_id", "machine_id")
        .unique()
        .sort(["location_id", "machine_id"])
        .to_dicts()
    )

    for pair in pair_rows:
        location_id = pair["location_id"]
        machine_id = pair["machine_id"]
        pair_df = txns.filter(
            (pl.col("location_id") == location_id)
            & (pl.col("machine_id") == machine_id)
        )
        if pair_df.is_empty():
            continue

        train_start_full_raw = pair_df.get_column("date").min()
        if train_start_full_raw is None:
            continue
        train_start_full = cast(Date, train_start_full_raw)

        projection_dates_raw = (
            pair_df.select("date")
            .unique()
            .sort("date")
            .tail(PROJECTION_DATES_BACK)
            .get_column("date")
            .to_list()
        )
        projection_dates = [
            cast(Date, d) for d in projection_dates_raw if d is not None
        ]

        for train_end in projection_dates:
            requested_start = train_end - timedelta(days=TRAINING_WINDOW_DAYS - 1)
            train_start = max(train_start_full, requested_start)

            training_df = pair_df.filter(
                (pl.col("date") >= train_start) & (pl.col("date") <= train_end)
            )
            if training_df.is_empty():
                continue

            calendar_df = _calendar(train_start, train_end)
            total_series = _total_units_series(training_df, calendar_df)
            forecast, model_name = _forecast_total(total_series, FORECAST_DAYS)
            forecast_dates = [
                train_end + timedelta(days=day) for day in range(1, FORECAST_DAYS + 1)
            ]

            product_mix = _product_mix(training_df, train_end)
            for rank, product_row in enumerate(product_mix, start=1):
                product_id = product_row["product_id"]
                share = float(product_row["share"])
                for idx, forecast_date in enumerate(forecast_dates):
                    rows.append(
                        SimDailyProjection(
                            run_id=run_id,
                            projection_date=train_end,
                            forecast_date=forecast_date,
                            training_start=train_start,
                            training_end=train_end,
                            location_id=location_id,
                            machine_id=machine_id,
                            product_id=product_id,
                            product_rank=rank,
                            is_long_tail=False,
                            long_tail_proportion=0.0,
                            forecast_units=forecast[idx] * share,
                            model_name=f"{model_name}_top_down_mix",
                            used_price_data=False,
                        )
                    )

            for idx, forecast_date in enumerate(forecast_dates):
                rows.append(
                    SimDailyProjection(
                        run_id=run_id,
                        projection_date=train_end,
                        forecast_date=forecast_date,
                        training_start=train_start,
                        training_end=train_end,
                        location_id=location_id,
                        machine_id=machine_id,
                        product_id=None,
                        product_rank=None,
                        is_long_tail=False,
                        long_tail_proportion=0.0,
                        forecast_units=forecast[idx],
                        model_name=model_name,
                        used_price_data=False,
                    )
                )

    with Session(sim_engine) as session:
        session.exec(
            delete(SimDailyProjection).where(SimDailyProjection.run_id == run_id)
        )
        session.commit()
        session.add_all(rows)
        session.commit()

    logger.info("Rebuilt daily projections: {count} rows.", count=len(rows))


if __name__ == "__main__":
    raise SystemExit(
        "Run via init_db.py (needs a sim run id). Example: python init_db.py"
    )
