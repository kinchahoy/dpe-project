import marimo

__generated_with = "0.19.9"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    import altair as alt
    import sqlite3

    return alt, mo, pl, sqlite3


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    # Prediction Accuracy: Daily Projections vs Actual Sales

    How well does the SARIMAX forecast track against real outcomes?
    This notebook compares `daily_projections` forecasts with `dailyproductsales` actuals,
    and visualises the forecast alongside recent history for a sanity check.
    """)
    return


@app.cell
def _(pl, sqlite3):
    _conn = sqlite3.connect("coffee.db")

    projections_df = pl.read_database(
        """
        SELECT
            dp.forecast_date, dp.projection_date,
            dp.forecast_units, dp.model_name, dp.used_price_data,
            dp.product_id, dp.location_id, dp.machine_id,
            dp.is_long_tail, dp.product_rank,
            p.name  AS product,
            l.name  AS location,
            m.name  AS machine
        FROM daily_projections dp
        JOIN product  p ON p.id = dp.product_id
        JOIN location l ON l.id = dp.location_id
        JOIN machine  m ON m.id = dp.machine_id
        ORDER BY dp.forecast_date
        """,
        _conn,
    ).with_columns(
        pl.col("forecast_date").cast(pl.Utf8).str.to_date("%Y-%m-%d"),
        pl.col("projection_date").cast(pl.Utf8).str.to_date("%Y-%m-%d"),
    )

    # Actuals: aggregate across cash_type so we get total units per day/product/location
    actuals_df = pl.read_database(
        """
        SELECT
            dps.date, dps.product_id, dps.location_id,
            SUM(dps.units_sold) AS actual_units,
            p.name  AS product,
            l.name  AS location
        FROM dailyproductsales dps
        JOIN product  p ON p.id = dps.product_id
        JOIN location l ON l.id = dps.location_id
        GROUP BY dps.date, dps.product_id, dps.location_id
        ORDER BY dps.date
        """,
        _conn,
    ).with_columns(
        pl.col("date").cast(pl.Utf8).str.to_date("%Y-%m-%d"),
    )

    _conn.close()
    return actuals_df, projections_df


@app.cell
def _(mo, projections_df):
    _locations = sorted(projections_df["location"].unique().to_list())
    location_dd = mo.ui.dropdown(
        options=_locations, value=_locations[0], label="Location"
    )

    _products = sorted(projections_df["product"].unique().to_list())
    product_dd = mo.ui.dropdown(options=_products, value=_products[0], label="Product")

    mo.hstack([location_dd, product_dd])
    return location_dd, product_dd


@app.cell
def _(actuals_df, location_dd, pl, product_dd, projections_df):
    # Filter projections to selected product/location
    proj_filtered = projections_df.filter(
        (pl.col("product") == product_dd.value)
        & (pl.col("location") == location_dd.value)
    )

    # Filter actuals to same product/location
    act_filtered = actuals_df.filter(
        (pl.col("product") == product_dd.value)
        & (pl.col("location") == location_dd.value)
    )

    # Join on date where overlap exists
    compared_df = proj_filtered.join(
        act_filtered.select(
            pl.col("date").alias("forecast_date"),
            "actual_units",
            pl.col("product_id").alias("_pid"),
            pl.col("location_id").alias("_lid"),
        ),
        on="forecast_date",
        how="left",
    ).with_columns(
        (pl.col("forecast_units") - pl.col("actual_units")).alias("error"),
        ((pl.col("forecast_units") - pl.col("actual_units")).abs()).alias("abs_error"),
    )

    has_overlap = compared_df.filter(pl.col("actual_units").is_not_null()).height > 0
    return act_filtered, compared_df, has_overlap, proj_filtered


@app.cell(hide_code=True)
def _(compared_df, has_overlap, mo, pl):
    if not has_overlap:
        mo.md(
            """
            > **No overlapping dates yet.** Forecasts start after the last day of
            > actual sales data. Once new sales are loaded, accuracy metrics will
            > appear here automatically.
            """
        )
    else:
        _overlap = compared_df.filter(pl.col("actual_units").is_not_null())
        _n = _overlap.height
        _mae = _overlap["abs_error"].mean()
        _rmse = (_overlap["error"].pow(2).mean()) ** 0.5
        # MAPE: avoid div-by-zero
        _mape_df = _overlap.filter(pl.col("actual_units") > 0).with_columns(
            (pl.col("abs_error") / pl.col("actual_units") * 100).alias("ape")
        )
        _mape = _mape_df["ape"].mean() if _mape_df.height > 0 else None
        _bias = _overlap["error"].mean()

        _mape_str = f"{_mape:.1f}%" if _mape is not None else "N/A"
        mo.md(f"""
        ## Accuracy Metrics ({_n} overlapping days)

        | Metric | Value | What it means |
        |--------|-------|---------------|
        | MAE | **{_mae:.2f}** units | Average size of miss — lower is better |
        | RMSE | **{_rmse:.2f}** units | Like MAE but penalises large misses more heavily |
        | MAPE | **{_mape_str}** | Average miss as a % of actual sales — scale-independent |
        | Bias | **{_bias:+.2f}** units | Positive = consistently over-forecasting, negative = under |
        """)
    return


@app.cell(hide_code=True)
def _(alt, compared_df, has_overlap, mo, pl):
    if has_overlap:
        _overlap = compared_df.filter(pl.col("actual_units").is_not_null())
        _max_val = (
            max(
                _overlap["forecast_units"].max(),
                _overlap["actual_units"].max(),
            )
            * 1.1
        )

        _perfect = (
            alt.Chart(pl.DataFrame({"x": [0.0, _max_val], "y": [0.0, _max_val]}))
            .mark_line(strokeDash=[4, 4], color="grey")
            .encode(x="x:Q", y="y:Q")
        )

        _scatter = (
            alt.Chart(_overlap)
            .mark_circle(size=50)
            .encode(
                x=alt.X("actual_units:Q", title="Actual units sold"),
                y=alt.Y("forecast_units:Q", title="Forecast units"),
                color="machine:N",
                tooltip=[
                    "forecast_date:T",
                    "machine:N",
                    alt.Tooltip("forecast_units:Q", format=",.1f"),
                    alt.Tooltip("actual_units:Q", format=",.0f"),
                    alt.Tooltip("error:Q", format="+,.1f"),
                ],
            )
        )

        _chart = (_perfect + _scatter).properties(
            title="Forecast vs Actual (perfect = diagonal line)",
            width=450,
            height=400,
        )
        mo.ui.altair_chart(_chart)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ---
    ## Prediction Accuracy by Product (1-day vs 7-day horizon)

    How does accuracy degrade as we forecast further out?
    Each projection run produces forecasts 1–10 days ahead. This table compares
    **1-day-ahead** and **7-day-ahead** accuracy per product for the selected location.

    | Column | Meaning |
    |--------|---------|
    | **MAE** | Average miss in units — lower is better |
    | **RMSE** | Like MAE but penalises large misses more heavily |
    | **bias** | Positive = over-forecasting, negative = under |
    | **MAPE_%** | Average miss as a % of actual — scale-independent |
    | **n_obs** | Number of forecast/actual pairs used |
    """)
    return


@app.cell
def _(actuals_df, location_dd, mo, pl, projections_df):
    # Compute horizon = forecast_date - projection_date (in days)
    # Sum forecast across machines per (projection_date, forecast_date, product, location)
    _proj_with_horizon = (
        projections_df.filter(pl.col("location") == location_dd.value)
        .with_columns(
            (pl.col("forecast_date") - pl.col("projection_date"))
            .dt.total_days()
            .cast(pl.Int32)
            .alias("horizon"),
        )
        .group_by("projection_date", "forecast_date", "horizon", "product", "location")
        .agg(pl.col("forecast_units").sum().alias("forecast_total"))
    )

    # Join with actuals on date + product + location names (avoids ID type mismatches)
    _act_for_join = actuals_df.filter(pl.col("location") == location_dd.value).select(
        pl.col("date"),
        "product",
        "location",
        "actual_units",
    )

    _with_actuals = _proj_with_horizon.join(
        _act_for_join,
        left_on=["forecast_date", "product", "location"],
        right_on=["date", "product", "location"],
        how="inner",
    ).with_columns(
        (pl.col("forecast_total") - pl.col("actual_units")).alias("error"),
        (pl.col("forecast_total") - pl.col("actual_units")).abs().alias("abs_error"),
    )

    def _metrics_for_horizon(df, h):
        _h = df.filter(pl.col("horizon") == h)
        if _h.height == 0:
            return pl.DataFrame()
        return _h.group_by("product").agg(
            pl.lit(h).alias("horizon_days"),
            pl.col("abs_error").mean().round(2).alias("MAE"),
            (pl.col("error").pow(2).mean().sqrt()).round(2).alias("RMSE"),
            pl.col("error").mean().round(2).alias("bias"),
            (
                pl.when(pl.col("actual_units") > 0)
                .then(pl.col("abs_error") / pl.col("actual_units") * 100)
                .otherwise(None)
            )
            .mean()
            .round(1)
            .alias("MAPE_%"),
            pl.col("error").len().alias("n_obs"),
        )

    _h1 = _metrics_for_horizon(_with_actuals, 1)
    _h7 = _metrics_for_horizon(_with_actuals, 7)

    _combined = pl.concat([df for df in [_h1, _h7] if df.height > 0])

    if _combined.height > 0:
        accuracy_by_horizon = _combined.sort("product", "horizon_days")
        mo.ui.table(accuracy_by_horizon)
    else:
        mo.md("> No overlapping actuals for 1-day or 7-day horizons yet.")
        accuracy_by_horizon = pl.DataFrame()
    return (accuracy_by_horizon,)


@app.cell(hide_code=True)
def _(accuracy_by_horizon, alt, mo, pl):
    if accuracy_by_horizon.height > 0:
        _melted = accuracy_by_horizon.unpivot(
            index=["product", "horizon_days"],
            on=["MAE", "RMSE"],
        ).with_columns(pl.col("horizon_days").cast(pl.Utf8).alias("horizon"))

        _chart = (
            alt.Chart(_melted)
            .mark_bar()
            .encode(
                x=alt.X("product:N", title="Product"),
                y=alt.Y("value:Q", title="Units"),
                color="horizon:N",
                xOffset="horizon:N",
                column=alt.Column("variable:N", title="Metric"),
                tooltip=[
                    "product:N",
                    "horizon:N",
                    alt.Tooltip("value:Q", format=",.2f"),
                ],
            )
            .properties(width=300, height=250)
        )
        mo.ui.altair_chart(_chart)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ---
    ## Each 10-Day Prediction vs Actuals

    Every thin line is one projection run's 10-day forecast.
    The **bold blue line** is actual daily sales, and the **dashed red line** is the
    7-day rolling average of actuals.
    """)
    return


@app.cell
def _(act_filtered, alt, mo, pl, proj_filtered):
    # Sum across machines per (projection_date, forecast_date) → one line per run
    _proj_lines = (
        proj_filtered.group_by("projection_date", "forecast_date")
        .agg(pl.col("forecast_units").sum().alias("forecast_total"))
        .sort("projection_date", "forecast_date")
        .with_columns(
            pl.col("projection_date").cast(pl.Utf8).alias("run"),
        )
    )

    # Actuals with 7-day rolling average
    _actuals = act_filtered.sort("date").with_columns(
        pl.col("actual_units").rolling_mean(window_size=7).alias("rolling_7d"),
    )

    # Window: from 60 days before earliest forecast through end
    _forecast_min = _proj_lines["forecast_date"].min()
    _act_cutoff = _forecast_min - pl.duration(days=60)
    _actuals_window = _actuals.filter(pl.col("date") >= _act_cutoff)

    # Each projection run as a thin line
    _spaghetti = (
        alt.Chart(_proj_lines)
        .mark_line(strokeWidth=0.8, opacity=0.35)
        .encode(
            x=alt.X("forecast_date:T", title="Date"),
            y=alt.Y("forecast_total:Q", title="Units"),
            detail="run:N",
            color=alt.value("#e45756"),
            tooltip=[
                alt.Tooltip("run:N", title="projection date"),
                "forecast_date:T",
                alt.Tooltip("forecast_total:Q", format=",.1f", title="forecast"),
            ],
        )
    )

    # Actual daily sales — bold
    _act_line = (
        alt.Chart(_actuals_window)
        .mark_line(color="steelblue", strokeWidth=2.5)
        .encode(
            x=alt.X("date:T"),
            y=alt.Y("actual_units:Q"),
            tooltip=["date:T", alt.Tooltip("actual_units:Q", format=",.0f")],
        )
    )

    # 7-day rolling average
    _roll_line = (
        alt.Chart(_actuals_window.filter(pl.col("rolling_7d").is_not_null()))
        .mark_line(color="pink", strokeWidth=2)
        .encode(
            x=alt.X("date:T"),
            y=alt.Y("rolling_7d:Q"),
            tooltip=[
                "date:T",
                alt.Tooltip("rolling_7d:Q", format=",.1f", title="7d avg"),
            ],
        )
    )

    _chart = (_act_line + _roll_line + _spaghetti).properties(
        title="Each 10-day projection run (thin red) vs actuals (blue) & 7d avg (dashed red)",
        width=750,
        height=350,
    )
    mo.ui.altair_chart(_chart)
    return


@app.cell(hide_code=True)
def _(alt, compared_df, has_overlap, mo, pl):
    if has_overlap:
        _overlap = compared_df.filter(pl.col("actual_units").is_not_null())
        _chart = (
            alt.Chart(_overlap)
            .mark_bar()
            .encode(
                x=alt.X("forecast_date:T", title="Date"),
                y=alt.Y("error:Q", title="Error (forecast − actual)"),
                color=alt.condition(
                    alt.datum.error > 0,
                    alt.value("coral"),
                    alt.value("steelblue"),
                ),
                tooltip=[
                    "forecast_date:T",
                    "machine:N",
                    alt.Tooltip("forecast_units:Q", format=",.1f"),
                    alt.Tooltip("actual_units:Q", format=",.0f"),
                    alt.Tooltip("error:Q", format="+,.1f"),
                ],
            )
            .properties(
                title="Daily forecast error (positive = over-forecast)",
                width=750,
                height=250,
            )
        )
        mo.ui.altair_chart(_chart)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ---
    ## Forecast Summary by Product & Location

    Compares the forecasted daily average against the recent historical daily average
    (last 30 days of actuals) as a reasonableness check.
    """)
    return


@app.cell
def _(actuals_df, mo, pl, projections_df):
    # Recent 30-day average from actuals
    _recent_cutoff = actuals_df["date"].max() - pl.duration(days=30)
    _recent_avg = (
        actuals_df.filter(pl.col("date") >= _recent_cutoff)
        .group_by("product", "location")
        .agg(
            pl.col("actual_units").mean().alias("recent_30d_avg"),
            pl.col("date").n_unique().alias("active_days"),
        )
    )

    # Forecast daily average (sum across machines for same product/location/date, then average)
    _forecast_avg = (
        projections_df.group_by("forecast_date", "product", "location")
        .agg(pl.col("forecast_units").sum())
        .group_by("product", "location")
        .agg(
            pl.col("forecast_units").mean().alias("forecast_avg"),
            pl.col("forecast_date").n_unique().alias("forecast_days"),
        )
    )

    _summary = (
        _forecast_avg.join(_recent_avg, on=["product", "location"], how="left")
        .with_columns(
            (
                (pl.col("forecast_avg") - pl.col("recent_30d_avg"))
                / pl.col("recent_30d_avg")
                * 100
            ).alias("pct_diff"),
        )
        .sort("product", "location")
    )
    mo.ui.table(_summary)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ---
    ## Per-Machine Forecast Breakdown

    Daily forecast by machine for the selected product and location.
    """)
    return


@app.cell
def _(alt, mo, proj_filtered):
    _chart = (
        alt.Chart(proj_filtered)
        .mark_line(point=alt.OverlayMarkDef(size=15))
        .encode(
            x=alt.X("forecast_date:T", title="Date"),
            y=alt.Y("forecast_units:Q", title="Forecast units"),
            color="machine:N",
            tooltip=[
                "forecast_date:T",
                "machine:N",
                alt.Tooltip("forecast_units:Q", format=",.1f"),
                alt.Tooltip("product_rank:Q"),
                "model_name:N",
            ],
        )
        .properties(
            title="Forecast by machine",
            width=750,
            height=300,
        )
    )
    mo.ui.altair_chart(_chart)
    return


if __name__ == "__main__":
    app.run()
