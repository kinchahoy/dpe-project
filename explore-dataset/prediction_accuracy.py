import marimo

__generated_with = "0.19.9"
app = marimo.App(width="medium")


@app.cell
def _():
    import altair as alt
    import marimo as mo
    import polars as pl

    from notebook_db import query_df, resolve_vending_db_paths

    return alt, mo, pl, query_df, resolve_vending_db_paths


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    # Prediction Accuracy

    "
        "Compares `sim_daily_projection` (forecast) vs `daily_product_sales` (actual).
    "
        "Focus: overlap health, horizon metrics, time-series fit, scatter fit.
    """)
    return


@app.cell
def _(pl, query_df, resolve_vending_db_paths):
    db_paths = resolve_vending_db_paths()

    run_df = query_df(
        db_paths.analysis_db,
        """
        SELECT id, seed_start_date, seed_end_date, created_at
        FROM sim_run
        ORDER BY created_at DESC
        """,
    )

    projection_df = (
        query_df(
            db_paths.analysis_db,
            """
            SELECT
                p.run_id,
                p.projection_date,
                p.forecast_date,
                p.location_id,
                p.machine_id,
                p.product_id,
                p.forecast_units,
                p.model_name,
                p.used_price_data,
                prod.name AS product_name,
                loc.name AS location_name,
                mach.name AS machine_name
            FROM sim_daily_projection p
            JOIN facts.product prod ON prod.id = p.product_id
            JOIN facts.location loc ON loc.id = p.location_id
            JOIN facts.machine mach ON mach.id = p.machine_id
            WHERE p.product_id IS NOT NULL
            """,
            attachments={"facts": db_paths.facts_db},
        )
        .with_columns(
            pl.col("projection_date").cast(pl.Utf8).str.to_date("%Y-%m-%d"),
            pl.col("forecast_date").cast(pl.Utf8).str.to_date("%Y-%m-%d"),
        )
        .with_columns(
            (pl.col("forecast_date") - pl.col("projection_date"))
            .dt.total_days()
            .cast(pl.Int32)
            .alias("horizon_days")
        )
    )

    actual_df = (
        query_df(
            db_paths.observed_db,
            """
            SELECT
                s.date,
                s.location_id,
                s.machine_id,
                s.product_id,
                SUM(s.units_sold) AS actual_units
            FROM daily_product_sales s
            GROUP BY s.date, s.location_id, s.machine_id, s.product_id
            """,
        )
        .with_columns(pl.col("date").cast(pl.Utf8).str.to_date("%Y-%m-%d"))
        .rename({"date": "forecast_date"})
    )
    return actual_df, projection_df, run_df


@app.cell
def _(mo, run_df):
    run_options = run_df["id"].to_list()
    run_select = mo.ui.dropdown(
        options=run_options, value=run_options[0], label="Run ID"
    )
    run_select
    return (run_select,)


@app.cell
def _(mo, pl, projection_df, run_select):
    run_projection_df = projection_df.filter(pl.col("run_id") == run_select.value)

    location_options = run_projection_df["location_name"].unique().sort().to_list()
    location_select = mo.ui.dropdown(
        options=location_options,
        value=location_options[0],
        label="Location",
    )
    location_select
    return location_select, run_projection_df


@app.cell
def _(location_select, mo, pl, run_projection_df):
    location_projection_df = run_projection_df.filter(
        pl.col("location_name") == location_select.value
    )

    machine_options = location_projection_df["machine_name"].unique().sort().to_list()
    machine_select = mo.ui.dropdown(
        options=machine_options,
        value=machine_options[0],
        label="Machine",
    )
    machine_select
    return location_projection_df, machine_select


@app.cell
def _(location_projection_df, machine_select, mo, pl):
    machine_projection_df = location_projection_df.filter(
        pl.col("machine_name") == machine_select.value
    )

    product_options = machine_projection_df["product_name"].unique().sort().to_list()
    product_select = mo.ui.dropdown(
        options=product_options,
        value=product_options[0],
        label="Product",
    )
    product_select
    return machine_projection_df, product_select


@app.cell
def _(actual_df, machine_projection_df, pl, product_select):
    compared_df = (
        machine_projection_df
        .filter(
            (pl.col("product_name") == product_select.value)
            & (pl.col("horizon_days") >= 1)
        )
        .join(
            actual_df,
            on=["forecast_date", "location_id", "machine_id", "product_id"],
            how="left",
        )
        .with_columns(
            (pl.col("forecast_units") - pl.col("actual_units")).alias("error"),
            (pl.col("forecast_units") - pl.col("actual_units"))
            .abs()
            .alias("abs_error"),
        )
    )

    overlap_df = compared_df.filter(pl.col("actual_units").is_not_null())
    has_overlap = overlap_df.height > 0
    return compared_df, has_overlap, overlap_df


@app.cell(hide_code=True)
def _(compared_df, has_overlap, mo, overlap_df, pl):
    mo.stop(
        compared_df.height == 0,
        mo.callout(
            mo.md("No forecast rows for this run/location/machine/product selection."),
            kind="warn",
        ),
    )

    min_horizon = int(compared_df["horizon_days"].min())
    max_horizon = int(compared_df["horizon_days"].max())
    forecast_start = compared_df["forecast_date"].min()
    forecast_end = compared_df["forecast_date"].max()

    mo.md(
        f"## Alignment Check\n\n"
        f"- Join keys: `forecast_date + location_id + machine_id + product_id`\n"
        f"- Forecast window in selection: **{forecast_start}** to **{forecast_end}**\n"
        f"- Horizon range in selection: **{min_horizon}** to **{max_horizon}** days"
    )

    mo.stop(
        not has_overlap,
        mo.callout(
            mo.md("No forecast/actual overlap for this selection yet."), kind="warn"
        ),
    )

    mae = float(overlap_df["abs_error"].mean())
    rmse = float((overlap_df["error"].pow(2).mean()) ** 0.5)
    bias = float(overlap_df["error"].mean())
    actual_mean = float(overlap_df["actual_units"].mean())
    actual_std_raw = overlap_df["actual_units"].std()
    actual_std = float(actual_std_raw) if actual_std_raw is not None else 0.0

    mape_df = overlap_df.filter(pl.col("actual_units") > 0).with_columns(
        (pl.col("abs_error") / pl.col("actual_units") * 100).alias("ape")
    )
    mape = float(mape_df["ape"].mean()) if mape_df.height > 0 else None
    mape_text = f"{mape:.1f}%" if mape is not None else "N/A"

    mo.md(
        f"## Accuracy Snapshot\n\n"
        f"- Overlap rows: **{overlap_df.height:,}**\n"
        f"- MAE: **{mae:.2f}**\n"
        f"- RMSE: **{rmse:.2f}**\n"
        f"- Bias: **{bias:+.2f}**\n"
        f"- MAPE: **{mape_text}**\n"
        f"- Actual mean: **{actual_mean:.2f}**\n"
        f"- Actual std dev: **{actual_std:.2f}**\n\n"
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ---
    ## Metrics by Horizon
    """)
    return


@app.cell
def _(compared_df, mo, pl):
    horizon_metrics_df = (
        compared_df
        .filter(pl.col("actual_units").is_not_null())
        .group_by("horizon_days")
        .agg(
            pl.len().alias("n_obs"),
            pl.col("abs_error").mean().alias("mae"),
            (pl.col("error").pow(2).mean() ** 0.5).alias("rmse"),
            pl.col("error").mean().alias("bias"),
        )
        .sort("horizon_days")
    )

    mo.ui.table(horizon_metrics_df)
    return


@app.cell
def _(compared_df, mo):
    horizon_options = (
        compared_df["horizon_days"].drop_nulls().unique().sort().to_list()
        if compared_df.height > 0
        else [1]
    )
    horizon_select = mo.ui.dropdown(
        options=horizon_options,
        value=horizon_options[0],
        label="Horizon (days)",
    )
    horizon_select
    return (horizon_select,)


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ---
    ## Time-Series Comparison
    """)
    return


@app.cell
def _(alt, compared_df, horizon_select, mo, pl):
    horizon_slice_df = (
        compared_df
        .filter(
            (pl.col("horizon_days") == int(horizon_select.value))
            & pl.col("actual_units").is_not_null()
        )
        .select("forecast_date", "forecast_units", "actual_units")
        .sort("forecast_date")
    )

    mo.stop(
        horizon_slice_df.height == 0,
        mo.callout(mo.md("No overlap rows for this horizon."), kind="info"),
    )

    series_df = horizon_slice_df.unpivot(
        index="forecast_date",
        on=["forecast_units", "actual_units"],
        variable_name="series",
        value_name="units",
    )

    series_chart = (
        alt
        .Chart(series_df)
        .mark_line(point=True)
        .encode(
            x=alt.X("forecast_date:T", title="Date"),
            y=alt.Y("units:Q", title="Units"),
            color=alt.Color("series:N", title="Series"),
            tooltip=[
                "forecast_date:T",
                "series",
                alt.Tooltip("units:Q", format=",.2f"),
            ],
        )
        .properties(width=760, height=320)
    )
    mo.ui.altair_chart(series_chart)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ---
    ## Scatter: Forecast vs Actual
    """)
    return


@app.cell
def _(alt, compared_df, mo, pl):
    overlap_scatter_df = compared_df.filter(pl.col("actual_units").is_not_null())

    mo.stop(
        overlap_scatter_df.height == 0,
        mo.callout(mo.md("No overlap rows available."), kind="warn"),
    )

    axis_max = float(
        max(
            overlap_scatter_df["forecast_units"].max(),
            overlap_scatter_df["actual_units"].max(),
        )
        * 1.1
    )
    parity_df = pl.DataFrame({
        "actual_units": [0.0, axis_max],
        "forecast_units": [0.0, axis_max],
    })

    parity_line = (
        alt
        .Chart(parity_df)
        .mark_line(strokeDash=[5, 5], color="gray")
        .encode(x="actual_units:Q", y="forecast_units:Q")
    )

    scatter_chart = (
        alt
        .Chart(overlap_scatter_df)
        .mark_circle(size=54, opacity=0.72)
        .encode(
            x=alt.X("actual_units:Q", title="Actual units"),
            y=alt.Y("forecast_units:Q", title="Forecast units"),
            color=alt.Color("horizon_days:Q", title="Horizon"),
            tooltip=[
                "forecast_date:T",
                "projection_date:T",
                "horizon_days",
                alt.Tooltip("actual_units:Q", format=",.2f"),
                alt.Tooltip("forecast_units:Q", format=",.2f"),
                alt.Tooltip("error:Q", format="+,.2f"),
            ],
        )
    )

    mo.ui.altair_chart((parity_line + scatter_chart).properties(width=560, height=460))
    return


if __name__ == "__main__":
    app.run()
