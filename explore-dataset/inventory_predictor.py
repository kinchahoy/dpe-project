import marimo

__generated_with = "0.19.9"
app = marimo.App(width="medium")


@app.cell
def _():
    from datetime import timedelta

    import altair as alt
    import marimo as mo
    import polars as pl

    from notebook_db import query_df, resolve_vending_db_paths

    return alt, mo, pl, query_df, resolve_vending_db_paths, timedelta


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        "# Ingredient Predictor\n\n"
        "Combines observed usage, projected usage, and machine capacities:\n"
        "- observed: `daily_ingredient_consumption`\n"
        "- projected: `sim_daily_ingredient_projection`\n"
        "- capacity: `machine_ingredient_capacity`"
    )
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

    observed_df = (
        query_df(
            db_paths.observed_db,
            """
            SELECT
                d.date,
                d.machine_id,
                d.ingredient_id,
                d.total_quantity,
                d.unit,
                i.name AS ingredient_name,
                m.name AS machine_name,
                m.model AS machine_model,
                l.name AS location_name
            FROM daily_ingredient_consumption d
            JOIN facts.ingredient i ON i.id = d.ingredient_id
            JOIN facts.machine m ON m.id = d.machine_id
            JOIN facts.location l ON l.id = m.location_id
            ORDER BY d.date, d.machine_id, d.ingredient_id
            """,
            attachments={"facts": db_paths.facts_db},
        )
        .with_columns(pl.col("date").cast(pl.Utf8).str.to_date("%Y-%m-%d"))
        .sort("date")
    )

    projected_df = query_df(
        db_paths.analysis_db,
        """
            SELECT
                p.run_id,
                p.projection_date,
                p.forecast_date,
                p.machine_id,
                p.ingredient_id,
                p.forecast_quantity,
                p.unit,
                i.name AS ingredient_name,
                m.name AS machine_name,
                m.model AS machine_model,
                l.name AS location_name
            FROM sim_daily_ingredient_projection p
            JOIN facts.ingredient i ON i.id = p.ingredient_id
            JOIN facts.machine m ON m.id = p.machine_id
            JOIN facts.location l ON l.id = m.location_id
            ORDER BY p.projection_date, p.forecast_date, p.machine_id, p.ingredient_id
            """,
        attachments={"facts": db_paths.facts_db},
    ).with_columns(
        pl.col("projection_date").cast(pl.Utf8).str.to_date("%Y-%m-%d"),
        pl.col("forecast_date").cast(pl.Utf8).str.to_date("%Y-%m-%d"),
    )

    capacity_df = query_df(
        db_paths.facts_db,
        """
        SELECT machine_model, ingredient_id, capacity, unit AS capacity_unit
        FROM machine_ingredient_capacity
        """,
    )

    return capacity_df, observed_df, projected_df, run_df


@app.cell
def _(mo, observed_df, pl, run_df):
    run_options = run_df["id"].to_list()
    run_select = mo.ui.dropdown(
        options=run_options, value=run_options[0], label="Run ID"
    )

    machine_options = (
        observed_df.select("location_name", "machine_name")
        .unique()
        .sort("location_name", "machine_name")
        .with_columns(
            (pl.col("location_name") + " | " + pl.col("machine_name")).alias("label")
        )["label"]
        .to_list()
    )
    machine_select = mo.ui.dropdown(
        options=machine_options,
        value=machine_options[0],
        label="Machine",
    )

    mo.hstack([run_select, machine_select])
    return machine_select, run_select


@app.cell
def _(machine_select, observed_df, pl, projected_df, run_select):
    machine_name_value = machine_select.value.split(" | ")[1]

    machine_observed_df = observed_df.filter(
        pl.col("machine_name") == machine_name_value
    )
    machine_projected_df = projected_df.filter(
        (pl.col("run_id") == run_select.value)
        & (pl.col("machine_name") == machine_name_value)
    )

    projection_anchor_date = (
        machine_projected_df["projection_date"].max()
        if machine_projected_df.height > 0
        else None
    )

    return machine_observed_df, machine_projected_df, projection_anchor_date


@app.cell
def _(
    capacity_df,
    machine_observed_df,
    machine_projected_df,
    mo,
    pl,
    projection_anchor_date,
    timedelta,
):
    mo.stop(
        machine_observed_df.height == 0,
        mo.callout(mo.md("No observed ingredient rows for this machine."), kind="warn"),
    )

    observed_max_date = machine_observed_df["date"].max()
    observed_cutoff_date = observed_max_date - timedelta(days=13)

    observed_recent_df = (
        machine_observed_df.filter(pl.col("date") >= observed_cutoff_date)
        .group_by("machine_model", "ingredient_id", "ingredient_name", "unit")
        .agg(pl.col("total_quantity").mean().alias("obs_14d_avg_daily_qty"))
    )

    projected_next7_df = (
        machine_projected_df.filter(
            (pl.col("projection_date") == projection_anchor_date)
            & (pl.col("forecast_date") > projection_anchor_date)
            & (pl.col("forecast_date") <= projection_anchor_date + timedelta(days=7))
        )
        .group_by("machine_model", "ingredient_id", "ingredient_name", "unit")
        .agg(pl.col("forecast_quantity").mean().alias("proj_next7_avg_daily_qty"))
    )

    summary_df = (
        observed_recent_df.join(
            projected_next7_df,
            on=["machine_model", "ingredient_id", "ingredient_name", "unit"],
            how="outer",
        )
        .join(capacity_df, on=["machine_model", "ingredient_id"], how="left")
        .with_columns(
            pl.coalesce([pl.col("unit"), pl.col("capacity_unit")]).alias("unit_final"),
            pl.when(pl.col("obs_14d_avg_daily_qty") > 0)
            .then(
                (pl.col("proj_next7_avg_daily_qty") - pl.col("obs_14d_avg_daily_qty"))
                / pl.col("obs_14d_avg_daily_qty")
                * 100
            )
            .otherwise(None)
            .alias("projected_vs_observed_delta_pct"),
            pl.when(pl.col("proj_next7_avg_daily_qty") > 0)
            .then(pl.col("capacity") / pl.col("proj_next7_avg_daily_qty"))
            .otherwise(None)
            .alias("capacity_days_cover_at_projected_rate"),
        )
        .sort("capacity_days_cover_at_projected_rate")
    )

    mo.md(
        f"## Summary\n\n"
        f"- Observed baseline window: **{observed_cutoff_date}** to **{observed_max_date}**\n"
        f"- Projection anchor date: **{projection_anchor_date}**"
    )

    mo.ui.table(
        summary_df.select(
            "ingredient_name",
            "unit_final",
            "obs_14d_avg_daily_qty",
            "proj_next7_avg_daily_qty",
            "projected_vs_observed_delta_pct",
            "capacity",
            "capacity_days_cover_at_projected_rate",
        )
    )

    return summary_df


@app.cell
def _(mo, summary_df):
    ingredient_options = summary_df["ingredient_name"].drop_nulls().sort().to_list()
    ingredient_select = mo.ui.dropdown(
        options=ingredient_options,
        value=ingredient_options[0],
        label="Ingredient",
    )
    ingredient_select
    return (ingredient_select,)


@app.cell
def _(
    alt,
    ingredient_select,
    machine_observed_df,
    machine_projected_df,
    mo,
    pl,
    projection_anchor_date,
    timedelta,
):
    observed_trend_df = machine_observed_df.filter(
        (pl.col("ingredient_name") == ingredient_select.value)
        & (pl.col("date") >= machine_observed_df["date"].max() - timedelta(days=29))
    ).select(
        pl.col("date").alias("x_date"),
        pl.col("total_quantity").alias("qty"),
        pl.lit("observed").alias("series"),
    )

    projected_trend_df = machine_projected_df.filter(
        (pl.col("ingredient_name") == ingredient_select.value)
        & (pl.col("projection_date") == projection_anchor_date)
        & (pl.col("forecast_date") > projection_anchor_date)
        & (pl.col("forecast_date") <= projection_anchor_date + timedelta(days=14))
    ).select(
        pl.col("forecast_date").alias("x_date"),
        pl.col("forecast_quantity").alias("qty"),
        pl.lit("projected").alias("series"),
    )

    trend_df = pl.concat([observed_trend_df, projected_trend_df], how="vertical")

    trend_chart = (
        alt.Chart(trend_df)
        .mark_line(point=True)
        .encode(
            x=alt.X("x_date:T", title="Date"),
            y=alt.Y("qty:Q", title="Quantity"),
            color=alt.Color("series:N", title="Series"),
            tooltip=["x_date:T", "series", alt.Tooltip("qty:Q", format=",.2f")],
        )
        .properties(
            width=760,
            height=300,
            title=f"{ingredient_select.value}: observed vs projected",
        )
    )
    mo.ui.altair_chart(trend_chart)
    return


@app.cell
def _(alt, ingredient_select, mo, observed_df, pl, timedelta):
    last_date = observed_df["date"].max()
    cutoff_date = last_date - timedelta(days=13)

    compare_df = (
        observed_df.filter(
            (pl.col("ingredient_name") == ingredient_select.value)
            & (pl.col("date") >= cutoff_date)
        )
        .group_by("location_name", "machine_name")
        .agg(pl.col("total_quantity").mean().alias("obs_14d_avg_daily_qty"))
        .sort("obs_14d_avg_daily_qty", descending=True)
    )

    compare_chart = (
        alt.Chart(compare_df)
        .mark_bar()
        .encode(
            x=alt.X("machine_name:N", sort="-y", title="Machine"),
            y=alt.Y("obs_14d_avg_daily_qty:Q", title="Observed 14d avg daily qty"),
            color=alt.Color("location_name:N", title="Location"),
            tooltip=[
                "location_name",
                "machine_name",
                alt.Tooltip("obs_14d_avg_daily_qty:Q", format=",.2f"),
            ],
        )
        .properties(
            width=760,
            height=280,
            title=f"Observed usage by machine: {ingredient_select.value}",
        )
    )
    mo.ui.altair_chart(compare_chart)
    return


if __name__ == "__main__":
    app.run()
