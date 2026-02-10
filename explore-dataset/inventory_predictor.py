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
    # Ingredient Consumption Over Time

    How has ingredient usage changed per machine?
    This notebook tracks daily consumption trends, highlights seasonal shifts,
    and spots ingredients whose usage is growing or declining.
    """)
    return


@app.cell
def _(pl, sqlite3):
    _conn = sqlite3.connect("coffee.db")
    consumption_df = pl.read_database(
        """
        SELECT
            d.date, d.total_quantity, d.unit,
            d.machine_id, d.ingredient_id,
            i.name AS ingredient,
            m.name AS machine,
            l.name AS location
        FROM dailyingredientconsumption d
        JOIN ingredient i ON i.id = d.ingredient_id
        JOIN machine m ON m.id = d.machine_id
        JOIN location l ON l.id = m.location_id
        ORDER BY d.date
        """,
        _conn,
    ).with_columns(
        pl.col("date").cast(pl.Utf8).str.to_date("%Y-%m-%d"),
    )
    _conn.close()
    return (consumption_df,)


@app.cell
def _(consumption_df, mo):
    _machines = sorted(consumption_df["machine"].unique().to_list())
    machine_dd = mo.ui.dropdown(options=_machines, value=_machines[0], label="Machine")
    machine_dd
    return (machine_dd,)


@app.cell
def _(consumption_df, machine_dd, pl):
    machine_df = consumption_df.filter(pl.col("machine") == machine_dd.value)
    machine_location = machine_df["location"][0] if machine_df.height > 0 else "?"
    return machine_df, machine_location


# ── Overview: daily consumption per ingredient ────────────────────────────────
@app.cell(hide_code=True)
def _(machine_dd, machine_df, machine_location, mo, pl):
    _summary = (
        machine_df.group_by("ingredient", "unit")
        .agg(
            pl.col("total_quantity").sum().alias("total"),
            pl.col("total_quantity").mean().alias("daily_avg"),
            pl.col("date").n_unique().alias("active_days"),
        )
        .sort("total", descending=True)
    )
    mo.md(f"## Ingredient summary for **{machine_dd.value}** ({machine_location})")
    mo.ui.table(_summary)
    return


@app.cell
def _(alt, machine_df, mo):
    _chart = (
        alt.Chart(machine_df)
        .mark_line(point=alt.OverlayMarkDef(size=15))
        .encode(
            x=alt.X("date:T", title="Date"),
            y=alt.Y("total_quantity:Q", title="Daily quantity"),
            color=alt.Color("ingredient:N"),
            tooltip=[
                "date:T",
                "ingredient:N",
                alt.Tooltip("total_quantity:Q", format=",.1f"),
                "unit:N",
            ],
        )
        .properties(title="Daily consumption — all ingredients", width=750, height=300)
    )
    mo.ui.altair_chart(_chart)
    return


# ── Per-ingredient deep dive ─────────────────────────────────────────────────
@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ---
    ## Per-ingredient trend

    Select an ingredient to see its consumption trend with a 7-day rolling average.
    """)
    return


@app.cell
def _(machine_df, mo):
    _ingredients = sorted(machine_df["ingredient"].unique().to_list())
    ingredient_dd = mo.ui.dropdown(
        options=_ingredients, value=_ingredients[0], label="Ingredient"
    )
    ingredient_dd
    return (ingredient_dd,)


@app.cell
def _(alt, ingredient_dd, machine_df, mo, pl):
    _ing_df = (
        machine_df.filter(pl.col("ingredient") == ingredient_dd.value)
        .sort("date")
        .with_columns(
            pl.col("total_quantity").rolling_mean(window_size=7).alias("rolling_7d"),
        )
    )
    _unit = _ing_df["unit"][0]

    _base = alt.Chart(_ing_df).encode(x=alt.X("date:T", title="Date"))

    _points = _base.mark_circle(size=20, opacity=0.4).encode(
        y=alt.Y("total_quantity:Q", title=f"Quantity ({_unit})"),
        tooltip=["date:T", alt.Tooltip("total_quantity:Q", format=",.1f")],
    )
    _line = _base.mark_line(color="firebrick", strokeWidth=2).encode(
        y="rolling_7d:Q",
    )

    _chart = (_points + _line).properties(
        title=f"{ingredient_dd.value} — daily consumption with 7-day rolling average",
        width=750,
        height=300,
    )
    mo.ui.altair_chart(_chart)
    return


# ── Month-over-month change ──────────────────────────────────────────────────
@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ---
    ## Month-over-month consumption change

    Percentage change in total monthly consumption compared to the previous month.
    Highlights ingredients ramping up or declining.
    """)
    return


@app.cell
def _(alt, machine_df, mo, pl):
    _monthly = (
        machine_df.with_columns(pl.col("date").dt.truncate("1mo").alias("month"))
        .group_by("month", "ingredient", "unit")
        .agg(pl.col("total_quantity").sum().alias("monthly_total"))
        .sort("ingredient", "month")
    )

    _mom = (
        _monthly.with_columns(
            pl.col("monthly_total")
            .shift(1)
            .over("ingredient")
            .alias("prev_month_total"),
        )
        .filter(pl.col("prev_month_total").is_not_null())
        .with_columns(
            (
                (pl.col("monthly_total") - pl.col("prev_month_total"))
                / pl.col("prev_month_total")
                * 100
            ).alias("pct_change"),
        )
    )

    _chart = (
        alt.Chart(_mom)
        .mark_bar()
        .encode(
            x=alt.X("month:T", title="Month"),
            y=alt.Y("pct_change:Q", title="% Change from Previous Month"),
            color="ingredient:N",
            tooltip=[
                "month:T",
                "ingredient:N",
                alt.Tooltip("pct_change:Q", format=".1f"),
                alt.Tooltip("monthly_total:Q", format=",.0f"),
                "unit:N",
            ],
        )
        .properties(
            title="Month-over-month consumption change (%)", width=750, height=300
        )
    )
    mo.ui.altair_chart(_chart)
    return


# ── Machine comparison ────────────────────────────────────────────────────────
@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ---
    ## Machine comparison

    Side-by-side monthly totals across all machines for each ingredient.
    """)
    return


@app.cell
def _(alt, consumption_df, mo, pl):
    _compare = (
        consumption_df.with_columns(pl.col("date").dt.truncate("1mo").alias("month"))
        .group_by("month", "ingredient", "machine")
        .agg(pl.col("total_quantity").sum().alias("monthly_total"))
        .sort("month")
    )

    _chart = (
        alt.Chart(_compare)
        .mark_line(point=True)
        .encode(
            x=alt.X("month:T", title="Month"),
            y=alt.Y("monthly_total:Q", title="Monthly total"),
            color="machine:N",
            strokeDash="machine:N",
            tooltip=[
                "month:T",
                "ingredient:N",
                "machine:N",
                alt.Tooltip("monthly_total:Q", format=",.0f"),
            ],
        )
        .properties(width=350, height=200)
        .facet(facet="ingredient:N", columns=2)
        .resolve_scale(y="independent")
    )
    mo.ui.altair_chart(_chart)
    return


if __name__ == "__main__":
    app.run()
