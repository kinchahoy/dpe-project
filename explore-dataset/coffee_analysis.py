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
        "# Coffee Ops Overview\n\n"
        "Updated for the split database setup used by `simple-agentic-framework`:\n"
        "- facts: `vending_machine_facts.db`\n"
        "- observed: `vending_sales_observed.db`\n"
        "- analysis: `vending_analysis.db`\n\n"
        "This notebook keeps only the highest-signal operational views: demand mix, peak hours, trend, and ingredient pressure."
    )
    return


@app.cell
def _(pl, query_df, resolve_vending_db_paths):
    db_paths = resolve_vending_db_paths()

    transactions_df = (
        query_df(
            db_paths.observed_db,
            """
            SELECT
                t.id AS txn_id,
                t.date,
                t.occurred_at,
                t.cash_type,
                t.amount,
                t.currency,
                t.product_id,
                t.machine_id,
                t.location_id,
                p.name AS product_name,
                m.name AS machine_name,
                m.model AS machine_model,
                l.name AS location_name
            FROM "transaction" t
            JOIN facts.product p ON p.id = t.product_id
            JOIN facts.machine m ON m.id = t.machine_id
            JOIN facts.location l ON l.id = t.location_id
            ORDER BY t.occurred_at
            """,
            attachments={"facts": db_paths.facts_db},
        )
        .with_columns(
            pl.col("date").cast(pl.Utf8).str.to_date("%Y-%m-%d"),
            pl.col("occurred_at")
            .cast(pl.Utf8)
            .str.to_datetime("%Y-%m-%d %H:%M:%S%.f", strict=False),
        )
        .with_columns(
            pl.col("occurred_at").dt.hour().alias("hour"),
            pl.col("occurred_at").dt.weekday().alias("weekday"),
        )
    )

    ingredient_daily_df = (
        query_df(
            db_paths.observed_db,
            """
            SELECT
                d.date,
                d.machine_id,
                d.ingredient_id,
                d.total_quantity,
                d.unit,
                ing.name AS ingredient_name,
                m.name AS machine_name,
                m.model AS machine_model,
                l.name AS location_name
            FROM daily_ingredient_consumption d
            JOIN facts.ingredient ing ON ing.id = d.ingredient_id
            JOIN facts.machine m ON m.id = d.machine_id
            JOIN facts.location l ON l.id = m.location_id
            ORDER BY d.date, d.machine_id, d.ingredient_id
            """,
            attachments={"facts": db_paths.facts_db},
        )
        .with_columns(pl.col("date").cast(pl.Utf8).str.to_date("%Y-%m-%d"))
        .sort("date")
    )

    capacities_df = query_df(
        db_paths.facts_db,
        """
        SELECT
            cap.machine_model,
            cap.ingredient_id,
            ing.name AS ingredient_name,
            cap.capacity,
            cap.unit AS capacity_unit
        FROM machine_ingredient_capacity cap
        JOIN ingredient ing ON ing.id = cap.ingredient_id
        """,
    )

    return capacities_df, ingredient_daily_df, transactions_df


@app.cell
def _(mo, transactions_df):
    location_options = transactions_df["location_name"].unique().sort().to_list()
    location_select = mo.ui.dropdown(
        options=location_options,
        value=location_options[0],
        label="Location",
    )
    location_select
    return (location_select,)


@app.cell
def _(ingredient_daily_df, location_select, mo, pl, transactions_df):
    location_tx_df = transactions_df.filter(
        pl.col("location_name") == location_select.value
    )
    location_ingredient_df = ingredient_daily_df.filter(
        pl.col("location_name") == location_select.value
    )

    mo.stop(
        location_tx_df.height == 0,
        mo.callout(mo.md("No transactions found for this location."), kind="warn"),
    )

    location_currency = location_tx_df["currency"][0]
    return location_currency, location_ingredient_df, location_tx_df


@app.cell(hide_code=True)
def _(location_currency, location_tx_df, mo, pl):
    tx_count = location_tx_df.height
    total_revenue = float(location_tx_df["amount"].sum())
    avg_ticket = total_revenue / tx_count
    card_share = (
        location_tx_df.filter(pl.col("cash_type") == "card").height / tx_count * 100
    )
    date_min = location_tx_df["date"].min()
    date_max = location_tx_df["date"].max()

    mo.md(
        f"## Snapshot\n\n"
        f"- Transactions: **{tx_count:,}**\n"
        f"- Date range: **{date_min}** to **{date_max}**\n"
        f"- Revenue: **{total_revenue:,.2f} {location_currency}**\n"
        f"- Avg ticket: **{avg_ticket:.2f} {location_currency}**\n"
        f"- Card share: **{card_share:.1f}%**"
    )
    return


@app.cell
def _(location_tx_df, mo):
    mo.ui.table(location_tx_df.sort("occurred_at", descending=True).head(120))
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("---\n## Demand by Product")
    return


@app.cell
def _(alt, location_currency, location_tx_df, mo, pl):
    product_mix_df = (
        location_tx_df.group_by("product_name")
        .agg(
            pl.len().alias("units"),
            pl.col("amount").sum().alias("revenue"),
        )
        .sort("units", descending=True)
    )

    product_mix_chart = (
        alt.Chart(product_mix_df)
        .mark_bar()
        .encode(
            x=alt.X("product_name:N", sort="-y", title="Product"),
            y=alt.Y("units:Q", title="Units sold"),
            color=alt.Color(
                "revenue:Q",
                title=f"Revenue ({location_currency})",
                scale=alt.Scale(scheme="goldorange"),
            ),
            tooltip=["product_name", "units", alt.Tooltip("revenue:Q", format=",.2f")],
        )
        .properties(width=720, height=320)
    )
    mo.ui.altair_chart(product_mix_chart)
    return


@app.cell
def _(alt, location_tx_df, mo, pl):
    top_products = (
        location_tx_df.group_by("product_name")
        .agg(pl.len().alias("units"))
        .sort("units", descending=True)
        .head(12)["product_name"]
        .to_list()
    )

    heatmap_df = (
        location_tx_df.filter(pl.col("product_name").is_in(top_products))
        .group_by("product_name", "hour")
        .agg(pl.len().alias("units"))
    )

    heatmap_chart = (
        alt.Chart(heatmap_df)
        .mark_rect()
        .encode(
            x=alt.X("hour:O", title="Hour"),
            y=alt.Y("product_name:N", title="Top products"),
            color=alt.Color("units:Q", title="Units", scale=alt.Scale(scheme="teals")),
            tooltip=["product_name", "hour", "units"],
        )
        .properties(width=720, height=340, title="Peak-hour pressure")
    )

    mo.ui.altair_chart(heatmap_chart)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("---\n## Daily Trend")
    return


@app.cell
def _(alt, location_currency, location_tx_df, mo, pl):
    daily_df = (
        location_tx_df.group_by("date")
        .agg(
            pl.len().alias("units"),
            pl.col("amount").sum().alias("revenue"),
        )
        .sort("date")
    )

    units_line = (
        alt.Chart(daily_df)
        .mark_line(point=True)
        .encode(
            x=alt.X("date:T", title="Date"),
            y=alt.Y("units:Q", title="Units"),
            tooltip=["date:T", "units"],
        )
        .properties(width=720, height=170, title="Daily units")
    )

    revenue_line = (
        alt.Chart(daily_df)
        .mark_line(point=True, color="#0b7285")
        .encode(
            x=alt.X("date:T", title="Date"),
            y=alt.Y("revenue:Q", title=f"Revenue ({location_currency})"),
            tooltip=["date:T", alt.Tooltip("revenue:Q", format=",.2f")],
        )
        .properties(width=720, height=170, title="Daily revenue")
    )

    mo.ui.altair_chart(alt.vconcat(units_line, revenue_line))
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("---\n## Ingredient Pressure")
    return


@app.cell
def _(capacities_df, location_ingredient_df, mo, pl, timedelta):
    mo.stop(
        location_ingredient_df.height == 0,
        mo.callout(
            mo.md("No ingredient-consumption rows for this location."), kind="warn"
        ),
    )

    latest_obs_date = location_ingredient_df["date"].max()
    recent_cutoff_date = latest_obs_date - timedelta(days=13)

    ingredient_recent_df = (
        location_ingredient_df.filter(pl.col("date") >= recent_cutoff_date)
        .group_by(
            "machine_id",
            "machine_name",
            "machine_model",
            "ingredient_id",
            "ingredient_name",
            "unit",
        )
        .agg(
            pl.col("total_quantity").mean().alias("avg_daily_qty_14d"),
            pl.col("total_quantity").max().alias("max_daily_qty_14d"),
        )
        .join(
            capacities_df,
            left_on=["machine_model", "ingredient_id"],
            right_on=["machine_model", "ingredient_id"],
            how="left",
        )
        .with_columns(
            pl.when(pl.col("avg_daily_qty_14d") > 0)
            .then(pl.col("capacity") / pl.col("avg_daily_qty_14d"))
            .otherwise(None)
            .alias("days_of_cover_at_capacity")
        )
        .sort("days_of_cover_at_capacity")
    )

    mo.ui.table(
        ingredient_recent_df.select(
            "machine_name",
            "ingredient_name",
            "unit",
            "avg_daily_qty_14d",
            "max_daily_qty_14d",
            "capacity",
            "days_of_cover_at_capacity",
        )
    )

    return ingredient_recent_df


@app.cell
def _(ingredient_recent_df, mo):
    machine_options = ingredient_recent_df["machine_name"].unique().sort().to_list()
    machine_select = mo.ui.dropdown(
        options=machine_options,
        value=machine_options[0],
        label="Machine",
    )
    machine_select
    return (machine_select,)


@app.cell
def _(ingredient_recent_df, machine_select, mo, pl):
    machine_risk_df = ingredient_recent_df.filter(
        pl.col("machine_name") == machine_select.value
    )

    ingredient_options = machine_risk_df["ingredient_name"].unique().sort().to_list()
    ingredient_select = mo.ui.dropdown(
        options=ingredient_options,
        value=ingredient_options[0],
        label="Ingredient",
    )

    mo.hstack([machine_select, ingredient_select])
    return ingredient_select, machine_risk_df


@app.cell
def _(alt, ingredient_select, location_ingredient_df, machine_risk_df, mo, pl):
    machine_name_value = machine_risk_df["machine_name"][0]

    ingredient_trend_df = (
        location_ingredient_df.filter(
            (pl.col("machine_name") == machine_name_value)
            & (pl.col("ingredient_name") == ingredient_select.value)
        )
        .sort("date")
        .with_columns(
            pl.col("total_quantity").rolling_mean(window_size=7).alias("rolling_7d")
        )
    )

    trend_chart = (
        alt.Chart(ingredient_trend_df)
        .mark_line(point=True)
        .encode(
            x=alt.X("date:T", title="Date"),
            y=alt.Y("total_quantity:Q", title="Daily quantity"),
            tooltip=["date:T", alt.Tooltip("total_quantity:Q", format=",.2f")],
        )
        .properties(
            width=720, height=220, title=f"{ingredient_select.value}: daily usage"
        )
    )

    rolling_chart = (
        alt.Chart(ingredient_trend_df)
        .mark_line(color="#e8590c")
        .encode(
            x=alt.X("date:T", title="Date"),
            y=alt.Y("rolling_7d:Q", title="7-day rolling avg"),
            tooltip=["date:T", alt.Tooltip("rolling_7d:Q", format=",.2f")],
        )
        .properties(width=720, height=140)
    )

    mo.ui.altair_chart(alt.vconcat(trend_chart, rolling_chart))
    return


if __name__ == "__main__":
    app.run()
