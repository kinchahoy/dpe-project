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
    mo.md(
        "# Price Driver Investigation\n\n"
        "Focused root-cause view using `sim_transaction_expanded` (machine-level):\n"
        "1. Charged vs expected price over time\n"
        "2. Off-price clustering by hour/weekday\n"
        "3. Highest-density anomaly buckets"
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

    tx_df = (
        query_df(
            db_paths.analysis_db,
            """
            SELECT
                t.run_id,
                t.date,
                t.occurred_at,
                t.location_id,
                t.machine_id,
                t.product_id,
                t.cash_type,
                t.amount,
                t.expected_price,
                t.currency,
                p.name AS product_name,
                l.name AS location_name,
                m.name AS machine_name
            FROM sim_transaction_expanded t
            JOIN facts.product p ON p.id = t.product_id
            JOIN facts.location l ON l.id = t.location_id
            JOIN facts.machine m ON m.id = t.machine_id
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
            pl.when(pl.col("expected_price") > 0)
            .then(
                (pl.col("amount") - pl.col("expected_price")) / pl.col("expected_price")
            )
            .otherwise(None)
            .alias("delta_pct"),
        )
    )

    return run_df, tx_df


@app.cell
def _(mo, run_df):
    run_options = run_df["id"].to_list()
    run_select = mo.ui.dropdown(
        options=run_options, value=run_options[0], label="Run ID"
    )
    run_select
    return (run_select,)


@app.cell
def _(mo, pl, run_select, tx_df):
    run_tx_df = tx_df.filter(pl.col("run_id") == run_select.value)

    location_options = run_tx_df["location_name"].unique().sort().to_list()
    location_select = mo.ui.dropdown(
        options=location_options,
        value=location_options[0],
        label="Location",
    )
    location_select
    return location_select, run_tx_df


@app.cell
def _(location_select, mo, pl, run_tx_df):
    location_tx_df = run_tx_df.filter(pl.col("location_name") == location_select.value)
    mo.stop(
        location_tx_df.height == 0,
        mo.callout(mo.md("No transactions found for this run/location."), kind="warn"),
    )

    machine_options = location_tx_df["machine_name"].unique().sort().to_list()
    machine_select = mo.ui.dropdown(
        options=machine_options,
        value=machine_options[0],
        label="Machine",
    )
    machine_select
    return location_tx_df, machine_select


@app.cell
def _(location_tx_df, machine_select, mo, pl):
    machine_tx_df = location_tx_df.filter(
        pl.col("machine_name") == machine_select.value
    )

    product_options = (
        machine_tx_df.group_by("product_name")
        .agg(pl.len().alias("n"))
        .sort("n", descending=True)["product_name"]
        .to_list()
    )
    product_select = mo.ui.dropdown(
        options=product_options,
        value=product_options[0],
        label="Product",
    )
    product_select
    return machine_tx_df, product_select


@app.cell
def _(machine_tx_df, pl, product_select):
    product_tx_df = machine_tx_df.filter(pl.col("product_name") == product_select.value)

    off_price_df = product_tx_df.filter(
        pl.col("delta_pct").is_not_null() & (pl.col("delta_pct").abs() >= 0.05)
    )
    return off_price_df, product_tx_df


@app.cell(hide_code=True)
def _(mo, off_price_df, product_select, product_tx_df):
    off_pct = (
        off_price_df.height / product_tx_df.height * 100
        if product_tx_df.height > 0
        else 0.0
    )
    mo.md(
        f"## {product_select.value}\n\n"
        f"- Total transactions: **{product_tx_df.height:,}**\n"
        f"- Off-price rows (>=5% delta): **{off_price_df.height:,} ({off_pct:.1f}%)**"
    )
    return


@app.cell
def _(alt, mo, product_tx_df):
    price_points = (
        alt.Chart(product_tx_df)
        .mark_circle(size=24, opacity=0.45)
        .encode(
            x=alt.X("occurred_at:T", title="Timestamp"),
            y=alt.Y("amount:Q", title="Charged price"),
            color=alt.Color("cash_type:N", title="Payment type"),
            tooltip=[
                "occurred_at:T",
                "machine_name",
                "cash_type",
                alt.Tooltip("amount:Q", format=",.2f"),
                alt.Tooltip("expected_price:Q", format=",.2f"),
                alt.Tooltip("delta_pct:Q", format="+.1%"),
            ],
        )
    )

    expected_line = (
        alt.Chart(product_tx_df)
        .mark_line(color="#e8590c", strokeWidth=2)
        .encode(
            x=alt.X("occurred_at:T", title="Timestamp"),
            y=alt.Y("expected_price:Q", title="Expected price"),
        )
    )

    mo.ui.altair_chart(
        (price_points + expected_line).properties(
            width=780,
            height=320,
            title="Charged vs expected price over time",
        )
    )
    return


@app.cell
def _(alt, mo, off_price_df, pl):
    mo.stop(
        off_price_df.height == 0,
        mo.callout(
            mo.md("No off-price rows for this product selection."), kind="success"
        ),
    )

    hourly_df = off_price_df.group_by("hour", "cash_type").agg(pl.len().alias("count"))

    hourly_chart = (
        alt.Chart(hourly_df)
        .mark_bar()
        .encode(
            x=alt.X("hour:O", title="Hour"),
            y=alt.Y("count:Q", title="Off-price count"),
            color=alt.Color("cash_type:N", title="Payment type"),
            tooltip=["hour", "cash_type", "count"],
        )
        .properties(width=780, height=220, title="Off-price clustering by hour")
    )

    mo.ui.altair_chart(hourly_chart)
    return


@app.cell
def _(alt, mo, off_price_df, pl):
    mo.stop(off_price_df.height == 0, None)

    dow_labels = {1: "Mon", 2: "Tue", 3: "Wed", 4: "Thu", 5: "Fri", 6: "Sat", 7: "Sun"}

    weekday_df = (
        off_price_df.group_by("weekday")
        .agg(pl.len().alias("count"))
        .with_columns(
            pl.col("weekday")
            .replace_strict(dow_labels, return_dtype=pl.Utf8)
            .alias("weekday_name")
        )
        .sort("weekday")
    )

    weekday_chart = (
        alt.Chart(weekday_df)
        .mark_bar(color="#4263eb")
        .encode(
            x=alt.X("weekday_name:N", sort=list(dow_labels.values()), title="Weekday"),
            y=alt.Y("count:Q", title="Off-price count"),
            tooltip=["weekday_name", "count"],
        )
        .properties(width=520, height=220, title="Off-price clustering by weekday")
    )

    mo.ui.altair_chart(weekday_chart)
    return


@app.cell
def _(mo, off_price_df, pl):
    mo.stop(off_price_df.height == 0, None)

    bucket_df = (
        off_price_df.group_by("machine_name", "date", "cash_type")
        .agg(
            pl.len().alias("off_price_count"),
            pl.col("delta_pct").mean().alias("avg_delta_pct"),
            pl.col("delta_pct").min().alias("worst_delta_pct"),
        )
        .sort("off_price_count", descending=True)
    )

    mo.md("## Highest-density anomaly buckets")
    mo.ui.table(bucket_df.head(120))
    return


if __name__ == "__main__":
    app.run()
