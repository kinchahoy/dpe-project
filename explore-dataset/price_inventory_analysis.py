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
        "# Price & Inventory Deep Dive\n\n"
        "Machine-level only (no cross-machine price mixing).\n\n"
        "Retained analyses:\n"
        "1. Pricing anomalies (expected vs charged)\n"
        "2. Machine-specific expected-price change events\n"
        "3. Machine-specific cash-vs-card premium"
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

    machine_df = query_df(
        db_paths.facts_db,
        """
        SELECT
            m.id AS machine_id,
            m.name AS machine_name,
            m.location_id,
            l.name AS location_name
        FROM machine m
        JOIN location l ON l.id = m.location_id
        ORDER BY l.id, m.id
        """,
    )

    simulated_tx_df = (
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
            pl.when(pl.col("expected_price") > 0)
            .then(
                (pl.col("amount") - pl.col("expected_price")) / pl.col("expected_price")
            )
            .otherwise(None)
            .alias("delta_pct")
        )
    )

    observed_price_df = query_df(
        db_paths.observed_db,
        """
            SELECT
                s.date,
                s.location_id,
                s.machine_id,
                s.product_id,
                s.cash_type,
                SUM(CAST(s.units_sold AS FLOAT)) AS units,
                SUM(CAST(s.revenue AS FLOAT)) AS revenue,
                p.name AS product_name,
                l.name AS location_name,
                m.name AS machine_name
            FROM daily_product_sales s
            JOIN facts.product p ON p.id = s.product_id
            JOIN facts.location l ON l.id = s.location_id
            JOIN facts.machine m ON m.id = s.machine_id
            GROUP BY
                s.date,
                s.location_id,
                s.machine_id,
                s.product_id,
                s.cash_type,
                p.name,
                l.name,
                m.name
            """,
        attachments={"facts": db_paths.facts_db},
    ).with_columns(
        pl.col("date").cast(pl.Utf8).str.to_date("%Y-%m-%d"),
        pl.when(pl.col("units") > 0)
        .then(pl.col("revenue") / pl.col("units"))
        .otherwise(None)
        .alias("effective_price"),
    )

    return machine_df, observed_price_df, run_df, simulated_tx_df


@app.cell
def _(machine_df, mo, run_df):
    run_options = run_df["id"].to_list() if run_df.height > 0 else [""]
    run_select = mo.ui.dropdown(
        options=run_options, value=run_options[0], label="Run ID"
    )

    location_options = machine_df["location_name"].unique().sort().to_list()
    location_select = mo.ui.dropdown(
        options=location_options,
        value=location_options[0],
        label="Location",
    )

    mo.hstack([run_select, location_select])
    return location_select, run_select


@app.cell
def _(location_select, machine_df, mo, pl):
    machine_options_df = machine_df.filter(
        pl.col("location_name") == location_select.value
    ).sort("machine_name")

    machine_options = {
        row["machine_name"]: int(row["machine_id"])
        for row in machine_options_df.iter_rows(named=True)
    }

    machine_select = mo.ui.dropdown(
        options=machine_options,
        value=list(machine_options.values())[0],
        label="Machine",
    )
    machine_select
    return (machine_select,)


@app.cell
def _(location_select, machine_select, mo, pl, run_select, simulated_tx_df):
    priced_tx_df = simulated_tx_df.filter(
        (pl.col("run_id") == run_select.value)
        & (pl.col("location_name") == location_select.value)
        & (pl.col("machine_id") == int(machine_select.value))
    )

    mo.stop(
        priced_tx_df.height == 0,
        mo.callout(
            mo.md(
                f"No `sim_transaction_expanded` rows for run `{run_select.value}` and machine `{machine_select.value}`."
            ),
            kind="warn",
        ),
    )

    return (priced_tx_df,)


@app.cell(hide_code=True)
def _(mo):
    mo.md("---\n## Pricing Anomalies (Expected vs Charged)")
    return


@app.cell
def _(pl, priced_tx_df):
    anomaly_tx_df = priced_tx_df.filter(
        pl.col("expected_price").is_not_null()
        & (pl.col("expected_price") > 0)
        & (pl.col("delta_pct").abs() >= 0.05)
    )

    anomaly_summary_df = (
        anomaly_tx_df.group_by("product_name")
        .agg(
            pl.len().alias("anomaly_count"),
            pl.col("delta_pct").mean().alias("avg_delta_pct"),
            pl.col("delta_pct").min().alias("min_delta_pct"),
            pl.col("delta_pct").max().alias("max_delta_pct"),
        )
        .sort("anomaly_count", descending=True)
    )
    return anomaly_summary_df, anomaly_tx_df


@app.cell
def _(anomaly_summary_df, mo):
    mo.ui.table(anomaly_summary_df)
    return


@app.cell
def _(alt, anomaly_tx_df, mo, pl):
    mo.stop(
        anomaly_tx_df.height == 0,
        mo.callout(
            mo.md("No >=5% anomalies found for this machine selection."), kind="success"
        ),
    )

    axis_max = float(
        max(
            anomaly_tx_df["amount"].max(),
            anomaly_tx_df["expected_price"].max(),
        )
        * 1.1
    )

    parity_df = pl.DataFrame(
        {
            "expected_price": [0.0, axis_max],
            "amount": [0.0, axis_max],
        }
    )

    parity_line = (
        alt.Chart(parity_df)
        .mark_line(strokeDash=[4, 4], color="gray")
        .encode(x="expected_price:Q", y="amount:Q")
    )

    anomaly_scatter = (
        alt.Chart(anomaly_tx_df)
        .mark_circle(size=56, opacity=0.72)
        .encode(
            x=alt.X("expected_price:Q", title="Expected price"),
            y=alt.Y("amount:Q", title="Charged amount"),
            color=alt.Color("cash_type:N", title="Payment type"),
            tooltip=[
                "date:T",
                "product_name",
                "machine_name",
                alt.Tooltip("expected_price:Q", format=",.2f"),
                alt.Tooltip("amount:Q", format=",.2f"),
                alt.Tooltip("delta_pct:Q", format="+.1%"),
            ],
        )
    )

    mo.ui.altair_chart(
        (parity_line + anomaly_scatter).properties(
            width=720,
            height=340,
            title="Machine-level anomalies vs expected baseline",
        )
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("---\n## Expected Price Change Events (Machine-Level)")
    return


@app.cell
def _(pl, priced_tx_df):
    daily_expected_df = (
        priced_tx_df.group_by("date", "product_name")
        .agg(pl.col("expected_price").median().alias("expected_price"))
        .sort("product_name", "date")
        .with_columns(
            pl.col("expected_price")
            .shift(1)
            .over("product_name")
            .alias("prev_expected_price")
        )
        .with_columns(
            (pl.col("expected_price") != pl.col("prev_expected_price")).alias(
                "is_change"
            )
        )
    )

    expected_change_df = daily_expected_df.filter(
        pl.col("prev_expected_price").is_not_null() & pl.col("is_change")
    )
    return daily_expected_df, expected_change_df


@app.cell
def _(alt, daily_expected_df, expected_change_df, mo):
    mo.ui.table(expected_change_df)

    expected_line = (
        alt.Chart(daily_expected_df)
        .mark_line(point=True)
        .encode(
            x=alt.X("date:T", title="Date"),
            y=alt.Y("expected_price:Q", title="Expected price"),
            color=alt.Color("product_name:N", title="Product"),
            tooltip=[
                "date:T",
                "product_name",
                alt.Tooltip("expected_price:Q", format=",.2f"),
            ],
        )
        .properties(width=760, height=320, title="Expected price timeline by product")
    )

    mo.ui.altair_chart(expected_line)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("---\n## Observed Cash vs Card Premium (Machine-Level)")
    return


@app.cell
def _(location_select, machine_select, observed_price_df, pl):
    machine_price_df = observed_price_df.filter(
        (pl.col("location_name") == location_select.value)
        & (pl.col("machine_id") == int(machine_select.value))
    )

    weekly_price_df = (
        machine_price_df.with_columns(pl.col("date").dt.truncate("1w").alias("week"))
        .group_by("week", "product_name", "cash_type")
        .agg(
            pl.col("units").sum().alias("units"),
            pl.col("revenue").sum().alias("revenue"),
        )
        .with_columns(
            pl.when(pl.col("units") > 0)
            .then(pl.col("revenue") / pl.col("units"))
            .otherwise(None)
            .alias("avg_price")
        )
    )

    premium_weekly_df = (
        weekly_price_df.pivot(
            index=["week", "product_name"],
            on="cash_type",
            values="avg_price",
            aggregate_function="first",
        )
        .with_columns(
            pl.when((pl.col("card") > 0) & pl.col("cash").is_not_null())
            .then((pl.col("cash") - pl.col("card")) / pl.col("card") * 100)
            .otherwise(None)
            .alias("cash_premium_pct")
        )
        .filter(pl.col("cash_premium_pct").is_not_null())
        .sort("week")
    )

    return (premium_weekly_df,)


@app.cell
def _(mo, premium_weekly_df):
    mo.ui.table(premium_weekly_df.sort("cash_premium_pct", descending=True).head(140))
    return


@app.cell
def _(alt, mo, pl, premium_weekly_df):
    mo.stop(
        premium_weekly_df.height == 0,
        mo.callout(
            mo.md("No weeks with both cash and card sales for this machine."),
            kind="warn",
        ),
    )

    premium_by_product_df = (
        premium_weekly_df.group_by("product_name")
        .agg(
            pl.len().alias("weeks_with_both"),
            pl.col("cash_premium_pct").mean().alias("avg_cash_premium_pct"),
        )
        .sort("avg_cash_premium_pct", descending=True)
    )

    premium_chart = (
        alt.Chart(premium_by_product_df)
        .mark_bar()
        .encode(
            x=alt.X("product_name:N", sort="-y", title="Product"),
            y=alt.Y("avg_cash_premium_pct:Q", title="Avg cash premium (%)"),
            color=alt.Color("weeks_with_both:Q", title="Weeks with both types"),
            tooltip=[
                "product_name",
                "weeks_with_both",
                alt.Tooltip("avg_cash_premium_pct:Q", format=",.2f"),
            ],
        )
        .properties(width=760, height=300)
    )

    mo.ui.altair_chart(premium_chart)
    return


if __name__ == "__main__":
    app.run()
