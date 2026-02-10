"""Marimo notebook to compare actual product prices vs expected prices.

Shows a timeline chart of actual transaction prices (amount) compared to
expected prices from the sim_transaction_expanded table. Useful for
identifying pricing anomalies and validating price calculations.
"""

import marimo

__generated_with = "0.10.14"
app = marimo.App(width="medium")


@app.cell
def _imports():
    import sys
    from pathlib import Path

    import marimo as mo
    import polars as pl
    from loguru import logger
    from sqlalchemy import text

    # Add project root to path
    PROJECT_ROOT = Path(__file__).resolve().parents[1]
    if str(PROJECT_ROOT) not in sys.path:
        sys.path.insert(0, str(PROJECT_ROOT))

    from db import (
        create_facts_db,
        create_sim_db,
        facts_engine,
        sim_engine,
    )

    return (
        mo,
        pl,
        logger,
        text,
        PROJECT_ROOT,
        create_facts_db,
        create_sim_db,
        facts_engine,
        sim_engine,
    )


@app.cell
def _initialize_dbs(mo, create_facts_db, create_sim_db):
    """Initialize databases."""
    mo.md(
        "# Price Comparison: Actual vs Expected\n\nCompare transaction prices against expected prices from simulations."
    )

    # Initialize databases
    create_facts_db()
    create_sim_db()

    return


@app.cell
def _get_machines(facts_engine, text):
    """Load machine list from facts database."""

    with facts_engine.connect() as _conn:
        _result = _conn.execute(text('SELECT id, name FROM "machine" ORDER BY name'))
        _machines = _result.fetchall()

    machines_list = [{"id": _row[0], "name": _row[1]} for _row in _machines]

    return (machines_list,)


@app.cell
def _machine_selector(mo, machines_list):
    """UI to select machine."""

    _options = {f"{_m['name']}": _m["id"] for _m in machines_list}

    # Try to default to UA-1, otherwise use first machine
    _default_machine = None
    for _name, _id in _options.items():
        if "UA-1" in _name:
            _default_machine = _name
            break
    if _default_machine is None and machines_list:
        _default_machine = machines_list[0]["name"]

    machine_selector = mo.ui.dropdown(
        options=_options,
        value=_default_machine,
        label="Select Machine:",
    )

    return (machine_selector,)


@app.cell
def _show_machine_selector(mo, machine_selector, machines_list):
    """Display machine selector."""
    mo.md(
        f"""
        ### Step 1: Choose a Machine

        Available machines: {len(machines_list)}

        {machine_selector}
        """
    )
    return


@app.cell
def _get_products(mo, sim_engine, facts_engine, text, machine_selector):
    """Load products available on selected machine."""

    # Stop if no machine selected
    mo.stop(not machine_selector.value)

    _machine_id = machine_selector.value

    # Get distinct product IDs that have transactions on this machine
    with sim_engine.connect() as _conn:
        _result = _conn.execute(
            text(
                """
                SELECT DISTINCT product_id
                FROM "sim_transaction_expanded"
                WHERE machine_id = :machine_id
                """
            ),
            {"machine_id": _machine_id},
        )
        _product_ids = [_row[0] for _row in _result.fetchall()]

    # Get product names from facts database
    if _product_ids:
        _placeholders = ",".join([f":id{_i}" for _i in range(len(_product_ids))])
        _params = {f"id{_i}": _pid for _i, _pid in enumerate(_product_ids)}

        with facts_engine.connect() as _conn:
            _result = _conn.execute(
                text(
                    f'SELECT id, name FROM "product" WHERE id IN ({_placeholders}) ORDER BY name'
                ),
                _params,
            )
            _products = _result.fetchall()

        products_list = [{"id": _row[0], "name": _row[1]} for _row in _products]
    else:
        products_list = []

    return (products_list,)


@app.cell
def _product_selector(mo, products_list):
    """UI to select product."""

    product_selector = mo.ui.dropdown(
        options={f"{_p['name']}": _p["id"] for _p in products_list},
        value=products_list[0]["name"] if products_list else None,
        label="Select Product:",
    )

    return (product_selector,)


@app.cell
def _show_product_selector(mo, product_selector, products_list):
    """Display product selector."""
    mo.md(
        f"""
        ### Step 2: Choose a Product

        Products available on this machine: {len(products_list)}

        {product_selector}
        """
    )
    return


@app.cell
def _query_transactions(mo, sim_engine, text, product_selector, machine_selector, pl):
    """Query transaction data for selected product and machine."""

    # Stop if no product selected
    mo.stop(not product_selector.value)

    _product_id = product_selector.value
    _machine_id = machine_selector.value

    # Query sim_transaction_expanded table
    with sim_engine.connect() as _conn:
        _result = _conn.execute(
            text(
                """
                SELECT
                    occurred_at,
                    date,
                    amount,
                    expected_price,
                    location_id,
                    machine_id,
                    cash_type
                FROM "sim_transaction_expanded"
                WHERE product_id = :product_id AND machine_id = :machine_id
                ORDER BY occurred_at
                """
            ),
            {"product_id": _product_id, "machine_id": _machine_id},
        )
        _rows = _result.fetchall()
        _columns = _result.keys()

    if not _rows:
        transaction_data = None
    else:
        # Convert to polars DataFrame for easier manipulation
        transaction_data = pl.DataFrame(
            {_col: [_row[_i] for _row in _rows] for _i, _col in enumerate(_columns)}
        )

    return (transaction_data,)


@app.cell
def _show_summary(mo, transaction_data):
    """Display summary statistics."""

    # Stop if no data
    mo.stop(transaction_data is None)

    _total_txns = len(transaction_data)
    _txns_with_expected = transaction_data.filter(
        pl.col("expected_price").is_not_null()
    )
    _count_with_expected = len(_txns_with_expected)

    if _count_with_expected > 0:
        _price_diffs = _txns_with_expected.select(
            (pl.col("amount") - pl.col("expected_price")).alias("diff")
        )
        _avg_diff = _price_diffs["diff"].mean()
        _max_diff = _price_diffs["diff"].max()
        _min_diff = _price_diffs["diff"].min()
        _anomalies = len(_price_diffs.filter(pl.col("diff").abs() > 0.01))

        summary_stats = mo.md(
            f"""
            ### Step 3: Summary Statistics

            - **Total Transactions**: {_total_txns:,}
            - **Transactions with Expected Price**: {_count_with_expected:,}
            - **Price Anomalies** (|diff| > $0.01): {_anomalies:,} ({_anomalies / _count_with_expected * 100:.1f}%)
            - **Average Difference**: ${_avg_diff:.4f}
            - **Max Difference**: ${_max_diff:.4f}
            - **Min Difference**: ${_min_diff:.4f}
            """
        )
    else:
        summary_stats = mo.md(
            f"""
            ### Step 3: Summary Statistics

            - **Total Transactions**: {_total_txns:,}
            - **Transactions with Expected Price**: 0
            - ⚠️ No transactions have expected_price values
            """
        )

    return (summary_stats,)


@app.cell
def _display_summary(summary_stats):
    """Display the summary."""
    summary_stats
    return


@app.cell
def _create_chart(mo, transaction_data, pl):
    """Create price comparison chart."""
    import plotly.graph_objects as go

    # Stop if no data
    mo.stop(transaction_data is None)

    _txns_with_expected = transaction_data.filter(
        pl.col("expected_price").is_not_null()
    )

    if len(_txns_with_expected) == 0:
        price_chart = mo.md("⚠️ No transactions with expected prices to chart")
    else:
        # Group by date and calculate modal (most common) prices
        _daily_stats = (
            _txns_with_expected.group_by("date")
            .agg(
                [
                    pl.col("amount").mode().first().alias("modal_actual"),
                    pl.col("expected_price").mode().first().alias("modal_expected"),
                ]
            )
            .sort("date")
        )

        _dates = _daily_stats["date"].to_list()
        _modal_actual = _daily_stats["modal_actual"].to_list()
        _modal_expected = _daily_stats["modal_expected"].to_list()

        # Create figure
        _fig = go.Figure()

        # Add actual price line
        _fig.add_trace(
            go.Scatter(
                x=_dates,
                y=_modal_actual,
                mode="lines+markers",
                name="Actual Price",
                line=dict(color="blue", width=2),
                marker=dict(size=6),
            )
        )

        # Add expected price line
        _fig.add_trace(
            go.Scatter(
                x=_dates,
                y=_modal_expected,
                mode="lines+markers",
                name="Expected Price",
                line=dict(color="green", width=2, dash="dash"),
                marker=dict(size=6),
            )
        )

        # Update layout
        _fig.update_layout(
            title="Daily Modal Price: Actual vs Expected",
            xaxis_title="Date",
            yaxis_title="Price ($)",
            height=500,
            showlegend=True,
            hovermode="x unified",
        )

        price_chart = mo.ui.plotly(_fig)

    return (price_chart,)


@app.cell
def _show_chart_header(mo):
    """Display chart section header."""
    mo.md("### Step 4: Timeline View")
    return


@app.cell
def _display_chart(price_chart):
    """Display the chart."""
    price_chart
    return


@app.cell
def _anomaly_table(mo, transaction_data, pl, products_list, product_selector):
    """Show table of significant anomalies."""

    # Stop if no data
    mo.stop(transaction_data is None)

    _txns_with_expected = transaction_data.filter(
        pl.col("expected_price").is_not_null()
    )

    if len(_txns_with_expected) == 0:
        anomaly_result = mo.md("⚠️ No transactions with expected prices")
    else:
        # Calculate differences and filter for anomalies
        _anomalies_df = (
            _txns_with_expected.select(
                [
                    pl.col("occurred_at"),
                    pl.col("date"),
                    pl.col("location_id"),
                    pl.col("machine_id"),
                    pl.col("amount"),
                    pl.col("expected_price"),
                    (pl.col("amount") - pl.col("expected_price")).alias("difference"),
                    pl.col("cash_type"),
                ]
            )
            .filter((pl.col("amount") - pl.col("expected_price")).abs() > 0.01)
            .sort("difference", descending=True)
        )

        if len(_anomalies_df) == 0:
            anomaly_result = mo.md(
                "✅ No significant price anomalies found (all within $0.01)"
            )
        else:
            # Convert to list of dicts for display
            _anomaly_rows = _anomalies_df.limit(20).to_dicts()

            _product_name = [
                k
                for k, v in product_selector.options.items()
                if v == product_selector.value
            ][0]

            anomaly_result = mo.vstack(
                [
                    mo.md(f"""
                ### Step 5: Price Anomalies

                Showing up to 20 transactions with |difference| > $0.01 for **{_product_name}**
                """),
                    mo.ui.table(data=_anomaly_rows, label="Price Anomalies"),
                ]
            )

    return (anomaly_result,)


@app.cell
def _display_anomalies(anomaly_result):
    """Display anomalies table."""
    anomaly_result
    return


if __name__ == "__main__":
    app.run()
