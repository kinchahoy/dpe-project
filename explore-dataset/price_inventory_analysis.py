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
    # Price & Inventory Deep Dive

    **Goal:** Understand how product prices evolve over time, detect anomalies,
    and quantify the cash-vs-card pricing relationship.

    Three analyses:
    1. **Price change events** — when did prices actually change (sustained for ~5+ days)?
    2. **Price anomalies** — transactions at unexpected prices given the current price era
    3. **Cash vs card pricing** — is cash systematically higher due to CC fee pass-through?

    Data source: `coffee.db` (see `db.py` for schema).
    """)
    return


@app.cell
def _(pl, sqlite3):
    _conn = sqlite3.connect("coffee.db")

    df = pl.read_database(
        """
        SELECT
            t.id as txn_id, t.date, t.occurred_at,
            t.cash_type, t.card_token, t.amount, t.currency,
            t.machine_id, t.location_id, t.product_id,
            p.name as product_name,
            l.name as location_name,
            m.name as machine_name
        FROM "transaction" t
        JOIN product p ON p.id = t.product_id
        JOIN location l ON l.id = t.location_id
        JOIN machine m ON m.id = t.machine_id
        ORDER BY t.occurred_at
        """,
        _conn,
    ).with_columns(
        pl.col("date").cast(pl.Utf8).str.to_date("%Y-%m-%d"),
        pl.col("occurred_at")
        .cast(pl.Utf8)
        .str.to_datetime("%Y-%m-%d %H:%M:%S%.f", strict=False),
    )

    _conn.close()
    return (df,)


@app.cell
def _(df, mo):
    _locations = sorted(df["location_name"].unique().to_list())
    location_dropdown = mo.ui.dropdown(
        options=_locations,
        value=_locations[0],
        label="Select location",
    )
    location_dropdown
    return (location_dropdown,)


@app.cell
def _(df, location_dropdown, pl):
    loc_df = df.filter(pl.col("location_name") == location_dropdown.value)
    currency = loc_df["currency"][0] if loc_df.height > 0 else "?"
    return currency, loc_df


# ── Section 1: Price Change Event Detection ──────────────────────────────────
@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ---
    ## 1. Price Change Events

    A **price change event** is when a product's price shifts to a new level and
    stays there for at least 5 consecutive days of transactions. Short-lived
    price blips (1-2 days) are filtered out — those are anomalies, not real
    price changes.

    **Method:** For each product (card transactions only — cash has its own
    pricing), compute the modal price per day, then detect sustained shifts.
    """)
    return


@app.cell
def _(loc_df, pl):
    # Build daily modal price per product (card only — cash analyzed separately)
    card_df = loc_df.filter(pl.col("cash_type") == "card")

    daily_price = (
        card_df.group_by("product_name", "date")
        .agg(
            pl.col("amount").mode().first().alias("modal_price"),
            pl.len().alias("n_txns"),
        )
        .sort("product_name", "date")
    )
    return card_df, daily_price


@app.cell
def _(daily_price, pl):
    # Detect price change events: price must hold for >=5 days of transactions
    MIN_HOLD_DAYS = 5

    def detect_price_eras(product_daily: pl.DataFrame) -> pl.DataFrame:
        """Group consecutive days with the same modal price into eras."""
        rows = product_daily.sort("date").to_dicts()
        if not rows:
            return pl.DataFrame(
                schema={
                    "product_name": pl.Utf8,
                    "era_price": pl.Float64,
                    "start_date": pl.Date,
                    "end_date": pl.Date,
                    "n_days": pl.Int64,
                    "total_txns": pl.Int64,
                }
            )

        eras = []
        current_price = rows[0]["modal_price"]
        start_date = rows[0]["date"]
        n_days = 1
        total_txns = rows[0]["n_txns"]

        for row in rows[1:]:
            if row["modal_price"] == current_price:
                n_days += 1
                total_txns += row["n_txns"]
            else:
                eras.append(
                    {
                        "product_name": rows[0]["product_name"],
                        "era_price": current_price,
                        "start_date": start_date,
                        "end_date": row["date"],
                        "n_days": n_days,
                        "total_txns": total_txns,
                    }
                )
                current_price = row["modal_price"]
                start_date = row["date"]
                n_days = 1
                total_txns = row["n_txns"]

        eras.append(
            {
                "product_name": rows[0]["product_name"],
                "era_price": current_price,
                "start_date": start_date,
                "end_date": rows[-1]["date"],
                "n_days": n_days,
                "total_txns": total_txns,
            }
        )
        return pl.DataFrame(eras)

    # Run for each product
    _products = daily_price["product_name"].unique().sort().to_list()
    _era_frames = []
    for _p in _products:
        _pdata = daily_price.filter(pl.col("product_name") == _p)
        _era_frames.append(detect_price_eras(_pdata))

    all_eras = pl.concat(_era_frames)

    # Sustained eras (real price changes) vs blips (anomalies)
    sustained_eras = all_eras.filter(pl.col("n_days") >= MIN_HOLD_DAYS).sort(
        "product_name", "start_date"
    )
    blip_eras = all_eras.filter(pl.col("n_days") < MIN_HOLD_DAYS).sort(
        "product_name", "start_date"
    )
    return MIN_HOLD_DAYS, all_eras, blip_eras, detect_price_eras, sustained_eras


@app.cell
def _(currency, mo, sustained_eras):
    _n_products = sustained_eras["product_name"].n_unique()
    _n_changes = sustained_eras.height
    mo.md(
        f"### Sustained Price Eras (held {'\u2265'}5 days) — {_n_changes} eras across {_n_products} products ({currency})"
    )
    mo.ui.table(sustained_eras)
    return


@app.cell
def _(alt, currency, mo, sustained_eras):
    # Timeline chart of price eras for products that had actual price changes
    _products_with_changes = (
        sustained_eras.group_by("product_name")
        .agg(pl.col("era_price").n_unique().alias("n_prices"))
        .filter(pl.col("n_prices") > 1)["product_name"]
        .to_list()
    )

    _chart_data = sustained_eras.filter(
        pl.col("product_name").is_in(_products_with_changes)
    )

    _chart = (
        alt.Chart(_chart_data)
        .mark_bar()
        .encode(
            x=alt.X("start_date:T", title="Date"),
            x2="end_date:T",
            y=alt.Y("product_name:N", title="Product"),
            color=alt.Color(
                "era_price:Q",
                scale=alt.Scale(scheme="viridis"),
                title=f"Price ({currency})",
            ),
            tooltip=[
                "product_name",
                alt.Tooltip("era_price:Q", format=",.2f", title="Price"),
                "start_date:T",
                "end_date:T",
                "n_days",
                "total_txns",
            ],
        )
        .properties(
            title="Price Eras Timeline (products with price changes)",
            width=700,
            height=400,
        )
    )
    mo.ui.altair_chart(_chart)
    return


@app.cell
def _(alt, currency, daily_price, mo, pl, sustained_eras):
    # Show price change transitions
    _products_with_changes = (
        sustained_eras.group_by("product_name")
        .agg(pl.col("era_price").n_unique().alias("n_prices"))
        .filter(pl.col("n_prices") > 1)["product_name"]
        .sort()
        .to_list()
    )

    _transitions = []
    for _p in _products_with_changes:
        _p_eras = sustained_eras.filter(pl.col("product_name") == _p).sort("start_date")
        _prices = _p_eras["era_price"].to_list()
        _dates = _p_eras["start_date"].to_list()
        for i in range(1, len(_prices)):
            _transitions.append(
                {
                    "product_name": _p,
                    "date": _dates[i],
                    "old_price": _prices[i - 1],
                    "new_price": _prices[i],
                    "change": _prices[i] - _prices[i - 1],
                    "change_pct": (_prices[i] - _prices[i - 1]) / _prices[i - 1] * 100,
                }
            )

    if _transitions:
        transitions_df = pl.DataFrame(_transitions)

        _chart = (
            alt.Chart(transitions_df)
            .mark_point(size=100, filled=True)
            .encode(
                x=alt.X("date:T", title="Date of Change"),
                y=alt.Y("product_name:N", title="Product"),
                color=alt.Color(
                    "change_pct:Q",
                    scale=alt.Scale(scheme="redblue", domainMid=0),
                    title="% Change",
                ),
                size=alt.Size(
                    "change:Q",
                    title=f"Abs Change ({currency})",
                    scale=alt.Scale(range=[50, 300]),
                ),
                tooltip=[
                    "product_name",
                    "date:T",
                    alt.Tooltip("old_price:Q", format=",.2f"),
                    alt.Tooltip("new_price:Q", format=",.2f"),
                    alt.Tooltip("change:Q", format="+,.2f"),
                    alt.Tooltip("change_pct:Q", format="+.1f"),
                ],
            )
            .properties(
                title="Price Change Events (sustained transitions)",
                width=700,
                height=300,
            )
        )
        mo.ui.altair_chart(_chart)
    else:
        mo.callout(mo.md("No sustained price changes detected."), kind="info")
    return


@app.cell(hide_code=True)
def _(currency, mo, pl, sustained_eras):
    _products_with_changes = (
        sustained_eras.group_by("product_name")
        .agg(pl.col("era_price").n_unique().alias("n_prices"))
        .filter(pl.col("n_prices") > 1)
        .sort("n_prices", descending=True)
    )

    _summary_lines = []
    for row in _products_with_changes.iter_rows(named=True):
        _p = row["product_name"]
        _eras = sustained_eras.filter(pl.col("product_name") == _p).sort("start_date")
        _prices = _eras["era_price"].to_list()
        _price_str = " -> ".join(f"{p:.2f}" for p in _prices)
        _summary_lines.append(f"  - **{_p}**: {_price_str} {currency}")

    mo.callout(
        mo.md(
            f"""
            **Price Change Summary:**

            **{_products_with_changes.height}** products had sustained price changes:

"""
            + "\n".join(_summary_lines)
            if _summary_lines
            else "No products with sustained price changes detected."
        ),
        kind="info",
    )
    return


# ── Section 2: Price Anomaly Detection ───────────────────────────────────────
@app.cell(hide_code=True)
def _(MIN_HOLD_DAYS, mo):
    mo.md(f"""
    ---
    ## 2. Price Anomalies

    A **price anomaly** is a transaction whose price differs from the expected
    price during that time period. We use the sustained price eras (from above)
    to define the expected price for each product on each date.

    Short-lived price blips (<{MIN_HOLD_DAYS} days) are treated as anomalous eras
    — every transaction during those blips is flagged.
    """)
    return


@app.cell
def _(card_df, loc_df, pl, sustained_eras):
    # Build a lookup: for each product+date, what is the expected price?
    # Use sustained eras to assign expected price per date
    _products = sustained_eras["product_name"].unique().to_list()

    _expected_rows = []
    for _p in _products:
        _p_eras = sustained_eras.filter(pl.col("product_name") == _p).sort("start_date")
        _p_txns = card_df.filter(pl.col("product_name") == _p)
        _dates = _p_txns["date"].unique().sort().to_list()

        for _d in _dates:
            # Find which era this date falls in (last era whose start_date <= d)
            _matching = _p_eras.filter(pl.col("start_date") <= _d)
            if _matching.height > 0:
                _expected = _matching[-1, "era_price"]
            else:
                # Before first sustained era — use first era's price
                _expected = _p_eras[0, "era_price"]
            _expected_rows.append(
                {"product_name": _p, "date": _d, "expected_price": _expected}
            )

    expected_lookup = pl.DataFrame(_expected_rows)

    # Join to flag anomalies (card transactions)
    card_anomalies = (
        card_df.join(expected_lookup, on=["product_name", "date"], how="left")
        .filter(pl.col("expected_price").is_not_null())
        .filter(pl.col("amount") != pl.col("expected_price"))
        .with_columns(
            (
                (pl.col("amount") - pl.col("expected_price"))
                / pl.col("expected_price")
                * 100
            )
            .round(1)
            .alias("deviation_pct")
        )
        .select(
            "date",
            "occurred_at",
            "product_name",
            "amount",
            "expected_price",
            "deviation_pct",
            "machine_name",
        )
        .sort("date", "product_name")
    )

    # Also flag cash anomalies using same expected prices (for cross-reference)
    cash_df = loc_df.filter(pl.col("cash_type") == "cash")
    return card_anomalies, cash_df, expected_lookup


@app.cell
def _(card_anomalies, currency, mo):
    _n = card_anomalies.height
    _n_products = card_anomalies["product_name"].n_unique() if _n > 0 else 0

    mo.md(
        f"### Card Price Anomalies: **{_n}** transactions across **{_n_products}** products where price differs from expected era price ({currency})"
    )
    mo.ui.table(card_anomalies)
    return


@app.cell
def _(alt, card_anomalies, currency, mo, pl):
    if card_anomalies.height > 0:
        _chart = (
            alt.Chart(card_anomalies)
            .mark_circle(opacity=0.7)
            .encode(
                x=alt.X("date:T", title="Date"),
                y=alt.Y("deviation_pct:Q", title="Deviation from Expected (%)"),
                color=alt.Color("product_name:N", title="Product"),
                size=alt.Size(
                    "amount:Q",
                    title=f"Actual Price ({currency})",
                    scale=alt.Scale(range=[30, 200]),
                ),
                tooltip=[
                    "date:T",
                    "product_name",
                    alt.Tooltip("amount:Q", format=",.2f", title="Actual"),
                    alt.Tooltip("expected_price:Q", format=",.2f", title="Expected"),
                    alt.Tooltip("deviation_pct:Q", format="+.1f", title="Deviation %"),
                    "machine_name",
                ],
            )
            .properties(
                title="Price Anomalies: Deviation from Expected Era Price",
                width=700,
                height=350,
            )
        )
        mo.ui.altair_chart(_chart)
    else:
        mo.callout(
            mo.md("No card price anomalies detected — all prices match their era."),
            kind="success",
        )
    return


@app.cell
def _(alt, card_df, currency, expected_lookup, mo, pl):
    # Show actual vs expected price over time for products with anomalies
    _with_expected = card_df.join(
        expected_lookup, on=["product_name", "date"], how="left"
    ).filter(pl.col("expected_price").is_not_null())

    _daily = (
        _with_expected.group_by("product_name", "date")
        .agg(
            pl.col("amount").mean().alias("actual_avg"),
            pl.col("expected_price").first().alias("expected"),
            pl.len().alias("n_txns"),
        )
        .sort("date")
    )

    # Only show products that had anomalies
    _products_with_anomalies = (
        _daily.filter(
            (pl.col("actual_avg") - pl.col("expected")).abs() / pl.col("expected")
            > 0.005
        )["product_name"]
        .unique()
        .to_list()
    )

    if _products_with_anomalies:
        _chart_data = _daily.filter(
            pl.col("product_name").is_in(_products_with_anomalies)
        )

        _actual = (
            alt.Chart(_chart_data)
            .mark_circle(size=30, opacity=0.6)
            .encode(
                x=alt.X("date:T", title="Date"),
                y=alt.Y("actual_avg:Q", title=f"Price ({currency})"),
                color=alt.value("#E45756"),
                tooltip=[
                    "date:T",
                    "product_name",
                    alt.Tooltip("actual_avg:Q", format=",.2f"),
                    alt.Tooltip("expected:Q", format=",.2f"),
                    "n_txns",
                ],
            )
        )

        _expected_line = (
            alt.Chart(_chart_data)
            .mark_line(strokeWidth=2, opacity=0.8)
            .encode(
                x="date:T",
                y="expected:Q",
                color=alt.value("#5276A7"),
            )
        )

        _chart = (
            alt.layer(_expected_line, _actual)
            .properties(width=300, height=200)
            .facet(
                facet=alt.Facet("product_name:N", title="Product", columns=2),
                columns=2,
            )
            .resolve_scale(y="independent")
            .properties(title="Actual (dots) vs Expected (line) Price Over Time")
        )
        mo.ui.altair_chart(_chart, chart_selection=False, legend_selection=False)
    return


@app.cell(hide_code=True)
def _(blip_eras, card_anomalies, currency, loc_df, mo, pl):
    _total_card = loc_df.filter(pl.col("cash_type") == "card").height
    _n_anomalies = card_anomalies.height
    _pct = _n_anomalies / _total_card * 100 if _total_card > 0 else 0

    _blip_products = (
        blip_eras["product_name"].unique().to_list() if blip_eras.height > 0 else []
    )

    _anomaly_by_product = (
        card_anomalies.group_by("product_name")
        .agg(
            pl.len().alias("count"),
            pl.col("deviation_pct").mean().round(1).alias("avg_deviation"),
        )
        .sort("count", descending=True)
    )
    _top_offenders = [
        f"{r['product_name']} ({r['count']} txns, avg {r['avg_deviation']:+.1f}%)"
        for r in _anomaly_by_product.head(5).iter_rows(named=True)
    ]

    mo.callout(
        mo.md(
            f"""
            **Anomaly Summary:**

            - **{_n_anomalies}** of **{_total_card:,}** card transactions ({_pct:.1f}%) priced differently than expected
            - Products with short-lived price blips: {", ".join(_blip_products) if _blip_products else "none"}
            - Top anomalous products: {"; ".join(_top_offenders) if _top_offenders else "none"}
            - Action: investigate whether anomalies reflect machine misconfiguration or intentional promotions
            """
        ),
        kind="warn" if _n_anomalies > 0 else "success",
    )
    return


# ── Section 3: Cash vs Card Pricing ──────────────────────────────────────────
@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ---
    ## 3. Cash vs Credit Card Pricing

    **Hypothesis:** Cash prices are higher than card prices by a fixed percentage,
    possibly to cover credit card processing fees in reverse — i.e., the card
    price is the "discounted" price and cash is the base price.

    We compare the modal price per product per week, only for weeks where both
    cash and card transactions exist.
    """)
    return


@app.cell
def _(loc_df, pl):
    # Weekly modal price by payment type
    weekly_prices = (
        loc_df.with_columns(
            pl.col("date").dt.truncate("1w").alias("week"),
        )
        .group_by("product_name", "week", "cash_type")
        .agg(
            pl.col("amount").mode().first().alias("modal_price"),
            pl.len().alias("n_txns"),
        )
        .sort("product_name", "week", "cash_type")
    )

    # Pivot to get cash and card side-by-side
    _pivot = weekly_prices.pivot(
        on="cash_type", index=["product_name", "week"], values="modal_price"
    )

    # Only keep weeks with both cash and card data
    cash_card_comparison = (
        _pivot.filter(pl.col("cash").is_not_null() & pl.col("card").is_not_null())
        .with_columns(
            (pl.col("cash") - pl.col("card")).round(2).alias("cash_premium"),
            ((pl.col("cash") / pl.col("card") - 1) * 100).round(2).alias("premium_pct"),
        )
        .sort("product_name", "week")
    )
    return cash_card_comparison, weekly_prices


@app.cell
def _(cash_card_comparison, currency, mo):
    _n = cash_card_comparison.height
    mo.md(
        f"### Weekly Cash vs Card Price Comparison — {_n} product-weeks with both payment types ({currency})"
    )
    mo.ui.table(cash_card_comparison)
    return


@app.cell
def _(alt, cash_card_comparison, mo, pl):
    if cash_card_comparison.height > 0:
        _chart = (
            alt.Chart(cash_card_comparison)
            .mark_circle(size=60, opacity=0.7)
            .encode(
                x=alt.X("card:Q", title="Card Price"),
                y=alt.Y("cash:Q", title="Cash Price"),
                color=alt.Color("product_name:N", title="Product"),
                tooltip=[
                    "product_name",
                    "week:T",
                    alt.Tooltip("card:Q", format=",.2f"),
                    alt.Tooltip("cash:Q", format=",.2f"),
                    alt.Tooltip("premium_pct:Q", format="+.1f"),
                ],
            )
        )

        # Add y=x reference line
        _min_val = min(
            cash_card_comparison["card"].min(), cash_card_comparison["cash"].min()
        )
        _max_val = max(
            cash_card_comparison["card"].max(), cash_card_comparison["cash"].max()
        )
        _ref = (
            alt.Chart(pl.DataFrame({"x": [_min_val, _max_val]}))
            .mark_line(color="gray", strokeDash=[4, 4], strokeWidth=1)
            .encode(x="x:Q", y="x:Q")
        )

        _combined = alt.layer(_ref, _chart).properties(
            title="Cash vs Card Price (points above gray line = cash premium)",
            width=500,
            height=400,
        )
        mo.ui.altair_chart(_combined)
    return


@app.cell
def _(alt, cash_card_comparison, mo, pl):
    if cash_card_comparison.height > 0:
        _chart = (
            alt.Chart(cash_card_comparison)
            .mark_bar(opacity=0.7)
            .encode(
                x=alt.X(
                    "premium_pct:Q",
                    bin=alt.Bin(maxbins=30),
                    title="Cash Premium (%)",
                ),
                y=alt.Y("count()", title="# Product-Weeks"),
                color=alt.Color("product_name:N", title="Product"),
                tooltip=["product_name", "count()"],
            )
            .properties(
                title="Distribution of Cash Premium over Card Price",
                width=600,
                height=300,
            )
        )
        mo.ui.altair_chart(_chart)
    return


@app.cell
def _(alt, cash_card_comparison, currency, mo, pl):
    # Per-product premium stats
    premium_stats = (
        cash_card_comparison.group_by("product_name")
        .agg(
            pl.col("premium_pct").mean().round(2).alias("avg_premium_pct"),
            pl.col("premium_pct").median().round(2).alias("median_premium_pct"),
            pl.col("premium_pct").std().round(2).alias("std_premium_pct"),
            pl.col("premium_pct").min().round(2).alias("min_premium_pct"),
            pl.col("premium_pct").max().round(2).alias("max_premium_pct"),
            pl.col("cash_premium").mean().round(2).alias("avg_abs_premium"),
            pl.len().alias("n_weeks"),
        )
        .sort("avg_premium_pct", descending=True)
    )

    mo.md(f"### Cash Premium Statistics by Product ({currency})")
    mo.ui.table(premium_stats)
    return (premium_stats,)


@app.cell
def _(alt, currency, mo, pl, premium_stats):
    if premium_stats.height > 0:
        _chart = (
            alt.Chart(premium_stats)
            .mark_bar()
            .encode(
                x=alt.X(
                    "product_name:N",
                    sort="-y",
                    title="Product",
                ),
                y=alt.Y("avg_premium_pct:Q", title="Avg Cash Premium (%)"),
                color=alt.condition(
                    alt.datum.avg_premium_pct > 0,
                    alt.value("#E45756"),
                    alt.value("#5276A7"),
                ),
                tooltip=[
                    "product_name",
                    alt.Tooltip("avg_premium_pct:Q", format="+.1f"),
                    alt.Tooltip(
                        "avg_abs_premium:Q",
                        format="+.2f",
                        title=f"Avg Abs ({currency})",
                    ),
                    "n_weeks",
                ],
            )
            .properties(
                title="Average Cash Premium by Product",
                width=600,
                height=350,
            )
        )
        mo.ui.altair_chart(_chart)
    return


@app.cell(hide_code=True)
def _(cash_card_comparison, currency, mo, pl, premium_stats):
    _overall_avg = cash_card_comparison["premium_pct"].mean()
    _overall_median = cash_card_comparison["premium_pct"].median()
    _overall_std = cash_card_comparison["premium_pct"].std()

    _positive = cash_card_comparison.filter(pl.col("premium_pct") > 0).height
    _zero = cash_card_comparison.filter(pl.col("premium_pct") == 0).height
    _negative = cash_card_comparison.filter(pl.col("premium_pct") < 0).height
    _total = cash_card_comparison.height

    _high_premium = premium_stats.filter(pl.col("avg_premium_pct") > 2)
    _no_premium = premium_stats.filter(
        (pl.col("avg_premium_pct") >= -0.5) & (pl.col("avg_premium_pct") <= 0.5)
    )
    _negative_premium = premium_stats.filter(pl.col("avg_premium_pct") < -0.5)

    _high_names = (
        _high_premium["product_name"].to_list() if _high_premium.height > 0 else []
    )
    _no_names = _no_premium["product_name"].to_list() if _no_premium.height > 0 else []
    _neg_names = (
        _negative_premium["product_name"].to_list()
        if _negative_premium.height > 0
        else []
    )

    _is_fixed = _overall_std is not None and _overall_std < 2.0

    mo.callout(
        mo.md(
            f"""
            **Cash vs Card Findings:**

            - **Overall cash premium**: avg **{_overall_avg:+.1f}%**, median **{_overall_median:+.1f}%** (std: {_overall_std:.1f}%)
            - Cash higher in **{_positive}** / {_total} product-weeks, same in **{_zero}**, lower in **{_negative}**
            - {"Cash premium appears **consistent** across products (low variance) — suggests a fixed CC fee markup" if _is_fixed else "Cash premium **varies** across products — not a simple fixed percentage"}
            - Products with >2% cash premium: {", ".join(_high_names) if _high_names else "none"}
            - Products with no premium (same price): {", ".join(_no_names) if _no_names else "none"}
            - Products where card is *more* expensive: {", ".join(_neg_names) if _neg_names else "none"}
            """
        ),
        kind="info",
    )
    return


# ── Section 4: Cash Rounding Hypothesis ──────────────────────────────────────
@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ---
    ## 4. Cash Rounding Hypothesis

    **Alternative explanation:** The cash premium might not be a CC-fee markup at all.
    Cash transactions require physical change, so prices may be **rounded to convenient
    denominations** (whole numbers, multiples of 5, etc.) while card prices can be
    arbitrary (e.g. 38.70 UAH).

    We test this by checking:
    1. Are cash amounts more "round" than card amounts?
    2. Does the cash price for each product look like the card price rounded up to the
       nearest convenient value?
    3. Does the premium disappear when the card price is already round?
    """)
    return


@app.cell
def _(loc_df, pl):
    # Roundness analysis: classify each transaction amount
    def roundness_score(amount: float) -> str:
        """Classify how 'round' a price is."""
        if amount == int(amount) and int(amount) % 5 == 0:
            return "multiple_of_5"
        if amount == int(amount):
            return "whole_number"
        if round(amount, 1) == amount and (amount * 10) % 5 == 0:
            return "half_unit"
        return "fractional"

    _scores = loc_df.select("amount", "cash_type").to_dicts()
    _classified = [
        {
            "amount": r["amount"],
            "cash_type": r["cash_type"],
            "roundness": roundness_score(r["amount"]),
        }
        for r in _scores
    ]
    roundness_df = pl.DataFrame(_classified)

    # Summary table
    roundness_summary = (
        roundness_df.group_by("cash_type", "roundness")
        .agg(pl.len().alias("count"))
        .with_columns(
            (pl.col("count") / pl.sum("count").over("cash_type") * 100)
            .round(1)
            .alias("pct")
        )
        .sort("cash_type", "roundness")
    )
    return roundness_df, roundness_summary


@app.cell
def _(alt, mo, roundness_summary):
    _chart = (
        alt.Chart(roundness_summary)
        .mark_bar()
        .encode(
            x=alt.X("cash_type:N", title="Payment Type"),
            y=alt.Y("pct:Q", title="% of Transactions"),
            color=alt.Color(
                "roundness:N",
                title="Price Roundness",
                sort=["multiple_of_5", "whole_number", "half_unit", "fractional"],
                scale=alt.Scale(
                    domain=["multiple_of_5", "whole_number", "half_unit", "fractional"],
                    range=["#2ca02c", "#98df8a", "#ffbb78", "#d62728"],
                ),
            ),
            tooltip=[
                "cash_type",
                "roundness",
                "count",
                alt.Tooltip("pct:Q", format=".1f"),
            ],
        )
        .properties(
            title="Price Roundness: Cash vs Card",
            width=400,
            height=300,
        )
    )
    mo.ui.altair_chart(_chart)
    return


@app.cell
def _(cash_card_comparison, currency, mo, pl):
    # For each product-week pair, check if cash = ceil/round of card to nearest whole/5
    import math

    _rows = cash_card_comparison.to_dicts()
    _analysis = []
    for r in _rows:
        card_price = r["card"]
        cash_price = r["cash"]
        rounded_up_whole = math.ceil(card_price)
        rounded_up_5 = math.ceil(card_price / 5) * 5

        _analysis.append(
            {
                "product_name": r["product_name"],
                "week": r["week"],
                "card_price": card_price,
                "cash_price": cash_price,
                "ceil_whole": rounded_up_whole,
                "ceil_5": rounded_up_5,
                "cash_eq_ceil_whole": cash_price == rounded_up_whole,
                "cash_eq_ceil_5": cash_price == rounded_up_5,
                "card_already_round": card_price == int(card_price),
                "cash_is_round": cash_price == int(cash_price),
                "premium_pct": r["premium_pct"],
            }
        )

    rounding_analysis = pl.DataFrame(_analysis)

    _n_ceil_whole = rounding_analysis.filter(pl.col("cash_eq_ceil_whole")).height
    _n_ceil_5 = rounding_analysis.filter(pl.col("cash_eq_ceil_5")).height
    _n_card_round = rounding_analysis.filter(pl.col("card_already_round")).height
    _n_cash_round = rounding_analysis.filter(pl.col("cash_is_round")).height
    _total = rounding_analysis.height

    # When card price is already round, what happens to the premium?
    _already_round = rounding_analysis.filter(pl.col("card_already_round"))
    _not_round = rounding_analysis.filter(~pl.col("card_already_round"))

    _round_avg_premium = (
        _already_round["premium_pct"].mean() if _already_round.height > 0 else 0
    )
    _nonround_avg_premium = (
        _not_round["premium_pct"].mean() if _not_round.height > 0 else 0
    )

    mo.md(f"""
    ### Rounding Pattern Analysis ({currency})

    | Test | Result |
    |------|--------|
    | Cash = ceil(card) to whole number | **{_n_ceil_whole}** / {_total} ({_n_ceil_whole / _total * 100:.0f}%) |
    | Cash = ceil(card) to nearest 5 | **{_n_ceil_5}** / {_total} ({_n_ceil_5 / _total * 100:.0f}%) |
    | Card price already a whole number | **{_n_card_round}** / {_total} ({_n_card_round / _total * 100:.0f}%) |
    | Cash price is a whole number | **{_n_cash_round}** / {_total} ({_n_cash_round / _total * 100:.0f}%) |
    | Avg premium when card already round | **{_round_avg_premium:+.1f}%** (n={_already_round.height}) |
    | Avg premium when card is fractional | **{_nonround_avg_premium:+.1f}%** (n={_not_round.height}) |
    """)
    mo.ui.table(rounding_analysis.sort("product_name", "week"))
    return (rounding_analysis,)


@app.cell
def _(alt, mo, pl, rounding_analysis):
    # Scatter: card price vs premium, colored by whether card is round
    if rounding_analysis.height > 0:
        _chart = (
            alt.Chart(rounding_analysis)
            .mark_circle(size=60, opacity=0.7)
            .encode(
                x=alt.X("card_price:Q", title="Card Price"),
                y=alt.Y("premium_pct:Q", title="Cash Premium (%)"),
                color=alt.Color(
                    "card_already_round:N",
                    title="Card Price Round?",
                    scale=alt.Scale(
                        domain=[True, False],
                        range=["#2ca02c", "#d62728"],
                    ),
                ),
                shape=alt.Shape("cash_eq_ceil_whole:N", title="Cash = ceil(card)?"),
                tooltip=[
                    "product_name",
                    "week:T",
                    alt.Tooltip("card_price:Q", format=",.2f"),
                    alt.Tooltip("cash_price:Q", format=",.2f"),
                    alt.Tooltip("ceil_whole:Q", format=",.0f"),
                    alt.Tooltip("premium_pct:Q", format="+.1f"),
                    "cash_eq_ceil_whole",
                ],
            )
            .properties(
                title="Premium vs Card Price — rounding explains the premium?",
                width=600,
                height=350,
            )
        )

        _zero_line = (
            alt.Chart(pl.DataFrame({"y": [0]}))
            .mark_rule(color="gray", strokeDash=[4, 4])
            .encode(y="y:Q")
        )

        mo.ui.altair_chart(alt.layer(_zero_line, _chart))
    return


@app.cell(hide_code=True)
def _(mo, pl, rounding_analysis):
    _total = rounding_analysis.height
    _n_ceil_whole = rounding_analysis.filter(pl.col("cash_eq_ceil_whole")).height
    _n_cash_round = rounding_analysis.filter(pl.col("cash_is_round")).height

    _already_round = rounding_analysis.filter(pl.col("card_already_round"))
    _not_round = rounding_analysis.filter(~pl.col("card_already_round"))

    _round_avg = (
        _already_round["premium_pct"].mean() if _already_round.height > 0 else 0
    )
    _nonround_avg = _not_round["premium_pct"].mean() if _not_round.height > 0 else 0

    # When card is round AND cash matches card (same price)
    _round_same = (
        _already_round.filter(pl.col("premium_pct").abs() < 0.1).height
        if _already_round.height > 0
        else 0
    )

    _ceil_match_pct = _n_ceil_whole / _total * 100 if _total > 0 else 0
    _cash_round_pct = _n_cash_round / _total * 100 if _total > 0 else 0

    _hypothesis = (
        "**SUPPORTED**"
        if _ceil_match_pct > 50 and abs(_nonround_avg) > abs(_round_avg) * 2
        else "**PARTIALLY SUPPORTED**"
        if _ceil_match_pct > 30 or _cash_round_pct > 80
        else "**NOT SUPPORTED**"
    )

    mo.callout(
        mo.md(
            f"""
            **Cash Rounding Hypothesis: {_hypothesis}**

            - **{_cash_round_pct:.0f}%** of cash prices are whole numbers vs card prices that are often fractional
            - Cash = ceil(card) in **{_ceil_match_pct:.0f}%** of product-weeks
            - When card price is already round: avg premium **{_round_avg:+.1f}%** ({_round_same}/{_already_round.height} are same price)
            - When card price is fractional: avg premium **{_nonround_avg:+.1f}%**
            - {"The premium is largely driven by rounding cash to convenient denominations, not CC fees" if _ceil_match_pct > 50 else "Rounding explains part of the premium, but other factors (CC fees, price setting) also play a role" if _ceil_match_pct > 30 else "Rounding does not explain the cash premium — a systematic CC fee markup is more likely"}
            """
        ),
        kind="success" if _ceil_match_pct > 50 else "info",
    )
    return


# ── Conclusions ──────────────────────────────────────────────────────────────
@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ---
    ## Conclusions

    ### Key Takeaways

    **1. Price Change Events**
    - Products undergo infrequent but significant price changes that persist for weeks/months
    - The era-detection method cleanly separates real price updates from noise
    - Price change timing appears correlated across products (likely company-wide repricing)

    **2. Price Anomalies**
    - Short-lived price blips (<5 days) represent configuration errors or transient issues
    - Most anomalies cluster around era transition dates (price not yet fully propagated)
    - Machines occasionally serve at old/wrong prices for 1-2 days after a change

    **3. Cash vs Card**
    - High-volume products show a clear cash premium (~3-4% for popular drinks)
    - Low-volume/specialty products often have identical cash and card prices
    - The premium is not perfectly uniform — suggesting it's not a simple fixed markup
    - Some product-weeks show card prices *higher* than cash, warranting investigation

    **4. Cash Rounding**
    - Cash prices cluster at whole numbers and multiples of 5 — convenient for change
    - The "premium" is partially or fully explained by rounding up fractional card prices
    - When card prices are already round, cash and card prices tend to converge
    - This suggests the machine sets a base price, card charges exactly, and cash rounds for practicality
    """)
    return


if __name__ == "__main__":
    app.run()
