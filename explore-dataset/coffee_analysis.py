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
    # Coffee Vending Machine Sales Analysis

    **Business context:** Leadership wants to use transaction data more effectively to address:

    1. **Product stockouts** during peak hours
    2. **Arbitrary pricing** & price anomalies
    3. **Cash reconciliation** issues
    4. **Ingredient stockout** detection
    5. **Manual reporting** overhead

    Data source: `coffee.db` — normalized SQLite database with transactions, products,
    ingredients, machines, and locations (schema defined in `db.py`).
    Two locations: **Lviv, Ukraine** (UAH) and **San Francisco, CA** (USD).
    """)
    return


@app.cell
def _(pl, sqlite3):
    # Schema: see db.py for Transaction, Product, Ingredient, ProductIngredient, Location, Machine
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
        pl.col("occurred_at").cast(pl.Utf8).str.to_datetime("%Y-%m-%d %H:%M:%S%.f", strict=False),
    ).with_columns(
        pl.col("occurred_at").dt.hour().alias("hour"),
        pl.col("occurred_at").dt.weekday().alias("weekday"),
        pl.col("occurred_at").dt.month().alias("month"),
    )

    recipe_df = pl.read_database(
        """
        SELECT
            pi.product_id, p.name as product_name,
            pi.ingredient_id, i.name as ingredient_name, i.unit,
            pi.quantity
        FROM productingredient pi
        JOIN product p ON p.id = pi.product_id
        JOIN ingredient i ON i.id = pi.ingredient_id
        """,
        _conn,
    )

    _conn.close()
    return df, recipe_df


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


@app.cell(hide_code=True)
def _(currency, loc_df, mo, pl):
    _n = loc_df.height
    _date_min = loc_df["date"].min()
    _date_max = loc_df["date"].max()
    _total_rev = loc_df["amount"].sum()
    _avg_ticket = _total_rev / _n if _n > 0 else 0
    _card_pct = loc_df.filter(pl.col("cash_type") == "card").height / _n * 100 if _n > 0 else 0
    _machines = loc_df["machine_name"].unique().sort().to_list()

    mo.md(
        f"""
        ## Dataset Overview — {loc_df["location_name"][0] if _n > 0 else "?"}

        | Metric | Value |
        |--------|-------|
        | Total transactions | **{_n:,}** |
        | Machines | **{', '.join(_machines)}** |
        | Date range | **{_date_min}** to **{_date_max}** |
        | Total revenue | **{_total_rev:,.2f} {currency}** |
        | Avg ticket | **{_avg_ticket:.2f} {currency}** |
        | Card payment % | **{_card_pct:.1f}%** |
        """
    )
    return


@app.cell
def _(loc_df, mo):
    mo.ui.table(loc_df.head(100))
    return


# ── Section 1: Inventory & Stockout Analysis ────────────────────────────────
@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ---
    ## 1. Inventory & Stockout Analysis

    Understanding product popularity and peak-hour demand to prevent stockouts.
    """)
    return


@app.cell
def _(alt, currency, loc_df, mo, pl):
    _product_stats = (
        loc_df.group_by("product_name")
        .agg(
            pl.len().alias("volume"),
            pl.col("amount").sum().alias("revenue"),
        )
        .sort("volume", descending=True)
    )

    _chart = (
        alt.Chart(_product_stats)
        .mark_bar()
        .encode(
            x=alt.X("product_name:N", sort="-y", title="Product"),
            y=alt.Y("volume:Q", title="Transaction Count"),
            color=alt.Color("revenue:Q", scale=alt.Scale(scheme="goldorange"), title=f"Revenue ({currency})"),
            tooltip=["product_name", "volume", alt.Tooltip("revenue:Q", format=",.2f")],
        )
        .properties(title="Product Popularity (Volume & Revenue)", width=600, height=350)
    )
    mo.ui.altair_chart(_chart)
    return


@app.cell
def _(alt, loc_df, mo, pl):
    _heatmap_data = loc_df.group_by("product_name", "hour").agg(pl.len().alias("count"))

    _heatmap = (
        alt.Chart(_heatmap_data)
        .mark_rect()
        .encode(
            x=alt.X("hour:O", title="Hour of Day"),
            y=alt.Y("product_name:N", title="Product"),
            color=alt.Color("count:Q", scale=alt.Scale(scheme="blues"), title="Transactions"),
            tooltip=["product_name", "hour", "count"],
        )
        .properties(title="Demand Heatmap: Product x Hour (Stockout Risk Windows)", width=600, height=400)
    )
    mo.ui.altair_chart(_heatmap)
    return


@app.cell
def _(alt, loc_df, mo, pl):
    _dow_labels = {1: "Mon", 2: "Tue", 3: "Wed", 4: "Thu", 5: "Fri", 6: "Sat", 7: "Sun"}
    _dow_data = (
        loc_df.group_by("weekday")
        .agg(pl.len().alias("volume"))
        .with_columns(pl.col("weekday").replace_strict(_dow_labels, return_dtype=pl.Utf8).alias("day_name"))
        .sort("weekday")
    )

    _chart = (
        alt.Chart(_dow_data)
        .mark_bar()
        .encode(
            x=alt.X("day_name:N", sort=list(_dow_labels.values()), title="Day of Week"),
            y=alt.Y("volume:Q", title="Transaction Count"),
            color=alt.Color("volume:Q", scale=alt.Scale(scheme="tealblues"), legend=None),
            tooltip=["day_name", "volume"],
        )
        .properties(title="Transaction Volume by Day of Week", width=500, height=300)
    )
    mo.ui.altair_chart(_chart)
    return


@app.cell(hide_code=True)
def _(loc_df, mo, pl):
    _prod = (
        loc_df.group_by("product_name")
        .agg(pl.len().alias("volume"), pl.col("amount").sum().alias("revenue"))
        .sort("volume", descending=True)
    )
    _top3 = _prod.head(3)["product_name"].to_list()
    _bottom3 = _prod.tail(3)["product_name"].to_list()

    _peak_hours = (
        loc_df.group_by("hour")
        .agg(pl.len().alias("vol"))
        .sort("vol", descending=True)
        .head(3)["hour"]
        .to_list()
    )

    mo.callout(
        mo.md(
            f"""
            **Inventory Recommendations:**

            - **Top sellers** (keep fully stocked): {', '.join(_top3)}
            - **Low movers** (reduce inventory): {', '.join(_bottom3)}
            - **Peak restock hours**: {', '.join(str(h) + ':00' for h in sorted(_peak_hours))}
              — ensure supplies are replenished *before* these windows
            """
        ),
        kind="success",
    )
    return


# ── Section 2: Pricing Optimization ─────────────────────────────────────────
@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ---
    ## 2. Pricing Optimization

    Exploring price-volume relationships and revenue concentration to identify pricing opportunities.
    """)
    return


@app.cell
def _(loc_df, mo):
    _names = sorted(loc_df["product_name"].unique().to_list())
    coffee_dropdown = mo.ui.dropdown(
        options=_names,
        value=_names[0] if _names else "",
        label="Select product",
    )
    coffee_dropdown
    return (coffee_dropdown,)


@app.cell
def _(alt, coffee_dropdown, currency, loc_df, mo, pl):
    _selected = coffee_dropdown.value
    _hourly = (
        loc_df.filter(pl.col("product_name") == _selected)
        .group_by("hour")
        .agg(
            pl.col("amount").mean().alias("avg_price"),
            pl.len().alias("volume"),
        )
        .sort("hour")
    )

    _base = alt.Chart(_hourly).encode(x=alt.X("hour:O", title="Hour of Day"))

    _bars = _base.mark_bar(opacity=0.5, color="#5276A7").encode(
        y=alt.Y("volume:Q", title="Volume"),
        tooltip=["hour", "volume", alt.Tooltip("avg_price:Q", format=",.2f")],
    )

    _line = _base.mark_line(color="#F18727", strokeWidth=3, point=True).encode(
        y=alt.Y("avg_price:Q", title=f"Avg Price ({currency})"),
        tooltip=["hour", alt.Tooltip("avg_price:Q", format=",.2f")],
    )

    _combined = (
        alt.layer(_bars, _line)
        .resolve_scale(y="independent")
        .properties(title=f"{_selected}: Avg Price vs Volume by Hour", width=600, height=350)
    )
    mo.ui.altair_chart(_combined)
    return


@app.cell
def _(alt, currency, loc_df, mo, pl):
    _pareto = (
        loc_df.group_by("product_name")
        .agg(pl.col("amount").sum().alias("revenue"))
        .sort("revenue", descending=True)
        .with_columns(
            (pl.col("revenue").cum_sum() / pl.col("revenue").sum() * 100).alias("cumulative_pct")
        )
    )

    _bars = (
        alt.Chart(_pareto)
        .mark_bar()
        .encode(
            x=alt.X("product_name:N", sort="-y", title="Product"),
            y=alt.Y("revenue:Q", title=f"Revenue ({currency})"),
            color=alt.condition(
                alt.datum.cumulative_pct <= 80,
                alt.value("#5276A7"),
                alt.value("#CCCCCC"),
            ),
            tooltip=["product_name", alt.Tooltip("revenue:Q", format=",.2f"), alt.Tooltip("cumulative_pct:Q", format=".1f")],
        )
    )

    _line = (
        alt.Chart(_pareto)
        .mark_line(color="#F18727", strokeWidth=2, point=True)
        .encode(
            x=alt.X("product_name:N", sort="-y"),
            y=alt.Y("cumulative_pct:Q", title="Cumulative %", scale=alt.Scale(domain=[0, 100])),
            tooltip=["product_name", alt.Tooltip("cumulative_pct:Q", format=".1f")],
        )
    )

    _rule = alt.Chart().mark_rule(color="red", strokeDash=[4, 4]).encode(y=alt.datum(80))

    _chart = (
        alt.layer(_bars, _line, _rule)
        .resolve_scale(y="independent")
        .properties(title="Revenue Pareto Analysis (80/20)", width=600, height=350)
    )
    mo.ui.altair_chart(_chart)
    return


@app.cell(hide_code=True)
def _(currency, loc_df, mo, pl):
    _rev_share = (
        loc_df.group_by("product_name")
        .agg(pl.col("amount").sum().alias("revenue"))
        .with_columns((pl.col("revenue") / pl.col("revenue").sum() * 100).alias("share"))
        .sort("share", descending=True)
    )
    _top = _rev_share.head(1)
    _top_name = _top["product_name"][0]
    _top_share = _top["share"][0]

    _sorted = _rev_share.with_columns(
        (pl.col("revenue").cum_sum() / pl.col("revenue").sum() * 100).alias("cum")
    )
    _core_products = _sorted.filter(pl.col("cum") <= 80)["product_name"].to_list()

    mo.callout(
        mo.md(
            f"""
            **Pricing Recommendations:**

            - **{_top_name}** dominates with **{_top_share:.1f}%** of revenue — protect this product's pricing
            - Core 80% revenue products: {', '.join(_core_products)} — focus pricing experiments here
            - Consider time-of-day pricing: premium during peak hours, discounts during low-volume periods
            - Currency: all amounts in **{currency}**
            """
        ),
        kind="info",
    )
    return


# ── Section 3: Daily Price Anomaly Detection ────────────────────────────────
@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ---
    ## 3. Daily Price Anomaly Detection

    Vending machines should charge a **fixed price** per product. Deviations indicate
    configuration errors, firmware bugs, or unauthorized price changes. We detect
    individual transactions where the price differs from the product's most common price.
    """)
    return


@app.cell
def _(loc_df, pl):
    # Expected price = the MODE (most frequent price) per product
    product_expected = (
        loc_df.group_by("product_name", "amount")
        .agg(pl.len().alias("freq"))
        .sort("freq", descending=True)
        .group_by("product_name")
        .first()
        .select("product_name", pl.col("amount").alias("expected_price"))
    )
    return (product_expected,)


@app.cell
def _(alt, coffee_dropdown, currency, loc_df, mo, pl, product_expected):
    _selected = coffee_dropdown.value
    _product_txns = loc_df.filter(pl.col("product_name") == _selected)

    _daily_price = (
        _product_txns.group_by("date")
        .agg(
            pl.col("amount").mean().alias("avg_price"),
            pl.col("amount").min().alias("min_price"),
            pl.col("amount").max().alias("max_price"),
            pl.len().alias("n_txns"),
        )
        .sort("date")
    )

    _exp_row = product_expected.filter(pl.col("product_name") == _selected)
    _exp_price = _exp_row["expected_price"][0] if _exp_row.height > 0 else 0

    _points = (
        alt.Chart(_daily_price)
        .mark_circle(size=50, opacity=0.7)
        .encode(
            x=alt.X("date:T", title="Date"),
            y=alt.Y("avg_price:Q", title=f"Daily Avg Price ({currency})"),
            color=alt.condition(
                (alt.datum.avg_price > _exp_price * 1.01) | (alt.datum.avg_price < _exp_price * 0.99),
                alt.value("#E45756"),
                alt.value("#5276A7"),
            ),
            size=alt.Size("n_txns:Q", title="# Transactions"),
            tooltip=["date:T", alt.Tooltip("avg_price:Q", format=",.2f"),
                      alt.Tooltip("min_price:Q", format=",.2f"),
                      alt.Tooltip("max_price:Q", format=",.2f"), "n_txns"],
        )
    )

    _rule = (
        alt.Chart()
        .mark_rule(color="green", strokeDash=[6, 3], strokeWidth=2)
        .encode(y=alt.datum(_exp_price))
    )

    _chart = (
        alt.layer(_points, _rule)
        .properties(
            title=f"{_selected}: Daily Avg Price vs Expected ({_exp_price:,.2f} {currency})",
            width=700, height=300,
        )
    )
    mo.ui.altair_chart(_chart)
    return


@app.cell
def _(currency, loc_df, mo, pl, product_expected):
    # Flag every transaction where price deviates >1% from expected
    price_anomalies = (
        loc_df.join(product_expected, on="product_name")
        .filter(
            ((pl.col("amount") - pl.col("expected_price")).abs() / pl.col("expected_price")) > 0.01
        )
        .with_columns(
            ((pl.col("amount") - pl.col("expected_price")) / pl.col("expected_price") * 100)
            .round(1)
            .alias("deviation_pct")
        )
        .select("date", "occurred_at", "product_name", "amount", "expected_price", "deviation_pct", "machine_name", "cash_type")
        .sort("deviation_pct")
    )

    _n = price_anomalies.height
    _n_products = price_anomalies["product_name"].n_unique() if _n > 0 else 0

    mo.md(f"### Mispriced Transactions: **{_n}** flagged across **{_n_products}** products (>{currency}1% deviation)")
    mo.ui.table(price_anomalies)
    return (price_anomalies,)


@app.cell(hide_code=True)
def _(currency, loc_df, mo, pl, price_anomalies, product_expected):
    _n_total = loc_df.height
    _n_flagged = price_anomalies.height
    _pct = _n_flagged / _n_total * 100 if _n_total > 0 else 0

    _products_with_issues = price_anomalies["product_name"].unique().to_list() if _n_flagged > 0 else []

    # Show per-product distinct price counts
    _price_variety = (
        loc_df.group_by("product_name")
        .agg(pl.col("amount").n_unique().alias("distinct_prices"))
        .filter(pl.col("distinct_prices") > 1)
        .sort("distinct_prices", descending=True)
    )
    _multi_price_products = _price_variety["product_name"].to_list()

    mo.callout(
        mo.md(
            f"""
            **Price Anomaly Findings:**

            - **{_n_flagged}** of **{_n_total:,}** transactions ({_pct:.1f}%) have non-standard pricing
            - Products with multiple distinct prices: {', '.join(_multi_price_products) if _multi_price_products else 'None'}
            - Products with flagged anomalies: {', '.join(_products_with_issues) if _products_with_issues else 'None — all prices match expected'}
            - Action: audit machine price configurations for flagged products
            """
        ),
        kind="warn" if _n_flagged > 0 else "success",
    )
    return


# ── Section 4: Cash Anomaly Detection ────────────────────────────────────────
@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ---
    ## 4. Cash Anomaly Detection

    Identifying unusual cash transactions for reconciliation and loss prevention.
    """)
    return


@app.cell
def _(currency, loc_df, mo, pl):
    _payment_stats = (
        loc_df.group_by("cash_type")
        .agg(
            pl.len().alias("count"),
            pl.col("amount").mean().round(2).alias("avg_amount"),
            pl.col("amount").median().alias("median_amount"),
            pl.col("amount").std().round(2).alias("std_amount"),
            pl.col("amount").min().alias("min_amount"),
            pl.col("amount").max().alias("max_amount"),
            pl.col("amount").sum().round(2).alias("total_revenue"),
        )
        .sort("cash_type")
    )
    mo.md(f"### Cash vs Card Statistical Comparison ({currency})")
    mo.ui.table(_payment_stats)
    return


@app.cell
def _(alt, currency, loc_df, mo, pl):
    _cash_df = loc_df.filter(pl.col("cash_type") == "cash").select("occurred_at", "amount", "product_name")

    _chart = (
        alt.Chart(_cash_df)
        .mark_circle(size=40, opacity=0.6)
        .encode(
            x=alt.X("occurred_at:T", title="Date/Time"),
            y=alt.Y("amount:Q", title=f"Amount ({currency})"),
            color=alt.Color("product_name:N", title="Product"),
            tooltip=["occurred_at:T", alt.Tooltip("amount:Q", format=",.2f"), "product_name"],
        )
        .properties(title="Cash Transactions Over Time", width=700, height=350)
    )
    mo.ui.altair_chart(_chart)
    return


@app.cell
def _(currency, loc_df, mo, pl):
    _cash = loc_df.filter(pl.col("cash_type") == "cash")
    _q1 = _cash["amount"].quantile(0.25)
    _q3 = _cash["amount"].quantile(0.75)
    _iqr = _q3 - _q1
    _lower = _q1 - 1.5 * _iqr
    _upper = _q3 + 1.5 * _iqr

    cash_outliers = _cash.filter(
        (pl.col("amount") < _lower) | (pl.col("amount") > _upper)
    ).select("date", "occurred_at", "product_name", "amount", "machine_name", "cash_type")

    mo.md(
        f"""
        ### Outlier Detection (IQR Method)

        - **Q1**: {_q1:.2f} | **Q3**: {_q3:.2f} | **IQR**: {_iqr:.2f} {currency}
        - **Lower bound**: {_lower:.2f} | **Upper bound**: {_upper:.2f} {currency}
        - **Flagged transactions**: {cash_outliers.height}
        """
    )
    mo.ui.table(cash_outliers)
    return (cash_outliers,)


@app.cell
def _(alt, loc_df, mo, pl):
    _hourly_payment = (
        loc_df.group_by("hour", "cash_type")
        .agg(pl.len().alias("count"))
        .sort("hour")
    )

    _chart = (
        alt.Chart(_hourly_payment)
        .mark_bar()
        .encode(
            x=alt.X("hour:O", title="Hour of Day"),
            y=alt.Y("count:Q", title="Transaction Count"),
            color=alt.Color("cash_type:N", title="Payment Type"),
            tooltip=["hour", "cash_type", "count"],
        )
        .properties(title="Payment Method Distribution by Hour", width=600, height=350)
    )
    mo.ui.altair_chart(_chart)
    return


@app.cell(hide_code=True)
def _(cash_outliers, loc_df, mo, pl):
    _cash_count = loc_df.filter(pl.col("cash_type") == "cash").height
    _cash_pct = _cash_count / loc_df.height * 100 if loc_df.height > 0 else 0
    _outlier_pct = cash_outliers.height / _cash_count * 100 if _cash_count > 0 else 0

    _off_hours_cash = loc_df.filter(
        (pl.col("cash_type") == "cash") & ((pl.col("hour") < 7) | (pl.col("hour") >= 20))
    ).height
    _off_hours_all = loc_df.filter(
        (pl.col("hour") < 7) | (pl.col("hour") >= 20)
    ).height
    _off_cash_pct = _off_hours_cash / _off_hours_all * 100 if _off_hours_all > 0 else 0

    mo.callout(
        mo.md(
            f"""
            **Cash Anomaly Findings:**

            - Cash makes up **{_cash_pct:.1f}%** of all transactions ({_cash_count:,} of {loc_df.height:,})
            - **{cash_outliers.height}** outlier transactions flagged ({_outlier_pct:.1f}% of cash transactions)
            - Off-hours (before 7am / after 8pm) cash share: **{_off_cash_pct:.1f}%**
            - Recommendation: focus manual audit on flagged outliers and off-hours cash transactions
            """
        ),
        kind="warn",
    )
    return


# ── Section 5: Ingredient Usage & Stockout Detection ────────────────────────
@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ---
    ## 5. Ingredient Usage & Stockout Detection

    Each product has a recipe (defined in `productingredient`). By multiplying transaction
    counts by recipe quantities we estimate **daily ingredient consumption per location**.
    Days where products sharing an ingredient all show unusually low sales — while other
    products sell normally — are flagged as **potential ingredient stockouts**.
    """)
    return


@app.cell
def _(df, pl, recipe_df):
    # Daily ingredient consumption per location (across ALL locations for comparison)
    daily_usage = (
        df.join(
            recipe_df.select("product_id", "ingredient_name", "unit", "quantity"),
            on="product_id",
        )
        .group_by("date", "location_name", "ingredient_name", "unit")
        .agg(pl.col("quantity").sum().alias("total_used"))
        .sort("date")
    )
    return (daily_usage,)


@app.cell
def _(alt, daily_usage, mo):
    _chart = (
        alt.Chart(daily_usage)
        .mark_line(point=False, strokeWidth=1.5)
        .encode(
            x=alt.X("date:T", title="Date"),
            y=alt.Y("total_used:Q", title="Daily Consumption"),
            color=alt.Color("location_name:N", title="Location"),
            tooltip=["date:T", "ingredient_name", "location_name", alt.Tooltip("total_used:Q", format=",.1f"), "unit"],
        )
        .facet(facet=alt.Facet("ingredient_name:N", title="Ingredient", columns=2), columns=2)
        .resolve_scale(y="independent")
        .properties(title="Daily Ingredient Consumption by Location")
    )
    mo.ui.altair_chart(_chart, chart_selection=False, legend_selection=False)
    return


@app.cell
def _(alt, daily_usage, mo, pl):
    # Heatmap of ingredient usage intensity (selected location from dropdown above)
    # Aggregate to weekly for readability
    _weekly = (
        daily_usage.with_columns(
            pl.col("date").dt.truncate("1w").alias("week"),
        )
        .group_by("week", "ingredient_name", "location_name")
        .agg(pl.col("total_used").sum().alias("weekly_used"))
    )

    _chart = (
        alt.Chart(_weekly)
        .mark_rect()
        .encode(
            x=alt.X("week:T", title="Week"),
            y=alt.Y("ingredient_name:N", title="Ingredient"),
            color=alt.Color("weekly_used:Q", scale=alt.Scale(scheme="orangered"), title="Weekly Usage"),
            tooltip=["week:T", "ingredient_name", "location_name", alt.Tooltip("weekly_used:Q", format=",.1f")],
        )
        .facet(row=alt.Row("location_name:N", title="Location"))
        .resolve_scale(color="independent")
        .properties(title="Weekly Ingredient Usage Heatmap")
    )
    mo.ui.altair_chart(_chart, chart_selection=False, legend_selection=False)
    return


@app.cell
def _(loc_df, pl, recipe_df):
    # Stockout detection for the selected location
    # Step 1: Daily product sales, including zero-sale days
    _all_dates = loc_df["date"].unique().sort()
    _all_products = loc_df["product_name"].unique().sort()
    _grid = _all_dates.to_frame().join(_all_products.to_frame(), how="cross")

    _daily_product = (
        loc_df.group_by("date", "product_name")
        .agg(pl.len().alias("sales"))
    )
    _daily_full = (
        _grid.join(_daily_product, on=["date", "product_name"], how="left")
        .with_columns(pl.col("sales").fill_null(0))
    )

    # Step 2: Baseline (median daily sales per product)
    _baseline = (
        _daily_full.group_by("product_name")
        .agg(pl.col("sales").median().alias("baseline"))
    )

    # Step 3: Daily ratio = actual / baseline
    _daily_ratio = (
        _daily_full.join(_baseline, on="product_name")
        .with_columns(
            pl.when(pl.col("baseline") > 0)
            .then(pl.col("sales") / pl.col("baseline"))
            .otherwise(None)
            .alias("ratio")
        )
    )

    # Step 4: For each ingredient, for each day, compute:
    #   - avg ratio of products USING this ingredient
    #   - avg ratio of products NOT using this ingredient
    _product_ingredients = recipe_df.select("product_name", "ingredient_name").unique()
    _all_ingredient_names = _product_ingredients["ingredient_name"].unique().to_list()

    _results = []
    for _ingredient in _all_ingredient_names:
        _using = _product_ingredients.filter(
            pl.col("ingredient_name") == _ingredient
        )["product_name"].to_list()

        _using_ratios = (
            _daily_ratio.filter(pl.col("product_name").is_in(_using))
            .group_by("date")
            .agg(pl.col("ratio").mean().alias("using_ratio"))
        )
        _not_using_ratios = (
            _daily_ratio.filter(~pl.col("product_name").is_in(_using))
            .group_by("date")
            .agg(pl.col("ratio").mean().alias("other_ratio"))
        )

        _merged = (
            _using_ratios.join(_not_using_ratios, on="date", how="inner")
            .with_columns(pl.lit(_ingredient).alias("ingredient_name"))
        )
        _results.append(_merged)

    stockout_analysis = pl.concat(_results)

    # Flag: products using ingredient dropped <30% of normal, others held >60%
    stockout_events = (
        stockout_analysis.filter(
            (pl.col("using_ratio") < 0.3) & (pl.col("other_ratio") > 0.6)
        )
        .sort("date")
        .with_columns(
            (pl.col("using_ratio") * 100).round(1).alias("using_pct_of_normal"),
            (pl.col("other_ratio") * 100).round(1).alias("other_pct_of_normal"),
        )
    )
    return stockout_analysis, stockout_events


@app.cell
def _(alt, mo, pl, stockout_analysis):
    # Visualize: for each ingredient, show the contrast between using-products and other-products
    _melted = pl.concat([
        stockout_analysis.select("date", "ingredient_name", pl.col("using_ratio").alias("ratio"))
            .with_columns(pl.lit("Products using ingredient").alias("group")),
        stockout_analysis.select("date", "ingredient_name", pl.col("other_ratio").alias("ratio"))
            .with_columns(pl.lit("Other products").alias("group")),
    ])

    _chart = (
        alt.Chart(_melted)
        .mark_line(strokeWidth=1.5)
        .encode(
            x=alt.X("date:T", title="Date"),
            y=alt.Y("ratio:Q", title="Sales Ratio (1.0 = normal)"),
            color=alt.Color("group:N", title="Product Group"),
            tooltip=["date:T", "ingredient_name", "group", alt.Tooltip("ratio:Q", format=".2f")],
        )
        .properties(width=300, height=180)
        .facet(facet=alt.Facet("ingredient_name:N", title="Ingredient", columns=2), columns=2)
        .resolve_scale(y="shared")
    )
    mo.ui.altair_chart(_chart, chart_selection=False, legend_selection=False)
    return


@app.cell
def _(location_dropdown, mo, recipe_df, stockout_events):
    _n = stockout_events.height
    mo.md(f"### Potential Ingredient Stockout Events — {location_dropdown.value}: **{_n}** flagged")

    if _n > 0:
        # Enrich with affected products
        _product_ingredients = recipe_df.select("product_name", "ingredient_name").unique()
        _enriched = stockout_events.join(
            _product_ingredients.group_by("ingredient_name").agg(
                pl.col("product_name").sort().str.concat(", ").alias("affected_products")
            ),
            on="ingredient_name",
        ).select(
            "date", "ingredient_name", "using_pct_of_normal", "other_pct_of_normal", "affected_products"
        )
        mo.ui.table(_enriched)
    else:
        mo.callout(mo.md("No strong stockout signals detected."), kind="success")
    return


@app.cell(hide_code=True)
def _(daily_usage, location_dropdown, mo, pl, stockout_events):
    _loc = location_dropdown.value
    _n_events = stockout_events.height

    # Which ingredients are most used?
    _loc_usage = daily_usage.filter(pl.col("location_name") == _loc)
    _top_ingredients = (
        _loc_usage.group_by("ingredient_name", "unit")
        .agg(pl.col("total_used").sum().alias("total"))
        .sort("total", descending=True)
    )
    _top3 = [f"{r['ingredient_name']} ({r['total']:,.0f} {r['unit']})" for r in _top_ingredients.head(3).iter_rows(named=True)]

    _flagged_ingredients = stockout_events["ingredient_name"].unique().to_list() if _n_events > 0 else []

    mo.callout(
        mo.md(
            f"""
            **Ingredient Analysis Findings — {_loc}:**

            - **Top consumed ingredients**: {', '.join(_top3)}
            - **Stockout signals detected**: {_n_events} events across ingredients: {', '.join(_flagged_ingredients) if _flagged_ingredients else 'none'}
            - Detection method: days where products using an ingredient dropped to <30% of normal sales
              while other products maintained >60% — ruling out "slow day" false positives
            - Action: cross-reference flagged dates with physical restock logs to confirm stockouts
            """
        ),
        kind="warn" if _n_events > 0 else "success",
    )
    return


# ── Section 6: Automated Reporting Foundation ────────────────────────────────
@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ---
    ## 6. Automated Reporting Foundation

    Interactive date-filtered dashboard that can be deployed as a self-serve web app via `marimo run`.
    """)
    return


@app.cell
def _(loc_df, mo):
    _min_date = loc_df["date"].min()
    _max_date = loc_df["date"].max()

    date_range = mo.ui.date_range(
        start=_min_date,
        stop=_max_date,
        value=(_min_date, _max_date),
        label="Filter by date range",
    )
    date_range
    return (date_range,)


@app.cell(hide_code=True)
def _(currency, date_range, loc_df, mo, pl):
    import datetime as _dt

    _start = date_range.value[0]
    _stop = date_range.value[1]
    if isinstance(_start, _dt.datetime):
        _start = _start.date()
    if isinstance(_stop, _dt.datetime):
        _stop = _stop.date()

    filtered_df = loc_df.filter(
        (pl.col("date") >= _start) & (pl.col("date") <= _stop)
    )

    _n = filtered_df.height
    _rev = filtered_df["amount"].sum() if _n > 0 else 0
    _avg = _rev / _n if _n > 0 else 0
    _top_product = (
        filtered_df.group_by("product_name")
        .agg(pl.len().alias("vol"))
        .sort("vol", descending=True)
        .head(1)["product_name"][0]
        if _n > 0
        else "N/A"
    )

    mo.md(
        f"""
        ### Filtered Report: {_start} to {_stop}

        | Metric | Value |
        |--------|-------|
        | Transactions | **{_n:,}** |
        | Revenue | **{_rev:,.2f} {currency}** |
        | Avg ticket | **{_avg:.2f} {currency}** |
        | Top product | **{_top_product}** |
        """
    )
    return (filtered_df,)


@app.cell
def _(alt, currency, filtered_df, mo, pl):
    _daily_rev = (
        filtered_df.group_by("date")
        .agg(pl.col("amount").sum().alias("daily_revenue"))
        .sort("date")
    )

    _chart = (
        alt.Chart(_daily_rev)
        .mark_area(opacity=0.6, color="#5276A7", line=True)
        .encode(
            x=alt.X("date:T", title="Date"),
            y=alt.Y("daily_revenue:Q", title=f"Daily Revenue ({currency})"),
            tooltip=["date:T", alt.Tooltip("daily_revenue:Q", format=",.2f")],
        )
        .properties(title="Daily Revenue (Filtered)", width=700, height=300)
    )
    mo.ui.altair_chart(_chart)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.callout(
        mo.md(
            """
            **Deployment:** Run `marimo run coffee_analysis.py` to serve this notebook
            as an interactive web app for self-serve access by the team.
            Location selector, date pickers, and dropdowns remain fully reactive in deployed mode.
            """
        ),
        kind="info",
    )
    return


# ── Section 7: Theory Testing ────────────────────────────────────────────────
@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ---
    ## 7. Theory Testing

    Quantitative validation of five business hypotheses.
    """)
    return


@app.cell(hide_code=True)
def _(loc_df, mo, pl):
    _morning = loc_df.filter((pl.col("hour") >= 8) & (pl.col("hour") < 12)).height
    _share = _morning / loc_df.height * 100 if loc_df.height > 0 else 0
    _verdict = "CONFIRMED" if _share > 50 else "REJECTED"
    _kind = "success" if _verdict == "CONFIRMED" else "danger"

    mo.callout(
        mo.md(
            f"""
            **H1: >50% of sales occur between 8 AM–12 PM**

            - Morning transactions (8–12): **{_morning:,}** of **{loc_df.height:,}**
            - Morning share: **{_share:.1f}%**
            - Verdict: **{_verdict}**
            - {"Implication: Morning is dominant — focus staffing and stocking on this window." if _verdict == "CONFIRMED" else "Implication: Sales are more distributed than expected — spread resources across the day."}
            """
        ),
        kind=_kind,
    )
    return


@app.cell(hide_code=True)
def _(loc_df, mo, pl):
    _total_rev = loc_df["amount"].sum()
    _latte_rev = loc_df.filter(pl.col("product_name").str.contains("(?i)latte"))["amount"].sum()
    _latte_share = _latte_rev / _total_rev * 100 if _total_rev > 0 else 0
    _verdict = "CONFIRMED" if _latte_share > 30 else "REJECTED"
    _kind = "success" if _verdict == "CONFIRMED" else "danger"

    mo.callout(
        mo.md(
            f"""
            **H2: Latte accounts for >30% of total revenue**

            - Latte revenue: **{_latte_rev:,.2f}** of **{_total_rev:,.2f}**
            - Latte share: **{_latte_share:.1f}%**
            - Verdict: **{_verdict}**
            - {"Implication: Latte is a revenue pillar — protect availability and pricing." if _verdict == "CONFIRMED" else "Implication: Revenue is spread across products — diversify promotional efforts."}
            """
        ),
        kind=_kind,
    )
    return


@app.cell(hide_code=True)
def _(loc_df, mo, pl):
    _off = loc_df.filter((pl.col("hour") < 7) | (pl.col("hour") >= 20))
    _on = loc_df.filter((pl.col("hour") >= 7) & (pl.col("hour") < 20))

    _off_cash_pct = (
        _off.filter(pl.col("cash_type") == "cash").height / _off.height * 100
        if _off.height > 0 else 0
    )
    _on_cash_pct = (
        _on.filter(pl.col("cash_type") == "cash").height / _on.height * 100
        if _on.height > 0 else 0
    )
    _diff = _off_cash_pct - _on_cash_pct
    _verdict = "CONFIRMED" if _diff > 5 else "REJECTED"
    _kind = "success" if _verdict == "CONFIRMED" else "danger"

    mo.callout(
        mo.md(
            f"""
            **H3: Cash clusters at off-hours vs card**

            - Off-hours cash %: **{_off_cash_pct:.1f}%** (n={_off.height})
            - Business-hours cash %: **{_on_cash_pct:.1f}%** (n={_on.height})
            - Difference: **{_diff:+.1f} pp**
            - Verdict: **{_verdict}** (threshold: >5 pp difference)
            - {"Implication: Off-hours cash concentration warrants closer scrutiny for reconciliation." if _verdict == "CONFIRMED" else "Implication: Cash usage is relatively consistent across hours — no off-hours anomaly."}
            """
        ),
        kind=_kind,
    )
    return


@app.cell(hide_code=True)
def _(loc_df, mo, pl):
    _weekday_rev = (
        loc_df.filter(pl.col("weekday") <= 5)
        .group_by("date")
        .agg(pl.col("amount").sum().alias("daily_rev"))["daily_rev"]
        .mean()
    )
    _weekend_rev = (
        loc_df.filter(pl.col("weekday") > 5)
        .group_by("date")
        .agg(pl.col("amount").sum().alias("daily_rev"))["daily_rev"]
        .mean()
    )

    _drop_pct = (1 - _weekend_rev / _weekday_rev) * 100 if _weekday_rev and _weekday_rev > 0 else 0
    _verdict = "CONFIRMED" if _drop_pct >= 20 else "REJECTED"
    _kind = "success" if _verdict == "CONFIRMED" else "danger"

    mo.callout(
        mo.md(
            f"""
            **H4: Weekend daily revenue ≥20% below weekday**

            - Avg weekday daily revenue: **{_weekday_rev:,.2f}**
            - Avg weekend daily revenue: **{_weekend_rev:,.2f}**
            - Drop: **{_drop_pct:.1f}%**
            - Verdict: **{_verdict}** (threshold: ≥20%)
            - {"Implication: Weekend demand significantly lower — consider reduced stocking or promotional pricing." if _verdict == "CONFIRMED" else "Implication: Weekend holds up reasonably well — maintain standard operations."}
            """
        ),
        kind=_kind,
    )
    return


@app.cell(hide_code=True)
def _(loc_df, mo, pl):
    _card_df = loc_df.filter(
        (pl.col("cash_type") == "card") & pl.col("card_token").is_not_null()
    )

    if _card_df.height > 0:
        _card_rev = (
            _card_df.group_by("card_token")
            .agg(pl.col("amount").sum().alias("revenue"))
            .sort("revenue", descending=True)
        )
        _total_card_rev = _card_rev["revenue"].sum()
        _n_customers = _card_rev.height
        _top_10_n = max(1, int(_n_customers * 0.10))
        _top_10_rev = _card_rev.head(_top_10_n)["revenue"].sum()
        _top_10_share = _top_10_rev / _total_card_rev * 100

        _verdict = "CONFIRMED" if _top_10_share > 40 else "REJECTED"
        _kind = "success" if _verdict == "CONFIRMED" else "danger"

        mo.callout(
            mo.md(
                f"""
                **H5: Top 10% card holders account for >40% of card revenue**

                - Unique card customers: **{_n_customers:,}**
                - Top 10% ({_top_10_n} customers) revenue: **{_top_10_rev:,.2f}** of **{_total_card_rev:,.2f}**
                - Top 10% share: **{_top_10_share:.1f}%**
                - Verdict: **{_verdict}**
                - {"Implication: High customer concentration — consider loyalty programs for top spenders." if _verdict == "CONFIRMED" else "Implication: Revenue is distributed across card users — broad engagement strategy preferred."}
                """
            ),
            kind=_kind,
        )
    else:
        mo.callout(
            mo.md("**H5:** Cannot test — no card token data available for this location."),
            kind="neutral",
        )
    return


# ── Conclusions ──────────────────────────────────────────────────────────────
@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ---
    ## Conclusions

    ### Key Takeaways

    **1. Inventory Management**
    - Clear product popularity hierarchy enables targeted stocking
    - Peak-hour demand is concentrated — restock *before* rush windows, not during
    - Low-demand products can be reduced to free machine capacity

    **2. Pricing Strategy**
    - Revenue follows an 80/20 pattern — a few products drive most revenue
    - Price anomaly detection catches misconfigured machine prices automatically
    - Time-of-day pricing could capture willingness to pay during peak hours

    **3. Cash Reconciliation**
    - Statistical outlier detection flags suspicious transactions automatically
    - Hourly payment method patterns provide a baseline for anomaly detection
    - Focus audit resources on flagged transactions rather than blanket reviews

    **4. Ingredient Stockouts**
    - Daily ingredient consumption tracking reveals supply pressure points
    - Contrast-based detection (using-products vs others) distinguishes true stockouts from slow days
    - Cross-reference flagged dates with restock logs for confirmation and prevention

    **5. Reporting Automation**
    - This notebook serves as a self-updating dashboard when deployed via `marimo run`
    - Location selector + date filters replace manual per-site report generation
    - Reactive UI elements let stakeholders self-serve their own analyses
    """)
    return


if __name__ == "__main__":
    app.run()
