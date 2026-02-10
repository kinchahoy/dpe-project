import marimo

__generated_with = "0.19.9"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    import altair as alt
    import kagglehub
    from kagglehub import KaggleDatasetAdapter

    return KaggleDatasetAdapter, alt, kagglehub, mo, pl


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    # Coffee Vending Machine Sales Analysis

    **Business context:** Leadership wants to use transaction data more effectively to address four pain points:

    1. **Product stockouts** during peak hours
    2. **Arbitrary pricing** without data backing
    3. **Cash reconciliation** issues
    4. **Manual reporting** overhead

    This notebook analyzes ~3,600+ vending machine transactions across **two machines** at a
    **Ukraine location**, from the
    [Kaggle Coffee Sales dataset](https://www.kaggle.com/datasets/ihelon/coffee-sales).
    Machine 1 (`index_1.csv`) includes card-holder IDs; Machine 2 (`index_2.csv`) does not.
    """)
    return


@app.cell
def _(KaggleDatasetAdapter, kagglehub, pl):
    # Load both CSV files — Machine 1 and Machine 2 at the Ukraine location
    # index_1.csv (Machine 1) has a 'card' column; index_2.csv (Machine 2) does not
    _df1 = (
        kagglehub.dataset_load(
            KaggleDatasetAdapter.POLARS, "ihelon/coffee-sales", "index_1.csv"
        )
        .collect()
        .with_columns(pl.lit("Machine 1").alias("machine"))
    )
    _df2 = (
        kagglehub.dataset_load(
            KaggleDatasetAdapter.POLARS, "ihelon/coffee-sales", "index_2.csv"
        )
        .collect()
        .with_columns(pl.lit("Machine 2").alias("machine"))
    )

    # Add missing 'card' column to df2 so schemas match, then concat
    _df2 = _df2.with_columns(pl.lit(None).cast(pl.Utf8).alias("card")).select(
        _df1.columns
    )
    raw = pl.concat([_df1, _df2])

    # Normalize coffee names (e.g. "Americano with Milk" vs "Americano with milk")
    raw = raw.with_columns(pl.col("coffee_name").str.to_titlecase())

    # Parse datetime (two formats: with/without milliseconds) and derive time columns
    df = raw.with_columns(
        pl.col("datetime")
        .str.to_datetime("%Y-%m-%d %H:%M:%S%.f", strict=False)
        .alias("dt"),
        pl.col("date").str.to_date("%Y-%m-%d").alias("date_parsed"),
    ).with_columns(
        pl.col("dt").dt.hour().alias("hour"),
        pl.col("dt").dt.weekday().alias("weekday"),  # 1=Mon, 7=Sun
        pl.col("dt").dt.month().alias("month"),
    )
    return (df,)


@app.cell(hide_code=True)
def _(df, mo, pl):
    _total_txns = df.height
    _date_min = df["date_parsed"].min()
    _date_max = df["date_parsed"].max()
    _total_revenue = df["money"].sum()
    _card_pct = df.filter(pl.col("cash_type") == "card").height / _total_txns * 100
    _avg_ticket = _total_revenue / _total_txns
    _m1_count = df.filter(pl.col("machine") == "Machine 1").height
    _m2_count = df.filter(pl.col("machine") == "Machine 2").height

    mo.md(
        f"""
        ## Dataset Overview — Ukraine Location

        | Metric | Value |
        |--------|-------|
        | Total transactions | **{_total_txns:,}** |
        | Machine 1 transactions | **{_m1_count:,}** |
        | Machine 2 transactions | **{_m2_count:,}** |
        | Date range | **{_date_min}** to **{_date_max}** |
        | Total revenue | **${_total_revenue:,.2f}** |
        | Avg ticket | **${_avg_ticket:.2f}** |
        | Card payment % | **{_card_pct:.1f}%** |
        """
    )
    return


@app.cell
def _(df, mo):
    mo.ui.table(df.head(100))
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ---
    ## 1. Inventory & Stockout Analysis

    Understanding product popularity and peak-hour demand to prevent stockouts.
    """)
    return


@app.cell
def _(alt, df, mo, pl):
    _product_stats = (
        df.group_by("coffee_name")
        .agg(
            pl.len().alias("volume"),
            pl.col("money").sum().alias("revenue"),
        )
        .sort("volume", descending=True)
    )

    _chart = (
        alt.Chart(_product_stats)
        .mark_bar()
        .encode(
            x=alt.X("coffee_name:N", sort="-y", title="Coffee Type"),
            y=alt.Y("volume:Q", title="Transaction Count"),
            color=alt.Color(
                "revenue:Q", scale=alt.Scale(scheme="goldorange"), title="Revenue ($)"
            ),
            tooltip=["coffee_name", "volume", alt.Tooltip("revenue:Q", format="$.2f")],
        )
        .properties(
            title="Product Popularity (Volume & Revenue)", width=600, height=350
        )
    )
    mo.ui.altair_chart(_chart)
    return


@app.cell
def _(alt, df, mo, pl):
    _heatmap_data = df.group_by("coffee_name", "hour").agg(pl.len().alias("count"))

    _heatmap = (
        alt.Chart(_heatmap_data)
        .mark_rect()
        .encode(
            x=alt.X("hour:O", title="Hour of Day"),
            y=alt.Y("coffee_name:N", title="Coffee Type"),
            color=alt.Color(
                "count:Q", scale=alt.Scale(scheme="blues"), title="Transactions"
            ),
            tooltip=["coffee_name", "hour", "count"],
        )
        .properties(
            title="Demand Heatmap: Product x Hour (Stockout Risk Windows)",
            width=600,
            height=350,
        )
    )
    mo.ui.altair_chart(_heatmap)
    return


@app.cell
def _(alt, df, mo, pl):
    _dow_labels = {1: "Mon", 2: "Tue", 3: "Wed", 4: "Thu", 5: "Fri", 6: "Sat", 7: "Sun"}
    _dow_data = (
        df.group_by("weekday")
        .agg(pl.len().alias("volume"))
        .with_columns(
            pl.col("weekday")
            .replace_strict(_dow_labels, return_dtype=pl.Utf8)
            .alias("day_name")
        )
        .sort("weekday")
    )

    _chart = (
        alt.Chart(_dow_data)
        .mark_bar()
        .encode(
            x=alt.X("day_name:N", sort=list(_dow_labels.values()), title="Day of Week"),
            y=alt.Y("volume:Q", title="Transaction Count"),
            color=alt.Color(
                "volume:Q", scale=alt.Scale(scheme="tealblues"), legend=None
            ),
            tooltip=["day_name", "volume"],
        )
        .properties(title="Transaction Volume by Day of Week", width=500, height=300)
    )
    mo.ui.altair_chart(_chart)
    return


@app.cell(hide_code=True)
def _(df, mo, pl):
    _prod = (
        df.group_by("coffee_name")
        .agg(pl.len().alias("volume"), pl.col("money").sum().alias("revenue"))
        .sort("volume", descending=True)
    )
    _top3 = _prod.head(3)["coffee_name"].to_list()
    _bottom3 = _prod.tail(3)["coffee_name"].to_list()

    _peak_hours = (
        df.group_by("hour")
        .agg(pl.len().alias("vol"))
        .sort("vol", descending=True)
        .head(3)["hour"]
        .to_list()
    )

    mo.callout(
        mo.md(
            f"""
            **Inventory Recommendations:**

            - **Top sellers** (keep fully stocked): {", ".join(_top3)}
            - **Low movers** (reduce inventory): {", ".join(_bottom3)}
            - **Peak restock hours**: {", ".join(str(h) + ":00" for h in sorted(_peak_hours))}
              — ensure supplies are replenished *before* these windows
            """
        ),
        kind="success",
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ---
    ## 2. Pricing Optimization

    Exploring price-volume relationships and revenue concentration to identify pricing opportunities.
    """)
    return


@app.cell
def _(df, mo):
    coffee_names = sorted(df["coffee_name"].unique().to_list())
    coffee_dropdown = mo.ui.dropdown(
        options=coffee_names,
        value=coffee_names[0],
        label="Select coffee type",
    )
    coffee_dropdown
    return (coffee_dropdown,)


@app.cell
def _(alt, coffee_dropdown, df, mo, pl):
    _selected = coffee_dropdown.value
    _hourly = (
        df.filter(pl.col("coffee_name") == _selected)
        .group_by("hour")
        .agg(
            pl.col("money").mean().alias("avg_price"),
            pl.len().alias("volume"),
        )
        .sort("hour")
    )

    _base = alt.Chart(_hourly).encode(x=alt.X("hour:O", title="Hour of Day"))

    _bars = _base.mark_bar(opacity=0.5, color="#5276A7").encode(
        y=alt.Y("volume:Q", title="Volume"),
        tooltip=["hour", "volume", alt.Tooltip("avg_price:Q", format="$.2f")],
    )

    _line = _base.mark_line(color="#F18727", strokeWidth=3, point=True).encode(
        y=alt.Y("avg_price:Q", title="Avg Price ($)"),
        tooltip=["hour", alt.Tooltip("avg_price:Q", format="$.2f")],
    )

    _combined = (
        alt.layer(_bars, _line)
        .resolve_scale(y="independent")
        .properties(
            title=f"{_selected}: Avg Price vs Volume by Hour", width=600, height=350
        )
    )
    mo.ui.altair_chart(_combined)
    return


@app.cell
def _(alt, df, mo, pl):
    _pareto = (
        df.group_by("coffee_name")
        .agg(pl.col("money").sum().alias("revenue"))
        .sort("revenue", descending=True)
        .with_columns(
            (pl.col("revenue").cum_sum() / pl.col("revenue").sum() * 100).alias(
                "cumulative_pct"
            )
        )
    )

    _bars = (
        alt.Chart(_pareto)
        .mark_bar()
        .encode(
            x=alt.X("coffee_name:N", sort="-y", title="Coffee Type"),
            y=alt.Y("revenue:Q", title="Revenue ($)"),
            color=alt.condition(
                alt.datum.cumulative_pct <= 80,
                alt.value("#5276A7"),
                alt.value("#CCCCCC"),
            ),
            tooltip=[
                "coffee_name",
                alt.Tooltip("revenue:Q", format="$.2f"),
                alt.Tooltip("cumulative_pct:Q", format=".1f"),
            ],
        )
    )

    _line = (
        alt.Chart(_pareto)
        .mark_line(color="#F18727", strokeWidth=2, point=True)
        .encode(
            x=alt.X("coffee_name:N", sort="-y"),
            y=alt.Y(
                "cumulative_pct:Q",
                title="Cumulative %",
                scale=alt.Scale(domain=[0, 100]),
            ),
            tooltip=["coffee_name", alt.Tooltip("cumulative_pct:Q", format=".1f")],
        )
    )

    _rule = (
        alt.Chart().mark_rule(color="red", strokeDash=[4, 4]).encode(y=alt.datum(80))
    )

    _chart = (
        alt.layer(_bars, _line, _rule)
        .resolve_scale(y="independent")
        .properties(title="Revenue Pareto Analysis (80/20)", width=600, height=350)
    )
    mo.ui.altair_chart(_chart)
    return


@app.cell(hide_code=True)
def _(df, mo, pl):
    _rev_share = (
        df.group_by("coffee_name")
        .agg(pl.col("money").sum().alias("revenue"))
        .with_columns(
            (pl.col("revenue") / pl.col("revenue").sum() * 100).alias("share")
        )
        .sort("share", descending=True)
    )
    _top = _rev_share.head(1)
    _top_name = _top["coffee_name"][0]
    _top_share = _top["share"][0]

    # Products in the 80% bucket
    _sorted = _rev_share.with_columns(
        (pl.col("revenue").cum_sum() / pl.col("revenue").sum() * 100).alias("cum")
    )
    _core_products = _sorted.filter(pl.col("cum") <= 80)["coffee_name"].to_list()

    mo.callout(
        mo.md(
            f"""
            **Pricing Recommendations:**

            - **{_top_name}** dominates with **{_top_share:.1f}%** of revenue — protect this product's pricing
            - Core 80% revenue products: {", ".join(_core_products)} — focus pricing experiments here
            - Consider time-of-day pricing: premium during peak hours, discounts during low-volume periods
            """
        ),
        kind="info",
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ---
    ## 3. Cash Anomaly Detection

    Identifying unusual cash transactions for reconciliation and loss prevention.
    """)
    return


@app.cell
def _(df, mo, pl):
    _payment_stats = (
        df.group_by("cash_type")
        .agg(
            pl.len().alias("count"),
            pl.col("money").mean().alias("avg_amount"),
            pl.col("money").median().alias("median_amount"),
            pl.col("money").std().alias("std_amount"),
            pl.col("money").min().alias("min_amount"),
            pl.col("money").max().alias("max_amount"),
            pl.col("money").sum().alias("total_revenue"),
        )
        .sort("cash_type")
    )
    mo.md("### Cash vs Card Statistical Comparison")
    mo.ui.table(_payment_stats)
    return


@app.cell
def _(alt, df, mo, pl):
    _cash_df = df.filter(pl.col("cash_type") == "cash").select(
        "dt", "money", "coffee_name"
    )

    _chart = (
        alt.Chart(_cash_df)
        .mark_circle(size=40, opacity=0.6)
        .encode(
            x=alt.X("dt:T", title="Date/Time"),
            y=alt.Y("money:Q", title="Amount ($)"),
            color=alt.Color("coffee_name:N", title="Coffee Type"),
            tooltip=["dt:T", alt.Tooltip("money:Q", format="$.2f"), "coffee_name"],
        )
        .properties(title="Cash Transactions Over Time", width=700, height=350)
    )
    mo.ui.altair_chart(_chart)
    return


@app.cell
def _(df, mo, pl):
    _cash = df.filter(pl.col("cash_type") == "cash")
    _q1 = _cash["money"].quantile(0.25)
    _q3 = _cash["money"].quantile(0.75)
    _iqr = _q3 - _q1
    _lower = _q1 - 1.5 * _iqr
    _upper = _q3 + 1.5 * _iqr

    outliers = _cash.filter(
        (pl.col("money") < _lower) | (pl.col("money") > _upper)
    ).select("datetime", "coffee_name", "money", "cash_type")

    mo.md(
        f"""
        ### Outlier Detection (IQR Method)

        - **Q1**: ${_q1:.2f} | **Q3**: ${_q3:.2f} | **IQR**: ${_iqr:.2f}
        - **Lower bound**: ${_lower:.2f} | **Upper bound**: ${_upper:.2f}
        - **Flagged transactions**: {outliers.height}
        """
    )
    mo.ui.table(outliers)
    return (outliers,)


@app.cell
def _(alt, df, mo, pl):
    _hourly_payment = (
        df.group_by("hour", "cash_type").agg(pl.len().alias("count")).sort("hour")
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
def _(df, mo, outliers, pl):
    _cash_count = df.filter(pl.col("cash_type") == "cash").height
    _card_count = df.filter(pl.col("cash_type") == "card").height
    _cash_pct = _cash_count / df.height * 100
    _outlier_pct = outliers.height / _cash_count * 100 if _cash_count > 0 else 0

    # Off-hours cash concentration (before 7am or after 8pm)
    _off_hours_cash = df.filter(
        (pl.col("cash_type") == "cash")
        & ((pl.col("hour") < 7) | (pl.col("hour") >= 20))
    ).height
    _off_hours_all = df.filter((pl.col("hour") < 7) | (pl.col("hour") >= 20)).height
    _off_cash_pct = _off_hours_cash / _off_hours_all * 100 if _off_hours_all > 0 else 0

    mo.callout(
        mo.md(
            f"""
            **Cash Anomaly Findings:**

            - Cash makes up **{_cash_pct:.1f}%** of all transactions ({_cash_count:,} of {df.height:,})
            - **{outliers.height}** outlier transactions flagged ({_outlier_pct:.1f}% of cash transactions)
            - Off-hours (before 7am / after 8pm) cash share: **{_off_cash_pct:.1f}%**
            - Recommendation: focus manual audit on flagged outliers and off-hours cash transactions
            """
        ),
        kind="warn",
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ---
    ## 4. Automated Reporting Foundation

    Interactive date-filtered dashboard that can be deployed as a self-serve web app via `marimo run`.
    """)
    return


@app.cell
def _(df, mo):
    _min_date = df["date_parsed"].min()
    _max_date = df["date_parsed"].max()

    date_range = mo.ui.date_range(
        start=_min_date,
        stop=_max_date,
        value=(_min_date, _max_date),
        label="Filter by date range",
    )
    date_range
    return (date_range,)


@app.cell(hide_code=True)
def _(date_range, df, mo, pl):
    import datetime as _dt

    _start = date_range.value[0]
    _stop = date_range.value[1]
    # Convert to date if they are datetime
    if isinstance(_start, _dt.datetime):
        _start = _start.date()
    if isinstance(_stop, _dt.datetime):
        _stop = _stop.date()

    filtered_df = df.filter(
        (pl.col("date_parsed") >= _start) & (pl.col("date_parsed") <= _stop)
    )

    _n = filtered_df.height
    _rev = filtered_df["money"].sum() if _n > 0 else 0
    _avg = _rev / _n if _n > 0 else 0
    _top_product = (
        filtered_df.group_by("coffee_name")
        .agg(pl.len().alias("vol"))
        .sort("vol", descending=True)
        .head(1)["coffee_name"][0]
        if _n > 0
        else "N/A"
    )

    mo.md(
        f"""
        ### Filtered Report: {_start} to {_stop}

        | Metric | Value |
        |--------|-------|
        | Transactions | **{_n:,}** |
        | Revenue | **${_rev:,.2f}** |
        | Avg ticket | **${_avg:.2f}** |
        | Top product | **{_top_product}** |
        """
    )
    return (filtered_df,)


@app.cell
def _(alt, filtered_df, mo, pl):
    _daily_rev = (
        filtered_df.group_by("date_parsed")
        .agg(pl.col("money").sum().alias("daily_revenue"))
        .sort("date_parsed")
    )

    _chart = (
        alt.Chart(_daily_rev)
        .mark_area(opacity=0.6, color="#5276A7", line=True)
        .encode(
            x=alt.X("date_parsed:T", title="Date"),
            y=alt.Y("daily_revenue:Q", title="Daily Revenue ($)"),
            tooltip=["date_parsed:T", alt.Tooltip("daily_revenue:Q", format="$.2f")],
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
            Date pickers and dropdowns remain fully reactive in deployed mode.
            """
        ),
        kind="info",
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ---
    ## 5. Theory Testing

    Quantitative validation of five business hypotheses.
    """)
    return


@app.cell(hide_code=True)
def _(df, mo, pl):
    _morning = df.filter((pl.col("hour") >= 8) & (pl.col("hour") < 12)).height
    _share = _morning / df.height * 100
    _verdict = "CONFIRMED" if _share > 50 else "REJECTED"
    _kind = "success" if _verdict == "CONFIRMED" else "danger"

    mo.callout(
        mo.md(
            f"""
            **H1: >50% of sales occur between 8 AM–12 PM**

            - Morning transactions (8–12): **{_morning:,}** of **{df.height:,}**
            - Morning share: **{_share:.1f}%**
            - Verdict: **{_verdict}**
            - {"Implication: Morning is dominant — focus staffing and stocking on this window." if _verdict == "CONFIRMED" else "Implication: Sales are more distributed than expected — spread resources across the day."}
            """
        ),
        kind=_kind,
    )
    return


@app.cell(hide_code=True)
def _(df, mo, pl):
    _total_rev = df["money"].sum()
    _latte_rev = df.filter(pl.col("coffee_name").str.contains("(?i)latte"))[
        "money"
    ].sum()
    _latte_share = _latte_rev / _total_rev * 100
    _verdict = "CONFIRMED" if _latte_share > 30 else "REJECTED"
    _kind = "success" if _verdict == "CONFIRMED" else "danger"

    mo.callout(
        mo.md(
            f"""
            **H2: Latte accounts for >30% of total revenue**

            - Latte revenue: **${_latte_rev:,.2f}** of **${_total_rev:,.2f}**
            - Latte share: **{_latte_share:.1f}%**
            - Verdict: **{_verdict}**
            - {"Implication: Latte is a revenue pillar — protect availability and pricing." if _verdict == "CONFIRMED" else "Implication: Revenue is spread across products — diversify promotional efforts."}
            """
        ),
        kind=_kind,
    )
    return


@app.cell(hide_code=True)
def _(df, mo, pl):
    # Off-hours: before 7am or after 8pm
    _off = df.filter((pl.col("hour") < 7) | (pl.col("hour") >= 20))
    _on = df.filter((pl.col("hour") >= 7) & (pl.col("hour") < 20))

    _off_cash_pct = (
        _off.filter(pl.col("cash_type") == "cash").height / _off.height * 100
        if _off.height > 0
        else 0
    )
    _on_cash_pct = (
        _on.filter(pl.col("cash_type") == "cash").height / _on.height * 100
        if _on.height > 0
        else 0
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
def _(df, mo, pl):
    # weekday: 1=Mon..5=Fri weekdays, 6=Sat 7=Sun weekend
    _weekday_rev = (
        df.filter(pl.col("weekday") <= 5)
        .group_by("date_parsed")
        .agg(pl.col("money").sum().alias("daily_rev"))["daily_rev"]
        .mean()
    )
    _weekend_rev = (
        df.filter(pl.col("weekday") > 5)
        .group_by("date_parsed")
        .agg(pl.col("money").sum().alias("daily_rev"))["daily_rev"]
        .mean()
    )

    _drop_pct = (1 - _weekend_rev / _weekday_rev) * 100 if _weekday_rev > 0 else 0
    _verdict = "CONFIRMED" if _drop_pct >= 20 else "REJECTED"
    _kind = "success" if _verdict == "CONFIRMED" else "danger"

    mo.callout(
        mo.md(
            f"""
            **H4: Weekend daily revenue ≥20% below weekday**

            - Avg weekday daily revenue: **${_weekday_rev:,.2f}**
            - Avg weekend daily revenue: **${_weekend_rev:,.2f}**
            - Drop: **{_drop_pct:.1f}%**
            - Verdict: **{_verdict}** (threshold: ≥20%)
            - {"Implication: Weekend demand significantly lower — consider reduced stocking or promotional pricing." if _verdict == "CONFIRMED" else "Implication: Weekend holds up reasonably well — maintain standard operations."}
            """
        ),
        kind=_kind,
    )
    return


@app.cell(hide_code=True)
def _(df, mo, pl):
    _card_df = df.filter((pl.col("cash_type") == "card") & pl.col("card").is_not_null())

    if "card" in df.columns and _card_df.height > 0:
        _card_rev = (
            _card_df.group_by("card")
            .agg(pl.col("money").sum().alias("revenue"))
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
                - Top 10% ({_top_10_n} customers) revenue: **${_top_10_rev:,.2f}** of **${_total_card_rev:,.2f}**
                - Top 10% share: **{_top_10_share:.1f}%**
                - Verdict: **{_verdict}**
                - {"Implication: High customer concentration — consider loyalty programs for top spenders." if _verdict == "CONFIRMED" else "Implication: Revenue is distributed across card users — broad engagement strategy preferred."}
                """
            ),
            kind=_kind,
        )
    else:
        mo.callout(
            mo.md(
                """
                **H5: Top 10% card holders account for >40% of card revenue**

                Cannot test — no card identifier column available in dataset.
                """
            ),
            kind="neutral",
        )
    return


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
    - Time-of-day pricing could capture willingness to pay during peak hours
    - Price sensitivity varies by product — experiment on mid-tier items first

    **3. Cash Reconciliation**
    - Statistical outlier detection flags suspicious transactions automatically
    - Hourly payment method patterns provide a baseline for anomaly detection
    - Focus audit resources on flagged transactions rather than blanket reviews

    **4. Reporting Automation**
    - This notebook serves as a self-updating dashboard when deployed via `marimo run`
    - Date-filtered views replace manual report generation
    - Reactive UI elements let stakeholders self-serve their own analyses
    """)
    return


if __name__ == "__main__":
    app.run()
