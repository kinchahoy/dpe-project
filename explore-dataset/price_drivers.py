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
    # Price Driver Investigation

    **Approach:** Prices change seasonally (a prevailing price holds for weeks/months,
    then shifts). We first detect these **price seasons**, then check whether hour
    or weekday explains any remaining variation *within* a season.
    """)
    return


@app.cell
def _(pl, sqlite3):
    _conn = sqlite3.connect("coffee.db")
    df = pl.read_database(
        """
        SELECT
            t.id as txn_id, t.date, t.occurred_at,
            t.cash_type, t.amount, t.currency,
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
    )
    _conn.close()
    return (df,)


@app.cell
def _(df, mo):
    _locations = sorted(df["location_name"].unique().to_list())
    location_dropdown = mo.ui.dropdown(options=_locations, value=_locations[0], label="Location")
    location_dropdown
    return (location_dropdown,)


@app.cell
def _(df, location_dropdown, pl):
    # Card transactions only — cash has its own rounding dynamics
    loc_df = df.filter(
        (pl.col("location_name") == location_dropdown.value)
        & (pl.col("cash_type") == "card")
    )
    currency = loc_df["currency"][0] if loc_df.height > 0 else "?"

    top_product = (
        loc_df.group_by("product_name")
        .agg(pl.len().alias("n"))
        .sort("n", descending=True)
        .head(1)["product_name"][0]
    )
    product_df = loc_df.filter(pl.col("product_name") == top_product)
    return currency, product_df, top_product


# ── Step 1: Detect price seasons ─────────────────────────────────────────────
@app.cell(hide_code=True)
def _(currency, mo, product_df, top_product):
    _n = product_df.height
    _prices = product_df["amount"].unique().sort().to_list()
    mo.md(f"""
    ---
    ## Step 1: Detect price seasons for **{top_product}**

    {_n:,} card transactions, distinct prices: **{', '.join(f'{p:.2f}' for p in _prices)}** {currency}
    """)
    return


@app.cell
def _(alt, currency, mo, product_df):
    _chart = (
        alt.Chart(product_df)
        .mark_circle(size=12, opacity=0.4)
        .encode(
            x=alt.X("occurred_at:T", title="Date"),
            y=alt.Y("amount:Q", title=f"Price ({currency})", scale=alt.Scale(zero=False)),
            tooltip=["occurred_at:T", alt.Tooltip("amount:Q", format=",.2f"), "machine_name"],
        )
        .properties(title="All card transaction prices over time", width=700, height=250)
    )
    mo.ui.altair_chart(_chart)
    return


@app.cell
def _(pl, product_df):
    # Daily modal price, then group consecutive days with same modal into eras
    _daily = (
        product_df.group_by("date")
        .agg(pl.col("amount").mode().first().alias("modal_price"), pl.len().alias("n"))
        .sort("date")
        .to_dicts()
    )

    _eras = []
    _cur = {"price": _daily[0]["modal_price"], "start": _daily[0]["date"], "end": _daily[0]["date"], "days": 1}
    for r in _daily[1:]:
        if r["modal_price"] == _cur["price"]:
            _cur["end"] = r["date"]
            _cur["days"] += 1
        else:
            _eras.append(_cur)
            _cur = {"price": r["modal_price"], "start": r["date"], "end": r["date"], "days": 1}
    _eras.append(_cur)

    # Merge short blips (<5 days) into their neighbors
    MIN_SEASON_DAYS = 5
    seasons = []
    for era in _eras:
        if era["days"] >= MIN_SEASON_DAYS:
            seasons.append(era.copy())
        elif seasons:
            # Blip — extend previous season's end date through it
            seasons[-1]["end"] = era["end"]
            seasons[-1]["days"] += era["days"]

    seasons_df = pl.DataFrame(seasons).rename({"price": "season_price", "start": "start_date", "end": "end_date", "days": "n_days"})
    return MIN_SEASON_DAYS, seasons, seasons_df


@app.cell
def _(alt, currency, mo, seasons_df):
    mo.md(f"### Price seasons ({seasons_df.height} detected)")

    _chart = (
        alt.Chart(seasons_df)
        .mark_bar()
        .encode(
            x=alt.X("start_date:T", title="Date"),
            x2="end_date:T",
            y=alt.value(20),
            color=alt.Color("season_price:N", title=f"Price ({currency})"),
            tooltip=[
                alt.Tooltip("season_price:Q", format=",.2f", title="Price"),
                "start_date:T",
                "end_date:T",
                "n_days",
            ],
        )
        .properties(title="Price season timeline", width=700, height=80)
    )
    mo.ui.altair_chart(_chart)
    mo.ui.table(seasons_df)
    return


# ── Step 2: Within each season, check hour/weekday variation ─────────────────
@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ---
    ## Step 2: Within each price season, does hour or weekday matter?

    If hour/weekday drive price, we'd see different prices clustering at specific
    times *within* a single season. If not, all off-price transactions are just noise.
    """)
    return


@app.cell
def _(mo, seasons_df):
    _options = {
        f"{r['season_price']:.2f} ({r['start_date']} to {r['end_date']}, {r['n_days']}d)": i
        for i, r in enumerate(seasons_df.iter_rows(named=True))
    }
    season_picker = mo.ui.dropdown(
        options=_options,
        value=list(_options.keys())[0],
        label="Select season",
    )
    season_picker
    return (season_picker,)


@app.cell
def _(currency, mo, pl, product_df, season_picker, seasons_df):
    _row = seasons_df.row(season_picker.value, named=True)
    _start = _row["start_date"]
    _end = _row["end_date"]
    season_price = _row["season_price"]

    season_df = product_df.filter(
        (pl.col("date") >= _start) & (pl.col("date") <= _end)
    )
    _n = season_df.height
    _n_match = season_df.filter(pl.col("amount") == season_price).height
    _n_off = _n - _n_match
    _off_pct = _n_off / _n * 100 if _n > 0 else 0

    mo.md(f"""
    **Season:** {season_price:.2f} {currency} &nbsp;|&nbsp; {_start} to {_end}
    &nbsp;|&nbsp; {_n:,} transactions &nbsp;|&nbsp; **{_n_off}** off-price ({_off_pct:.1f}%)
    """)
    return season_df, season_price


@app.cell
def _(alt, currency, mo, pl, season_df, season_price):
    # Price by hour within this season
    _hourly = (
        season_df.group_by("hour", "amount")
        .agg(pl.len().alias("count"))
        .sort("hour", "amount")
    )
    _chart = (
        alt.Chart(_hourly)
        .mark_bar()
        .encode(
            x=alt.X("hour:O", title="Hour of Day"),
            y=alt.Y("count:Q", title="# Transactions"),
            color=alt.Color("amount:N", title=f"Price ({currency})"),
            tooltip=["hour", alt.Tooltip("amount:N", title="Price"), "count"],
        )
        .properties(title=f"Within season ({season_price:.2f}): price by hour", width=600, height=250)
    )
    mo.ui.altair_chart(_chart)
    return


@app.cell
def _(alt, currency, mo, pl, season_df, season_price):
    # Price by weekday within this season
    _dow_labels = {1: "Mon", 2: "Tue", 3: "Wed", 4: "Thu", 5: "Fri", 6: "Sat", 7: "Sun"}
    _dow = (
        season_df.group_by("weekday", "amount")
        .agg(pl.len().alias("count"))
        .with_columns(pl.col("weekday").replace_strict(_dow_labels, return_dtype=pl.Utf8).alias("day_name"))
        .sort("weekday", "amount")
    )
    _chart = (
        alt.Chart(_dow)
        .mark_bar()
        .encode(
            x=alt.X("day_name:N", sort=list(_dow_labels.values()), title="Day of Week"),
            y=alt.Y("count:Q", title="# Transactions"),
            color=alt.Color("amount:N", title=f"Price ({currency})"),
            tooltip=["day_name", alt.Tooltip("amount:N", title="Price"), "count"],
        )
        .properties(title=f"Within season ({season_price:.2f}): price by weekday", width=500, height=250)
    )
    mo.ui.altair_chart(_chart)
    return


# ── Step 3: Root-cause off-price transactions ────────────────────────────────
@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ---
    ## Step 3: Root-cause off-price transactions
    """)
    return


@app.cell
def _(currency, mo, pl, season_df, season_price, top_product):
    _off = season_df.filter(pl.col("amount") != season_price)

    if _off.height == 0:
        mo.callout(
            mo.md(f"Every transaction in this season is at **{season_price:.2f} {currency}** — no anomalies."),
            kind="success",
        )
    else:
        _by_price = _off.group_by("amount").agg(pl.len().alias("count")).sort("count", descending=True)
        _by_machine = _off.group_by("machine_name").agg(pl.len().alias("count")).sort("count", descending=True)
        _by_date = _off.group_by("date").agg(pl.len().alias("count")).sort("date")

        _price_str = ", ".join(f"{r['amount']:.2f} ({r['count']}x)" for r in _by_price.iter_rows(named=True))
        _machine_str = ", ".join(f"{r['machine_name']} ({r['count']}x)" for r in _by_machine.iter_rows(named=True))

        # Check if anomalies cluster on specific dates (suggesting a brief price test)
        _date_list = _by_date["date"].to_list()
        _consecutive = len(_date_list) > 1 and all(
            (_date_list[i] - _date_list[i - 1]).days <= 2 for i in range(1, len(_date_list))
        )

        mo.callout(
            mo.md(f"""
            **{_off.height}** off-price transactions for {top_product} in this season:

            - Prices: {_price_str} {currency}
            - Machines: {_machine_str}
            - Dates: {_date_list[0]} to {_date_list[-1]} ({len(_date_list)} days)
            - {'Dates are consecutive — looks like a brief price change or rollout' if _consecutive else 'Dates are scattered — looks like random glitches'}
            """),
            kind="warn",
        )
        mo.ui.table(
            _off.select("date", "occurred_at", "amount", "machine_name", "hour", "weekday")
            .sort("occurred_at")
        )
    return


if __name__ == "__main__":
    app.run()
