"""Quick spot-checks: coffee.db vs index_1.csv + index_2.csv consistency."""

import sqlite3
import polars as pl
from loguru import logger

DB = "coffee.db"
CSV_DIR = None  # resolved via kagglehub below


def load_csvs() -> pl.DataFrame:
    import kagglehub
    from kagglehub import KaggleDatasetAdapter

    df1 = (
        kagglehub.dataset_load(
            KaggleDatasetAdapter.POLARS, "ihelon/coffee-sales", "index_1.csv"
        )
        .collect()
        .with_columns(pl.lit("index_1.csv").alias("source_file"))
    )
    df2 = (
        kagglehub.dataset_load(
            KaggleDatasetAdapter.POLARS, "ihelon/coffee-sales", "index_2.csv"
        )
        .collect()
        .with_columns(
            pl.lit(None).cast(pl.Utf8).alias("card"),
            pl.lit("index_2.csv").alias("source_file"),
        )
    )
    df2 = df2.select(df1.columns)
    return pl.concat([df1, df2])


def load_db_transactions() -> pl.DataFrame:
    conn = sqlite3.connect(DB)
    rows = conn.execute(
        """
        SELECT t.id, t.product_id, p.name as coffee_name,
               t.location_id, t.machine_id, t.date, t.occurred_at,
               t.cash_type, t.card_token, t.amount, t.currency,
               t.source_file, t.source_row
        FROM "transaction" t
        JOIN product p ON p.id = t.product_id
        """
    ).fetchall()
    cols = [
        "id",
        "product_id",
        "coffee_name",
        "location_id",
        "machine_id",
        "date",
        "occurred_at",
        "cash_type",
        "card_token",
        "amount",
        "currency",
        "source_file",
        "source_row",
    ]
    conn.close()
    return pl.DataFrame(dict(zip(cols, zip(*rows))), orient="col")


def check(label: str, ok: bool, detail: str = ""):
    if ok:
        logger.success(f"PASS: {label}" + (f" — {detail}" if detail else ""))
    else:
        logger.error(f"FAIL: {label}" + (f" — {detail}" if detail else ""))


def main():
    logger.info("Loading CSVs via kagglehub...")
    csv_df = load_csvs()
    logger.info(
        f"CSV rows: {csv_df.height} (index_1: {csv_df.filter(pl.col('source_file') == 'index_1.csv').height}, index_2: {csv_df.filter(pl.col('source_file') == 'index_2.csv').height})"
    )

    logger.info("Loading coffee.db transactions...")
    db_df = load_db_transactions()
    logger.info(f"DB rows: {db_df.height}")

    # --- Check 1: Row counts ---
    # DB has dual-currency rows (UAH + USD) per CSV row, so filter to UAH for Ukraine
    db_ua = db_df.filter(pl.col("location_id") == 1)
    logger.info(f"DB Ukraine (location_id=1) rows: {db_ua.height}")
    check(
        "Ukraine DB row count matches CSV total",
        db_ua.height == csv_df.height,
        f"DB Ukraine={db_ua.height}, CSV={csv_df.height}",
    )

    # --- Check 2: Source file mapping ---
    db_src_counts = (
        db_ua.group_by("source_file").agg(pl.len().alias("n")).sort("source_file")
    )
    csv_src_counts = (
        csv_df.group_by("source_file").agg(pl.len().alias("n")).sort("source_file")
    )
    logger.info(f"DB source_file counts:\n{db_src_counts}")
    logger.info(f"CSV source_file counts:\n{csv_src_counts}")
    check(
        "Source file row counts match",
        db_src_counts["n"].to_list() == csv_src_counts["n"].to_list(),
    )

    # --- Check 3: Machine assignment ---
    # index_1 → machine 1, index_2 → machine 2
    m1_files = db_ua.filter(pl.col("machine_id") == 1)["source_file"].unique().to_list()
    m2_files = db_ua.filter(pl.col("machine_id") == 2)["source_file"].unique().to_list()
    check("Machine 1 → index_1.csv", m1_files == ["index_1.csv"], f"got {m1_files}")
    check("Machine 2 → index_2.csv", m2_files == ["index_2.csv"], f"got {m2_files}")

    # --- Check 4: Product names match ---
    csv_products = set(csv_df["coffee_name"].unique().to_list())
    db_products = set(db_ua["coffee_name"].unique().to_list())
    missing_in_db = csv_products - db_products
    extra_in_db = db_products - csv_products
    check(
        "All CSV product names exist in DB",
        len(missing_in_db) == 0,
        f"missing: {missing_in_db}" if missing_in_db else "all present",
    )
    if extra_in_db:
        logger.warning(f"Extra products in DB not in CSV: {extra_in_db}")

    # --- Check 5: Spot-check amounts (first 5 rows of each CSV) ---
    csv_idx1 = csv_df.filter(pl.col("source_file") == "index_1.csv").sort("datetime")
    db_idx1 = db_ua.filter(pl.col("source_file") == "index_1.csv").sort("source_row")
    mismatches = 0
    n_check = min(10, csv_idx1.height, db_idx1.height)
    for i in range(n_check):
        csv_amt = csv_idx1["money"][i]
        db_amt = db_idx1["amount"][i]
        csv_name = csv_idx1["coffee_name"][i]
        db_name = db_idx1["coffee_name"][i]
        csv_cash = csv_idx1["cash_type"][i]
        db_cash = db_idx1["cash_type"][i]
        ok = csv_amt == db_amt and csv_cash == db_cash
        if not ok:
            mismatches += 1
            logger.warning(
                f"Row {i}: CSV({csv_name}, {csv_amt}, {csv_cash}) vs DB({db_name}, {db_amt}, {db_cash})"
            )
    check(
        f"Spot-check first {n_check} index_1 rows: amounts & cash_type match",
        mismatches == 0,
        f"{mismatches} mismatches" if mismatches else "all match",
    )

    # --- Check 6: Date range consistency ---
    csv_dates = (csv_df["date"].min(), csv_df["date"].max())
    db_dates = (db_ua["date"].min(), db_ua["date"].max())
    check(
        "Date range matches",
        csv_dates == db_dates,
        f"CSV={csv_dates}, DB={db_dates}",
    )

    # --- Check 7: Cash type values ---
    csv_cash_types = sorted(csv_df["cash_type"].unique().to_list())
    db_cash_types = sorted(db_ua["cash_type"].unique().to_list())
    check(
        "Cash type values match",
        csv_cash_types == db_cash_types,
        f"CSV={csv_cash_types}, DB={db_cash_types}",
    )

    # --- Check 8: Total revenue matches ---
    csv_total = csv_df["money"].sum()
    db_total = db_ua["amount"].sum()
    check(
        "Total revenue matches (UAH)",
        abs(csv_total - db_total) < 0.01,
        f"CSV={csv_total:.2f}, DB={db_total:.2f}, diff={abs(csv_total - db_total):.2f}",
    )

    logger.info("Done.")


if __name__ == "__main__":
    main()
