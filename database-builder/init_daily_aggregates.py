from __future__ import annotations

from loguru import logger
from sqlalchemy import text

from db import FACTS_DB_FILE, create_observed_db, observed_engine


def rebuild_daily_aggregates() -> None:
    """
    Rebuild day-level observed tables/views in `artifacts/vending_sales_observed.db`.

    - `daily_product_sales` is a view (recreated by `create_observed_db()`).
    - `daily_ingredient_consumption` is a derived table rebuilt from:
      observed transactions + facts recipes (product_ingredient) + ingredient units.
    """
    create_observed_db()
    # SQLite requires ATTACH/DETACH outside an explicit transaction.
    with observed_engine.connect().execution_options(
        isolation_level="AUTOCOMMIT"
    ) as conn:
        conn.execute(
            text("ATTACH DATABASE :facts AS facts"),
            {"facts": str(FACTS_DB_FILE)},
        )
        conn.execute(text('DELETE FROM "daily_ingredient_consumption"'))
        conn.execute(
            text(
                """
                INSERT INTO "daily_ingredient_consumption" (
                    date,
                    machine_id,
                    ingredient_id,
                    total_quantity,
                    unit
                )
                SELECT
                    t.date AS date,
                    t.machine_id AS machine_id,
                    pi.ingredient_id AS ingredient_id,
                    SUM(pi.quantity) AS total_quantity,
                    ing.unit AS unit
                FROM "transaction" AS t
                JOIN facts."product_ingredient" AS pi
                    ON pi.product_id = t.product_id
                JOIN facts."ingredient" AS ing
                    ON ing.id = pi.ingredient_id
                GROUP BY
                    t.date,
                    t.machine_id,
                    pi.ingredient_id
                """
            )
        )
        conn.execute(text("DETACH DATABASE facts"))
    logger.info(
        "Rebuilt observed daily aggregates (view: daily_product_sales, table: daily_ingredient_consumption)."
    )


if __name__ == "__main__":
    rebuild_daily_aggregates()
