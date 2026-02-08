from __future__ import annotations

from loguru import logger

from db import create_db


def rebuild_daily_aggregates() -> None:
    # Daily aggregates are now schema views created by `db.create_db()`.
    create_db()
    logger.info(
        "Daily aggregate views ensured (daily_product_sales, daily_ingredient_consumption)."
    )


if __name__ == "__main__":
    rebuild_daily_aggregates()
