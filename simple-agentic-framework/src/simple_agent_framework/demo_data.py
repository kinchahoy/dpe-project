from __future__ import annotations

import sqlite3
from datetime import date, datetime, time, timedelta
from random import Random

from loguru import logger

from .db import fetch_all


def inject_synthetic_week(
    *,
    db_path,
    start_day: date,
    location_id: int = 2,
    machine_ids: tuple[int, int] = (3, 4),
) -> int:
    """Insert deterministic synthetic data into SF for demo reproducibility.

    Idempotent:
    - If `demo_synth` transactions already exist and have matching `transaction_expanded` rows, no-op.
    - If transactions exist but expanded rows are missing (older partial run), backfill expanded rows.
    """

    end_day = start_day + timedelta(days=6)

    existing = fetch_all(
        db_path,
        """
        SELECT COUNT(*) AS n FROM "transaction"
        WHERE source_file = 'demo_synth' AND date BETWEEN ? AND ?
        """,
        (start_day.isoformat(), end_day.isoformat()),
    )
    existing_tx = int(existing[0]["n"]) if existing else 0

    existing_expanded = fetch_all(
        db_path,
        """
        SELECT COUNT(*) AS n FROM transaction_expanded
        WHERE source_file = 'demo_synth' AND date BETWEEN ? AND ?
        """,
        (start_day.isoformat(), end_day.isoformat()),
    )
    existing_expanded_n = int(existing_expanded[0]["n"]) if existing_expanded else 0

    if existing_tx > 0 and existing_expanded_n == existing_tx:
        return 0

    products = fetch_all(
        db_path,
        """
        SELECT DISTINCT product_id
        FROM transactions
        WHERE location_id = ?
        ORDER BY product_id
        LIMIT 6
        """,
        (location_id,),
    )
    product_ids = [int(row["product_id"]) for row in products] or [1]

    rng = Random(42)
    inserted = 0
    source_row = 1

    def base_price(product_id: int) -> float:
        return 3.0 + (product_id % 5) * 0.25

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        if existing_tx > 0 and existing_expanded_n < existing_tx:
            rows = conn.execute(
                """
                SELECT t.id, t.product_id, t.location_id, t.machine_id, t.date, t.occurred_at,
                       t.cash_type, t.card_token, t.amount, t.currency, t.source_file, t.source_row
                FROM "transaction" t
                LEFT JOIN transaction_expanded te ON te.transaction_id = t.id
                WHERE t.source_file = 'demo_synth'
                  AND t.date BETWEEN ? AND ?
                  AND te.transaction_id IS NULL
                ORDER BY t.id
                """,
                (start_day.isoformat(), end_day.isoformat()),
            ).fetchall()
            for row in rows:
                conn.execute(
                    """
                    INSERT INTO transaction_expanded(
                        transaction_id, product_id, location_id, machine_id, date, occurred_at,
                        cash_type, card_token, amount, expected_price, product_group, currency,
                        source_file, source_row
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        row[0],
                        row[1],
                        row[2],
                        row[3],
                        row[4],
                        row[5],
                        row[6],
                        row[7],
                        row[8],
                        row[8],
                        "synthetic",
                        row[9],
                        row[10],
                        row[11],
                    ),
                )
                inserted += 1
            conn.commit()
            logger.info(
                "Backfilled {} transaction_expanded rows for demo_synth", inserted
            )
            return inserted

        for day_offset in range(7):
            day = start_day + timedelta(days=day_offset)
            for machine_id in machine_ids:
                for hour in (7, 8, 9, 12, 13, 15, 17):
                    repeats = 8
                    if day_offset == 1 and machine_id == machine_ids[0]:
                        repeats = 22
                    if day_offset == 6 and machine_id == machine_ids[1]:
                        repeats = 1

                    for _ in range(repeats):
                        product_id = product_ids[
                            (day_offset + machine_id + hour) % len(product_ids)
                        ]
                        expected = base_price(product_id)
                        amount = expected + (rng.random() - 0.5) * 0.2
                        cash_type = "card"
                        card_token = f"tok_{rng.randint(100, 999)}"

                        if day_offset == 2 and machine_id == machine_ids[0]:
                            cash_type = "cash"
                            card_token = None
                        if day_offset == 4 and machine_id == machine_ids[1]:
                            amount = expected * 0.55

                        occurred_at = datetime.combine(
                            day, time(hour=hour, minute=rng.randint(0, 59))
                        )
                        cur = conn.execute(
                            """
                            INSERT INTO "transaction"(
                                product_id, location_id, machine_id, date, occurred_at,
                                cash_type, card_token, amount, currency, source_file, source_row
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'USD', 'demo_synth', ?)
                            """,
                            (
                                product_id,
                                location_id,
                                machine_id,
                                day.isoformat(),
                                occurred_at.isoformat(sep=" "),
                                cash_type,
                                card_token,
                                amount,
                                source_row,
                            ),
                        )
                        tx_id = int(cur.lastrowid)
                        conn.execute(
                            """
                            INSERT INTO transaction_expanded(
                                transaction_id, product_id, location_id, machine_id, date, occurred_at,
                                cash_type, card_token, amount, expected_price, product_group, currency,
                                source_file, source_row
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'USD', 'demo_synth', ?)
                            """,
                            (
                                tx_id,
                                product_id,
                                location_id,
                                machine_id,
                                day.isoformat(),
                                occurred_at.isoformat(sep=" "),
                                cash_type,
                                card_token,
                                amount,
                                expected,
                                "synthetic",
                                source_row,
                            ),
                        )
                        source_row += 1
                        inserted += 1
        conn.commit()
    finally:
        conn.close()

    logger.info("Inserted {} synthetic demo transactions", inserted)
    return inserted
