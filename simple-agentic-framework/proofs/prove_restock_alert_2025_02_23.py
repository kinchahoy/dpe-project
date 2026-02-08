from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from loguru import logger

from simple_agent_framework.sandbox_context import build_sandbox_context
from simple_agent_framework.scripts import restock_predictor

ALERT_ID = "9794514b-8e48-4be8-bd0b-9adc7e920dd7"
DEFAULT_DAY = date(2025, 2, 23)


@dataclass(frozen=True)
class AlertRow:
    alert_id: str
    run_date: date
    script_name: str
    script_version: str
    status: str
    location_id: int
    machine_id: int
    ingredient_id: int
    title: str
    summary: str
    evidence: dict[str, Any]
    recommended_actions: list[dict[str, Any]]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Proof that restock alert 9794514b-8e48-4be8-bd0b-9adc7e920dd7 is correct"
        )
    )
    parser.add_argument("--db", default="coffee.db", help="Path to SQLite DB")
    parser.add_argument("--alert-id", default=ALERT_ID)
    parser.add_argument(
        "--default-day",
        default=DEFAULT_DAY.isoformat(),
        help="Expected day for this proof",
    )
    return parser.parse_args()


def setup_logging(log_path: Path) -> None:
    logger.remove()
    logger.add(sys.stdout, format="{message}")
    logger.add(
        log_path,
        mode="w",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
    )


def load_alert(conn: sqlite3.Connection, alert_id: str) -> AlertRow:
    row = conn.execute(
        """
        SELECT alert_id, run_date, script_name, script_version, status,
               location_id, machine_id, ingredient_id, title, summary,
               evidence_json, recommended_actions_json
        FROM alert
        WHERE alert_id = ?
        """,
        (alert_id,),
    ).fetchone()
    if row is None:
        raise ValueError(f"Alert not found: {alert_id}")

    return AlertRow(
        alert_id=str(row["alert_id"]),
        run_date=date.fromisoformat(str(row["run_date"])),
        script_name=str(row["script_name"]),
        script_version=str(row["script_version"]),
        status=str(row["status"]),
        location_id=int(row["location_id"]),
        machine_id=int(row["machine_id"]),
        ingredient_id=int(row["ingredient_id"]),
        title=str(row["title"]),
        summary=str(row["summary"]),
        evidence=json.loads(str(row["evidence_json"])),
        recommended_actions=json.loads(str(row["recommended_actions_json"])),
    )


def resolve_currency(conn: sqlite3.Connection, location_id: int) -> str:
    row = conn.execute(
        """
        SELECT COALESCE((
            SELECT t.currency
            FROM transactions t
            WHERE t.location_id = ?
            ORDER BY t.date DESC
            LIMIT 1
        ), 'USD') AS currency
        """,
        (location_id,),
    ).fetchone()
    return str(row["currency"] if row else "USD")


def summarize_3d_forecast(
    rows: list[dict[str, Any]], as_of: date
) -> tuple[list[tuple[int, float]], list[tuple[str, int, float]]]:
    by_ingredient: dict[int, float] = defaultdict(float)
    by_day_ingredient: dict[tuple[str, int], float] = defaultdict(float)
    cutoff = as_of + timedelta(days=3)

    for row in rows:
        forecast_day = date.fromisoformat(str(row["forecast_date"]))
        if forecast_day <= cutoff:
            ingredient_id = int(row["ingredient_id"])
            qty = float(row["forecast_quantity"])
            by_ingredient[ingredient_id] += qty
            by_day_ingredient[(forecast_day.isoformat(), ingredient_id)] += qty

    totals = sorted(by_ingredient.items(), key=lambda item: item[1], reverse=True)
    day_rows = sorted(
        (
            (day, ingredient_id, qty)
            for (day, ingredient_id), qty in by_day_ingredient.items()
        ),
        key=lambda item: (item[0], item[1]),
    )
    return totals, day_rows


def build_expected_payload(
    *,
    run_date: date,
    location_id: int,
    machine_id: int,
    top_ingredient_id: int,
    top_qty: float,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    evidence = {
        "top_ingredient_id": top_ingredient_id,
        "projected_3d_qty": round(top_qty, 2),
    }
    actions = [
        {
            "action_type": "RESTOCK_MACHINE",
            "params": {
                "machine_id": machine_id,
                "restock_level_by_ingredient": {
                    str(top_ingredient_id): round(top_qty * 1.25, 2)
                },
                "deadline": (run_date + timedelta(days=1)).isoformat(),
            },
        },
        {
            "action_type": "ORDER_INGREDIENTS",
            "params": {
                "location_id": location_id,
                "ingredient_id": top_ingredient_id,
                "quantity": round(top_qty * 1.35, 2),
                "deadline": (run_date + timedelta(days=2)).isoformat(),
            },
        },
    ]
    return evidence, actions


def main() -> int:
    args = parse_args()
    db_path = Path(args.db).resolve()
    expected_day = date.fromisoformat(args.default_day)

    log_path = (
        Path(__file__).resolve().parent / "logs" / "prove_restock_alert_2025_02_23.log"
    )
    setup_logging(log_path)

    logger.info("PROOF START")
    logger.info("Step 1/8: Open DB and load alert {}", args.alert_id)

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        alert = load_alert(conn, args.alert_id)
        logger.info(
            "Loaded alert: run_date={}, script={} v{}, status={}, loc={}, machine={}, ingredient={}",
            alert.run_date.isoformat(),
            alert.script_name,
            alert.script_version,
            alert.status,
            alert.location_id,
            alert.machine_id,
            alert.ingredient_id,
        )

        logger.info("Step 2/8: Validate alert identity and default day")
        assert alert.script_name == "restock_predictor", alert.script_name
        assert alert.script_version == "1.0", alert.script_version
        assert alert.status == "OPEN", alert.status
        assert alert.run_date == expected_day, (
            f"Expected run_date {expected_day}, got {alert.run_date}"
        )
        logger.info("Identity checks passed")

        logger.info("Step 3/8: Rebuild sandbox context for same day/location/machine")
        currency = resolve_currency(conn, alert.location_id)
        logger.info("Resolved currency from engine query: {}", currency)
        payload = build_sandbox_context(
            db_path=db_path,
            as_of_date=alert.run_date,
            location_id=alert.location_id,
            machine_id=alert.machine_id,
            currency=currency,
        )
        ingredient_forecasts = payload["frames"]["ingredient_forecasts"]
        logger.info(
            "Bundle contains {} ingredient forecast rows (window: >{} to <=+10d)",
            len(ingredient_forecasts),
            alert.run_date.isoformat(),
        )

        logger.info("Step 4/8: Recompute the script's 3-day ingredient aggregation")
        totals, by_day = summarize_3d_forecast(ingredient_forecasts, alert.run_date)
        assert totals, "No 3-day ingredient forecast rows found"
        top_ingredient_id, top_qty = totals[0]
        logger.info(
            "Top ingredient by 3-day qty: {} -> {:.6f}", top_ingredient_id, top_qty
        )
        logger.info("Top 5 ingredients by 3-day qty:")
        for ingredient_id, qty in totals[:5]:
            logger.info("  ingredient={} qty_3d={:.6f}", ingredient_id, qty)
        logger.info("3-day per-day totals for top ingredient {}:", top_ingredient_id)
        for day, ingredient_id, qty in by_day:
            if ingredient_id == top_ingredient_id:
                logger.info(
                    "  day={} ingredient={} qty={:.6f}", day, ingredient_id, qty
                )

        logger.info("Step 5/8: Apply restock_predictor formulas")
        assert top_qty >= 180.0, f"Threshold check failed: top_qty={top_qty}"
        expected_evidence, expected_actions = build_expected_payload(
            run_date=alert.run_date,
            location_id=alert.location_id,
            machine_id=alert.machine_id,
            top_ingredient_id=top_ingredient_id,
            top_qty=top_qty,
        )
        logger.info(
            "Expected evidence: top_ingredient_id={}, projected_3d_qty={}",
            expected_evidence["top_ingredient_id"],
            expected_evidence["projected_3d_qty"],
        )
        logger.info(
            "Expected restock level (x1.25): {}",
            expected_actions[0]["params"]["restock_level_by_ingredient"],
        )
        logger.info(
            "Expected order quantity (x1.35): {}",
            expected_actions[1]["params"]["quantity"],
        )

        logger.info("Step 6/8: Execute script analyze(ctx) with rebuilt payload")
        emitted = restock_predictor.analyze(payload)
        assert len(emitted) == 1, f"Expected one emitted alert, got {len(emitted)}"
        script_alert = emitted[0]
        logger.info("Script emitted one alert: {}", script_alert["title"])

        logger.info("Step 7/8: Compare computed payload to stored alert fields")
        assert script_alert["alert_type"] == "restock_risk"
        assert script_alert["severity"] == "HIGH"
        assert script_alert["title"] == alert.title
        assert script_alert["summary"] == alert.summary
        assert int(script_alert["location_id"]) == alert.location_id
        assert int(script_alert["machine_id"]) == alert.machine_id
        assert int(script_alert["ingredient_id"]) == alert.ingredient_id
        assert script_alert["evidence"] == expected_evidence
        assert script_alert["recommended_actions"] == expected_actions
        logger.info("Script output matches formula-based expectations")

        logger.info("Step 8/8: Compare stored DB payload to expected payload")
        assert alert.evidence == expected_evidence
        assert alert.recommended_actions == expected_actions
        logger.info("Stored DB evidence/actions exactly match recomputed values")

        logger.info("PROOF RESULT: PASS")
        logger.info("Log file written to: {}", log_path)
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
