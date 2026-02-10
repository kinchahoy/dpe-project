from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

from loguru import logger
from sqlmodel import Session, select

from simple_agent_framework.alert_payload import AlertPayload
from simple_agent_framework.engine import DailyAlertEngine
from simple_agent_framework.models import AlertSuppression
from simple_agent_framework.types import Severity


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Proof that 'suppress for X days' hides future alerts of the same "
            "alert_type for the same (location_id, machine_id)."
        )
    )
    parser.add_argument(
        "--db-dir",
        default=None,
        help="Directory containing vending DBs (defaults to repo's database-builder/db).",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=5,
        help="Days to suppress (default: 5).",
    )
    return parser.parse_args()


def setup_logging(log_path: Path) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logger.remove()
    logger.add(sys.stdout, format="{message}")
    logger.add(
        log_path,
        mode="w",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
    )


def main() -> int:
    args = parse_args()
    proof_dir = Path(__file__).resolve().parent
    log_path = proof_dir / "logs" / "prove_alert_suppression_for_machine_type.log"
    setup_logging(log_path)

    tmp_dir = proof_dir / "tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    state_db = tmp_dir / "prove_alert_suppression_for_machine_type.agent.db"
    if state_db.exists():
        state_db.unlink()

    logger.info("PROOF START")
    logger.info("Step 1/6: Create engine with state DB {}", state_db.as_posix())
    engine = DailyAlertEngine(db_dir=args.db_dir, state_db=state_db)

    logger.info("Step 2/6: Run current day and load at least one alert")
    summary = engine.run_current_day()
    logger.info(
        "Engine run: run_date={}, executed_scripts={}, emitted_alerts={}",
        summary.run_date,
        summary.executed_scripts,
        summary.emitted_alerts,
    )
    alerts = engine.list_alerts(status="OPEN", limit=200)
    if not alerts:
        raise RuntimeError("No OPEN alerts emitted; cannot prove suppression behavior.")

    chosen = next((a for a in alerts if a.get("machine_id") is not None), alerts[0])
    alert_id = str(chosen["alert_id"])
    location_id = int(chosen["location_id"])
    machine_id = (
        int(chosen["machine_id"]) if chosen.get("machine_id") is not None else None
    )
    alert_type = str(chosen["alert_type"])
    run_day = date.fromisoformat(str(chosen["run_date"]))
    logger.info(
        "Selected alert: id={}, run_day={}, alert_type={}, location_id={}, machine_id={}",
        alert_id,
        run_day.isoformat(),
        alert_type,
        location_id,
        machine_id,
    )

    logger.info("Step 3/6: Suppress this alert_type for {} days", args.days)
    res = engine.snooze_alert(alert_id, days=args.days)
    logger.info(
        "Snoozed alert: id={}, status={}, snoozed_until={}",
        res["alert_id"],
        res["status"],
        res["snoozed_until"],
    )

    logger.info("Step 4/6: Verify suppression row exists and is active")
    with Session(engine.sql_engine) as session:
        suppression = session.exec(
            select(AlertSuppression)
            .where(AlertSuppression.location_id == location_id)
            .where(AlertSuppression.machine_id == machine_id)
            .where(AlertSuppression.alert_type == alert_type)
        ).first()
        if suppression is None:
            raise RuntimeError(
                "Expected AlertSuppression row to exist, but none found."
            )
        logger.info(
            "Suppression row: id={}, suppressed_until={}",
            suppression.id,
            suppression.suppressed_until.isoformat(timespec="seconds"),
        )

    logger.info(
        "Step 5/6: Attempt to persist a new alert with the same (alert_type, location_id, machine_id)"
    )
    blocked = engine._persist_alert(
        run_day=run_day,
        script_name="proof_suppression_other_script",
        script_version="0.0",
        payload=AlertPayload(
            alert_type=alert_type,
            severity=Severity.LOW,
            title="proof",
            summary="proof",
            evidence={"proof": True},
            recommended_actions=[],
            location_id=location_id,
            machine_id=machine_id,
        ),
    )
    assert blocked is False, "Expected suppression to block alert persistence"
    logger.info("Persistence blocked as expected")

    logger.info(
        "Step 6/6: Persist a different alert_type for the same machine (should pass)"
    )
    allowed = engine._persist_alert(
        run_day=run_day,
        script_name="proof_suppression_other_script",
        script_version="0.0",
        payload=AlertPayload(
            alert_type=f"{alert_type}__proof_variant",
            severity=Severity.LOW,
            title="proof variant",
            summary="proof variant",
            evidence={"proof": True},
            recommended_actions=[],
            location_id=location_id,
            machine_id=machine_id,
        ),
    )
    assert allowed is True, "Expected different alert_type to be allowed"
    logger.info("Persistence allowed as expected")

    logger.info("PROOF OK")
    logger.info("Wrote log to {}", log_path.as_posix())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
