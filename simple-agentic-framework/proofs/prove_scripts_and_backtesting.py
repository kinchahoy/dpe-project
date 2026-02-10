"""Proof: baseline scripts, script modification, and backtesting idempotency.

Demonstrates:
  1. Baseline scripts run and produce alerts for the current day.
  2. Backtesting over a date range is idempotent (two runs → same counts).
  3. A modified script (simulated LLM edit) produces different alert counts.
  4. The RunLog-invalidation fix lets re-running the current day pick up changes.

Fully idempotent: uses a temp state DB that is deleted and recreated each run.

Usage:
    uv run python proofs/prove_scripts_and_backtesting.py [--db-dir DIR]
"""

from __future__ import annotations

import argparse
import hashlib
import sys
from datetime import date, timedelta
from pathlib import Path

from loguru import logger
from sqlmodel import Session, select

from simple_agent_framework.engine import DailyAlertEngine
from simple_agent_framework.models import RunLog, ScriptRevision, ScriptSetting
from simple_agent_framework.script_registry import discover_scripts


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-dir", default=None, help="Vending DB directory")
    return parser.parse_args()


def setup_logging(log_path: Path) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logger.remove()
    logger.add(sys.stdout, format="{message}")
    logger.add(log_path, mode="w", format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}")


# A trivially modified version of service_due_predictor that never fires
# (window set to 0 days, so it's impossible to be "in window").
MODIFIED_SERVICE_SCRIPT = """\
# service_due_predictor.py
# PROOF VARIANT: window set to 0 so this script never fires.
# Used to verify that activating a modified script changes alert output.

SERVICE_INTERVAL_DAYS = 110
SERVICE_WINDOW_DAYS = 0

as_of = ctx["meta"]["as_of_date"]
last_serviced_raw = (ctx.get("entities", {}).get("machine", {}) or {}).get("last_serviced_at")

if not as_of or not last_serviced_raw:
    result = []
else:
    last_service_date = str(last_serviced_raw)[:10]
    days_since = max(0, days_between(last_service_date, as_of))
    days_until_due = SERVICE_INTERVAL_DAYS - days_since
    if days_until_due > SERVICE_WINDOW_DAYS:
        result = []
    else:
        result = [
            alert(
                "service_due",
                "MEDIUM",
                "Service window likely due",
                f"Preventive service is due in {days_until_due} day(s).",
                {"days_since_service": days_since, "days_until_due": days_until_due, "last_service_date": last_service_date},
                [("SCHEDULE_SERVICE", {"machine_id": ctx["ids"]["machine_id"], "priority": "MEDIUM", "reason": "proof variant", "suggested_date": date_add(as_of, 2)})],
            )
        ]
"""


def main() -> int:
    args = parse_args()
    proof_dir = Path(__file__).resolve().parent
    log_path = proof_dir / "logs" / "prove_scripts_and_backtesting.log"
    setup_logging(log_path)
    tmp_dir = proof_dir / "tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    state_db = tmp_dir / "prove_scripts_and_backtesting.agent.db"

    # Clean slate each run (idempotent).
    if state_db.exists():
        state_db.unlink()

    logger.info("=" * 60)
    logger.info("PROOF START: scripts, modification, and backtesting idempotency")
    logger.info("=" * 60)

    # ── Step 1: Create engine, verify baseline scripts load ─────────
    logger.info("")
    logger.info("Step 1/7: Create engine and verify baseline scripts")
    engine = DailyAlertEngine(db_dir=args.db_dir, state_db=state_db)
    scripts = discover_scripts()
    script_names = [name for name, _ in scripts]
    logger.info("  Discovered {} scripts: {}", len(scripts), ", ".join(script_names))
    assert len(scripts) >= 3, f"Expected at least 3 scripts, got {len(scripts)}"
    logger.info("  OK")

    # ── Step 2: Run current day, verify alerts are produced ─────────
    logger.info("")
    logger.info("Step 2/7: Run baseline scripts for current day")
    summary = engine.run_current_day()
    logger.info(
        "  run_date={}, executed_scripts={}, emitted_alerts={}",
        summary.run_date, summary.executed_scripts, summary.emitted_alerts,
    )
    baseline_alerts = engine.list_alerts(status="OPEN", limit=500)
    baseline_alert_types = {}
    for a in baseline_alerts:
        t = a["alert_type"]
        baseline_alert_types[t] = baseline_alert_types.get(t, 0) + 1
    logger.info("  Baseline alerts by type: {}", baseline_alert_types)
    assert summary.executed_scripts > 0, "Expected scripts to execute"
    assert len(baseline_alerts) > 0, "Expected at least one alert from baseline scripts"
    logger.info("  OK: {} total alerts", len(baseline_alerts))

    # ── Step 3: Run current day again → idempotent (RunLog guard) ───
    logger.info("")
    logger.info("Step 3/7: Re-run current day (RunLog idempotency check)")
    summary2 = engine.run_current_day()
    logger.info(
        "  run_date={}, executed_scripts={}, emitted_alerts={}",
        summary2.run_date, summary2.executed_scripts, summary2.emitted_alerts,
    )
    assert summary2.executed_scripts == 0, "RunLog guard should prevent re-execution"
    assert summary2.emitted_alerts == 0, "No new alerts expected from idempotent re-run"
    alerts_after = engine.list_alerts(status="OPEN", limit=500)
    assert len(alerts_after) == len(baseline_alerts), (
        f"Alert count should be unchanged: {len(alerts_after)} != {len(baseline_alerts)}"
    )
    logger.info("  OK: RunLog guard prevented re-execution, alert count unchanged")

    # ── Step 4: Backtest over a date range ──────────────────────────
    logger.info("")
    logger.info("Step 4/7: Backtest over date range")
    engine_state = engine.get_state()
    start_day = date.fromisoformat(engine_state["start_day"])
    end_day = date.fromisoformat(engine_state["end_day"])
    # Use a 5-day window from the middle of the range for speed.
    bt_start = start_day + timedelta(days=max(0, (end_day - start_day).days // 2 - 2))
    bt_end = min(bt_start + timedelta(days=4), end_day)
    logger.info("  Backtest range: {} to {}", bt_start.isoformat(), bt_end.isoformat())

    bt_results_1 = engine.run_backtest(start_day=bt_start, end_day=bt_end)
    bt_total_1 = sum(r["emitted_alerts"] for r in bt_results_1)
    logger.info("  Run 1: {} days, {} total alerts", len(bt_results_1), bt_total_1)
    for r in bt_results_1:
        logger.info("    {} → scripts={}, alerts={}", r["run_date"], r["executed_scripts"], r["emitted_alerts"])

    # ── Step 5: Backtest same range again → idempotent ──────────────
    logger.info("")
    logger.info("Step 5/7: Re-backtest same range (idempotency check)")
    bt_results_2 = engine.run_backtest(start_day=bt_start, end_day=bt_end)
    bt_total_2 = sum(r["emitted_alerts"] for r in bt_results_2)
    logger.info("  Run 2: {} days, {} total alerts", len(bt_results_2), bt_total_2)

    assert len(bt_results_1) == len(bt_results_2), "Same number of days"
    for r1, r2 in zip(bt_results_1, bt_results_2):
        assert r1["run_date"] == r2["run_date"], f"Date mismatch: {r1} vs {r2}"
        assert r1["emitted_alerts"] == r2["emitted_alerts"], (
            f"Alert count mismatch on {r1['run_date']}: {r1['emitted_alerts']} vs {r2['emitted_alerts']}"
        )
    logger.info("  OK: backtest is idempotent (identical alert counts)")

    # ── Step 6: Activate a modified script, verify different output ──
    logger.info("")
    logger.info("Step 6/7: Activate modified script and re-run current day")
    script_name = "service_due_predictor"
    code_sha = hashlib.sha256(MODIFIED_SERVICE_SCRIPT.encode("utf-8")).hexdigest()[:12]
    logger.info("  Script: {}, modified SHA: {}", script_name, code_sha)

    # Insert the revision and activate it.
    with Session(engine.sql_engine) as session:
        rev = ScriptRevision(
            script_name=script_name,
            base_sha="baseline",
            instruction="proof: set SERVICE_WINDOW_DAYS=0 so script never fires",
            code=MODIFIED_SERVICE_SCRIPT,
        )
        session.add(rev)
        session.commit()
        session.refresh(rev)
        revision_id = rev.id

    engine.activate_script_revision(script_name, revision_id)
    logger.info("  Activated revision {}", revision_id)

    # The activate call should have invalidated the RunLog for the current day.
    # Verify the RunLog was cleared.
    current_day = date.fromisoformat(engine.get_state()["current_day"])
    with Session(engine.sql_engine) as session:
        run_log = session.get(RunLog, current_day)
        assert run_log is None, (
            f"Expected RunLog for {current_day} to be cleared after activation, but it still exists"
        )
    logger.info("  RunLog for {} was correctly invalidated", current_day.isoformat())

    # Re-run the current day with the modified script.
    summary3 = engine.run_current_day()
    logger.info(
        "  Re-run: run_date={}, executed_scripts={}, emitted_alerts={}",
        summary3.run_date, summary3.executed_scripts, summary3.emitted_alerts,
    )
    assert summary3.executed_scripts > 0, "Scripts should re-execute after RunLog invalidation"

    # Compare emitted alert counts from run summaries (not list_alerts, which
    # includes historical alerts from earlier steps that remain OPEN).
    logger.info(
        "  Emitted alerts: baseline={}, modified={}",
        summary.emitted_alerts, summary3.emitted_alerts,
    )
    assert summary3.emitted_alerts < summary.emitted_alerts, (
        f"Modified script should emit fewer alerts than baseline: {summary3.emitted_alerts} >= {summary.emitted_alerts}"
    )
    logger.info("  OK: modified script changed alert output as expected")

    # ── Step 7: Revert to baseline and verify restoration ───────────
    logger.info("")
    logger.info("Step 7/7: Revert to baseline and verify restoration")
    engine.revert_script_to_baseline(script_name)

    # Revert also invalidates RunLog, so re-run should work.
    summary4 = engine.run_current_day()
    logger.info(
        "  Reverted re-run: run_date={}, executed_scripts={}, emitted_alerts={}",
        summary4.run_date, summary4.executed_scripts, summary4.emitted_alerts,
    )
    assert summary4.executed_scripts > 0, "Scripts should re-execute after revert"

    logger.info(
        "  Emitted alerts: reverted={}, original_baseline={}",
        summary4.emitted_alerts, summary.emitted_alerts,
    )
    assert summary4.emitted_alerts >= summary.emitted_alerts, (
        f"Reverted run should emit at least as many alerts as baseline: {summary4.emitted_alerts} < {summary.emitted_alerts}"
    )
    logger.info("  OK: reverted to baseline, alert output restored")

    # ── Done ────────────────────────────────────────────────────────
    logger.info("")
    logger.info("=" * 60)
    logger.info("PROOF OK: all assertions passed")
    logger.info("=" * 60)
    logger.info("Log written to {}", log_path.as_posix())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
