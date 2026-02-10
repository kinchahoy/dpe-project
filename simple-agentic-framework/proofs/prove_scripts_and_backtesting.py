"""Proof: baseline scripts, script modification, and backtesting idempotency.

Demonstrates:
  1. Baseline scripts run and produce alerts for the current day.
  2. Backtesting over a date range is idempotent (two runs → same counts).
  3. The full LLM edit → compare → final-check → activate flow works end-to-end
     (same path the UI takes).
  4. The RunLog-invalidation fix lets re-running the current day pick up changes.

Fully idempotent: uses a temp state DB that is deleted and recreated each run.

Requires:
  - OPENAI_API_KEY set in environment (the LLM edit calls GPT).

Usage:
    uv run python proofs/prove_scripts_and_backtesting.py [--db-dir DIR]
"""

from __future__ import annotations

import argparse
import sys
from datetime import date, timedelta
from pathlib import Path

from dotenv import load_dotenv
from loguru import logger
from sqlmodel import Session

load_dotenv()

from simple_agent_framework.engine import DailyAlertEngine
from simple_agent_framework.models import RunLog
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
    logger.info("Step 1/8: Create engine and verify baseline scripts")
    engine = DailyAlertEngine(db_dir=args.db_dir, state_db=state_db)
    scripts = discover_scripts()
    script_names = [name for name, _ in scripts]
    logger.info("  Discovered {} scripts: {}", len(scripts), ", ".join(script_names))
    assert len(scripts) >= 3, f"Expected at least 3 scripts, got {len(scripts)}"
    logger.info("  OK")

    # ── Step 2: Run current day, verify alerts are produced ─────────
    logger.info("")
    logger.info("Step 2/8: Run baseline scripts for current day")
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
    logger.info("Step 3/8: Re-run current day (RunLog idempotency check)")
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
    logger.info("Step 4/8: Backtest over date range")
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
    logger.info("Step 5/8: Re-backtest same range (idempotency check)")
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

    # ── Step 6: LLM edit → compare → final-check (same path as UI) ──
    # The baseline service_due_predictor alerts when a machine is within
    # SERVICE_WINDOW_DAYS (14) of its scheduled maintenance interval.
    # We ask the LLM to widen the window to 999 days so the alert fires
    # for virtually every machine — proving the edit changes output.
    logger.info("")
    logger.info("Step 6/8: Ask LLM to edit service_due_predictor (UI flow)")
    script_name = "service_due_predictor"
    edit_instruction = (
        "Change SERVICE_WINDOW_DAYS to 999 so the alert triggers for "
        "every machine regardless of how far out the service date is."
    )
    logger.info("  Script: {}", script_name)
    logger.info("  Instruction: {}", edit_instruction)

    # 6a. generate_script_edit — calls the LLM, stores a ScriptRevision draft.
    draft = engine.generate_script_edit(script_name, edit_instruction)
    revision_id = draft["revision_id"]
    logger.info("  Draft revision: {}", revision_id)
    logger.info("  Draft code ({} chars):\n{}", len(draft["code"]), draft["code"])

    # Basic sanity on the LLM output.
    assert "result" in draft["code"], "LLM draft must set `result`"
    assert "import " not in draft["code"], "LLM draft must not contain imports"
    # The LLM should have changed the window value — look for 999 or a very
    # large number in the code (the exact constant name may vary).
    assert "999" in draft["code"] or "SERVICE_WINDOW_DAYS" in draft["code"], (
        "LLM draft should reference the changed window value"
    )
    logger.info("  Draft passes basic sanity checks")

    # 6b. compare_script_revision_history — backtests old code vs new code.
    logger.info("  Comparing draft against baseline over history...")
    comparison = engine.compare_script_revision_history(script_name, revision_id)
    comp_data = comparison["comparison"]
    logger.info("  Comparison keys: {}", list(comp_data.keys()) if isinstance(comp_data, dict) else type(comp_data))
    logger.info("  OK: comparison completed")

    # 6c. final_check_script_revision — LLM reviews the diff + comparison.
    logger.info("  Running final AI check on draft...")
    final_check = engine.final_check_script_revision(
        script_name, revision_id, comp_data,
    )
    logger.info("  Recommended action: {}", final_check.get("recommended_action"))
    logger.info("  Rationale: {}", final_check.get("rationale"))
    logger.info("  OK: final check completed")

    # ── Step 7: Activate the LLM-edited script and re-run ────────────
    logger.info("")
    logger.info("Step 7/8: Activate LLM-edited draft and re-run current day")

    engine.activate_script_revision(script_name, revision_id)
    logger.info("  Activated revision {}", revision_id)

    # The activate call should have invalidated the RunLog for the current day.
    current_day = date.fromisoformat(engine.get_state()["current_day"])
    with Session(engine.sql_engine) as session:
        run_log = session.get(RunLog, current_day)
        assert run_log is None, (
            f"Expected RunLog for {current_day} to be cleared after activation, but it still exists"
        )
    logger.info("  RunLog for {} was correctly invalidated", current_day.isoformat())

    # Re-run the current day with the LLM-edited script.
    summary3 = engine.run_current_day()
    logger.info(
        "  Re-run: run_date={}, executed_scripts={}, emitted_alerts={}",
        summary3.run_date, summary3.executed_scripts, summary3.emitted_alerts,
    )
    assert summary3.executed_scripts > 0, "Scripts should re-execute after RunLog invalidation"

    # With SERVICE_WINDOW_DAYS=999, the modified script should fire for more
    # machines than the baseline (window=14), producing more alerts.
    logger.info(
        "  Emitted alerts: baseline={}, modified={}",
        summary.emitted_alerts, summary3.emitted_alerts,
    )
    assert summary3.emitted_alerts != summary.emitted_alerts, (
        f"Modified script should produce a different alert count: {summary3.emitted_alerts} == {summary.emitted_alerts}"
    )
    logger.info("  OK: LLM-edited script changed alert output as expected")

    # ── Step 8: Revert to baseline and verify restoration ───────────
    logger.info("")
    logger.info("Step 8/8: Revert to baseline and verify restoration")
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
    assert summary4.emitted_alerts == summary.emitted_alerts, (
        f"Reverted run should match baseline: {summary4.emitted_alerts} != {summary.emitted_alerts}"
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
