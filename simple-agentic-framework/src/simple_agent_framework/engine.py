from __future__ import annotations

import hashlib
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from itertools import count
from pathlib import Path
from typing import Any

from loguru import logger
from pydantic import ValidationError
from sqlalchemy import delete, desc
from sqlmodel import Session, select

from .alert_payload import AlertPayload
from .db import (
    VendingDbPaths,
    resolve_vending_db_paths,
    ensure_agent_schema,
    query_all,
    query_one,
    make_engine,
)
from .llm_review import review_alert_with_ai
from .models import (
    Alert,
    AlertSuppression,
    EngineState,
    InventoryState,
    ManagerAction,
    RunLog,
    ScriptRevision,
    ScriptSetting,
)
from .script_context import build_script_context
from .script_registry import discover_scripts
from .script_runner import ScriptExecutionError, run_script
from .time_utils import utc_now
from .types import ActionType


@dataclass(frozen=True)
class RunSummary:
    run_date: str
    executed_scripts: int
    emitted_alerts: int


class DailyAlertEngine:
    def __init__(
        self,
        dbs: VendingDbPaths | None = None,
        db_dir: str | Path | None = None,
        state_db: str | Path = "agent.db",
        cooldown_hours: int = 24,
    ) -> None:
        self.dbs = dbs or resolve_vending_db_paths(db_dir=db_dir)
        self.state_db = Path(state_db)
        self.cooldown_hours = cooldown_hours

        ensure_agent_schema(self.state_db)
        self.sql_engine = make_engine(self.state_db)

        self._run_id = self._resolve_latest_run_id()
        self._ensure_state()
        self._ensure_inventory_seed()
        self._clear_alerts()

    def _resolve_latest_run_id(self) -> str | None:
        row = query_one(
            self.dbs.analysis_db,
            "SELECT id FROM sim_run ORDER BY created_at DESC LIMIT 1",
            readonly=True,
        )
        return str(row["id"]) if row else None

    def _ensure_state(self) -> None:
        tx_range = query_one(
            self.dbs.observed_db,
            'SELECT MIN(date) AS min_date, MAX(date) AS max_date FROM "transaction"',
            readonly=True,
        )
        if (
            tx_range is None
            or tx_range["min_date"] is None
            or tx_range["max_date"] is None
        ):
            raise RuntimeError("transaction table has no date range")

        max_day = date.fromisoformat(str(tx_range["max_date"]))
        min_day = date.fromisoformat(str(tx_range["min_date"]))
        start_day = max(min_day, max_day - timedelta(days=29))

        with Session(self.sql_engine) as session:
            state = session.exec(select(EngineState).where(EngineState.id == 1)).first()
            if state is None:
                state = EngineState(
                    start_day=start_day, end_day=max_day, current_day=start_day
                )
                session.add(state)
                session.commit()

    def _facts_machines(self) -> list[dict[str, Any]]:
        return query_all(
            self.dbs.facts_db,
            """
            SELECT m.id AS machine_id,
                   m.name AS machine_name,
                   m.model AS machine_model,
                   m.location_id AS location_id,
                   l.name AS location_name
            FROM machine m
            JOIN location l ON l.id = m.location_id
            ORDER BY m.location_id, m.id
            """,
            readonly=True,
        )

    def _facts_capacities(self) -> list[dict[str, Any]]:
        return query_all(
            self.dbs.facts_db,
            """
            SELECT cap.machine_model AS machine_model,
                   cap.ingredient_id AS ingredient_id,
                   i.name AS ingredient_name,
                   cap.capacity AS capacity,
                   cap.unit AS unit
            FROM machine_ingredient_capacity cap
            JOIN ingredient i ON i.id = cap.ingredient_id
            ORDER BY cap.machine_model, i.name
            """,
            readonly=True,
        )

    def _ensure_inventory_seed(self) -> None:
        with Session(self.sql_engine) as session:
            state = session.exec(select(EngineState).where(EngineState.id == 1)).one()
            start_day = state.start_day
            machines = self._facts_machines()
            capacities = self._facts_capacities()
            caps_by_model: dict[str, list[dict[str, Any]]] = {}
            for row in capacities:
                caps_by_model.setdefault(str(row["machine_model"]), []).append(row)

            existing_pairs = session.exec(
                select(InventoryState.machine_id, InventoryState.ingredient_id).where(
                    InventoryState.date == start_day
                )
            ).all()
            existing_keys = {(int(mid), int(iid)) for mid, iid in existing_pairs}

            to_add: list[InventoryState] = []
            for m in machines:
                model = str(m["machine_model"])
                for cap in caps_by_model.get(model, []):
                    key = (int(m["machine_id"]), int(cap["ingredient_id"]))
                    if key in existing_keys:
                        continue
                    capacity = float(cap["capacity"])
                    qty = max(0.0, capacity * 0.9)
                    to_add.append(
                        InventoryState(
                            date=start_day,
                            machine_id=int(m["machine_id"]),
                            ingredient_id=int(cap["ingredient_id"]),
                            quantity_on_hand=qty,
                            unit=str(cap["unit"]),
                        )
                    )
            if to_add:
                session.add_all(to_add)
                session.commit()

    def _projection_anchor_date(
        self, *, run_id: str, machine_id: int, as_of: date
    ) -> str | None:
        row = query_one(
            self.dbs.analysis_db,
            """
            SELECT COALESCE(
                (
                    SELECT MAX(projection_date)
                    FROM sim_daily_ingredient_projection
                    WHERE run_id = ? AND machine_id = ? AND projection_date <= ?
                ),
                (
                    SELECT MAX(projection_date)
                    FROM sim_daily_ingredient_projection
                    WHERE run_id = ? AND machine_id = ?
                )
            ) AS projection_date
            """,
            (run_id, machine_id, as_of.isoformat(), run_id, machine_id),
            readonly=True,
        )
        if row is None or row.get("projection_date") is None:
            return None
        return str(row["projection_date"])

    def _predicted_consumption_for_day(
        self, *, run_id: str, machine_id: int, day: date
    ) -> dict[int, tuple[float, str]]:
        rows = query_all(
            self.dbs.analysis_db,
            """
            SELECT ingredient_id, forecast_quantity AS qty, unit
            FROM sim_daily_ingredient_projection
            WHERE run_id = ?
              AND machine_id = ?
              AND forecast_date = ?
              AND projection_date = (
                  SELECT MAX(projection_date)
                  FROM sim_daily_ingredient_projection
                  WHERE run_id = ?
                    AND machine_id = ?
                    AND forecast_date = ?
                    AND projection_date <= ?
              )
            """,
            (
                run_id,
                machine_id,
                day.isoformat(),
                run_id,
                machine_id,
                day.isoformat(),
                day.isoformat(),
            ),
            readonly=True,
        )
        out: dict[int, tuple[float, str]] = {}
        for r in rows:
            iid = int(r["ingredient_id"])
            out[iid] = (float(r.get("qty") or 0.0), str(r.get("unit") or ""))
        return out

    def schedule_machine_restock(self, *, machine_id: int) -> dict[str, Any]:
        with Session(self.sql_engine) as session:
            state = session.exec(select(EngineState).where(EngineState.id == 1)).one()
            effective_date = state.current_day + timedelta(days=1)

            machine = query_one(
                self.dbs.facts_db,
                "SELECT id, location_id FROM machine WHERE id = ?",
                (machine_id,),
                readonly=True,
            )
            if machine is None:
                raise ValueError(f"Machine {machine_id} not found")
            location_id = int(machine["location_id"])

            existing = session.exec(
                select(ManagerAction)
                .where(ManagerAction.machine_id == machine_id)
                .where(ManagerAction.effective_date == effective_date)
                .where(ManagerAction.action_type == str(ActionType.RESTOCK_MACHINE))
                .limit(1)
            ).first()
            if existing:
                return {
                    "scheduled": False,
                    "effective_date": effective_date.isoformat(),
                    "machine_id": machine_id,
                }

            action = ManagerAction(
                effective_date=effective_date,
                location_id=location_id,
                machine_id=machine_id,
                action_type=str(ActionType.RESTOCK_MACHINE),
                details_json=json.dumps({"mode": "top_up_to_capacity"}, sort_keys=True),
            )
            session.add(action)
            session.commit()
            return {
                "scheduled": True,
                "effective_date": effective_date.isoformat(),
                "machine_id": machine_id,
            }

    def get_state(self) -> dict[str, str]:
        with Session(self.sql_engine) as session:
            state = session.exec(select(EngineState).where(EngineState.id == 1)).one()
            return {
                "start_day": state.start_day.isoformat(),
                "end_day": state.end_day.isoformat(),
                "current_day": state.current_day.isoformat(),
                "updated_at": state.updated_at.isoformat(timespec="seconds"),
            }

    def reset_state(self) -> dict[str, str]:
        with Session(self.sql_engine) as session:
            session.exec(delete(Alert))
            session.exec(delete(InventoryState))
            session.exec(delete(ManagerAction))
            session.exec(delete(RunLog))
            state = session.exec(select(EngineState).where(EngineState.id == 1)).one()
            state.current_day = state.start_day
            state.updated_at = utc_now()
            session.add(state)
            session.commit()
        self._ensure_inventory_seed()
        return self.get_state()

    def _run_for_day_once(self, run_day: date) -> RunSummary:
        with Session(self.sql_engine) as session:
            existing = session.get(RunLog, run_day)
            if existing is not None:
                return RunSummary(
                    run_date=run_day.isoformat(),
                    executed_scripts=0,
                    emitted_alerts=0,
                )

        summary = self.run_for_day(run_day)

        with Session(self.sql_engine) as session:
            existing = session.get(RunLog, run_day)
            if existing is None:
                session.add(
                    RunLog(
                        run_date=run_day,
                        executed_scripts=summary.executed_scripts,
                        emitted_alerts=summary.emitted_alerts,
                    )
                )
                session.commit()

        return summary

    def _invalidate_current_day_run(self) -> None:
        """Clear the RunLog for the current day so scripts are re-executed."""
        with Session(self.sql_engine) as session:
            state = session.exec(select(EngineState).where(EngineState.id == 1)).one()
            existing = session.get(RunLog, state.current_day)
            if existing is not None:
                session.delete(existing)
                session.commit()

    def _sha12(self, text: str) -> str:
        return hashlib.sha256(text.encode("utf-8")).hexdigest()[:12]

    def _baseline_scripts(self) -> dict[str, str]:
        return {name: code for name, code in discover_scripts()}

    def list_scripts(self) -> list[dict[str, Any]]:
        baseline = self._baseline_scripts()
        script_names = sorted(baseline.keys())
        if not script_names:
            return []

        with Session(self.sql_engine) as session:
            settings = session.exec(
                select(ScriptSetting).where(ScriptSetting.script_name.in_(script_names))
            ).all()
            settings_by_name = {s.script_name: s for s in settings}

            active_ids = [
                s.active_revision_id
                for s in settings
                if s.active_revision_id is not None
            ]
            revisions_by_id: dict[str, ScriptRevision] = {}
            if active_ids:
                revisions = session.exec(
                    select(ScriptRevision).where(ScriptRevision.id.in_(active_ids))
                ).all()
                revisions_by_id = {r.id: r for r in revisions}

        output: list[dict[str, Any]] = []
        for script_name in script_names:
            baseline_code = baseline[script_name]
            setting = settings_by_name.get(script_name)
            enabled = setting.enabled if setting else True
            active_revision_id = setting.active_revision_id if setting else None

            source = "baseline"
            active_code = baseline_code
            if active_revision_id:
                rev = revisions_by_id.get(active_revision_id)
                if rev and rev.script_name == script_name:
                    active_code = rev.code
                    source = "override"
                else:
                    active_revision_id = None

            output.append(
                {
                    "script_name": script_name,
                    "enabled": enabled,
                    "active_source": source,
                    "active_revision_id": active_revision_id,
                    "baseline_sha": self._sha12(baseline_code),
                    "active_sha": self._sha12(active_code),
                }
            )
        return output

    def get_script(self, script_name: str) -> dict[str, Any]:
        baseline = self._baseline_scripts()
        if script_name not in baseline:
            raise ValueError(f"Unknown script: {script_name}")

        baseline_code = baseline[script_name]
        with Session(self.sql_engine) as session:
            setting = session.get(ScriptSetting, script_name)
            enabled = setting.enabled if setting else True
            active_revision_id = setting.active_revision_id if setting else None

            active_source = "baseline"
            active_code = baseline_code
            if active_revision_id:
                rev = session.get(ScriptRevision, active_revision_id)
                if rev and rev.script_name == script_name:
                    active_code = rev.code
                    active_source = "override"
                else:
                    active_revision_id = None

        return {
            "script_name": script_name,
            "enabled": enabled,
            "active_source": active_source,
            "active_revision_id": active_revision_id,
            "baseline_code": baseline_code,
            "active_code": active_code,
            "baseline_sha": self._sha12(baseline_code),
            "active_sha": self._sha12(active_code),
        }

    def set_script_enabled(self, script_name: str, enabled: bool) -> dict[str, Any]:
        _ = self.get_script(script_name)
        with Session(self.sql_engine) as session:
            setting = session.get(ScriptSetting, script_name)
            if setting is None:
                setting = ScriptSetting(script_name=script_name, enabled=enabled)
            else:
                setting.enabled = enabled
                setting.updated_at = utc_now()
            session.add(setting)
            session.commit()
        return self.get_script(script_name)

    def activate_script_revision(
        self, script_name: str, revision_id: str
    ) -> dict[str, Any]:
        _ = self.get_script(script_name)
        with Session(self.sql_engine) as session:
            rev = session.get(ScriptRevision, revision_id)
            if rev is None or rev.script_name != script_name:
                raise ValueError(f"Revision {revision_id} not found for {script_name}")

            setting = session.get(ScriptSetting, script_name)
            if setting is None:
                setting = ScriptSetting(
                    script_name=script_name,
                    enabled=True,
                    active_revision_id=revision_id,
                )
            else:
                setting.active_revision_id = revision_id
                setting.updated_at = utc_now()
            session.add(setting)
            session.commit()
        self._invalidate_current_day_run()
        return self.get_script(script_name)

    def revert_script_to_baseline(self, script_name: str) -> dict[str, Any]:
        _ = self.get_script(script_name)
        with Session(self.sql_engine) as session:
            setting = session.get(ScriptSetting, script_name)
            if setting is None:
                return self.get_script(script_name)
            setting.active_revision_id = None
            setting.updated_at = utc_now()
            session.add(setting)
            session.commit()
        self._invalidate_current_day_run()
        return self.get_script(script_name)

    def generate_script_edit(
        self, script_name: str, instruction: str
    ) -> dict[str, Any]:
        from .llm_script_edit import edit_script_with_ai

        detail = self.get_script(script_name)
        current_code = str(detail["active_code"])
        edited_code = edit_script_with_ai(
            script_name=script_name,
            current_code=current_code,
            instruction=instruction,
        )

        with Session(self.sql_engine) as session:
            rev = ScriptRevision(
                script_name=script_name,
                base_sha=self._sha12(current_code),
                instruction=instruction,
                code=edited_code,
            )
            session.add(rev)
            session.commit()
            session.refresh(rev)

        return {
            "revision_id": rev.id,
            "script_name": script_name,
            "base_sha": rev.base_sha,
            "code": rev.code,
        }

    def compare_script_revision_history(
        self, script_name: str, revision_id: str
    ) -> dict[str, Any]:
        from .llm_script_edit import validate_sandbox_compatibility

        detail = self.get_script(script_name)
        old_code = str(detail["active_code"])

        with Session(self.sql_engine) as session:
            rev = session.get(ScriptRevision, revision_id)
            if rev is None or rev.script_name != script_name:
                raise ValueError(f"Revision {revision_id} not found for {script_name}")

        validate_sandbox_compatibility(rev.code)
        comparison = self._compare_script_codes_over_history(
            script_name=script_name,
            old_code=old_code,
            new_code=rev.code,
        )

        return {
            "script_name": script_name,
            "revision_id": revision_id,
            "comparison": comparison,
        }

    def final_check_script_revision(
        self,
        script_name: str,
        revision_id: str,
        comparison: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        from .llm_script_edit import validate_sandbox_compatibility
        from .llm_script_final_check import final_check_script_draft_with_ai

        detail = self.get_script(script_name)
        old_code = str(detail["active_code"])

        with Session(self.sql_engine) as session:
            rev = session.get(ScriptRevision, revision_id)
            if rev is None or rev.script_name != script_name:
                raise ValueError(f"Revision {revision_id} not found for {script_name}")

        validate_sandbox_compatibility(rev.code)
        compare_payload = comparison
        if compare_payload is None:
            compare_payload = self._compare_script_codes_over_history(
                script_name=script_name,
                old_code=old_code,
                new_code=rev.code,
            )

        ai_review = final_check_script_draft_with_ai(
            script_name=script_name,
            edit_instruction=str(rev.instruction or ""),
            old_code=old_code,
            new_code=rev.code,
            comparison=compare_payload,
        )
        return {
            "script_name": script_name,
            "revision_id": revision_id,
            "reviewed_at": utc_now().isoformat(timespec="seconds"),
            "recommended_action": ai_review.get("recommended_action"),
            "rationale": ai_review.get("rationale"),
            "retry_instruction": ai_review.get("retry_instruction"),
        }

    def _compare_one_day(
        self,
        *,
        day: date,
        machines: list[dict[str, Any]],
        location_currency: dict[int, str],
        script_name: str,
        old_code: str,
        new_code: str,
    ) -> tuple[int, int]:
        old_day_alerts = 0
        new_day_alerts = 0
        for row in machines:
            location_id = int(row["location_id"])
            machine_id = int(row["machine_id"])
            currency = location_currency.get(location_id, "USD")
            context = build_script_context(
                dbs=self.dbs,
                as_of_date=day,
                location_id=location_id,
                machine_id=machine_id,
                currency=currency,
                state_db=self.state_db,
                inventory_override=self._inventory_override_for_machine(
                    run_day=day, machine_id=machine_id
                ),
            )

            try:
                old_emitted = run_script(
                    script_name=script_name, code=old_code, context=context
                )
                new_emitted = run_script(
                    script_name=script_name, code=new_code, context=context
                )
            except ScriptExecutionError as exc:
                raise ValueError(
                    f"Script comparison failed on day={day.isoformat()} machine={machine_id}: {exc}"
                ) from exc

            old_day_alerts += len(old_emitted)
            new_day_alerts += len(new_emitted)
        return old_day_alerts, new_day_alerts

    def _compare_script_codes_over_history(
        self,
        *,
        script_name: str,
        old_code: str,
        new_code: str,
    ) -> dict[str, Any]:
        with Session(self.sql_engine) as session:
            state = session.exec(select(EngineState).where(EngineState.id == 1)).one()
            start_day = state.start_day
            end_day = state.current_day

        self._ensure_inventory_through_day(target_day=end_day)
        machines = query_all(
            self.dbs.facts_db,
            """
            SELECT id AS machine_id, location_id
            FROM machine
            ORDER BY location_id, id
            """,
            readonly=True,
        )
        location_currency = self._location_currency()

        old_days_triggered = 0
        new_days_triggered = 0
        old_total_alerts = 0
        new_total_alerts = 0
        changed_days: list[dict[str, Any]] = []
        old_script_version = hashlib.sha256(old_code.encode()).hexdigest()[:12]
        new_script_version = hashlib.sha256(new_code.encode()).hexdigest()[:12]

        days = []
        d = start_day
        while d <= end_day:
            days.append(d)
            d += timedelta(days=1)

        with ThreadPoolExecutor(max_workers=min(4, len(days))) as pool:
            futures = {
                pool.submit(
                    self._compare_one_day,
                    day=d,
                    machines=machines,
                    location_currency=location_currency,
                    script_name=script_name,
                    old_code=old_code,
                    new_code=new_code,
                ): d
                for d in days
            }
            for future in as_completed(futures):
                d = futures[future]
                old_day_alerts, new_day_alerts = future.result()
                old_total_alerts += old_day_alerts
                new_total_alerts += new_day_alerts
                if old_day_alerts > 0:
                    old_days_triggered += 1
                if new_day_alerts > 0:
                    new_days_triggered += 1
                if old_day_alerts != new_day_alerts:
                    changed_days.append(
                        {
                            "date": d.isoformat(),
                            "old_alerts": old_day_alerts,
                            "new_alerts": new_day_alerts,
                        }
                    )

        changed_days.sort(key=lambda x: x["date"])

        return {
            "start_day": start_day.isoformat(),
            "end_day": end_day.isoformat(),
            "total_days": (end_day - start_day).days + 1,
            "old_script_version": old_script_version,
            "new_script_version": new_script_version,
            "old_days_triggered": old_days_triggered,
            "new_days_triggered": new_days_triggered,
            "old_total_alerts": old_total_alerts,
            "new_total_alerts": new_total_alerts,
            "changed_days": changed_days,
        }

    def _active_scripts(self) -> list[tuple[str, str]]:
        baseline = self._baseline_scripts()
        script_names = sorted(baseline.keys())
        if not script_names:
            return []

        with Session(self.sql_engine) as session:
            settings = session.exec(
                select(ScriptSetting).where(ScriptSetting.script_name.in_(script_names))
            ).all()
            settings_by_name = {s.script_name: s for s in settings}

            active_ids = [
                s.active_revision_id
                for s in settings
                if s.active_revision_id is not None
            ]
            revisions_by_id: dict[str, ScriptRevision] = {}
            if active_ids:
                revisions = session.exec(
                    select(ScriptRevision).where(ScriptRevision.id.in_(active_ids))
                ).all()
                revisions_by_id = {r.id: r for r in revisions}

        scripts: list[tuple[str, str]] = []
        for script_name in script_names:
            setting = settings_by_name.get(script_name)
            if setting and not setting.enabled:
                continue
            code = baseline[script_name]
            if setting and setting.active_revision_id:
                rev = revisions_by_id.get(setting.active_revision_id)
                if rev and rev.script_name == script_name:
                    code = rev.code
            scripts.append((script_name, code))
        return scripts

    def _clear_alerts(self) -> None:
        with Session(self.sql_engine) as session:
            session.exec(delete(Alert))
            session.commit()

    def skip_to_day(self, target_day: date) -> dict[str, str]:
        with Session(self.sql_engine) as session:
            state = session.exec(select(EngineState).where(EngineState.id == 1)).one()
            if target_day < state.start_day:
                target_day = state.start_day
            if target_day > state.end_day:
                target_day = state.end_day
            state.current_day = target_day
            state.updated_at = utc_now()
            session.add(state)
            session.commit()
        return self.get_state()

    def run_current_day(self) -> RunSummary:
        with Session(self.sql_engine) as session:
            state = session.exec(select(EngineState).where(EngineState.id == 1)).one()
            run_day = state.current_day
            next_day = state.current_day + timedelta(days=1)

        summary = self._run_for_day_once(run_day)

        if self._run_id is not None:
            with Session(self.sql_engine) as session:
                self._persist_next_day_inventory(
                    session=session,
                    run_id=self._run_id,
                    run_day=run_day,
                    next_day=next_day,
                    overwrite=True,
                )

        return summary

    def advance_day(self) -> dict[str, Any]:
        # 1. Run scripts for current day N (idempotent via RunLog guard)
        with Session(self.sql_engine) as session:
            state = session.exec(select(EngineState).where(EngineState.id == 1)).one()
            run_day = state.current_day

        summary = self._run_for_day_once(run_day)

        # 2. Persist inventory for N→N+1, advance current_day to N+1
        with Session(self.sql_engine) as session:
            state = session.exec(select(EngineState).where(EngineState.id == 1)).one()
            end_day = state.end_day
            if state.current_day < end_day:
                if self._run_id is not None:
                    self._persist_next_day_inventory(
                        session=session,
                        run_id=self._run_id,
                        run_day=state.current_day,
                        next_day=state.current_day + timedelta(days=1),
                    )
                state.current_day = state.current_day + timedelta(days=1)
            state.updated_at = utc_now()
            session.add(state)
            session.commit()
            new_day = state.current_day

        # 3. Run scripts for the new day so alerts are visible immediately
        summary = self._run_for_day_once(new_day)

        # 4. Persist inventory for new_day→new_day+1
        if self._run_id is not None and new_day < end_day:
            with Session(self.sql_engine) as session:
                self._persist_next_day_inventory(
                    session=session,
                    run_id=self._run_id,
                    run_day=new_day,
                    next_day=new_day + timedelta(days=1),
                    overwrite=True,
                )

        return {
            "state": {
                "start_day": run_day.isoformat(),
                "end_day": end_day.isoformat(),
                "current_day": new_day.isoformat(),
            },
            "summary": {
                "run_date": summary.run_date,
                "executed_scripts": summary.executed_scripts,
                "emitted_alerts": summary.emitted_alerts,
            },
        }

    def _persist_next_day_inventory(
        self,
        *,
        session: Session,
        run_id: str,
        run_day: date,
        next_day: date,
        overwrite: bool = True,
    ) -> None:
        self._ensure_inventory_seed()
        machines = self._facts_machines()
        capacities = self._facts_capacities()
        caps_by_model_ing: dict[tuple[str, int], dict[str, Any]] = {
            (str(r["machine_model"]), int(r["ingredient_id"])): r for r in capacities
        }
        machine_model_by_id = {
            int(m["machine_id"]): str(m["machine_model"]) for m in machines
        }

        actions = session.exec(
            select(ManagerAction)
            .where(ManagerAction.effective_date == next_day)
            .where(ManagerAction.action_type == str(ActionType.RESTOCK_MACHINE))
        ).all()
        restock_machine_ids = {int(a.machine_id) for a in actions}

        if overwrite:
            session.exec(delete(InventoryState).where(InventoryState.date == next_day))
            session.commit()
        else:
            existing_next = session.exec(
                select(InventoryState.id)
                .where(InventoryState.date == next_day)
                .limit(1)
            ).first()
            if existing_next:
                return

        to_add: list[InventoryState] = []
        for mid, model in machine_model_by_id.items():
            start_rows = session.exec(
                select(InventoryState)
                .where(InventoryState.date == run_day)
                .where(InventoryState.machine_id == mid)
            ).all()
            if not start_rows:
                continue
            consumption = self._predicted_consumption_for_day(
                run_id=run_id, machine_id=mid, day=run_day
            )
            for r in start_rows:
                iid = int(r.ingredient_id)
                used, _unit = consumption.get(iid, (0.0, str(r.unit)))
                next_qty = max(0.0, float(r.quantity_on_hand) - float(used))
                if mid in restock_machine_ids:
                    cap = caps_by_model_ing.get((model, iid))
                    if cap is not None and str(cap.get("unit") or "") == str(r.unit):
                        next_qty = float(cap["capacity"])
                to_add.append(
                    InventoryState(
                        date=next_day,
                        machine_id=mid,
                        ingredient_id=iid,
                        quantity_on_hand=next_qty,
                        unit=str(r.unit),
                    )
                )

        session.add_all(to_add)
        session.commit()

    def _ensure_inventory_through_day(self, *, target_day: date) -> None:
        self._ensure_inventory_seed()
        with Session(self.sql_engine) as session:
            latest = session.exec(
                select(InventoryState.date)
                .where(InventoryState.date <= target_day)
                .order_by(desc(InventoryState.date))
                .limit(1)
            ).first()
            if latest is None:
                return
            latest_day = latest
            for _ in count():
                if latest_day >= target_day:
                    break
                next_day = latest_day + timedelta(days=1)
                self._persist_next_day_inventory(
                    session=session,
                    run_id=str(self._run_id or ""),
                    run_day=latest_day,
                    next_day=next_day,
                    overwrite=False,
                )
                latest_day = next_day

    def run_for_day(self, run_day: date) -> RunSummary:
        self._ensure_inventory_through_day(target_day=run_day)
        machine_rows = query_all(
            self.dbs.facts_db,
            """
            SELECT id AS machine_id, location_id
            FROM machine
            ORDER BY location_id, id
            """,
            readonly=True,
        )

        scripts = self._active_scripts()
        location_currency = self._location_currency()

        alerts_created = 0
        script_count = 0
        for row in machine_rows:
            location_id = int(row["location_id"])
            machine_id = int(row["machine_id"])
            currency = location_currency.get(location_id, "USD")

            context = build_script_context(
                dbs=self.dbs,
                as_of_date=run_day,
                location_id=location_id,
                machine_id=machine_id,
                currency=currency,
                state_db=self.state_db,
                inventory_override=self._inventory_override_for_machine(
                    run_day=run_day, machine_id=machine_id
                ),
            )

            for script_name, code in scripts:
                script_count += 1
                script_version = hashlib.sha256(code.encode()).hexdigest()[:12]

                try:
                    emitted = run_script(
                        script_name=script_name,
                        code=code,
                        context=context,
                    )
                except ScriptExecutionError as exc:
                    logger.error("{} failed: {}", script_name, exc)
                    continue

                for alert_payload in emitted:
                    try:
                        parsed = AlertPayload.model_validate(alert_payload)
                    except ValidationError as exc:
                        logger.error(
                            "Invalid alert payload script={} error={}", script_name, exc
                        )
                        continue
                    if self._persist_alert(
                        run_day=run_day,
                        script_name=script_name,
                        script_version=script_version,
                        payload=parsed,
                    ):
                        alerts_created += 1

        logger.info(
            "Daily run complete day={} scripts={} alerts={}",
            run_day.isoformat(),
            script_count,
            alerts_created,
        )
        return RunSummary(
            run_date=run_day.isoformat(),
            executed_scripts=script_count,
            emitted_alerts=alerts_created,
        )

    def _inventory_override_for_machine(
        self, *, run_day: date, machine_id: int
    ) -> dict[str, Any]:
        self._ensure_inventory_through_day(target_day=run_day)
        with Session(self.sql_engine) as session:
            rows = session.exec(
                select(InventoryState)
                .where(InventoryState.date == run_day)
                .where(InventoryState.machine_id == machine_id)
            ).all()

        machine_row = query_one(
            self.dbs.facts_db,
            "SELECT model FROM machine WHERE id = ?",
            (machine_id,),
            readonly=True,
        )
        model = str(machine_row["model"]) if machine_row else ""
        capacities = self._facts_capacities()
        caps_by_ing = {
            int(r["ingredient_id"]): r
            for r in capacities
            if str(r["machine_model"]) == model
        }

        out_rows: list[dict[str, Any]] = []
        for r in rows:
            cap = caps_by_ing.get(int(r.ingredient_id))
            out_rows.append(
                {
                    "snapshot_date": run_day.isoformat(),
                    "ingredient_id": int(r.ingredient_id),
                    "ingredient_name": cap.get("ingredient_name") if cap else None,
                    "quantity_on_hand": float(r.quantity_on_hand),
                    "unit": str(r.unit),
                    "capacity": float(cap["capacity"])
                    if cap and cap.get("capacity") is not None
                    else None,
                    "capacity_unit": cap.get("unit") if cap else None,
                }
            )

        return {"snapshot_date": run_day.isoformat(), "rows": out_rows}

    def _persist_alert(
        self,
        *,
        run_day: date,
        script_name: str,
        script_version: str,
        payload: AlertPayload,
    ) -> bool:
        location_id = int(payload.location_id)
        machine_id = payload.machine_id
        product_id = payload.product_id
        ingredient_id = payload.ingredient_id
        alert_type = str(payload.alert_type)

        if self._is_suppressed(
            location_id=location_id,
            machine_id=int(machine_id) if machine_id is not None else None,
            alert_type=alert_type,
        ):
            return False

        identity = {
            "script_name": script_name,
            "alert_type": alert_type,
            "location_id": location_id,
            "machine_id": machine_id,
            "product_id": product_id,
            "ingredient_id": ingredient_id,
        }
        fingerprint = hashlib.sha256(
            json.dumps(identity, sort_keys=True).encode("utf-8")
        ).hexdigest()
        evidence_json = json.dumps(payload.evidence, sort_keys=True, default=str)
        evidence_hash = hashlib.sha256(evidence_json.encode("utf-8")).hexdigest()
        now = utc_now()

        with Session(self.sql_engine) as session:
            existing_open = session.exec(
                select(Alert)
                .where(Alert.fingerprint == fingerprint)
                .where(Alert.status == "OPEN")
                .order_by(desc(Alert.created_at))
            ).all()

            if existing_open:
                current = existing_open[0]
                current.created_at = now
                current.run_date = run_day
                current.script_name = script_name
                current.script_version = script_version
                current.fingerprint = fingerprint
                current.evidence_hash = evidence_hash
                current.severity = str(payload.severity)
                current.alert_type = alert_type
                current.location_id = location_id
                current.machine_id = int(machine_id) if machine_id is not None else None
                current.product_id = int(product_id) if product_id is not None else None
                current.ingredient_id = (
                    int(ingredient_id) if ingredient_id is not None else None
                )
                current.title = str(payload.title)
                current.summary = str(payload.summary)
                current.evidence_json = evidence_json
                current.recommended_actions_json = json.dumps(
                    [a.model_dump(mode="json") for a in payload.recommended_actions],
                    sort_keys=True,
                )
                current.status = "OPEN"
                current.snoozed_until = None
                current.decision = None
                current.decision_note = None
                current.decided_at = None
                current.feedback_loop_id = None
                session.add(current)

                for prior in existing_open[1:]:
                    prior.status = "REPLACED"
                    prior.decision = "AUTO_REPLACED"
                    prior.decision_note = (
                        "Superseded by a newer alert with the same identity."
                    )
                    prior.decided_at = now
                    session.add(prior)

                session.commit()
                return True

            alert = Alert(
                run_date=run_day,
                script_name=script_name,
                script_version=script_version,
                fingerprint=fingerprint,
                evidence_hash=evidence_hash,
                severity=str(payload.severity),
                alert_type=alert_type,
                location_id=location_id,
                machine_id=int(machine_id) if machine_id is not None else None,
                product_id=int(product_id) if product_id is not None else None,
                ingredient_id=int(ingredient_id) if ingredient_id is not None else None,
                title=str(payload.title),
                summary=str(payload.summary),
                evidence_json=evidence_json,
                recommended_actions_json=json.dumps(
                    [a.model_dump(mode="json") for a in payload.recommended_actions],
                    sort_keys=True,
                ),
                status="OPEN",
            )
            session.add(alert)
            session.commit()
        return True

    def _is_suppressed(
        self, *, location_id: int, machine_id: int | None, alert_type: str
    ) -> bool:
        now = utc_now()
        with Session(self.sql_engine) as session:
            suppression = session.exec(
                select(AlertSuppression)
                .where(AlertSuppression.location_id == location_id)
                .where(AlertSuppression.machine_id == machine_id)
                .where(AlertSuppression.alert_type == alert_type)
                .where(AlertSuppression.suppressed_until > now)
            ).first()
            return suppression is not None

    def _upsert_suppression(
        self,
        *,
        session: Session,
        location_id: int,
        machine_id: int | None,
        alert_type: str,
        days: int,
    ) -> datetime:
        now = utc_now()
        until = now + timedelta(days=days)
        existing = session.exec(
            select(AlertSuppression)
            .where(AlertSuppression.location_id == location_id)
            .where(AlertSuppression.machine_id == machine_id)
            .where(AlertSuppression.alert_type == alert_type)
        ).first()
        if existing is None:
            session.add(
                AlertSuppression(
                    alert_type=alert_type,
                    location_id=location_id,
                    machine_id=machine_id,
                    suppressed_until=until,
                    created_at=now,
                    updated_at=now,
                )
            )
        else:
            existing.suppressed_until = max(existing.suppressed_until, until)
            existing.updated_at = now
            session.add(existing)
            until = existing.suppressed_until
        return until

    def list_alerts(
        self,
        *,
        status: str | None = None,
        location_id: int | None = None,
        include_snoozed: bool = False,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        def normalize_iso_dt(value: Any) -> Any:
            if value is None:
                return None
            if isinstance(value, datetime):
                return value.isoformat(timespec="seconds")
            if not isinstance(value, str):
                return value

            raw = value.strip()
            if not raw:
                return value
            candidate = raw.replace(" ", "T", 1)
            try:
                parsed = datetime.fromisoformat(candidate)
            except ValueError:
                return value
            return parsed.isoformat(timespec="seconds")

        query = """
        SELECT *
        FROM alert
        WHERE 1 = 1
        """
        params: list[Any] = []
        if status:
            query += " AND status = ?"
            params.append(status)
        elif not include_snoozed:
            query += " AND status = 'OPEN'"

        if location_id is not None:
            query += " AND location_id = ?"
            params.append(location_id)

        query += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)

        rows = query_all(self.state_db, query, tuple(params))
        for row in rows:
            for key in ("created_at", "snoozed_until", "decided_at"):
                if key in row:
                    row[key] = normalize_iso_dt(row.get(key))
            row["evidence"] = json.loads(row["evidence_json"])
            row["recommended_actions"] = json.loads(row["recommended_actions_json"])
        return rows

    def accept_alert(
        self, alert_id: str, decision_note: str | None = None
    ) -> dict[str, Any]:
        with Session(self.sql_engine) as session:
            alert = session.exec(
                select(Alert).where(Alert.alert_id == alert_id)
            ).first()
            if alert is None:
                raise ValueError(f"Alert {alert_id} not found")

            state = session.exec(select(EngineState).where(EngineState.id == 1)).one()
            effective_date = state.current_day + timedelta(days=1)
            actionable_types = {
                str(ActionType.RESTOCK_MACHINE),
                str(ActionType.ORDER_INGREDIENTS),
                str(ActionType.ADJUST_PRICE),
                str(ActionType.SCHEDULE_SERVICE),
                str(ActionType.PROPOSE_DISCONTINUE),
            }

            manager_actions: list[dict[str, Any]] = []

            def queue_manager_action(
                *, action_type: str, params: dict[str, Any] | None = None
            ) -> None:
                raw_params = params if isinstance(params, dict) else {}
                machine_raw = raw_params.get("machine_id", alert.machine_id)
                if machine_raw is None:
                    manager_actions.append(
                        {
                            "action_type": action_type,
                            "scheduled": False,
                            "reason": "missing_machine_id",
                        }
                    )
                    return
                try:
                    machine_id = int(machine_raw)
                except Exception:
                    manager_actions.append(
                        {
                            "action_type": action_type,
                            "scheduled": False,
                            "reason": "invalid_machine_id",
                        }
                    )
                    return

                try:
                    location_id = int(raw_params.get("location_id", alert.location_id))
                except Exception:
                    location_id = int(alert.location_id)

                existing = session.exec(
                    select(ManagerAction)
                    .where(ManagerAction.machine_id == machine_id)
                    .where(ManagerAction.effective_date == effective_date)
                    .where(ManagerAction.action_type == action_type)
                    .limit(1)
                ).first()

                scheduled = False
                if existing is None:
                    details = dict(raw_params)
                    if action_type == str(ActionType.RESTOCK_MACHINE):
                        details.setdefault("mode", "top_up_to_capacity")
                    details["source_alert_id"] = alert.alert_id
                    session.add(
                        ManagerAction(
                            effective_date=effective_date,
                            location_id=location_id,
                            machine_id=machine_id,
                            action_type=action_type,
                            details_json=json.dumps(details, sort_keys=True),
                        )
                    )
                    scheduled = True

                manager_actions.append(
                    {
                        "action_type": action_type,
                        "machine_id": machine_id,
                        "effective_date": effective_date.isoformat(),
                        "scheduled": scheduled,
                    }
                )

            saw_action = False
            try:
                recommended = json.loads(alert.recommended_actions_json)
            except Exception:
                recommended = []
            if isinstance(recommended, list):
                for action in recommended:
                    if not isinstance(action, dict):
                        continue
                    action_type = str(action.get("action_type") or "").upper()
                    if action_type not in actionable_types:
                        continue
                    params = action.get("params")
                    queue_manager_action(
                        action_type=action_type,
                        params=params if isinstance(params, dict) else {},
                    )
                    saw_action = True

            if not saw_action and "restock" in str(alert.alert_type or "").lower():
                queue_manager_action(
                    action_type=str(ActionType.RESTOCK_MACHINE), params={}
                )

            alert.status = "RESOLVED"
            alert.decision = "ACCEPT_ACTION"
            alert.decision_note = decision_note
            alert.decided_at = utc_now()
            session.add(alert)
            session.commit()
            session.refresh(alert)
            result: dict[str, Any] = {
                "alert_id": alert.alert_id,
                "status": alert.status,
                "decision": alert.decision,
            }
            if manager_actions:
                result["manager_actions"] = manager_actions
                restock_actions = [
                    a
                    for a in manager_actions
                    if a.get("action_type") == str(ActionType.RESTOCK_MACHINE)
                ]
                if restock_actions:
                    latest_restock = restock_actions[-1]
                    result["restock_scheduled"] = bool(latest_restock.get("scheduled"))
                    result["restock_effective_date"] = latest_restock.get(
                        "effective_date"
                    )
            return result

    def snooze_alert(self, alert_id: str, days: int) -> dict[str, Any]:
        with Session(self.sql_engine) as session:
            alert = session.exec(
                select(Alert).where(Alert.alert_id == alert_id)
            ).first()
            if alert is None:
                raise ValueError(f"Alert {alert_id} not found")

            suppressed_until = self._upsert_suppression(
                session=session,
                location_id=int(alert.location_id),
                machine_id=int(alert.machine_id)
                if alert.machine_id is not None
                else None,
                alert_type=str(alert.alert_type),
                days=days,
            )

            alert.status = "SNOOZED"
            alert.snoozed_until = suppressed_until
            alert.decision = "SUPPRESS"
            alert.decided_at = utc_now()
            session.add(alert)
            session.commit()
            session.refresh(alert)
            return {
                "alert_id": alert.alert_id,
                "status": alert.status,
                "snoozed_until": alert.snoozed_until.isoformat(timespec="seconds"),
            }

    def review_alert(
        self, alert_id: str, manager_note: str | None = None
    ) -> dict[str, Any]:
        with Session(self.sql_engine) as session:
            alert = session.exec(
                select(Alert).where(Alert.alert_id == alert_id)
            ).first()
            if alert is None:
                raise ValueError(f"Alert {alert_id} not found")
            state = session.exec(select(EngineState).where(EngineState.id == 1)).one()
            current_sim_date = state.current_day.isoformat()

            related = session.exec(
                select(Alert)
                .where(Alert.script_name == alert.script_name)
                .where(Alert.status == "OPEN")
                .order_by(desc(Alert.created_at))
                .limit(20)
            ).all()

            payload = {
                "alert_id": alert.alert_id,
                "script_name": alert.script_name,
                "alert_type": alert.alert_type,
                "location_id": alert.location_id,
                "machine_id": alert.machine_id,
                "current_date": current_sim_date,
                "run_date": alert.run_date.isoformat(),
                "evidence": json.loads(alert.evidence_json),
            }

            if alert.machine_id is not None:
                inv = self._inventory_override_for_machine(
                    run_day=state.current_day, machine_id=alert.machine_id
                )
                payload["inventory_snapshot"] = inv.get("rows", [])

            related_payloads = [
                {
                    "alert_id": item.alert_id,
                    "run_date": item.run_date.isoformat(),
                    "summary": item.summary,
                }
                for item in related
            ]
            review = review_alert_with_ai(
                alert=payload,
                related_open_alerts=related_payloads,
                manager_note=manager_note,
            )

            if review.get("optional_script_change"):
                known_scripts = {s["script_name"] for s in self.list_scripts()}
                suggested = review["optional_script_change"]["script_name"]
                if suggested not in known_scripts:
                    review["optional_script_change"]["script_name"] = alert.script_name

            alert.feedback_loop_id = review["feedback_loop_id"]
            session.add(alert)
            session.commit()

            return review

    async def review_alert_async(
        self, alert_id: str, manager_note: str | None = None
    ) -> dict[str, Any]:
        """Run review_alert in a thread so it doesn't block the event loop."""
        import asyncio

        return await asyncio.to_thread(self.review_alert, alert_id, manager_note)

    def dashboard_summary(
        self, *, days: int = 14, location_id: int | None = None
    ) -> dict[str, Any]:
        with Session(self.sql_engine) as session:
            state = session.exec(select(EngineState).where(EngineState.id == 1)).one()
            end_day = state.current_day

        start_day = end_day - timedelta(days=max(1, days) - 1)
        params: list[Any] = [start_day.isoformat(), end_day.isoformat()]
        location_clause = ""
        if location_id is not None:
            location_clause = " AND t.location_id = ?"
            params.append(location_id)

        revenue_rows = query_all(
            self.dbs.observed_db,
            f"""
            SELECT t.location_id,
                   t.date,
                   ROUND(SUM(t.amount), 2) AS revenue,
                   COUNT(*) AS tx_count
            FROM "transaction" t
            WHERE t.date BETWEEN ? AND ? {location_clause}
            GROUP BY t.location_id, t.date
            ORDER BY t.location_id, t.date
            """,
            tuple(params),
            readonly=True,
        )

        machine_revenue_rows = query_all(
            self.dbs.observed_db,
            f"""
            SELECT t.location_id,
                   t.machine_id,
                   ROUND(SUM(t.amount), 2) AS revenue,
                   COUNT(*) AS tx_count
            FROM "transaction" t
            WHERE t.date BETWEEN ? AND ? {location_clause}
            GROUP BY t.location_id, t.machine_id
            ORDER BY t.location_id, t.machine_id
            """,
            tuple(params),
            readonly=True,
        )

        alert_rows = query_all(
            self.state_db,
            """
            SELECT location_id, severity, alert_type, COUNT(*) AS n
            FROM alert
            WHERE run_date BETWEEN ? AND ?
            GROUP BY location_id, severity, alert_type
            ORDER BY n DESC
            LIMIT 30
            """,
            (start_day.isoformat(), end_day.isoformat()),
        )
        location_currency = self._location_currency()

        return {
            "start_day": start_day.isoformat(),
            "end_day": end_day.isoformat(),
            "daily_revenue": revenue_rows,
            "machine_revenue": machine_revenue_rows,
            "top_alert_patterns": alert_rows,
            "location_currency": location_currency,
        }

    def machine_sales_by_group(self, *, machine_id: int) -> dict[str, Any]:
        with Session(self.sql_engine) as session:
            state = session.exec(select(EngineState).where(EngineState.id == 1)).one()
            current_day = state.current_day

        machine = query_one(
            self.dbs.facts_db,
            """
            SELECT m.id AS machine_id,
                   m.name AS machine_name,
                   m.location_id AS location_id,
                   l.name AS location_name
            FROM machine m
            JOIN location l ON l.id = m.location_id
            WHERE m.id = ?
            """,
            (machine_id,),
            readonly=True,
        )
        if machine is None:
            raise ValueError(f"Machine {machine_id} not found")

        rows: list[dict[str, Any]] = []
        if self._run_id is not None:
            rows = query_all(
                self.dbs.analysis_db,
                """
                SELECT COALESCE(NULLIF(TRIM(product_group), ''), 'unknown') AS product_group,
                       COUNT(*) AS tx_count,
                       ROUND(SUM(amount), 2) AS revenue,
                       ROUND(AVG(amount), 4) AS avg_price,
                       ROUND(AVG(expected_price), 4) AS avg_expected_price
                FROM sim_transaction_expanded
                WHERE run_id = ?
                  AND machine_id = ?
                  AND date = ?
                GROUP BY COALESCE(NULLIF(TRIM(product_group), ''), 'unknown')
                ORDER BY revenue DESC, tx_count DESC, product_group
                """,
                (self._run_id, machine_id, current_day.isoformat()),
                readonly=True,
            )

        grouped: list[dict[str, Any]] = []
        total_tx = 0
        total_revenue = 0.0
        currency = self._location_currency().get(int(machine["location_id"]), "USD")
        for row in rows:
            tx_count = int(row.get("tx_count") or 0)
            revenue = float(row.get("revenue") or 0.0)
            total_tx += tx_count
            total_revenue += revenue
            grouped.append(
                {
                    "product_group": str(row.get("product_group") or "unknown"),
                    "tx_count": tx_count,
                    "revenue": round(revenue, 2),
                    "avg_price": (
                        float(row["avg_price"])
                        if row.get("avg_price") is not None
                        else None
                    ),
                    "avg_expected_price": (
                        float(row["avg_expected_price"])
                        if row.get("avg_expected_price") is not None
                        else None
                    ),
                }
            )

        return {
            "date": current_day.isoformat(),
            "machine_id": int(machine["machine_id"]),
            "machine_name": str(machine["machine_name"]),
            "location_id": int(machine["location_id"]),
            "location_name": str(machine["location_name"]),
            "currency": currency,
            "totals": {
                "tx_count": total_tx,
                "revenue": round(total_revenue, 2),
            },
            "groups": grouped,
        }

    def get_inventory(self) -> dict[str, Any]:
        """Return current day start inventory and next-day start as end-of-day proxy."""
        with Session(self.sql_engine) as session:
            state = session.exec(select(EngineState).where(EngineState.id == 1)).one()
            current_day = state.current_day

        next_day = current_day + timedelta(days=1)
        self._ensure_inventory_through_day(target_day=next_day)

        location_currency = self._location_currency()
        machines = self._facts_machines()
        capacities = self._facts_capacities()
        caps_by_model_ing: dict[tuple[str, int], dict[str, Any]] = {
            (str(r["machine_model"]), int(r["ingredient_id"])): r for r in capacities
        }
        machine_meta = {int(m["machine_id"]): m for m in machines}

        with Session(self.sql_engine) as session:
            start_rows = session.exec(
                select(InventoryState).where(InventoryState.date == current_day)
            ).all()
            end_rows = session.exec(
                select(InventoryState).where(InventoryState.date == next_day)
            ).all()
            actions = session.exec(
                select(ManagerAction)
                .where(ManagerAction.effective_date == next_day)
                .where(ManagerAction.action_type == str(ActionType.RESTOCK_MACHINE))
            ).all()
        restock_machine_ids = {int(a.machine_id) for a in actions}

        by_key_start: dict[tuple[int, int], InventoryState] = {
            (int(r.machine_id), int(r.ingredient_id)): r for r in start_rows
        }
        by_key_end: dict[tuple[int, int], InventoryState] = {
            (int(r.machine_id), int(r.ingredient_id)): r for r in end_rows
        }

        projected_end_by_key: dict[tuple[int, int], float] = {}
        consumption_by_machine: dict[int, dict[int, tuple[float, str]]] = {}
        for (mid, iid), r in by_key_start.items():
            qty = float(r.quantity_on_hand)
            if self._run_id is not None:
                consumption = consumption_by_machine.get(mid)
                if consumption is None:
                    consumption = self._predicted_consumption_for_day(
                        run_id=self._run_id, machine_id=mid, day=current_day
                    )
                    consumption_by_machine[mid] = consumption
                used, _unit = consumption.get(iid, (0.0, str(r.unit)))
                qty = max(0.0, qty - float(used))

            if mid in restock_machine_ids:
                m = machine_meta.get(mid)
                model = str(m["machine_model"]) if m else ""
                cap = caps_by_model_ing.get((model, iid))
                if cap is not None and str(cap.get("unit") or "") == str(r.unit):
                    qty = float(cap["capacity"])

            projected_end_by_key[(mid, iid)] = qty

        locations: dict[int, dict[str, Any]] = {}
        for mid, m in machine_meta.items():
            loc_id = int(m["location_id"])
            loc = locations.setdefault(
                loc_id,
                {
                    "location_id": loc_id,
                    "location_name": m["location_name"],
                    "machines": {},
                },
            )
            loc["machines"][mid] = {
                "machine_id": mid,
                "machine_name": m["machine_name"],
                "ingredients": [],
            }

        for mid, m in machine_meta.items():
            model = str(m["machine_model"])
            machine_out = locations[int(m["location_id"])]["machines"][mid]
            for (cap_model, iid), cap in caps_by_model_ing.items():
                if cap_model != model:
                    continue
                key = (mid, iid)
                s = by_key_start.get(key)
                start_qty = float(s.quantity_on_hand) if s else 0.0
                e = by_key_end.get(key)
                end_qty = (
                    projected_end_by_key.get(key)
                    if key in projected_end_by_key
                    else (float(e.quantity_on_hand) if e else start_qty)
                )
                machine_out["ingredients"].append(
                    {
                        "ingredient_id": iid,
                        "name": cap.get("ingredient_name"),
                        "quantity": round(start_qty, 1),
                        "unit": cap.get("unit"),
                        "capacity": round(float(cap["capacity"]), 1)
                        if cap.get("capacity") is not None
                        else None,
                        "capacity_unit": cap.get("unit"),
                        "start_quantity": round(start_qty, 1),
                        "end_quantity": round(float(end_qty), 1)
                        if end_qty is not None
                        else None,
                    }
                )

            machine_out["ingredients"] = sorted(
                machine_out["ingredients"], key=lambda r: str(r.get("name") or "")
            )

        result_locations = []
        for loc in sorted(locations.values(), key=lambda x: x["location_id"]):
            result_locations.append(
                {
                    "location_id": loc["location_id"],
                    "location_name": loc["location_name"],
                    "currency": location_currency.get(loc["location_id"], "USD"),
                    "machines": sorted(
                        loc["machines"].values(), key=lambda x: x["machine_id"]
                    ),
                }
            )

        return {"snapshot_date": current_day.isoformat(), "locations": result_locations}

    def _location_currency(self) -> dict[int, str]:
        rows = query_all(
            self.dbs.observed_db,
            """
            SELECT location_id, currency
            FROM (
                SELECT location_id,
                       currency,
                       ROW_NUMBER() OVER (
                           PARTITION BY location_id
                           ORDER BY date DESC, id DESC
                       ) AS rn
                FROM "transaction"
            )
            WHERE rn = 1
            """,
            readonly=True,
        )
        return {int(row["location_id"]): str(row["currency"]) for row in rows}

    def run_backtest(self, *, start_day: date, end_day: date) -> list[dict[str, Any]]:
        if end_day < start_day:
            raise ValueError("end_day must be >= start_day")

        outputs = []
        cursor = start_day
        while cursor <= end_day:
            summary = self.run_for_day(cursor)
            outputs.append(
                {
                    "run_date": summary.run_date,
                    "executed_scripts": summary.executed_scripts,
                    "emitted_alerts": summary.emitted_alerts,
                }
            )
            cursor += timedelta(days=1)
        return outputs
