from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from loguru import logger
from sqlalchemy import desc
from sqlmodel import Session, select

from .db import ensure_agent_schema, fetch_all, fetch_one, make_engine
from .llm_review import review_alert_with_ai
from .models import Alert, EngineState, MachineInventory, ScriptVersion
from .script_context import build_script_context
from .script_registry import discover_scripts
from .script_runner import ScriptExecutionError, run_script
from .time_utils import utc_now


@dataclass(frozen=True)
class RunSummary:
    run_date: str
    executed_scripts: int
    emitted_alerts: int


class DailyAlertEngine:
    def __init__(
        self,
        data_db: str | Path = "coffee.db",
        state_db: str | Path = "agent.db",
        cooldown_hours: int = 24,
    ) -> None:
        self.data_db = Path(data_db)
        self.state_db = Path(state_db)
        self.cooldown_hours = cooldown_hours

        ensure_agent_schema(self.state_db)
        self.sql_engine = make_engine(self.state_db)

        self._ensure_state()

    def _ensure_state(self) -> None:
        tx_range = fetch_one(
            self.data_db,
            'SELECT MIN(date) AS min_date, MAX(date) AS max_date FROM "transaction"',
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
            state = session.exec(select(EngineState).where(EngineState.id == 1)).one()
            state.current_day = state.start_day
            state.updated_at = utc_now()
            session.add(state)
            session.commit()
        return self.get_state()

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
        return self.run_for_day(run_day)

    def advance_day(self) -> dict[str, Any]:
        with Session(self.sql_engine) as session:
            state = session.exec(select(EngineState).where(EngineState.id == 1)).one()
            run_day = state.current_day

        summary = self.run_for_day(run_day)

        with Session(self.sql_engine) as session:
            state = session.exec(select(EngineState).where(EngineState.id == 1)).one()
            if state.current_day < state.end_day:
                state.current_day = state.current_day + timedelta(days=1)
            state.updated_at = utc_now()
            session.add(state)
            session.commit()
            updated = {
                "start_day": state.start_day.isoformat(),
                "end_day": state.end_day.isoformat(),
                "current_day": state.current_day.isoformat(),
            }

        return {
            "state": updated,
            "summary": {
                "run_date": summary.run_date,
                "executed_scripts": summary.executed_scripts,
                "emitted_alerts": summary.emitted_alerts,
            },
        }

    def run_for_day(self, run_day: date) -> RunSummary:
        machine_rows = fetch_all(
            self.data_db,
            """
            SELECT m.id AS machine_id, m.location_id AS location_id,
                   COALESCE((
                       SELECT t.currency
                       FROM transactions t
                       WHERE t.location_id = m.location_id
                       ORDER BY t.date DESC
                       LIMIT 1
                   ), 'USD') AS currency
            FROM machines m
            ORDER BY m.location_id, m.id
            """,
        )

        scripts = discover_scripts()

        alerts_created = 0
        script_count = 0
        for row in machine_rows:
            location_id = int(row["location_id"])
            machine_id = int(row["machine_id"])
            currency = str(row["currency"])

            context = build_script_context(
                data_db=self.data_db,
                as_of_date=run_day,
                location_id=location_id,
                machine_id=machine_id,
                currency=currency,
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
                    if self._persist_alert(
                        run_day=run_day,
                        script_name=script_name,
                        script_version=script_version,
                        payload=alert_payload,
                    ):
                        alerts_created += 1

        self.sync_inventory(run_day)

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

    def _persist_alert(
        self,
        *,
        run_day: date,
        script_name: str,
        script_version: str,
        payload: dict[str, Any],
    ) -> bool:
        location_id = int(payload["location_id"])
        machine_id = payload.get("machine_id")
        product_id = payload.get("product_id")
        ingredient_id = payload.get("ingredient_id")
        alert_type = str(payload["alert_type"])

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
        evidence_json = json.dumps(payload.get("evidence", {}), sort_keys=True)
        evidence_hash = hashlib.sha256(evidence_json.encode("utf-8")).hexdigest()
        cooldown_since = utc_now() - timedelta(hours=self.cooldown_hours)

        with Session(self.sql_engine) as session:
            existing = session.exec(
                select(Alert)
                .where(Alert.fingerprint == fingerprint)
                .where(Alert.status == "OPEN")
                .where(Alert.created_at >= cooldown_since)
                .order_by(desc(Alert.created_at))
            ).first()
            if existing and existing.evidence_hash == evidence_hash:
                return False

            alert = Alert(
                run_date=run_day,
                script_name=script_name,
                script_version=script_version,
                fingerprint=fingerprint,
                evidence_hash=evidence_hash,
                severity=str(payload.get("severity", "MEDIUM")),
                alert_type=alert_type,
                location_id=location_id,
                machine_id=int(machine_id) if machine_id is not None else None,
                product_id=int(product_id) if product_id is not None else None,
                ingredient_id=int(ingredient_id) if ingredient_id is not None else None,
                title=str(payload.get("title", "Untitled alert")),
                summary=str(payload.get("summary", "")),
                evidence_json=evidence_json,
                recommended_actions_json=json.dumps(
                    payload.get("recommended_actions", []), sort_keys=True
                ),
                status="OPEN",
            )
            session.add(alert)
            session.commit()
        return True

    def list_alerts(
        self,
        *,
        status: str | None = None,
        location_id: int | None = None,
        include_snoozed: bool = False,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
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

        rows = fetch_all(self.state_db, query, tuple(params))
        for row in rows:
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
            alert.status = "RESOLVED"
            alert.decision = "ACCEPT_ACTION"
            alert.decision_note = decision_note
            alert.decided_at = utc_now()
            session.add(alert)
            session.commit()
            session.refresh(alert)
            return {
                "alert_id": alert.alert_id,
                "status": alert.status,
                "decision": alert.decision,
            }

    def snooze_alert(self, alert_id: str, days: int) -> dict[str, Any]:
        with Session(self.sql_engine) as session:
            alert = session.exec(
                select(Alert).where(Alert.alert_id == alert_id)
            ).first()
            if alert is None:
                raise ValueError(f"Alert {alert_id} not found")
            alert.status = "SNOOZED"
            alert.snoozed_until = utc_now() + timedelta(days=days)
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
                "evidence": json.loads(alert.evidence_json),
            }
            related_payloads = [
                {
                    "alert_id": item.alert_id,
                    "created_at": item.created_at.isoformat(timespec="seconds"),
                    "summary": item.summary,
                }
                for item in related
            ]
            review = review_alert_with_ai(
                alert=payload,
                related_open_alerts=related_payloads,
                manager_note=manager_note,
            )

            alert.status = "ACKNOWLEDGED"
            alert.decision = "REVIEW_BY_AI"
            alert.decision_note = manager_note
            alert.feedback_loop_id = review["feedback_loop_id"]
            alert.decided_at = utc_now()
            session.add(alert)
            session.commit()

            return review

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

        revenue_rows = fetch_all(
            self.data_db,
            f"""
            SELECT t.location_id,
                   t.date,
                   ROUND(SUM(t.amount), 2) AS revenue,
                   COUNT(*) AS tx_count
            FROM transactions t
            WHERE t.date BETWEEN ? AND ? {location_clause}
            GROUP BY t.location_id, t.date
            ORDER BY t.location_id, t.date
            """,
            tuple(params),
        )

        alert_rows = fetch_all(
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

        return {
            "start_day": start_day.isoformat(),
            "end_day": end_day.isoformat(),
            "daily_revenue": revenue_rows,
            "top_alert_patterns": alert_rows,
        }

    def sync_inventory(self, as_of: date) -> int:
        """Copy inventory_snapshots from coffee.db into agent.db for the given date.

        Uses the closest available snapshot_date <= as_of.  Returns the number
        of rows written.
        """
        # Find the closest snapshot date that is <= as_of
        closest = fetch_one(
            self.data_db,
            """
            SELECT MAX(snapshot_date) AS snap_date
            FROM inventory_snapshots
            WHERE snapshot_date <= ?
            """,
            (as_of.isoformat(),),
        )
        snap_date = closest["snap_date"] if closest else None
        if snap_date is None:
            # Fall back to the earliest available snapshot
            earliest = fetch_one(
                self.data_db,
                "SELECT MIN(snapshot_date) AS snap_date FROM inventory_snapshots",
            )
            snap_date = earliest["snap_date"] if earliest else None
        if snap_date is None:
            return 0

        rows = fetch_all(
            self.data_db,
            """
            SELECT s.snapshot_date, s.machine_id, s.ingredient_id,
                   s.quantity_on_hand, s.unit,
                   m.name AS machine_name, m.location_id,
                   l.name AS location_name,
                   i.name AS ingredient_name
            FROM inventory_snapshots s
            JOIN machine m ON m.id = s.machine_id
            JOIN location l ON l.id = m.location_id
            JOIN ingredient i ON i.id = s.ingredient_id
            WHERE s.snapshot_date = ?
            ORDER BY m.location_id, m.id, i.name
            """,
            (snap_date,),
        )

        with Session(self.sql_engine) as session:
            # Clear previous snapshot for this date
            session.exec(  # type: ignore[call-overload]
                select(MachineInventory).where(
                    MachineInventory.snapshot_date == as_of
                )
            )
            existing = session.exec(
                select(MachineInventory).where(
                    MachineInventory.snapshot_date == as_of
                )
            ).all()
            for item in existing:
                session.delete(item)

            for row in rows:
                inv = MachineInventory(
                    snapshot_date=as_of,
                    location_id=int(row["location_id"]),
                    location_name=str(row["location_name"]),
                    machine_id=int(row["machine_id"]),
                    machine_name=str(row["machine_name"]),
                    ingredient_id=int(row["ingredient_id"]),
                    ingredient_name=str(row["ingredient_name"]),
                    quantity_on_hand=float(row["quantity_on_hand"]),
                    unit=str(row["unit"]),
                )
                session.add(inv)
            session.commit()

        logger.info("Synced {} inventory rows for {}", len(rows), as_of.isoformat())
        return len(rows)

    def get_inventory(self) -> dict[str, Any]:
        """Return the latest inventory snapshot from agent.db, grouped by location."""
        with Session(self.sql_engine) as session:
            state = session.exec(select(EngineState).where(EngineState.id == 1)).one()
            current_day = state.current_day

        # Try current_day, then fall back to latest available in agent.db
        rows = fetch_all(
            self.state_db,
            """
            SELECT * FROM machine_inventory
            WHERE snapshot_date = ?
            ORDER BY location_id, machine_id, ingredient_name
            """,
            (current_day.isoformat(),),
        )
        if not rows:
            rows = fetch_all(
                self.state_db,
                """
                SELECT * FROM machine_inventory
                WHERE snapshot_date = (
                    SELECT MAX(snapshot_date) FROM machine_inventory
                )
                ORDER BY location_id, machine_id, ingredient_name
                """,
            )

        snap_date = rows[0]["snapshot_date"] if rows else current_day.isoformat()

        # Group: location → machines → ingredients
        locations: dict[int, dict[str, Any]] = {}
        for row in rows:
            loc_id = int(row["location_id"])
            if loc_id not in locations:
                locations[loc_id] = {
                    "location_id": loc_id,
                    "location_name": row["location_name"],
                    "machines": {},
                }
            machines = locations[loc_id]["machines"]
            mid = int(row["machine_id"])
            if mid not in machines:
                machines[mid] = {
                    "machine_id": mid,
                    "machine_name": row["machine_name"],
                    "ingredients": [],
                }
            machines[mid]["ingredients"].append({
                "ingredient_id": int(row["ingredient_id"]),
                "name": row["ingredient_name"],
                "quantity": round(float(row["quantity_on_hand"]), 1),
                "unit": row["unit"],
            })

        # Flatten machines dict to list
        result = []
        for loc in sorted(locations.values(), key=lambda x: x["location_id"]):
            loc_out = {
                "location_id": loc["location_id"],
                "location_name": loc["location_name"],
                "machines": sorted(loc["machines"].values(), key=lambda x: x["machine_id"]),
            }
            result.append(loc_out)

        return {
            "snapshot_date": str(snap_date),
            "locations": result,
        }

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
