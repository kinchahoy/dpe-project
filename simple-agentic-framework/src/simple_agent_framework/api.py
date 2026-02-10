from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from .engine import DailyAlertEngine


class SkipDayRequest(BaseModel):
    date: date


class SnoozeRequest(BaseModel):
    days: int = Field(default=3, ge=1, le=30)


class AcceptRequest(BaseModel):
    decision_note: str | None = None


class ReviewRequest(BaseModel):
    manager_note: str | None = None


class BacktestRequest(BaseModel):
    start_day: date
    end_day: date


class DashboardRequest(BaseModel):
    days: int = Field(default=14, ge=1, le=60)
    location_id: int | None = None


class ScriptEnabledRequest(BaseModel):
    enabled: bool


class ScriptEditRequest(BaseModel):
    instruction: str = Field(min_length=1, max_length=2000)


class ScriptActivateRequest(BaseModel):
    revision_id: str = Field(min_length=1)


class ScriptCompareRequest(BaseModel):
    revision_id: str = Field(min_length=1)


class ScriptFinalCheckRequest(BaseModel):
    revision_id: str = Field(min_length=1)
    comparison: dict[str, Any] | None = None


class RestockMachineRequest(BaseModel):
    machine_id: int


def create_app(
    db_dir: str | Path | None = None,
    state_db: str | Path = "agent.db",
    startover_on_launch: bool = True,
) -> FastAPI:
    app = FastAPI(title="Daily Alert Engine")
    engine = DailyAlertEngine(
        db_dir=db_dir,
        state_db=state_db,
    )
    if startover_on_launch:
        engine.reset_state()

    static_dir = Path(__file__).parent / "web" / "static"
    app.mount("/static", StaticFiles(directory=static_dir), name="static")

    @app.get("/", response_class=HTMLResponse)
    def index() -> str:
        return (static_dir / "index.html").read_text(encoding="utf-8")

    @app.get("/api/state")
    def get_state() -> dict[str, Any]:
        return engine.get_state()

    @app.post("/api/state/reset")
    def reset_state() -> dict[str, Any]:
        return engine.reset_state()

    @app.post("/api/state/skip")
    def skip_state(req: SkipDayRequest) -> dict[str, Any]:
        return engine.skip_to_day(req.date)

    @app.post("/api/state/next")
    def next_state() -> dict[str, Any]:
        return engine.advance_day()

    @app.post("/api/run-current")
    def run_current() -> dict[str, Any]:
        summary = engine.run_current_day()
        return {
            "run_date": summary.run_date,
            "executed_scripts": summary.executed_scripts,
            "emitted_alerts": summary.emitted_alerts,
        }

    @app.get("/api/alerts")
    def list_alerts(
        status: str | None = None,
        location_id: int | None = None,
        include_snoozed: bool = False,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        return engine.list_alerts(
            status=status,
            location_id=location_id,
            include_snoozed=include_snoozed,
            limit=limit,
        )

    @app.post("/api/alerts/{alert_id}/accept")
    def accept_alert(alert_id: str, req: AcceptRequest) -> dict[str, Any]:
        try:
            return engine.accept_alert(alert_id, req.decision_note)
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.post("/api/alerts/{alert_id}/snooze")
    def snooze_alert(alert_id: str, req: SnoozeRequest) -> dict[str, Any]:
        try:
            return engine.snooze_alert(alert_id, req.days)
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.post("/api/alerts/{alert_id}/review-ai")
    def review_alert(alert_id: str, req: ReviewRequest) -> dict[str, Any]:
        try:
            return engine.review_alert(alert_id, req.manager_note)
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.get("/api/dashboard")
    def dashboard(days: int = 14, location_id: int | None = None) -> dict[str, Any]:
        return engine.dashboard_summary(days=days, location_id=location_id)

    @app.get("/api/inventory")
    def inventory() -> dict[str, Any]:
        return engine.get_inventory()

    @app.get("/api/machine-sales")
    def machine_sales(machine_id: int) -> dict[str, Any]:
        try:
            return engine.machine_sales_by_group(machine_id=machine_id)
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.post("/api/backtest")
    def backtest(req: BacktestRequest) -> list[dict[str, Any]]:
        return engine.run_backtest(start_day=req.start_day, end_day=req.end_day)

    @app.post("/api/restock-machine")
    def restock_machine(req: RestockMachineRequest) -> dict[str, Any]:
        return engine.schedule_machine_restock(machine_id=req.machine_id)

    @app.get("/api/scripts")
    def list_scripts() -> list[dict[str, Any]]:
        return engine.list_scripts()

    @app.get("/api/scripts/{script_name}")
    def get_script(script_name: str) -> dict[str, Any]:
        try:
            return engine.get_script(script_name)
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.post("/api/scripts/{script_name}/enabled")
    def set_script_enabled(
        script_name: str, req: ScriptEnabledRequest
    ) -> dict[str, Any]:
        try:
            return engine.set_script_enabled(script_name, req.enabled)
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.post("/api/scripts/{script_name}/generate-edit")
    def generate_script_edit(
        script_name: str, req: ScriptEditRequest
    ) -> dict[str, Any]:
        try:
            return engine.generate_script_edit(script_name, req.instruction)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc

    @app.post("/api/scripts/{script_name}/compare-draft")
    def compare_script_draft(
        script_name: str, req: ScriptCompareRequest
    ) -> dict[str, Any]:
        try:
            return engine.compare_script_revision_history(script_name, req.revision_id)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc

    @app.post("/api/scripts/{script_name}/final-check")
    def final_check_script_draft(
        script_name: str, req: ScriptFinalCheckRequest
    ) -> dict[str, Any]:
        try:
            return engine.final_check_script_revision(
                script_name, req.revision_id, req.comparison
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc

    @app.post("/api/scripts/{script_name}/activate")
    def activate_script_revision(
        script_name: str, req: ScriptActivateRequest
    ) -> dict[str, Any]:
        try:
            return engine.activate_script_revision(script_name, req.revision_id)
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.post("/api/scripts/{script_name}/revert")
    def revert_script(script_name: str) -> dict[str, Any]:
        try:
            return engine.revert_script_to_baseline(script_name)
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    return app


app = create_app()
