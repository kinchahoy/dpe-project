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


def create_app(
    data_db: str | Path = "coffee.db",
    state_db: str | Path = "agent.db",
) -> FastAPI:
    app = FastAPI(title="Daily Alert Engine")
    engine = DailyAlertEngine(data_db=data_db, state_db=state_db)

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

    @app.post("/api/inventory/sync")
    def sync_inventory() -> dict[str, Any]:
        st = engine.get_state()
        current_day = date.fromisoformat(st["current_day"])
        n = engine.sync_inventory(current_day)
        return {"synced": n, "date": st["current_day"]}

    @app.post("/api/backtest")
    def backtest(req: BacktestRequest) -> list[dict[str, Any]]:
        return engine.run_backtest(start_day=req.start_day, end_day=req.end_day)

    return app


app = create_app()
