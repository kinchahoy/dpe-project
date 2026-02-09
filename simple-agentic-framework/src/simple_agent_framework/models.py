from __future__ import annotations

from datetime import date, datetime
from typing import Optional
from uuid import uuid4

from sqlalchemy import Index, UniqueConstraint
from sqlmodel import Column, DateTime, Field, SQLModel, Text

from .time_utils import utc_now


class ScriptVersion(SQLModel, table=True):
    __tablename__ = "script_versions"

    id: str = Field(default_factory=lambda: str(uuid4()), primary_key=True)
    script_name: str = Field(index=True)
    script_version: str = Field(index=True)
    notes: str = Field(default="")
    created_at: datetime = Field(
        default_factory=utc_now,
        sa_column=Column(DateTime(timezone=False), nullable=False),
    )

    __table_args__ = (
        UniqueConstraint("script_name", "script_version", name="uq_script_version"),
    )


class Alert(SQLModel, table=True):
    __tablename__ = "alert"

    alert_id: str = Field(default_factory=lambda: str(uuid4()), primary_key=True)
    created_at: datetime = Field(
        default_factory=utc_now,
        sa_column=Column(DateTime(timezone=False), nullable=False, index=True),
    )
    run_date: date = Field(index=True)
    script_name: str = Field(index=True)
    script_version: str = Field(default="1.0")

    fingerprint: str = Field(index=True)
    evidence_hash: str = Field(index=True)

    severity: str = Field(index=True)
    alert_type: str = Field(index=True)
    location_id: int = Field(index=True)
    machine_id: Optional[int] = Field(default=None, index=True)
    product_id: Optional[int] = Field(default=None, index=True)
    ingredient_id: Optional[int] = Field(default=None, index=True)

    title: str
    summary: str
    evidence_json: str = Field(sa_column=Column(Text, nullable=False))
    recommended_actions_json: str = Field(sa_column=Column(Text, nullable=False))

    status: str = Field(default="OPEN", index=True)
    snoozed_until: Optional[datetime] = Field(default=None)
    decision: Optional[str] = Field(default=None)
    decision_note: Optional[str] = Field(default=None)
    decided_at: Optional[datetime] = Field(default=None)

    feedback_loop_id: Optional[str] = Field(default=None, index=True)

    __table_args__ = (
        Index(
            "ix_alert_dedup",
            "script_name",
            "alert_type",
            "location_id",
            "machine_id",
            "product_id",
            "ingredient_id",
            "status",
            "created_at",
        ),
    )


class MachineInventory(SQLModel, table=True):
    __tablename__ = "machine_inventory"

    id: int = Field(default=None, primary_key=True)
    snapshot_date: date = Field(index=True)
    location_id: int = Field(index=True)
    location_name: str = Field(default="")
    machine_id: int = Field(index=True)
    machine_name: str = Field(default="")
    ingredient_id: int
    ingredient_name: str = Field(default="")
    quantity_on_hand: float
    unit: str

    __table_args__ = (
        UniqueConstraint(
            "snapshot_date", "machine_id", "ingredient_id",
            name="uq_inventory_snapshot",
        ),
    )


class EngineState(SQLModel, table=True):
    __tablename__ = "engine_state"

    id: int = Field(default=1, primary_key=True)
    start_day: date
    end_day: date
    current_day: date
    updated_at: datetime = Field(
        default_factory=utc_now,
        sa_column=Column(DateTime(timezone=False), nullable=False),
    )
