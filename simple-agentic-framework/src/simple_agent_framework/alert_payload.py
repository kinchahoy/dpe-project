from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from .types import ActionType, Severity


class RecommendedAction(BaseModel):
    action_type: ActionType
    params: dict[str, Any] = Field(default_factory=dict)


class AlertPayload(BaseModel):
    alert_type: str
    severity: Severity
    title: str
    summary: str
    evidence: dict[str, Any] = Field(default_factory=dict)
    recommended_actions: list[RecommendedAction] = Field(
        default_factory=list, max_length=3
    )
    location_id: int
    machine_id: int | None = None
    product_id: int | None = None
    ingredient_id: int | None = None
