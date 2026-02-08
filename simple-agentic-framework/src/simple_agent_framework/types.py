from __future__ import annotations

from enum import StrEnum


class AlertStatus(StrEnum):
    OPEN = "OPEN"
    ACKNOWLEDGED = "ACKNOWLEDGED"
    SNOOZED = "SNOOZED"
    RESOLVED = "RESOLVED"
    DISMISSED = "DISMISSED"


class NextStep(StrEnum):
    LLM_REVIEW = "llm_review"
    MANAGER_INBOX = "manager_inbox"


class Severity(StrEnum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class ActionType(StrEnum):
    RESTOCK_MACHINE = "RESTOCK_MACHINE"
    ORDER_INGREDIENTS = "ORDER_INGREDIENTS"
    ADJUST_PRICE = "ADJUST_PRICE"
    SCHEDULE_SERVICE = "SCHEDULE_SERVICE"
    CHECK_MACHINE = "CHECK_MACHINE"
    PROPOSE_DISCONTINUE = "PROPOSE_DISCONTINUE"
    DEBUG_LLM_CALL = "DEBUG_LLM_CALL"
