from __future__ import annotations

import json
from typing import Any, Literal
from typing import cast
from uuid import uuid4

from pydantic import BaseModel, Field
from pydantic_ai import Agent
from pydantic_ai.models.openai import OpenAIResponsesModel, OpenAIResponsesModelSettings

from .time_utils import utc_now

ActionType = Literal[
    "RESTOCK_MACHINE",
    "ORDER_INGREDIENTS",
    "ADJUST_PRICE",
    "SCHEDULE_SERVICE",
    "CHECK_MACHINE",
    "PROPOSE_DISCONTINUE",
]


class SuggestedAction(BaseModel):
    action_type: ActionType
    reason: str = Field(description="Why this action is recommended")
    params: dict[str, Any] = Field(
        default_factory=dict, description="Action-specific parameters"
    )


class ScriptChange(BaseModel):
    script_name: str = Field(description="Name of the script to modify")
    change_description: str = Field(description="What to change and why")
    edit_instruction: str = Field(
        description="One concrete instruction to pass directly to the script-edit model"
    )


class AlertReview(BaseModel):
    assessment: str = Field(
        description="Brief assessment of the alert's validity and urgency"
    )
    suggested_action: SuggestedAction = Field(description="Recommended next action")
    script_change: ScriptChange | None = Field(
        default=None,
        description="Suggested script threshold change, if the alert pattern looks noisy",
    )


SYSTEM_PROMPT = """\
You are an operations analyst for a vending machine network.
You review automated alerts and decide what action to take.

When reviewing an alert:
1. Assess whether the alert evidence is actionable or likely noise.
2. Recommend a concrete next action for the operations team.
3. If multiple related alerts are open for the same script, consider whether \
the detection threshold should be tightened (suggest a script change).

If you suggest a script change, provide both:
- a brief human explanation (`change_description`)
- a direct implementation instruction (`edit_instruction`) for the script editor.

Be concise and specific. Reference the evidence numbers in your assessment.\
"""

_agent: Agent[None, AlertReview] | None = None


def _get_agent() -> Agent[None, AlertReview]:
    global _agent
    if _agent is None:
        model = OpenAIResponsesModel("gpt-5.2")
        settings = OpenAIResponsesModelSettings(
            openai_reasoning_effort="medium",
            openai_reasoning_summary="concise",
        )
        _agent = cast(
            Agent[None, AlertReview],
            Agent(
                model,
                instructions=SYSTEM_PROMPT,
                output_type=AlertReview,
                model_settings=settings,
            ),
        )
    assert _agent is not None
    return _agent


def _build_review_prompt(
    *,
    alert: dict[str, Any],
    related_open_alerts: list[dict[str, Any]],
    manager_note: str | None,
) -> str:
    prompt_parts = [
        f"## Alert to review\n```json\n{json.dumps(alert, indent=2, default=str)}\n```",
    ]
    if related_open_alerts:
        prompt_parts.append(
            f"## Related open alerts ({len(related_open_alerts)} total)\n"
            f"```json\n{json.dumps(related_open_alerts[:5], indent=2, default=str)}\n```"
        )
    if manager_note:
        prompt_parts.append(f"## Manager note\n{manager_note}")
    return "\n\n".join(prompt_parts)


def _format_review(review: AlertReview, alert: dict[str, Any]) -> dict[str, Any]:
    return {
        "feedback_loop_id": str(uuid4()),
        "reviewed_at": utc_now().isoformat(timespec="seconds"),
        "assessment": review.assessment,
        "new_alert_action": {
            "action_type": review.suggested_action.action_type,
            "reason": review.suggested_action.reason,
            "params": {
                "location_id": alert["location_id"],
                "machine_id": alert.get("machine_id"),
                **review.suggested_action.params,
            },
        },
        "optional_script_change": {
            "script_name": review.script_change.script_name,
            "change_hint": review.script_change.change_description,
            "edit_instruction": review.script_change.edit_instruction,
        }
        if review.script_change
        else None,
    }


def review_alert_with_ai(
    *,
    alert: dict[str, Any],
    related_open_alerts: list[dict[str, Any]],
    manager_note: str | None,
) -> dict[str, Any]:
    """Send alert to LLM for review and return structured response."""
    prompt = _build_review_prompt(
        alert=alert,
        related_open_alerts=related_open_alerts,
        manager_note=manager_note,
    )
    result = _get_agent().run_sync(prompt)
    return _format_review(result.output, alert)


