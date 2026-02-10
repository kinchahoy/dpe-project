from __future__ import annotations

import json
from typing import Any, Literal, cast

from pydantic import BaseModel, Field
from pydantic_ai import Agent
from pydantic_ai.models.openai import OpenAIResponsesModel, OpenAIResponsesModelSettings


class ScriptDraftFinalCheck(BaseModel):
    recommended_action: Literal["accept_draft", "try_again"] = Field(
        description="Choose exactly one: accept_draft or try_again"
    )
    rationale: str = Field(
        description="Short evidence-based rationale referencing comparison metrics"
    )
    retry_instruction: str | None = Field(
        default=None,
        description="If try_again, one concrete instruction for the next edit attempt",
    )


SYSTEM_PROMPT = """\
You are reviewing a candidate alert-script edit for a vending simulation.
You must choose exactly one recommendation:
- accept_draft
- try_again

Decision policy:
- Prefer accept_draft when the new script has equal/better signal quality and no obvious risk increase.
- Prefer try_again when the new script appears noisier, less actionable, or likely to miss important patterns.
- Base reasoning on the provided old/new code and historical trigger comparison metrics.

Output must be concise and practical.
If you choose try_again, include a concrete retry_instruction.
"""

_agent: Agent[None, ScriptDraftFinalCheck] | None = None


def _get_agent() -> Agent[None, ScriptDraftFinalCheck]:
    global _agent
    if _agent is None:
        model = OpenAIResponsesModel("gpt-5.2")
        settings = OpenAIResponsesModelSettings(
            openai_reasoning_effort="medium",
            openai_reasoning_summary="concise",
        )
        _agent = cast(
            Agent[None, ScriptDraftFinalCheck],
            Agent(
                model,
                instructions=SYSTEM_PROMPT,
                output_type=ScriptDraftFinalCheck,
                model_settings=settings,
            ),
        )
    assert _agent is not None
    return _agent


def final_check_script_draft_with_ai(
    *,
    script_name: str,
    edit_instruction: str,
    old_code: str,
    new_code: str,
    comparison: dict[str, Any],
) -> dict[str, Any]:
    prompt = "\n\n".join(
        [
            f"## Script name\n{script_name}",
            f"## Edit instruction\n{edit_instruction or '(none provided)'}",
            f"## Old script\n```python\n{old_code}\n```",
            f"## New draft script\n```python\n{new_code}\n```",
            f"## Historical comparison\n```json\n{json.dumps(comparison, indent=2, default=str)}\n```",
        ]
    )
    result = _get_agent().run_sync(prompt)
    out: ScriptDraftFinalCheck = result.output
    return out.model_dump(mode="json")
