from __future__ import annotations

import ast
import re

from pydantic_ai import Agent
from pydantic_ai.models.openai import OpenAIResponsesModel, OpenAIResponsesModelSettings


SYSTEM_PROMPT = """\
You edit Python alert scripts for a vending machine monitoring system.

The script runs in a sandbox with NO imports allowed. Write plain Python only.

Constraints:
- NO imports (the sandbox blocks all imports)
- The script MUST start with a comment header: the file name on line 1,
  then 2-3 lines explaining what the script monitors and when it fires,
  written for a non-technical operations manager (no jargon)
- Set `result` to a `list` of alert dicts (empty list = no alert)
- Max ~100 lines (be concise)
- Use only `ctx` and the helper functions already used in the script
- Do NOT use keyword arguments in any function call (sandbox compatibility constraint)
- `sorted()` only supports a single positional iterable argument in this sandbox
- Return ONLY the Python code, no markdown fences, no explanation
"""

_agent: Agent[None, str] | None = None


def _get_agent() -> Agent[None, str]:
    global _agent
    if _agent is None:
        model = OpenAIResponsesModel("gpt-5.2")
        settings = OpenAIResponsesModelSettings(
            openai_reasoning_effort="medium",
            openai_reasoning_summary="concise",
        )
        _agent = Agent(
            model,
            instructions=SYSTEM_PROMPT,
            output_type=str,
            model_settings=settings,
        )
    return _agent


def _strip_markdown_fences(text: str) -> str:
    cleaned = text.strip()
    if "```" not in cleaned:
        return cleaned

    # Prefer ```python ... ```
    match = re.search(r"```python\s*(.*?)\s*```", cleaned, flags=re.DOTALL)
    if match:
        return match.group(1).strip()

    match = re.search(r"```\s*(.*?)\s*```", cleaned, flags=re.DOTALL)
    if match:
        return match.group(1).strip()

    return cleaned


def validate_sandbox_compatibility(code: str) -> None:
    try:
        tree = ast.parse(code)
    except SyntaxError as exc:
        raise ValueError(f"Edited script has invalid syntax: {exc}") from exc

    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            if node.keywords:
                func_name = "call"
                if isinstance(node.func, ast.Name):
                    func_name = node.func.id
                elif isinstance(node.func, ast.Attribute):
                    func_name = node.func.attr
                raise ValueError(
                    f"Edited script uses keyword arguments in `{func_name}(...)`, which is not supported in the sandbox"
                )
            if isinstance(node.func, ast.Name) and node.func.id == "sorted":
                if len(node.args) != 1:
                    raise ValueError(
                        "Edited script uses `sorted()` with multiple arguments; this sandbox only supports `sorted(iterable)`"
                    )


def edit_script_with_ai(
    *, script_name: str, current_code: str, instruction: str
) -> str:
    prompt = (
        f"## Script name\n{script_name}\n\n"
        f"## Current code\n```python\n{current_code}\n```\n\n"
        f"## Instruction\n{instruction}\n"
    )
    result = _get_agent().run_sync(prompt)
    edited = _strip_markdown_fences(str(result.output))
    edited = edited.strip() + "\n"

    if re.search(r"(^|\n)\s*import\s+", edited):
        raise ValueError("Edited script contains an import, which is not allowed")
    if "result" not in edited:
        raise ValueError("Edited script must set `result`")
    validate_sandbox_compatibility(edited)

    return edited
