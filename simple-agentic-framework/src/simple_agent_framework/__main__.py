from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

import uvicorn
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

from .api import create_app
from .demo_data import inject_synthetic_week
from .engine import DailyAlertEngine


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Daily alert engine")
    parser.add_argument("--data-db", default="coffee.db", help="Path to data SQLite DB (read-only)")
    parser.add_argument("--state-db", default="agent.db", help="Path to agent state SQLite DB")
    # Keep --db as hidden alias for backwards compat
    parser.add_argument("--db", default=None, help=argparse.SUPPRESS)

    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("run-current")
    sub.add_parser("advance")

    inject = sub.add_parser("inject-demo-week")
    inject.add_argument("--start-day", required=True)

    api = sub.add_parser("serve")
    api.add_argument("--host", default="127.0.0.1")
    api.add_argument("--port", type=int, default=8000)

    gen = sub.add_parser("generate-script")
    gen.add_argument("description", help="Natural language description of the alert script to generate")
    gen.add_argument("--output-dir", default=None, help="Directory to save generated script")

    return parser


def _resolve_db_paths(args: argparse.Namespace) -> tuple[str, str]:
    """Return (data_db, state_db) respecting --db legacy alias."""
    data_db = args.data_db
    state_db = args.state_db
    if args.db is not None:
        data_db = args.db
    return data_db, state_db


def main() -> int:
    args = build_parser().parse_args()
    data_db, state_db = _resolve_db_paths(args)

    if args.command == "serve":
        uvicorn.run(
            create_app(data_db=data_db, state_db=state_db),
            host=args.host,
            port=args.port,
        )
        return 0

    if args.command == "generate-script":
        return _generate_script(args)

    if args.command == "inject-demo-week":
        start_day = date.fromisoformat(args.start_day)
        inserted = inject_synthetic_week(db_path=data_db, start_day=start_day)
        logger.info("inserted_rows={}", inserted)
        return 0

    engine = DailyAlertEngine(data_db=data_db, state_db=state_db)

    if args.command == "run-current":
        summary = engine.run_current_day()
        logger.info("{}", summary)
        return 0

    if args.command == "advance":
        result = engine.advance_day()
        logger.info("{}", result)
        return 0

    return 1


def _generate_script(args: argparse.Namespace) -> int:
    from .script_prompt import build_generation_prompt
    from .script_registry import SCRIPTS_DIR

    prompt = build_generation_prompt(args.description)

    try:
        import litellm
    except ImportError:
        logger.error("litellm is required for script generation: uv add litellm")
        return 1

    logger.info("Generating script for: {}", args.description)
    response = litellm.completion(
        model="anthropic/claude-sonnet-4-20250514",
        messages=[{"role": "user", "content": prompt}],
        max_tokens=2048,
    )
    raw = response.choices[0].message.content

    # Extract code from markdown fences if present
    code = raw
    if "```python" in raw:
        code = raw.split("```python", 1)[1].split("```", 1)[0]
    elif "```" in raw:
        code = raw.split("```", 1)[1].split("```", 1)[0]
    code = code.strip() + "\n"

    # Derive filename from description
    slug = args.description.lower()
    for ch in " -/,.;:!?'\"()[]{}":
        slug = slug.replace(ch, "_")
    while "__" in slug:
        slug = slug.replace("__", "_")
    slug = slug.strip("_")[:60]

    output_dir = Path(args.output_dir) if args.output_dir else SCRIPTS_DIR
    output_path = output_dir / f"{slug}.py"
    output_path.write_text(code, encoding="utf-8")
    logger.info("Generated script saved to: {}", output_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
