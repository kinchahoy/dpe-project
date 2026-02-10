from __future__ import annotations

import argparse
from pathlib import Path

import uvicorn
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

from .api import create_app
from .engine import DailyAlertEngine


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Daily alert engine")
    parser.add_argument(
        "--db-dir",
        default=None,
        help="Directory containing vending SQLite DBs (defaults to database-builder/db)",
    )
    parser.add_argument(
        "--state-db", default="agent.db", help="Path to agent state SQLite DB"
    )

    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("run-current")
    sub.add_parser("advance")
    sub.add_parser("startover")

    api = sub.add_parser("serve")
    api.add_argument("--host", default="127.0.0.1")
    api.add_argument("--port", type=int, default=8000)
    api.add_argument(
        "--startover",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Reset agent state on server launch (default: true).",
    )

    gen = sub.add_parser("generate-script")
    gen.add_argument(
        "description",
        help="Natural language description of the alert script to generate",
    )
    gen.add_argument(
        "--output-dir", default=None, help="Directory to save generated script"
    )

    return parser


def main() -> int:
    args = build_parser().parse_args()

    if args.command == "serve":
        uvicorn.run(
            create_app(
                db_dir=args.db_dir,
                state_db=args.state_db,
                startover_on_launch=args.startover,
            ),
            host=args.host,
            port=args.port,
        )
        return 0

    if args.command == "generate-script":
        return _generate_script(args)

    engine = DailyAlertEngine(
        db_dir=args.db_dir,
        state_db=args.state_db,
    )

    if args.command == "run-current":
        summary = engine.run_current_day()
        logger.info("{}", summary)
        return 0

    if args.command == "advance":
        result = engine.advance_day()
        logger.info("{}", result)
        return 0

    if args.command == "startover":
        state = engine.reset_state()
        logger.info("{}", state)
        return 0

    return 1


def _generate_script(args: argparse.Namespace) -> int:
    from .script_prompt import build_generation_prompt
    from .script_registry import SCRIPTS_DIR

    prompt = build_generation_prompt(args.description)

    try:
        import litellm  # ty: ignore[unresolved-import]
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
