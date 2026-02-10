from __future__ import annotations

from pathlib import Path

MAX_SCRIPT_BUDGET = 10

SCRIPTS_DIR = Path(__file__).parent / "scripts_sandbox"


def discover_scripts(scripts_dir: Path | None = None) -> list[tuple[str, str]]:
    """Scan the scripts directory and return (name, code) tuples.

    Excludes files starting with ``_`` (e.g. ``__init__.py``).
    """
    directory = scripts_dir or SCRIPTS_DIR
    scripts: list[tuple[str, str]] = []
    for path in sorted(directory.glob("*.py")):
        if path.name.startswith("_"):
            continue
        scripts.append((path.stem, path.read_text(encoding="utf-8")))

    if len(scripts) > MAX_SCRIPT_BUDGET:
        raise RuntimeError(
            f"Script budget exceeded: {len(scripts)} > {MAX_SCRIPT_BUDGET}. "
            "Consolidate or remove an existing script before adding more."
        )
    return scripts
