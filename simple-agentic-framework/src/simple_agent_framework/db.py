from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

import polars as pl
from sqlmodel import SQLModel, create_engine


def make_sqlite_url(db_path: Path | str) -> str:
    return f"sqlite:///{Path(db_path).resolve()}"


def make_engine(db_path: Path | str):
    return create_engine(make_sqlite_url(db_path), echo=False)


def ensure_agent_schema(state_db: Path | str) -> None:
    """Create Alert, ScriptVersion, EngineState tables in the state DB only."""
    engine = make_engine(state_db)
    SQLModel.metadata.create_all(engine)


def sqlite_conn(db_path: Path | str) -> sqlite3.Connection:
    conn = sqlite3.connect(Path(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def read_frame(
    db_path: Path | str, query: str, params: tuple[Any, ...] = ()
) -> pl.DataFrame:
    with sqlite_conn(db_path) as conn:
        rows = conn.execute(query, params).fetchall()
    if not rows:
        return pl.DataFrame()
    return pl.DataFrame([dict(row) for row in rows])


def execute(db_path: Path | str, query: str, params: tuple[Any, ...] = ()) -> None:
    with sqlite_conn(db_path) as conn:
        conn.execute(query, params)
        conn.commit()


def fetch_all(
    db_path: Path | str, query: str, params: tuple[Any, ...] = ()
) -> list[dict[str, Any]]:
    with sqlite_conn(db_path) as conn:
        rows = conn.execute(query, params).fetchall()
    return [dict(row) for row in rows]


def fetch_one(
    db_path: Path | str, query: str, params: tuple[Any, ...] = ()
) -> dict[str, Any] | None:
    with sqlite_conn(db_path) as conn:
        row = conn.execute(query, params).fetchone()
    return dict(row) if row else None


def table_exists(db_path: Path | str, name: str) -> bool:
    row = fetch_one(
        db_path,
        "SELECT name FROM sqlite_master WHERE type IN ('table','view') AND name = ?",
        (name,),
    )
    return row is not None


def pick_existing_relation(db_path: Path | str, candidates: list[str]) -> str:
    for candidate in candidates:
        if table_exists(db_path, candidate):
            return candidate
    joined = ", ".join(candidates)
    raise RuntimeError(f"None of the expected relations exist: {joined}")
