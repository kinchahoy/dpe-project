from __future__ import annotations

import sqlite3
from collections.abc import Mapping
from dataclasses import dataclass
import os
from pathlib import Path
from typing import Any

import polars as pl
from sqlmodel import SQLModel, create_engine


@dataclass(frozen=True)
class VendingDbPaths:
    facts_db: Path
    observed_db: Path
    analysis_db: Path


def default_vending_db_dir() -> Path:
    """Best-effort default for this repo checkout (database-builder/db)."""
    repo_root = Path(__file__).resolve().parents[3]
    return repo_root / "db"


def vending_db_paths_from_dir(db_dir: Path | str) -> VendingDbPaths:
    db_dir = Path(db_dir).expanduser().resolve()
    return VendingDbPaths(
        facts_db=db_dir / "vending_machine_facts.db",
        observed_db=db_dir / "vending_sales_observed.db",
        analysis_db=db_dir / "vending_analysis.db",
    )


def resolve_vending_db_paths(
    *,
    db_dir: Path | str | None = None,
) -> VendingDbPaths:
    if db_dir is None:
        db_dir = os.environ.get("VENDING_DB_DIR") or default_vending_db_dir()
    return vending_db_paths_from_dir(db_dir)


def make_sqlite_url(db_path: Path | str) -> str:
    return f"sqlite:///{Path(db_path).resolve()}"


def make_engine(db_path: Path | str):
    return create_engine(make_sqlite_url(db_path), echo=False)


def ensure_agent_schema(state_db: Path | str) -> None:
    """Create agent-owned tables in the state DB only."""
    engine = make_engine(state_db)
    SQLModel.metadata.create_all(engine)


def sqlite_conn(db_path: Path | str, *, readonly: bool = False) -> sqlite3.Connection:
    db_path = Path(db_path).resolve()
    if readonly:
        if not db_path.exists():
            raise FileNotFoundError(str(db_path))
        conn = sqlite3.connect(f"file:{db_path.as_posix()}?mode=ro", uri=True)
        conn.execute("PRAGMA query_only = ON")
    else:
        conn = sqlite3.connect(str(db_path), uri=False)
    conn.row_factory = sqlite3.Row
    return conn


def _attach_database(
    conn: sqlite3.Connection,
    *,
    alias: str,
    db_path: Path | str,
    readonly: bool = True,
) -> None:
    alias = alias.strip()
    if not alias or any(ch.isspace() for ch in alias):
        raise ValueError(f"Invalid sqlite attach alias: {alias!r}")

    # Note: query_only makes the whole connection read-only, so we do not rely on
    # SQLite URI 'mode=ro' working for ATTACH in every environment.
    if readonly:
        conn.execute("PRAGMA query_only = ON")
    db_path = Path(db_path).resolve()
    if readonly and not db_path.exists():
        raise FileNotFoundError(str(db_path))
    attach_target = f"file:{db_path.as_posix()}?mode=ro" if readonly else str(db_path)
    conn.execute(f'ATTACH DATABASE ? AS "{alias}"', (attach_target,))


def query_all(
    db_path: Path | str,
    query: str,
    params: tuple[Any, ...] = (),
    *,
    attachments: Mapping[str, Path | str] | None = None,
    readonly: bool = False,
) -> list[dict[str, Any]]:
    with sqlite_conn(db_path, readonly=readonly) as conn:
        if attachments:
            for alias, path in attachments.items():
                _attach_database(conn, alias=alias, db_path=path, readonly=readonly)
        rows = conn.execute(query, params).fetchall()
    return [dict(row) for row in rows]


def query_one(
    db_path: Path | str,
    query: str,
    params: tuple[Any, ...] = (),
    *,
    attachments: Mapping[str, Path | str] | None = None,
    readonly: bool = False,
) -> dict[str, Any] | None:
    with sqlite_conn(db_path, readonly=readonly) as conn:
        if attachments:
            for alias, path in attachments.items():
                _attach_database(conn, alias=alias, db_path=path, readonly=readonly)
        row = conn.execute(query, params).fetchone()
    return dict(row) if row else None


def query_df(
    db_path: Path | str,
    query: str,
    params: tuple[Any, ...] = (),
    *,
    attachments: Mapping[str, Path | str] | None = None,
    readonly: bool = False,
) -> pl.DataFrame:
    rows = query_all(db_path, query, params, attachments=attachments, readonly=readonly)
    if not rows:
        return pl.DataFrame()
    return pl.DataFrame(rows)


def execute(
    db_path: Path | str,
    query: str,
    params: tuple[Any, ...] = (),
    *,
    attachments: Mapping[str, Path | str] | None = None,
) -> None:
    with sqlite_conn(db_path, readonly=False) as conn:
        if attachments:
            for alias, path in attachments.items():
                _attach_database(conn, alias=alias, db_path=path, readonly=False)
        conn.execute(query, params)
        conn.commit()
