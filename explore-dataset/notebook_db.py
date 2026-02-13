from __future__ import annotations

import sqlite3
from dataclasses import dataclass
import os
from pathlib import Path
from typing import Any

import polars as pl


@dataclass(frozen=True)
class VendingDbPaths:
    facts_db: Path
    observed_db: Path
    analysis_db: Path


def default_vending_db_dir() -> Path:
    repo_root = Path(__file__).resolve().parents[1]
    return repo_root / "db"


def resolve_vending_db_paths(*, db_dir: Path | str | None = None) -> VendingDbPaths:
    if db_dir is None:
        db_dir = os.environ.get("VENDING_DB_DIR") or default_vending_db_dir()
    db_dir = Path(db_dir).expanduser().resolve()
    return VendingDbPaths(
        facts_db=db_dir / "vending_machine_facts.db",
        observed_db=db_dir / "vending_sales_observed.db",
        analysis_db=db_dir / "vending_analysis.db",
    )


def sqlite_conn(db_path: Path | str, *, readonly: bool = True) -> sqlite3.Connection:
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

    if readonly:
        conn.execute("PRAGMA query_only = ON")
    db_path = Path(db_path).resolve()
    if readonly and not db_path.exists():
        raise FileNotFoundError(str(db_path))

    attach_target = f"file:{db_path.as_posix()}?mode=ro" if readonly else str(db_path)
    conn.execute(f'ATTACH DATABASE ? AS "{alias}"', (attach_target,))


def query_df(
    db_path: Path | str,
    query: str,
    params: tuple[Any, ...] = (),
    *,
    attachments: dict[str, Path | str] | None = None,
    readonly: bool = True,
) -> pl.DataFrame:
    with sqlite_conn(db_path, readonly=readonly) as conn:
        if attachments:
            for alias, path in attachments.items():
                _attach_database(conn, alias=alias, db_path=path, readonly=readonly)
        rows = conn.execute(query, params).fetchall()

    if not rows:
        return pl.DataFrame()
    return pl.DataFrame([dict(row) for row in rows])


def query_one(
    db_path: Path | str,
    query: str,
    params: tuple[Any, ...] = (),
    *,
    attachments: dict[str, Path | str] | None = None,
    readonly: bool = True,
) -> dict[str, Any] | None:
    with sqlite_conn(db_path, readonly=readonly) as conn:
        if attachments:
            for alias, path in attachments.items():
                _attach_database(conn, alias=alias, db_path=path, readonly=readonly)
        row = conn.execute(query, params).fetchone()
    return dict(row) if row else None


def latest_sim_run_id(dbs: VendingDbPaths) -> str | None:
    row = query_one(
        dbs.analysis_db,
        "SELECT id FROM sim_run ORDER BY created_at DESC LIMIT 1",
        readonly=True,
    )
    return str(row["id"]) if row and row.get("id") else None


def latest_projection_date(
    dbs: VendingDbPaths,
    *,
    run_id: str,
    location_id: int | None = None,
    machine_id: int | None = None,
) -> str | None:
    clauses = ["run_id = ?"]
    params: list[Any] = [run_id]
    if location_id is not None:
        clauses.append("location_id = ?")
        params.append(location_id)
    if machine_id is not None:
        clauses.append("machine_id = ?")
        params.append(machine_id)

    where_sql = " AND ".join(clauses)
    row = query_one(
        dbs.analysis_db,
        f"SELECT MAX(projection_date) AS projection_date FROM sim_daily_projection WHERE {where_sql}",
        tuple(params),
        readonly=True,
    )
    if row is None or row.get("projection_date") is None:
        return None
    return str(row["projection_date"])
