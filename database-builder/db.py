from __future__ import annotations

from datetime import date as Date
from datetime import datetime, time
from pathlib import Path
from typing import Optional

from sqlalchemy import MetaData, text
from sqlmodel import Field, SQLModel, create_engine

ARTIFACTS_DIR = Path("artifacts")

FACTS_DB_FILE = ARTIFACTS_DIR / "vending_machine_facts.db"
OBSERVED_DB_FILE = ARTIFACTS_DIR / "vending_sales_observed.db"
SIM_DB_FILE = ARTIFACTS_DIR / "vending_analysis.db"

facts_metadata = MetaData()
observed_metadata = MetaData()
sim_metadata = MetaData()


class FactsBase(SQLModel):
    metadata = facts_metadata


class ObservedBase(SQLModel):
    metadata = observed_metadata


class SimBase(SQLModel):
    metadata = sim_metadata


# ----------------------------
# Facts DB (stable dimensions)
# ----------------------------


class Location(FactsBase, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    external_id: str = Field(index=True, unique=True)
    timezone: str
    region: str
    address: str


class Machine(FactsBase, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    serial_number: str = Field(index=True, unique=True)
    model: str = Field(index=True)
    installed_at: datetime
    last_serviced_at: datetime
    current_hours: int
    location_id: int = Field(foreign_key="location.id", index=True)


class Product(FactsBase, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(index=True, unique=True)


class Ingredient(FactsBase, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(index=True, unique=True)
    unit: str


class ProductIngredient(FactsBase, table=True):
    __tablename__ = "product_ingredient"

    id: Optional[int] = Field(default=None, primary_key=True)
    product_id: int = Field(foreign_key="product.id", index=True)
    ingredient_id: int = Field(foreign_key="ingredient.id", index=True)
    quantity: float


class MachineIngredientCapacity(FactsBase, table=True):
    __tablename__ = "machine_ingredient_capacity"

    id: Optional[int] = Field(default=None, primary_key=True)
    machine_model: str = Field(index=True)
    ingredient_id: int = Field(foreign_key="ingredient.id", index=True)
    capacity: float
    unit: str
    notes: str = ""


# ------------------------------------
# Observed DB (ingested events + daily)
# ------------------------------------


class Transaction(ObservedBase, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    product_id: int = Field(index=True)
    location_id: int = Field(index=True)
    machine_id: int = Field(index=True)
    date: Date = Field(index=True)
    occurred_at: datetime = Field(index=True)
    cash_type: str = Field(index=True)
    card_token: Optional[str] = Field(default=None, index=True)
    amount: float
    currency: str = Field(index=True)
    source_file: Optional[str] = None
    source_row: int


class DailyIngredientConsumption(ObservedBase, table=True):
    __tablename__ = "daily_ingredient_consumption"

    id: Optional[int] = Field(default=None, primary_key=True)
    date: Date = Field(index=True)
    machine_id: int = Field(index=True)
    ingredient_id: int = Field(index=True)
    total_quantity: float
    unit: str


# ------------------------------------------
# Simulation DB (ephemeral, per-run artifacts)
# ------------------------------------------


class SimRun(SimBase, table=True):
    __tablename__ = "sim_run"

    id: str = Field(primary_key=True)
    created_at: datetime = Field(index=True)
    seed_start_date: Date = Field(index=True)
    seed_end_date: Date = Field(index=True)
    notes: str = ""


class SimPriceChange(SimBase, table=True):
    __tablename__ = "sim_price_change"

    id: Optional[int] = Field(default=None, primary_key=True)
    run_id: str = Field(foreign_key="sim_run.id", index=True)
    product_id: int = Field(index=True)
    location_id: int = Field(index=True)
    currency: str = Field(index=True)
    change_date: Date = Field(index=True)
    old_price: Optional[float] = None
    new_price: float
    tod_start_time: Optional[time] = None
    tod_end_time: Optional[time] = None
    tod_delta: float = 0.0


class SimTransactionExpanded(SimBase, table=True):
    __tablename__ = "sim_transaction_expanded"

    id: Optional[int] = Field(default=None, primary_key=True)
    run_id: str = Field(foreign_key="sim_run.id", index=True)
    transaction_id: int = Field(index=True)
    product_id: int = Field(index=True)
    location_id: int = Field(index=True)
    machine_id: int = Field(index=True)
    date: Date = Field(index=True)
    occurred_at: datetime = Field(index=True)
    cash_type: str = Field(index=True)
    card_token: Optional[str] = Field(default=None, index=True)
    amount: float
    expected_price: Optional[float] = Field(default=None, index=True)
    product_group: str = Field(index=True)
    currency: str = Field(index=True)
    source_file: Optional[str] = None
    source_row: int


class SimInventoryDayStart(SimBase, table=True):
    __tablename__ = "sim_inventory_day_start"

    id: Optional[int] = Field(default=None, primary_key=True)
    run_id: str = Field(foreign_key="sim_run.id", index=True)
    date: Date = Field(index=True)
    machine_id: int = Field(index=True)
    ingredient_id: int = Field(index=True)
    quantity_on_hand: float
    unit: str


class SimRefillEvent(SimBase, table=True):
    __tablename__ = "sim_refill_event"

    id: Optional[int] = Field(default=None, primary_key=True)
    run_id: str = Field(foreign_key="sim_run.id", index=True)
    occurred_at: datetime = Field(index=True)
    date: Date = Field(index=True)
    machine_id: int = Field(index=True)
    ingredient_id: int = Field(index=True)
    quantity_added: float
    unit: str
    reason: str = ""


class SimAlert(SimBase, table=True):
    __tablename__ = "sim_alert"

    id: Optional[int] = Field(default=None, primary_key=True)
    run_id: str = Field(foreign_key="sim_run.id", index=True)
    created_at: datetime = Field(index=True)
    script_name: str = Field(index=True)
    alert_type: str = Field(index=True)
    severity: str = Field(index=True)
    status: str = Field(index=True, default="OPEN")  # OPEN | CLOSED | SUPPRESSED
    location_id: int = Field(index=True)
    machine_id: Optional[int] = Field(default=None, index=True)
    evidence_json: str = "{}"
    summary: str = ""

    reviewed_at: Optional[datetime] = Field(default=None, index=True)
    assessment: Optional[str] = None
    suggested_action_type: Optional[str] = Field(default=None, index=True)
    suggested_action_reason: Optional[str] = None
    suggested_action_params_json: str = "{}"
    optional_script_change_name: Optional[str] = None
    optional_script_change_description: Optional[str] = None


class SimDailyProjection(SimBase, table=True):
    __tablename__ = "sim_daily_projection"

    id: Optional[int] = Field(default=None, primary_key=True)
    run_id: str = Field(foreign_key="sim_run.id", index=True)
    projection_date: Date = Field(index=True)
    forecast_date: Date = Field(index=True)
    training_start: Date
    training_end: Date
    location_id: int = Field(index=True)
    machine_id: int = Field(index=True)
    product_id: Optional[int] = Field(default=None, index=True)
    product_rank: Optional[int] = Field(default=None, index=True)
    is_long_tail: bool = Field(default=False, index=True)
    long_tail_proportion: float = 0.0
    forecast_units: float
    model_name: str = Field(index=True)
    used_price_data: bool = Field(default=False)


class SimDailyIngredientProjection(SimBase, table=True):
    __tablename__ = "sim_daily_ingredient_projection"

    id: Optional[int] = Field(default=None, primary_key=True)
    run_id: str = Field(foreign_key="sim_run.id", index=True)
    projection_date: Date = Field(index=True)
    forecast_date: Date = Field(index=True)
    training_start: Date
    training_end: Date
    location_id: int = Field(index=True)
    machine_id: int = Field(index=True)
    ingredient_id: int = Field(index=True)
    forecast_quantity: float
    unit: str
    model_name: str = Field(index=True)


facts_engine = create_engine(url=f"sqlite:///{FACTS_DB_FILE}")
observed_engine = create_engine(url=f"sqlite:///{OBSERVED_DB_FILE}")
sim_engine = create_engine(url=f"sqlite:///{SIM_DB_FILE}")


def _drop_view(conn, name: str) -> None:
    if conn.execute(
        text("SELECT 1 FROM sqlite_master WHERE type='view' AND name = :name"),
        {"name": name},
    ).scalar():
        conn.execute(text(f'DROP VIEW "{name}"'))


def create_facts_db() -> None:
    FACTS_DB_FILE.parent.mkdir(parents=True, exist_ok=True)
    facts_metadata.create_all(facts_engine)


def create_observed_db() -> None:
    OBSERVED_DB_FILE.parent.mkdir(parents=True, exist_ok=True)
    observed_metadata.create_all(observed_engine)
    with observed_engine.begin() as conn:
        _drop_view(conn, "daily_product_sales")
        conn.execute(
            text(
                """
                CREATE VIEW "daily_product_sales" AS
                SELECT
                    t.date AS date,
                    t.location_id AS location_id,
                    t.machine_id AS machine_id,
                    t.product_id AS product_id,
                    t.currency AS currency,
                    t.cash_type AS cash_type,
                    COUNT(*) AS units_sold,
                    SUM(t.amount) AS revenue
                FROM "transaction" AS t
                GROUP BY
                    t.date,
                    t.location_id,
                    t.machine_id,
                    t.product_id,
                    t.currency,
                    t.cash_type
                """
            )
        )


def create_sim_db() -> None:
    SIM_DB_FILE.parent.mkdir(parents=True, exist_ok=True)
    sim_metadata.create_all(sim_engine)
