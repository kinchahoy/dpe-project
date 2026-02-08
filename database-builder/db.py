from __future__ import annotations

from datetime import date as Date
from datetime import datetime, time
from typing import Optional

from sqlmodel import Field, Relationship, SQLModel, create_engine
from sqlalchemy import text


class Location(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    external_id: str
    timezone: str
    region: str
    address: str


class Machine(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    serial_number: str
    model: str
    installed_at: datetime
    last_serviced_at: datetime
    current_hours: int
    location_id: int = Field(foreign_key="location.id")

    location: Location = Relationship()


class Product(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(index=True, unique=True)


class Ingredient(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(index=True, unique=True)
    unit: str


class ProductIngredient(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    product_id: int = Field(foreign_key="product.id", index=True)
    ingredient_id: int = Field(foreign_key="ingredient.id", index=True)
    quantity: float

    product: Product = Relationship()
    ingredient: Ingredient = Relationship()


class Transaction(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    product_id: int = Field(foreign_key="product.id", index=True)
    location_id: int = Field(foreign_key="location.id", index=True)
    machine_id: int = Field(foreign_key="machine.id", index=True)
    date: Date
    occurred_at: datetime = Field(index=True)
    cash_type: str = Field(index=True)
    card_token: Optional[str] = Field(default=None, index=True)
    amount: float
    currency: str
    source_file: Optional[str] = None
    source_row: int

    product: Product = Relationship()
    location: Location = Relationship()
    machine: Machine = Relationship()


class PriceChange(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    product_id: int = Field(foreign_key="product.id", index=True)
    location_id: int = Field(foreign_key="location.id", index=True)
    currency: str = Field(index=True)
    change_date: Date = Field(index=True)
    old_price: Optional[float] = None
    new_price: float
    tod_start_time: Optional[time] = None
    tod_end_time: Optional[time] = None
    tod_delta: float = 0.0

    product: Product = Relationship()


class TransactionExpanded(SQLModel, table=True):
    __tablename__ = "transaction_expanded"

    id: Optional[int] = Field(default=None, primary_key=True)
    transaction_id: int = Field(foreign_key="transaction.id", index=True, unique=True)
    product_id: int = Field(foreign_key="product.id", index=True)
    location_id: int = Field(foreign_key="location.id", index=True)
    machine_id: int = Field(foreign_key="machine.id", index=True)
    date: Date
    occurred_at: datetime = Field(index=True)
    cash_type: str = Field(index=True)
    card_token: Optional[str] = Field(default=None, index=True)
    amount: float
    expected_price: Optional[float] = Field(default=None, index=True)
    product_group: str = Field(index=True)
    currency: str
    source_file: Optional[str] = None
    source_row: int

    transaction: Transaction = Relationship()
    product: Product = Relationship()
    location: Location = Relationship()
    machine: Machine = Relationship()


class InventorySnapshot(SQLModel, table=True):
    __tablename__ = "inventory_snapshots"

    id: Optional[int] = Field(default=None, primary_key=True)
    snapshot_date: Date = Field(index=True)
    machine_id: int = Field(foreign_key="machine.id", index=True)
    ingredient_id: int = Field(foreign_key="ingredient.id", index=True)
    quantity_on_hand: float
    unit: str

    machine: Machine = Relationship()
    ingredient: Ingredient = Relationship()


class DailyProjection(SQLModel, table=True):
    __tablename__ = "daily_projections"

    id: Optional[int] = Field(default=None, primary_key=True)
    projection_date: Date = Field(index=True)
    forecast_date: Date = Field(index=True)
    training_start: Date
    training_end: Date
    location_id: int = Field(foreign_key="location.id", index=True)
    machine_id: int = Field(foreign_key="machine.id", index=True)
    product_id: Optional[int] = Field(
        default=None, foreign_key="product.id", index=True
    )
    product_rank: Optional[int] = Field(default=None, index=True)
    is_long_tail: bool = Field(default=False, index=True)
    long_tail_proportion: float = 0.0
    forecast_units: float
    model_name: str = Field(index=True)
    used_price_data: bool = Field(default=False)

    location: Location = Relationship()
    machine: Machine = Relationship()
    product: Product = Relationship()


class DailyIngredientProjection(SQLModel, table=True):
    __tablename__ = "daily_ingredient_projections"

    id: Optional[int] = Field(default=None, primary_key=True)
    projection_date: Date = Field(index=True)
    forecast_date: Date = Field(index=True)
    training_start: Date
    training_end: Date
    location_id: int = Field(foreign_key="location.id", index=True)
    machine_id: int = Field(foreign_key="machine.id", index=True)
    ingredient_id: int = Field(foreign_key="ingredient.id", index=True)
    forecast_quantity: float
    unit: str
    model_name: str = Field(index=True)

    location: Location = Relationship()
    machine: Machine = Relationship()
    ingredient: Ingredient = Relationship()


engine = create_engine(url="sqlite:///coffee.db")


def _drop_sqlite_object(conn, name: str) -> None:
    obj_type = conn.execute(
        text("SELECT type FROM sqlite_master WHERE name = :name"),
        {"name": name},
    ).scalar()
    if obj_type == "table":
        conn.execute(text(f'DROP TABLE "{name}"'))
    elif obj_type == "view":
        conn.execute(text(f'DROP VIEW "{name}"'))


def _sqlite_table_columns(conn, table_name: str) -> set[str]:
    rows = conn.execute(text(f'PRAGMA table_info("{table_name}")')).fetchall()
    return {row[1] for row in rows}


def _ensure_transaction_expanded_schema(conn) -> None:
    """
    Ensure `transaction_expanded` exists with the expected columns.

    This table is derived and safe to drop/rebuild.
    """
    if conn.execute(
        text(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='enhancedtransaction'"
        )
    ).scalar():
        conn.execute(text('DROP TABLE "enhancedtransaction"'))

    required = {
        "id",
        "transaction_id",
        "product_id",
        "location_id",
        "machine_id",
        "date",
        "occurred_at",
        "cash_type",
        "card_token",
        "amount",
        "expected_price",
        "product_group",
        "currency",
        "source_file",
        "source_row",
    }
    cols = _sqlite_table_columns(conn, "transaction_expanded")
    if cols and not required.issubset(cols):
        conn.execute(text('DROP TABLE "transaction_expanded"'))


def ensure_schema_views() -> None:
    """
    Create human-friendly, auto-discoverable views.

    Notes
    - Views are used for derived aggregates so we don't persist redundant tables.
    - We keep compatibility views for the old camel/concatenated table names.
    """
    with engine.begin() as conn:
        # Drop old derived aggregate tables if present (safe to regenerate).
        for name in (
            "dailyproductsales",
            "dailyingredientconsumption",
            "daily_product_sales",
            "daily_ingredient_consumption",
            "transactions",
            "products",
            "ingredients",
            "machines",
            "locations",
            "product_ingredients",
            "price_changes",
            "enhanced_transactions",
            "transaction_withprice",
            "daily_product_projections",
        ):
            _drop_sqlite_object(conn, name)

        # Clean naming aliases for core tables.
        conn.execute(text('CREATE VIEW "transactions" AS SELECT * FROM "transaction"'))
        conn.execute(text('CREATE VIEW "products" AS SELECT * FROM "product"'))
        conn.execute(text('CREATE VIEW "ingredients" AS SELECT * FROM "ingredient"'))
        conn.execute(text('CREATE VIEW "machines" AS SELECT * FROM "machine"'))
        conn.execute(text('CREATE VIEW "locations" AS SELECT * FROM "location"'))
        conn.execute(
            text(
                'CREATE VIEW "product_ingredients" AS SELECT * FROM "productingredient"'
            )
        )
        conn.execute(text('CREATE VIEW "price_changes" AS SELECT * FROM "pricechange"'))

        # Clean naming alias for product projections table.
        conn.execute(
            text(
                'CREATE VIEW "daily_product_projections" AS SELECT * FROM "daily_projections"'
            )
        )

        # Derived aggregates as views.
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
                    COUNT(*) AS units_sold
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
        conn.execute(
            text(
                """
                CREATE VIEW "daily_ingredient_consumption" AS
                SELECT
                    t.date AS date,
                    t.machine_id AS machine_id,
                    pi.ingredient_id AS ingredient_id,
                    SUM(pi.quantity) AS total_quantity,
                    ing.unit AS unit
                FROM "transaction" AS t
                JOIN "productingredient" AS pi
                    ON pi.product_id = t.product_id
                JOIN "ingredient" AS ing
                    ON ing.id = pi.ingredient_id
                GROUP BY
                    t.date,
                    t.machine_id,
                    pi.ingredient_id
                """
            )
        )

        # Compatibility view names matching old derived table names.
        # dailyproductsales historically omitted machine_id; we keep that behavior here.
        conn.execute(
            text(
                """
                CREATE VIEW "dailyproductsales" AS
                SELECT
                    date,
                    location_id,
                    product_id,
                    currency,
                    cash_type,
                    SUM(units_sold) AS units_sold
                FROM "daily_product_sales"
                GROUP BY
                    date,
                    location_id,
                    product_id,
                    currency,
                    cash_type
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE VIEW "dailyingredientconsumption" AS
                SELECT
                    date,
                    machine_id,
                    ingredient_id,
                    total_quantity,
                    unit
                FROM "daily_ingredient_consumption"
                """
            )
        )


def create_db() -> None:
    with engine.begin() as conn:
        _ensure_transaction_expanded_schema(conn)
    SQLModel.metadata.create_all(engine)
    ensure_schema_views()
