from __future__ import annotations

import csv
from dataclasses import dataclass
from glob import glob
from pathlib import Path

from product_catalog import canonicalize_product_name

PRODUCT_LIST_PATH = Path("product-list.txt")


@dataclass(frozen=True)
class IngredientSpec:
    name: str
    quantity: float
    unit: str


def write_product_list_from_csvs(
    pattern: str = "index_*.csv", output_path: Path = PRODUCT_LIST_PATH
) -> list[str]:
    products: set[str] = set()
    for f in sorted(glob(pattern)):
        with open(f, newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                products.add(canonicalize_product_name(row["coffee_name"]))
    items = sorted(products)
    output_path.write_text("\n".join(items) + ("\n" if items else ""), encoding="utf-8")
    return items


def load_product_list(path: Path = PRODUCT_LIST_PATH) -> list[str]:
    if not path.exists():
        return []
    return [
        line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line
    ]


# --- PLACEHOLDER DATA (keep all placeholders together) ---
PRODUCT_INGREDIENTS: dict[str, list[IngredientSpec]] = {
    "Americano": [
        IngredientSpec("espresso_shot", 1, "shot"),
    ],
    "Americano with Milk": [
        IngredientSpec("espresso_shot", 1, "shot"),
        IngredientSpec("milk", 30, "ml"),
    ],
    "Americano with milk": [
        IngredientSpec("espresso_shot", 1, "shot"),
        IngredientSpec("milk", 30, "ml"),
    ],
    "Cappuccino": [
        IngredientSpec("espresso_shot", 1, "shot"),
        IngredientSpec("milk", 150, "ml"),
    ],
    "Caramel": [
        IngredientSpec("espresso_shot", 1, "shot"),
        IngredientSpec("milk", 150, "ml"),
        IngredientSpec("caramel_syrup", 15, "ml"),
    ],
    "Caramel coffee": [
        IngredientSpec("espresso_shot", 1, "shot"),
        IngredientSpec("caramel_syrup", 15, "ml"),
    ],
    "Caramel with Irish whiskey": [
        IngredientSpec("espresso_shot", 1, "shot"),
        IngredientSpec("milk", 150, "ml"),
        IngredientSpec("caramel_syrup", 10, "ml"),
        IngredientSpec("whiskey", 30, "ml"),
    ],
    "Caramel with chocolate": [
        IngredientSpec("espresso_shot", 1, "shot"),
        IngredientSpec("milk", 150, "ml"),
        IngredientSpec("caramel_syrup", 10, "ml"),
        IngredientSpec("chocolate_powder", 8, "g"),
    ],
    "Caramel with milk": [
        IngredientSpec("espresso_shot", 1, "shot"),
        IngredientSpec("milk", 200, "ml"),
        IngredientSpec("caramel_syrup", 15, "ml"),
    ],
    "Chocolate": [
        IngredientSpec("chocolate_powder", 20, "g"),
        IngredientSpec("milk", 200, "ml"),
    ],
    "Chocolate with coffee": [
        IngredientSpec("espresso_shot", 1, "shot"),
        IngredientSpec("milk", 200, "ml"),
        IngredientSpec("chocolate_powder", 15, "g"),
    ],
    "Chocolate with milk": [
        IngredientSpec("chocolate_powder", 20, "g"),
        IngredientSpec("milk", 220, "ml"),
    ],
    "Cocoa": [
        IngredientSpec("chocolate_powder", 15, "g"),
        IngredientSpec("milk", 200, "ml"),
    ],
    "Coffee with Irish whiskey": [
        IngredientSpec("espresso_shot", 1, "shot"),
        IngredientSpec("whiskey", 30, "ml"),
    ],
    "Coffee with chocolate": [
        IngredientSpec("espresso_shot", 1, "shot"),
        IngredientSpec("water", 150, "ml"),
        IngredientSpec("chocolate_powder", 10, "g"),
    ],
    "Cortado": [
        IngredientSpec("espresso_shot", 1, "shot"),
        IngredientSpec("milk", 60, "ml"),
    ],
    "Double Irish whiskey": [
        IngredientSpec("espresso_shot", 1, "shot"),
        IngredientSpec("whiskey", 60, "ml"),
    ],
    "Double chocolate": [
        IngredientSpec("chocolate_powder", 30, "g"),
        IngredientSpec("milk", 220, "ml"),
    ],
    "Double espresso": [
        IngredientSpec("espresso_shot", 2, "shot"),
    ],
    "Double espresso with milk": [
        IngredientSpec("espresso_shot", 2, "shot"),
        IngredientSpec("milk", 120, "ml"),
    ],
    "Double ristretto": [
        IngredientSpec("espresso_shot", 2, "shot"),
    ],
    "Double vanilla": [
        IngredientSpec("espresso_shot", 1, "shot"),
        IngredientSpec("vanilla_syrup", 30, "ml"),
    ],
    "Espresso": [
        IngredientSpec("espresso_shot", 1, "shot"),
    ],
    "Hot Chocolate": [
        IngredientSpec("chocolate_powder", 25, "g"),
        IngredientSpec("milk", 220, "ml"),
    ],
    "Hot milkshake": [
        IngredientSpec("milk", 250, "ml"),
        IngredientSpec("chocolate_powder", 10, "g"),
    ],
    "Irish whiskey": [
        IngredientSpec("espresso_shot", 1, "shot"),
        IngredientSpec("whiskey", 30, "ml"),
    ],
    "Irish whiskey with milk": [
        IngredientSpec("espresso_shot", 1, "shot"),
        IngredientSpec("milk", 200, "ml"),
        IngredientSpec("whiskey", 30, "ml"),
    ],
    "Irish with chocolate": [
        IngredientSpec("espresso_shot", 1, "shot"),
        IngredientSpec("milk", 200, "ml"),
        IngredientSpec("whiskey", 30, "ml"),
        IngredientSpec("chocolate_powder", 10, "g"),
    ],
    "Latte": [
        IngredientSpec("espresso_shot", 1, "shot"),
        IngredientSpec("milk", 200, "ml"),
    ],
    "Mochaccino": [
        IngredientSpec("espresso_shot", 1, "shot"),
        IngredientSpec("milk", 150, "ml"),
        IngredientSpec("chocolate_powder", 8, "g"),
    ],
    "Super chocolate": [
        IngredientSpec("chocolate_powder", 35, "g"),
        IngredientSpec("milk", 220, "ml"),
    ],
    "Tea": [
        IngredientSpec("tea_bag", 1, "count"),
    ],
    "Vanilla coffee": [
        IngredientSpec("espresso_shot", 1, "shot"),
        IngredientSpec("vanilla_syrup", 15, "ml"),
    ],
    "Vanilla with Irish whiskey": [
        IngredientSpec("espresso_shot", 1, "shot"),
        IngredientSpec("vanilla_syrup", 15, "ml"),
        IngredientSpec("whiskey", 30, "ml"),
    ],
}
# --- END PLACEHOLDER DATA ---


def ingredient_units() -> dict[str, str]:
    units: dict[str, str] = {}
    for specs in PRODUCT_INGREDIENTS.values():
        for spec in specs:
            units.setdefault(spec.name, spec.unit)
    return units
