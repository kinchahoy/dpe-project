from __future__ import annotations

from dataclasses import dataclass


class ProductGroup:
    CORE_COFFEE = "core_coffee"
    FLAVORED_COFFEE = "flavored_coffee"
    CHOCOLATE_COCOA = "chocolate_cocoa"
    NON_CAFFEINATED_DRINK = "non_caffeinated_drink"
    ALCOHOLIC = "alcoholic"


ALIASES: dict[str, str] = {
    "Americano with milk": "Americano with Milk",
}


GROUP_BY_CANONICAL_NAME: dict[str, str] = {
    "Americano": ProductGroup.CORE_COFFEE,
    "Americano with Milk": ProductGroup.CORE_COFFEE,
    "Cappuccino": ProductGroup.CORE_COFFEE,
    "Cortado": ProductGroup.CORE_COFFEE,
    "Double espresso": ProductGroup.CORE_COFFEE,
    "Double espresso with milk": ProductGroup.CORE_COFFEE,
    "Double ristretto": ProductGroup.CORE_COFFEE,
    "Espresso": ProductGroup.CORE_COFFEE,
    "Latte": ProductGroup.CORE_COFFEE,
    "Mochaccino": ProductGroup.CORE_COFFEE,
    "Caramel coffee": ProductGroup.FLAVORED_COFFEE,
    "Chocolate with coffee": ProductGroup.FLAVORED_COFFEE,
    "Coffee with chocolate": ProductGroup.FLAVORED_COFFEE,
    "Vanilla coffee": ProductGroup.FLAVORED_COFFEE,
    "Chocolate": ProductGroup.CHOCOLATE_COCOA,
    "Chocolate with milk": ProductGroup.CHOCOLATE_COCOA,
    "Cocoa": ProductGroup.CHOCOLATE_COCOA,
    "Double chocolate": ProductGroup.CHOCOLATE_COCOA,
    "Hot Chocolate": ProductGroup.CHOCOLATE_COCOA,
    "Super chocolate": ProductGroup.CHOCOLATE_COCOA,
    "Caramel": ProductGroup.NON_CAFFEINATED_DRINK,
    "Caramel with milk": ProductGroup.NON_CAFFEINATED_DRINK,
    "Double vanilla": ProductGroup.NON_CAFFEINATED_DRINK,
    "Hot milkshake": ProductGroup.NON_CAFFEINATED_DRINK,
    "Tea": ProductGroup.NON_CAFFEINATED_DRINK,
    "Caramel with Irish whiskey": ProductGroup.ALCOHOLIC,
    "Coffee with Irish whiskey": ProductGroup.ALCOHOLIC,
    "Double Irish whiskey": ProductGroup.ALCOHOLIC,
    "Irish whiskey": ProductGroup.ALCOHOLIC,
    "Irish whiskey with milk": ProductGroup.ALCOHOLIC,
    "Irish with chocolate": ProductGroup.ALCOHOLIC,
    "Vanilla with Irish whiskey": ProductGroup.ALCOHOLIC,
    "Caramel with chocolate": ProductGroup.CHOCOLATE_COCOA,
}


@dataclass(frozen=True)
class CanonicalProduct:
    name: str
    group: str


def canonicalize_product_name(name: str) -> str:
    normalized = name.strip()
    return ALIASES.get(normalized, normalized)


def canonical_product(name: str) -> CanonicalProduct:
    canonical_name = canonicalize_product_name(name)
    return CanonicalProduct(
        name=canonical_name,
        group=GROUP_BY_CANONICAL_NAME.get(
            canonical_name, ProductGroup.NON_CAFFEINATED_DRINK
        ),
    )
