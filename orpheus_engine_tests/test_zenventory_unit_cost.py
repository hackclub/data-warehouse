from decimal import Decimal

from orpheus_engine.defs.zenventory_inventory_airtable_sync.definitions import (
    resolve_unit_cost,
)


def test_accepted_po_wins_over_inbound_and_item():
    cost, source = resolve_unit_cost(
        {"accepted": Decimal("0.1254"), "inbound": Decimal("0.356")}, 0.356
    )
    assert (cost, source) == (0.13, "accepted_po")


def test_falls_back_to_inbound_when_no_accepted_po():
    cost, source = resolve_unit_cost({"accepted": None, "inbound": Decimal("0.3682")}, 0.0)
    assert (cost, source) == (0.37, "inbound_po")


def test_falls_back_to_item_unit_cost_when_no_po_costs():
    assert resolve_unit_cost(None, 12.5) == (12.5, "item")
    assert resolve_unit_cost({"accepted": None, "inbound": None}, 12.5) == (12.5, "item")


def test_no_cost_anywhere_returns_none():
    assert resolve_unit_cost(None, 0.0) == (None, "none")
    assert resolve_unit_cost(None, None) == (None, "none")
    assert resolve_unit_cost({"accepted": Decimal("0"), "inbound": None}, 0) == (None, "none")
