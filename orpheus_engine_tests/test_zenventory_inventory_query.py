"""
INVENTORY_QUERY runs against a throwaway local Postgres so the real SQL is
checked. Zenventory picks placed orders immediately and picked units are
already out of inventory in_stock, so only placed-but-unpicked units may be
subtracted; draft orders must not be.

Skipped when initdb/pg_ctl are not installed.
"""

import glob
import json
import os
import shutil
import subprocess
import tempfile

import psycopg2
import pytest

from orpheus_engine.defs.zenventory_inventory_airtable_sync.definitions import (
    INVENTORY_QUERY,
)


def _pg_bin_dir():
    initdb = shutil.which("initdb")
    if initdb and shutil.which("pg_ctl"):
        return os.path.dirname(initdb)
    for bindir in sorted(glob.glob("/usr/lib/postgresql/*/bin"), reverse=True):
        if os.path.exists(os.path.join(bindir, "initdb")):
            return bindir
    return None


@pytest.fixture(scope="module")
def pg_conn():
    bindir = _pg_bin_dir()
    if not bindir:
        pytest.skip("initdb/pg_ctl not installed")

    tmp = tempfile.mkdtemp(prefix="zen_inv_pg_")
    datadir = os.path.join(tmp, "data")
    subprocess.run(
        [os.path.join(bindir, "initdb"), "-D", datadir, "-U", "postgres", "--auth=trust"],
        check=True,
        capture_output=True,
    )
    # Unix socket only: no TCP listener.
    subprocess.run(
        [
            os.path.join(bindir, "pg_ctl"), "-D", datadir, "-w",
            "-l", os.path.join(tmp, "log"),
            "-o", f"-c listen_addresses='' -k {tmp}",
            "start",
        ],
        check=True,
        capture_output=True,
    )
    conn = psycopg2.connect(host=tmp, user="postgres", dbname="postgres")
    conn.autocommit = True
    try:
        yield conn
    finally:
        conn.close()
        subprocess.run(
            [os.path.join(bindir, "pg_ctl"), "-D", datadir, "-m", "immediate", "stop"],
            capture_output=True,
        )
        shutil.rmtree(tmp, ignore_errors=True)


def _line(sku, quantity, picked=None):
    line = {"sku": sku, "quantity": quantity, "orderQuantity": quantity}
    if picked is not None:
        line["pickedQuantity"] = picked
        line["allocatedQuantity"] = picked
    return line


@pytest.fixture(scope="module")
def inventory_by_sku(pg_conn):
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            CREATE SCHEMA agh_fulfillment_zenventory;
            CREATE TABLE agh_fulfillment_zenventory.items (
                id BIGINT PRIMARY KEY, sku TEXT, description TEXT, unit_cost DOUBLE PRECISION
            );
            CREATE TABLE agh_fulfillment_zenventory.inventory (
                item_id BIGINT, lot_number TEXT, in_stock INTEGER
            );
            CREATE TABLE agh_fulfillment_zenventory.customer_orders (
                id BIGINT PRIMARY KEY, order_placed BOOLEAN, completed BOOLEAN,
                cancelled BOOLEAN, items JSONB
            );
            """
        )
        items = [
            (1, "TEST-PICKED"),
            (2, "TEST-PARTIAL"),
            (3, "TEST-DRAFT"),
            (4, "TEST-CLOSED"),
            (5, "TEST-NO-INV"),
            (6, "TEST-NO-PICKED-KEY"),
        ]
        cur.executemany(
            "INSERT INTO agh_fulfillment_zenventory.items VALUES (%s, %s, 'test item', 1.0)",
            items,
        )
        cur.executemany(
            "INSERT INTO agh_fulfillment_zenventory.inventory VALUES (%s, %s, %s)",
            [
                (1, "", 78),
                (2, "A", 3),
                (2, "B", 2),
                (3, "", 50),
                (4, "", 10),
                (6, "", 7),
            ],
        )
        orders = [
            # Placed and fully picked: already out of in_stock.
            (101, True, False, False, [_line("TEST-PICKED", 78, 78), _line("TEST-PARTIAL", 4, 1)]),
            # Placed, short-picked: 8 of 10 still owed -> backorder.
            (102, True, False, False, [_line("TEST-PARTIAL", 10, 2)]),
            # Draft (not placed): holds no stock.
            (103, False, False, False, [_line("TEST-DRAFT", 4000, 0), _line("TEST-PICKED", 5, 0)]),
            # Completed and cancelled orders never count.
            (104, True, True, False, [_line("TEST-CLOSED", 9, 0)]),
            (105, True, False, True, [_line("TEST-CLOSED", 9, 0)]),
            # Placed with no pickedQuantity key: treated as unpicked.
            (106, True, False, False, [_line("TEST-NO-PICKED-KEY", 2)]),
        ]
        cur.executemany(
            "INSERT INTO agh_fulfillment_zenventory.customer_orders VALUES (%s, %s, %s, %s, %s)",
            [(i, p, c, x, json.dumps(lines)) for i, p, c, x, lines in orders],
        )
        cur.execute(INVENTORY_QUERY)
        return {row[1]: row[4] for row in cur.fetchall()}


def test_picked_placed_orders_are_not_subtracted_again(inventory_by_sku):
    assert inventory_by_sku["TEST-PICKED"] == 78


def test_draft_orders_are_not_subtracted(inventory_by_sku):
    assert inventory_by_sku["TEST-DRAFT"] == 50


def test_unpicked_placed_units_are_subtracted_and_can_go_negative(inventory_by_sku):
    # in_stock 5 across lots, minus unpicked (4-1) + (10-2) = 11
    assert inventory_by_sku["TEST-PARTIAL"] == -6


def test_completed_and_cancelled_orders_are_ignored(inventory_by_sku):
    assert inventory_by_sku["TEST-CLOSED"] == 10


def test_item_without_inventory_rows_is_zero(inventory_by_sku):
    assert inventory_by_sku["TEST-NO-INV"] == 0


def test_missing_picked_quantity_counts_as_unpicked(inventory_by_sku):
    assert inventory_by_sku["TEST-NO-PICKED-KEY"] == 5
