# @name setup_status
"""Seed `analytics.db` with a small orders table.

Run this once before the SQL cells. It writes a SQLite file in the
notebook directory; subsequent edits to other cells leave it alone.
The file is the side-effect artifact of this cell — Strata's cache
is on the returned status string, not on the database.
"""

import sqlite3
from pathlib import Path

db = Path("analytics.db")
if db.exists():
    db.unlink()

conn = sqlite3.connect(db)
conn.executescript("""
    CREATE TABLE products (
        sku TEXT PRIMARY KEY,
        category TEXT NOT NULL
    );
    CREATE TABLE orders (
        id INTEGER PRIMARY KEY,
        customer TEXT NOT NULL,
        sku TEXT NOT NULL REFERENCES products(sku),
        amount REAL NOT NULL,
        ordered_at TEXT NOT NULL
    );
""")

conn.executemany(
    "INSERT INTO products (sku, category) VALUES (?, ?)",
    [
        ("WIDGET-A", "widgets"),
        ("WIDGET-B", "widgets"),
        ("GADGET-A", "gadgets"),
        ("GADGET-B", "gadgets"),
        ("THINGAMAJIG", "misc"),
    ],
)
conn.executemany(
    "INSERT INTO orders (id, customer, sku, amount, ordered_at) VALUES (?, ?, ?, ?, ?)",
    [
        (1, "alice", "WIDGET-A", 25.50, "2026-04-01"),
        (2, "alice", "GADGET-A", 199.99, "2026-04-02"),
        (3, "bob", "WIDGET-B", 75.00, "2026-04-02"),
        (4, "bob", "GADGET-B", 350.00, "2026-04-05"),
        (5, "carol", "WIDGET-A", 42.00, "2026-04-08"),
        (6, "carol", "THINGAMAJIG", 500.00, "2026-04-10"),
        (7, "dave", "WIDGET-B", 18.00, "2026-04-12"),
        (8, "alice", "GADGET-A", 199.99, "2026-04-15"),
        (9, "bob", "WIDGET-A", 25.50, "2026-04-18"),
        (10, "carol", "GADGET-B", 350.00, "2026-04-20"),
    ],
)
conn.commit()
conn.close()

setup_status = f"seeded {db.absolute()}"
setup_status
