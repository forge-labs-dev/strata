# @name Typed helper using PEP 563 annotations
#
# ``Table`` is *not* imported in this cell, but the future import keeps
# the annotation from being evaluated at module load. The function
# still works at call time because we only access duck-typed
# attributes (``num_rows``, ``num_columns``).

from __future__ import annotations


def describe_table(table: Table) -> dict:  # noqa: F821 - PEP 563 stringified
    """Summarize an Arrow-like table without needing the type at module scope."""
    return {"rows": table.num_rows, "columns": table.num_columns}
