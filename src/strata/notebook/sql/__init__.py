"""SQL cell support for Strata notebooks.

Public surface:
- ``DriverAdapter`` — protocol every backend implements
- ``AdapterCapabilities`` — capability flags (per-table fingerprint,
  snapshot support, separate probe-conn requirement)
- ``QualifiedTable`` — fully qualified table reference (catalog.schema.name)
- ``FreshnessToken`` / ``SchemaFingerprint`` — opaque equality tokens
  folded into the SQL cell provenance hash
- ``register_adapter`` / ``get_adapter`` — driver registry

Per-driver implementations live in ``strata.notebook.sql.drivers.*``.
Each driver module registers its adapter at import time.
"""

from strata.notebook.sql.adapter import (
    AdapterCapabilities,
    DriverAdapter,
    FreshnessToken,
    QualifiedTable,
    SchemaFingerprint,
    hash_connection_identity,
)
from strata.notebook.sql.registry import (
    get_adapter,
    known_drivers,
    register_adapter,
)

__all__ = [
    "AdapterCapabilities",
    "DriverAdapter",
    "FreshnessToken",
    "QualifiedTable",
    "SchemaFingerprint",
    "get_adapter",
    "hash_connection_identity",
    "known_drivers",
    "register_adapter",
]
