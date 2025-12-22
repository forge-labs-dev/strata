"""Core types for Strata."""

import hashlib
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel

if TYPE_CHECKING:
    import pyarrow as pa


@dataclass(frozen=True)
class TableIdentity:
    """Canonical table identity for deterministic cache keys and metrics.

    Production systems hate ambiguity. This class provides a single,
    canonical way to identify a table regardless of how the user
    specified it (via URI, catalog reference, etc.).

    Format: <catalog>.<namespace>.<table>

    Examples:
        - strata.test_db.events
        - default.analytics.page_views
    """

    catalog: str
    namespace: str
    table: str

    def __str__(self) -> str:
        """Return the canonical string representation."""
        return f"{self.catalog}.{self.namespace}.{self.table}"

    @classmethod
    def from_table_id(cls, table_id: str, catalog: str = "strata") -> "TableIdentity":
        """Create from a table_id like 'namespace.table'.

        Args:
            table_id: Table identifier in format 'namespace.table'
            catalog: Catalog name (default: 'strata')

        Returns:
            TableIdentity with canonical representation
        """
        parts = table_id.split(".")
        if len(parts) != 2:
            raise ValueError(f"Invalid table_id '{table_id}': expected 'namespace.table' format")
        return cls(catalog=catalog, namespace=parts[0], table=parts[1])


class FilterOp(Enum):
    """Supported filter operations."""

    EQ = "="
    NE = "!="
    LT = "<"
    LE = "<="
    GT = ">"
    GE = ">="


@dataclass(frozen=True)
class Filter:
    """A simple column filter for pruning."""

    column: str
    op: FilterOp
    value: Any

    def matches_stats(self, min_val: Any, max_val: Any) -> bool:
        """Check if this filter could match given min/max statistics.

        Returns True if the row group might contain matching rows.
        """
        if min_val is None or max_val is None:
            return True  # No stats, can't prune

        match self.op:
            case FilterOp.EQ:
                return min_val <= self.value <= max_val
            case FilterOp.NE:
                return not (min_val == max_val == self.value)
            case FilterOp.LT:
                return min_val < self.value
            case FilterOp.LE:
                return min_val <= self.value
            case FilterOp.GT:
                return max_val > self.value
            case FilterOp.GE:
                return max_val >= self.value


class CacheGranularity(Enum):
    """Cache granularity options.

    Controls what is included in the cache key:

    - ROW_GROUP_PROJECTION (default): Cache key includes row_group_id + projection
      Finest granularity. Different column selections get separate cache entries.
      Best for: Workloads with consistent projections, memory-constrained systems.

    - ROW_GROUP: Cache key includes row_group_id only (ignores projection)
      Same row group = same cache entry regardless of columns requested.
      Note: The first query for a row group determines what columns are cached.
      Subsequent queries may need to re-fetch if they need columns not in cache.
      Best for: Workloads that always request all columns, or when cache reuse
      across different projections is more valuable than projection efficiency.
    """

    ROW_GROUP_PROJECTION = "row_group_projection"
    ROW_GROUP = "row_group"


@dataclass(frozen=True)
class CacheKey:
    """Immutable cache key for a row group.

    v1 Cache Key Contract:
    - Key format: {table_identity}|{snapshot_id}|{file_path}|{row_group_id}[|{projection}]
    - Hash: SHA-256 of the key string
    - Storage: cache_dir/v{VERSION}/{hash[:2]}/{hash[2:4]}/{hash}.arrowstream
    - Version is baked into path (see cache.CACHE_VERSION)

    Uses TableIdentity (e.g., 'catalog.namespace.table') instead of the
    user-supplied URI to ensure cache hits work regardless of URI format
    variations like:
    - file:///path#ns.table vs /path#ns.table
    - Different hostnames pointing to same data

    The canonical identity prevents subtle cache duplication bugs where
    the same data is cached multiple times under different keys.

    Cache granularity is controlled by whether projection_fingerprint
    is included in the key hash:
    - ROW_GROUP_PROJECTION: includes projection (default)
    - ROW_GROUP: ignores projection (caches all columns)
    """

    table_identity: TableIdentity  # Canonical identity like 'strata.namespace.table'
    snapshot_id: int
    file_path: str
    row_group_id: int
    projection_fingerprint: str  # Used only if granularity includes projection

    @property
    def table_id(self) -> str:
        """Return canonical table identity string for compatibility."""
        return str(self.table_identity)

    def to_hex(self, granularity: CacheGranularity = CacheGranularity.ROW_GROUP_PROJECTION) -> str:
        """Generate a hex digest for filesystem storage.

        Args:
            granularity: Controls what is included in the hash.
                - ROW_GROUP_PROJECTION: includes projection fingerprint (default)
                - ROW_GROUP: excludes projection, cache stores all columns
        """
        if granularity == CacheGranularity.ROW_GROUP:
            # Ignore projection - cache all columns
            key_str = (
                f"{self.table_identity}|{self.snapshot_id}|{self.file_path}|{self.row_group_id}"
            )
        else:
            # Include projection in key
            key_str = (
                f"{self.table_identity}|{self.snapshot_id}|"
                f"{self.file_path}|{self.row_group_id}|{self.projection_fingerprint}"
            )
        return hashlib.sha256(key_str.encode()).hexdigest()

    @staticmethod
    def compute_projection_fingerprint(columns: list[str] | None) -> str:
        """Compute a fingerprint for the column projection.

        Column order is preserved in the fingerprint because it matters for
        consumers who expect columns in a specific order.
        """
        if columns is None:
            return "*"
        return hashlib.sha256(",".join(columns).encode()).hexdigest()[:16]


@dataclass
class Task:
    """A single read task for one row group."""

    file_path: str
    row_group_id: int
    cache_key: CacheKey
    num_rows: int
    columns: list[str] | None = None

    # Estimated size from Parquet metadata (for pre-flight checks)
    estimated_bytes: int = 0

    # Populated after fetch
    cached: bool = False
    bytes_read: int = 0


@dataclass
class ReadPlan:
    """A plan for reading data from an Iceberg snapshot.

    Note: table_uri is kept for backwards compatibility and debugging,
    but table_identity is the canonical identifier used for cache keys,
    metrics, and logs.
    """

    table_uri: str  # Original user input (for debugging/display only)
    table_identity: TableIdentity  # Canonical identity for cache/metrics/logs
    snapshot_id: int
    tasks: list[Task] = field(default_factory=list)
    columns: list[str] | None = None
    filters: list[Filter] = field(default_factory=list)

    # Schema from Parquet metadata (no IO at query time)
    schema: "pa.Schema | None" = None

    # Unique scan identifier (generated once at creation)
    scan_id: str = field(default_factory=lambda: uuid.uuid4().hex[:16])

    # Metrics
    total_row_groups: int = 0
    pruned_row_groups: int = 0
    planning_time_ms: float = 0.0

    # Estimated response size (sum of row group sizes from Parquet metadata)
    # Used for pre-flight size checks before streaming begins
    estimated_bytes: int = 0


class ScanRequest(BaseModel):
    """API request to scan a table."""

    table_uri: str
    snapshot_id: int | None = None  # None means current snapshot
    columns: list[str] | None = None  # None means all columns
    filters: list[dict[str, Any]] | None = None  # Serialized filters

    def parse_filters(self) -> list[Filter]:
        """Parse serialized filters into Filter objects."""
        if not self.filters:
            return []
        result = []
        for f in self.filters:
            result.append(
                Filter(
                    column=f["column"],
                    op=FilterOp(f["op"]),
                    value=_deserialize_value(f["value"]),
                )
            )
        return result


class ScanResponse(BaseModel):
    """API response containing scan metadata.

    v1 Contract:
    - All fields are stable and will not be removed
    - New optional fields may be added in future versions
    """

    scan_id: str
    snapshot_id: int
    num_tasks: int
    total_row_groups: int
    pruned_row_groups: int
    columns: list[str]
    planning_time_ms: float
    estimated_bytes: int  # Estimated response size from Parquet metadata


class ErrorResponse(BaseModel):
    """Standard error response format.

    v1 Error Codes:
    - 400 Bad Request: Invalid table URI, malformed filters, too many row groups
    - 404 Not Found: Scan ID not found
    - 413 Payload Too Large: Response exceeds max_response_bytes
    - 499 Client Closed Request: Client disconnected mid-scan (logged only)
    - 500 Internal Server Error: Unexpected server error
    - 503 Service Unavailable: Server draining or at capacity
    - 504 Gateway Timeout: Planning or scan exceeded timeout
    """

    detail: str  # Human-readable error message
    error_code: str | None = None  # Optional machine-readable code


def _deserialize_value(value: Any) -> Any:
    """Deserialize filter values from JSON."""
    if isinstance(value, str) and value.startswith("__datetime__:"):
        return datetime.fromisoformat(value.replace("__datetime__:", ""))
    return value


def serialize_filter(f: Filter) -> dict[str, Any]:
    """Serialize a Filter for JSON transport."""
    value = f.value
    if isinstance(value, datetime):
        value = f"__datetime__:{value.isoformat()}"
    return {"column": f.column, "op": f.op.value, "value": value}
