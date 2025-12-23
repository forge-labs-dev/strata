"""Read planner: builds ReadPlan from snapshot + filters + projection."""

import time
from pathlib import Path

from strata.config import StrataConfig
from strata.iceberg import PyIcebergCatalog
from strata.metadata_cache import (
    ManifestCache,
    ManifestEntry,
    ManifestResolution,
    ParquetMetadataCache,
    get_manifest_cache,
    get_parquet_cache,
)
from strata.types import CacheKey, Filter, ReadPlan, TableIdentity, Task

# Type alias for compiled filters: list of (parquet_column_index, filter)
CompiledFilters = list[tuple[int, Filter]]


def _build_column_index_map(schema) -> dict[str, int]:
    """Build a mapping from column name to Parquet leaf column index.

    Only includes flat (non-nested) columns that can be reliably used
    for row group pruning via statistics.

    Args:
        schema: Parquet schema from file metadata

    Returns:
        Dict mapping column name to its physical column index
    """
    col_map: dict[str, int] = {}
    for i in range(len(schema)):
        col = schema.column(i)
        # Skip nested/repeated fields - path contains '.' for nested
        if "." in col.path:
            continue
        col_map[col.name] = i
    return col_map


def _compile_filters(
    filters: list[Filter], col_index_map: dict[str, int]
) -> CompiledFilters:
    """Compile filters into (column_index, filter) pairs for fast evaluation.

    Filters referencing columns not in the map (nested or missing) are dropped.

    Args:
        filters: List of Filter objects
        col_index_map: Mapping from column name to Parquet column index

    Returns:
        List of (column_index, filter) tuples for columns that exist
    """
    compiled: CompiledFilters = []
    for f in filters:
        col_idx = col_index_map.get(f.column)
        if col_idx is not None:
            compiled.append((col_idx, f))
    return compiled


class ReadPlanner:
    """Plans reads from Iceberg tables with row-group pruning.

    Uses metadata caches to avoid redundant reads:
    - ParquetMetadataCache: Caches Parquet file metadata (schema, row groups, stats)
    - ManifestCache: Caches Iceberg manifest resolution per snapshot

    When cache_dir is provided, metadata is persisted to SQLite for fast
    planning after server restarts.
    """

    def __init__(
        self,
        config: StrataConfig,
        parquet_cache: ParquetMetadataCache | None = None,
        manifest_cache: ManifestCache | None = None,
    ) -> None:
        self.config = config
        self.catalog = PyIcebergCatalog(config)
        # Enable persistence by passing cache_dir
        cache_dir = config.cache_dir

        # Create S3 filesystem if configured
        s3_filesystem = None
        if config.s3_region or config.s3_access_key or config.s3_anonymous:
            s3_filesystem = config.get_s3_filesystem()

        self.parquet_cache = parquet_cache or get_parquet_cache(
            cache_dir=cache_dir, s3_filesystem=s3_filesystem
        )
        self.manifest_cache = manifest_cache or get_manifest_cache(cache_dir=cache_dir)

    def plan(
        self,
        table_uri: str,
        snapshot_id: int | None = None,
        columns: list[str] | None = None,
        filters: list[Filter] | None = None,
    ) -> ReadPlan:
        """Create a read plan for the given table and options.

        Args:
            table_uri: Table identifier (path#namespace.table or just namespace.table)
            snapshot_id: Specific snapshot to read (None for current)
            columns: Columns to project (None for all)
            filters: Filters for row-group pruning

        Returns:
            ReadPlan with tasks for each row group to read
        """
        start_time = time.perf_counter()
        filters = filters or []

        # Parse table URI and build canonical TableIdentity
        # table_uri is treated as input only; table_identity is the canonical ID
        warehouse_path, table_id = self.catalog.parse_table_uri(table_uri)
        catalog_name = self.config.catalog_name if warehouse_path is None else "strata"
        table_identity = TableIdentity.from_table_id(table_id, catalog=catalog_name)

        # Load table and resolve snapshot
        table = self.catalog.load_table(table_uri)
        resolved_snapshot_id = self.catalog.get_snapshot_id(table, snapshot_id)

        # Get the snapshot's manifest
        snapshot = table.snapshot_by_id(resolved_snapshot_id)
        if snapshot is None:
            raise ValueError(f"Snapshot {resolved_snapshot_id} not found")

        # Compute projection fingerprint
        proj_fingerprint = CacheKey.compute_projection_fingerprint(columns)

        # Collect all data files from the snapshot
        plan = ReadPlan(
            table_uri=table_uri,
            table_identity=table_identity,
            snapshot_id=resolved_snapshot_id,
            columns=columns,
            filters=filters,
        )

        # Get data files from manifest cache or resolve fresh
        catalog_name = self.config.catalog_name
        table_identity_str = str(table_identity)
        manifest_resolution = self.manifest_cache.get(
            catalog_name, table_identity_str, resolved_snapshot_id
        )

        if manifest_resolution is None:
            # Cache miss: resolve manifests and cache result
            scan = table.scan(snapshot_id=resolved_snapshot_id)
            data_files = list(scan.plan_files())

            # Build manifest entries with resolved paths
            entries = []
            for file_task in data_files:
                file_path = file_task.file.file_path
                actual_path = self._resolve_file_path(table_uri, file_path)
                entries.append(ManifestEntry(file_path=file_path, actual_path=actual_path))

            manifest_resolution = ManifestResolution(data_files=entries)
            self.manifest_cache.put(
                catalog_name, table_identity_str, resolved_snapshot_id, manifest_resolution
            )

        total_row_groups = 0
        pruned_row_groups = 0
        arrow_schema = None
        estimated_bytes = 0

        # Batch load Parquet metadata for all files
        actual_paths = [entry.actual_path for entry in manifest_resolution.data_files]
        try:
            pq_meta_batch = self.parquet_cache.get_or_load_many(actual_paths)
        except Exception as e:
            raise RuntimeError(f"Failed to read Parquet metadata: {e}") from e

        for entry in manifest_resolution.data_files:
            file_path = entry.file_path
            actual_path = entry.actual_path

            pq_meta = pq_meta_batch.get(actual_path)
            if pq_meta is None:
                raise RuntimeError(f"Failed to load Parquet metadata for {actual_path}")

            # Capture schema from first file (all files should have same schema)
            if arrow_schema is None:
                arrow_schema = pq_meta.arrow_schema

            # Build column index map once per file and compile filters
            # This avoids O(num_columns × num_filters × num_row_groups) scanning
            col_index_map = _build_column_index_map(pq_meta.parquet_schema)
            compiled_filters = _compile_filters(filters, col_index_map)

            for rg_idx in range(pq_meta.num_row_groups):
                total_row_groups += 1
                rg_meta = pq_meta.row_group_metadata[rg_idx]

                # Check if we can prune this row group using compiled filters
                if self._should_prune_row_group(rg_meta, compiled_filters):
                    pruned_row_groups += 1
                    continue

                cache_key = CacheKey(
                    table_identity=table_identity,
                    snapshot_id=resolved_snapshot_id,
                    file_path=file_path,
                    row_group_id=rg_idx,
                    projection_fingerprint=proj_fingerprint,
                )

                # Get estimated size from Parquet metadata
                # Works with both our RowGroupMeta and PyArrow's RowGroupMetaData
                rg_size = getattr(rg_meta, "total_byte_size", 0)

                task = Task(
                    file_path=actual_path,
                    row_group_id=rg_idx,
                    cache_key=cache_key,
                    num_rows=rg_meta.num_rows,
                    columns=columns,
                    estimated_bytes=rg_size,
                )
                plan.tasks.append(task)
                estimated_bytes += rg_size

        plan.total_row_groups = total_row_groups
        plan.pruned_row_groups = pruned_row_groups
        plan.estimated_bytes = estimated_bytes
        plan.planning_time_ms = (time.perf_counter() - start_time) * 1000

        # Set schema: prefer Parquet schema (may have column projection),
        # fall back to Iceberg table schema for empty tables/scans
        if arrow_schema is not None:
            plan.schema = arrow_schema
        else:
            # No data files - get schema from Iceberg table metadata
            # This ensures empty scans still have a valid schema
            plan.schema = table.schema().as_arrow()

        return plan

    def _resolve_file_path(self, table_uri: str, file_path: str) -> str:
        """Resolve a file path from the table metadata to an actual path."""
        # Handle S3 paths - keep as-is
        if file_path.startswith("s3://"):
            return file_path

        # Handle file:// prefix
        if file_path.startswith("file://"):
            return file_path[7:]

        # If it's already absolute, use it
        if file_path.startswith("/"):
            return file_path

        # Try to resolve relative to warehouse
        if "#" in table_uri:
            warehouse_path = table_uri.split("#")[0]
            # S3 relative paths
            if warehouse_path.startswith("s3://"):
                return f"{warehouse_path}/{file_path}"
            # Local filesystem relative paths
            warehouse_path = warehouse_path.replace("file://", "")
            candidate = Path(warehouse_path) / file_path
            if candidate.exists():
                return str(candidate)

        return file_path

    def _should_prune_row_group(
        self,
        rg_meta,
        compiled_filters: CompiledFilters,
    ) -> bool:
        """Check if a row group can be pruned based on compiled filters and stats.

        Uses pre-compiled filters with resolved column indices for efficiency.
        Column index mapping is done once per file, not per row group.

        Limitations (v0):
        - Only flat, primitive columns are supported for pruning
        - Complex Parquet timestamps may not convert correctly; use int64 epoch
        - Filters use AND semantics (all must match for row to be included)

        If pruning cannot be safely determined, we err on the side of NOT pruning
        (i.e., we read the row group rather than risk missing data).
        """
        if not compiled_filters:
            return False

        for col_idx, f in compiled_filters:
            try:
                col_meta = rg_meta.column(col_idx)
                if not col_meta.is_stats_set:
                    continue

                stats = col_meta.statistics
                if stats is None:
                    continue

                min_val = stats.min
                max_val = stats.max

                # Convert to comparable types if needed
                min_val, max_val = self._convert_stats(min_val, max_val, f.value)

                if not f.matches_stats(min_val, max_val):
                    return True

            except Exception:
                # If we can't get stats, don't prune (safe default)
                continue

        return False

    def _convert_stats(self, min_val, max_val, filter_val):
        """Convert statistics to comparable types.

        Limitations (v0):
        - Only basic type conversions are supported
        - Numeric types (int, float) work directly with Parquet stats
        - String comparisons work if both filter and stats are strings
        - Timestamp pruning:
          * For int64 epoch micros (recommended): use int filter values
          * For datetime filters: stats must return datetime-compatible objects
          * Type mismatches will raise and be caught (no pruning, safe)
        - Decimals, bytes, and complex types may not compare correctly

        Long-term: use Iceberg schema for type-aware conversions.
        """
        from datetime import datetime

        # Convert PyArrow scalars to Python types if needed
        if hasattr(min_val, "as_py"):
            min_val = min_val.as_py()
        if hasattr(max_val, "as_py"):
            max_val = max_val.as_py()

        # For datetime filters, ensure stats are also datetime
        # (this handles the case where Parquet stores timestamp with stats)
        if isinstance(filter_val, datetime):
            # Stats should already be datetime after as_py() conversion
            # If not, comparison will raise and we won't prune (safe)
            pass

        return min_val, max_val
