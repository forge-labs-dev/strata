"""Pydantic models for notebook.toml and notebook state."""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field


class CellStatus(StrEnum):
    """Execution status of a cell."""

    IDLE = "idle"
    RUNNING = "running"
    READY = "ready"
    ERROR = "error"
    STALE = "stale"


class StalenessReason(StrEnum):
    """Reason a cell is stale (has invalidated cache)."""

    SELF = "self"  # Cell source code changed
    UPSTREAM = "upstream"  # Upstream artifact changed
    ENV = "env"  # Environment/lockfile changed
    FORCED = "forced"  # Forced re-run despite cache hit


class ContentType(StrEnum):
    """Serialization format for cell outputs."""

    ARROW_IPC = "arrow/ipc"
    JSON = "json/object"
    PICKLE = "pickle/object"
    ERROR = "error"


class CellStaleness(BaseModel):
    """Staleness status for a cell."""

    status: CellStatus = Field(
        ..., description="Status: ready, stale, idle, running, error"
    )
    reasons: list[StalenessReason] = Field(
        default_factory=list, description="List of staleness reasons"
    )


class ArtifactInfo(BaseModel):
    """Lightweight artifact metadata for API responses."""

    id: str = Field(..., description="Artifact ID")
    version: int = Field(..., description="Version number")
    provenance_hash: str = Field(..., description="Provenance hash for deduplication")
    content_type: str = Field(
        ..., description="Content type (arrow/ipc, json/object, pickle/object)"
    )
    rows: int | None = Field(default=None, description="Number of rows (for tables)")
    bytes: int = Field(default=0, description="Size in bytes")
    created_at: float = Field(..., description="Creation timestamp")


class CellMeta(BaseModel):
    """Metadata for a single cell in notebook.toml."""

    id: str = Field(..., description="Unique cell ID (UUID-like)")
    file: str = Field(..., description="Path to cell source file (relative to cells/)")
    language: str = Field(default="python", description="Programming language")
    order: float = Field(default=0, description="Display order in notebook")


class NotebookToml(BaseModel):
    """Notebook metadata from notebook.toml."""

    notebook_id: str = Field(..., description="Unique notebook ID")
    name: str = Field(default="Untitled Notebook", description="Human-readable name")
    created_at: datetime = Field(default_factory=lambda: datetime.now(tz=UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(tz=UTC))
    cells: list[CellMeta] = Field(default_factory=list, description="Cell metadata")
    # Preserved in TOML round-trip but not used at runtime
    artifacts: dict = Field(default_factory=dict)
    environment: dict = Field(default_factory=dict)
    cache: dict = Field(default_factory=dict)

    model_config = ConfigDict(arbitrary_types_allowed=True)


class CellOutput(BaseModel):
    """Output variable metadata from cell execution."""

    content_type: str = Field(
        ..., description="Type of content (arrow/ipc, json/object, pickle/object, error)"
    )
    rows: int | None = Field(
        default=None, description="Number of rows (for tables)"
    )
    columns: list[str] | None = Field(
        default=None, description="Column names (for tables)"
    )
    bytes: int = Field(default=0, description="Size in bytes")
    preview: int | float | str | bool | list | dict | None = Field(
        default=None,
        description="Preview data (first 20 rows for tables, value for scalars)",
    )
    error: str | None = Field(
        default=None, description="Error message if serialization failed"
    )


class CellState(BaseModel):
    """A cell with its source code loaded."""

    id: str = Field(..., description="Cell ID")
    source: str = Field(default="", description="Cell source code")
    language: str = Field(default="python", description="Programming language")
    order: float = Field(default=0, description="Display order in notebook")
    status: CellStatus = Field(
        default=CellStatus.IDLE,
        description="Execution status",
    )
    defines: list[str] = Field(
        default_factory=list,
        description="Variable names defined by this cell",
    )
    references: list[str] = Field(
        default_factory=list,
        description="Variable names referenced by this cell",
    )
    upstream_ids: list[str] = Field(
        default_factory=list, description="Cell IDs this cell depends on"
    )
    downstream_ids: list[str] = Field(
        default_factory=list, description="Cell IDs that depend on this cell"
    )
    is_leaf: bool = Field(
        default=False,
        description="Whether this is a leaf node (no downstream consumers)",
    )
    staleness: CellStaleness | None = Field(
        default=None, description="Staleness status"
    )
    artifact_uri: str | None = Field(
        default=None, description="URI of last stored artifact (legacy single-var)"
    )
    artifact_uris: dict[str, str] = Field(
        default_factory=dict,
        description="Per-variable artifact URIs: {var_name: uri}",
    )
    cache_hit: bool = Field(
        default=False,
        description="Whether last execution was a cache hit",
    )


class NotebookState(BaseModel):
    """Full notebook state for API responses."""

    id: str = Field(..., description="Notebook ID")
    name: str = Field(default="Untitled Notebook", description="Notebook name")
    cells: list[CellState] = Field(default_factory=list, description="Cells with source")
    path: Path | None = Field(
        default=None,
        exclude=True,
        description="Path to notebook directory (not serialized)",
    )
    created_at: datetime | None = Field(default=None)
    updated_at: datetime | None = Field(default=None)

    model_config = ConfigDict(arbitrary_types_allowed=True)
