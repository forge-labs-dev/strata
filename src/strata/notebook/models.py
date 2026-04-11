"""Pydantic models for notebook.toml and notebook state."""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class MountMode(StrEnum):
    """Access mode for a filesystem mount."""

    READ_ONLY = "ro"
    READ_WRITE = "rw"


class MountSpec(BaseModel):
    """A filesystem mount declaration.

    Mounts give cells transparent access to local and remote directories
    via standard ``pathlib.Path`` operations.  The mount ``name`` becomes
    a variable in the cell namespace bound to a local ``Path`` that the
    executor resolves before execution.

    Supported URI schemes: ``file://``, ``s3://``, ``gs://``, ``az://``.
    """

    name: str = Field(
        ...,
        description="Mount name — injected as a Path variable in the cell namespace",
        pattern=r"^[a-zA-Z_][a-zA-Z0-9_]*$",
    )
    uri: str = Field(
        ...,
        description="URI: file:///path, s3://bucket/prefix, gs://bucket/prefix, az://container/prefix",
    )
    mode: MountMode = Field(
        default=MountMode.READ_ONLY,
        description="Access mode: 'ro' (read-only) or 'rw' (read-write)",
    )
    pin: str | None = Field(
        default=None,
        description="Pinned version/etag — disables fingerprinting when set",
    )


class WorkerBackendType(StrEnum):
    """Execution backend type for notebook workers."""

    LOCAL = "local"
    EXECUTOR = "executor"


class WorkerSpec(BaseModel):
    """A named worker declaration."""

    name: str = Field(
        ...,
        description="Worker name used in notebook metadata and cell overrides",
        pattern=r"^[a-zA-Z0-9][a-zA-Z0-9._-]*$",
    )
    backend: WorkerBackendType = Field(
        default=WorkerBackendType.LOCAL,
        description="Worker backend type",
    )
    runtime_id: str | None = Field(
        default=None,
        description="Stable runtime fingerprint override for provenance",
    )
    config: dict[str, Any] = Field(
        default_factory=dict,
        description="Backend-specific worker configuration",
    )


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
    IMAGE_PNG = "image/png"
    TEXT_MARKDOWN = "text/markdown"
    PICKLE = "pickle/object"
    ERROR = "error"


class CellStaleness(BaseModel):
    """Staleness status for a cell."""

    status: CellStatus = Field(..., description="Status: ready, stale, idle, running, error")
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
    worker: str | None = Field(
        default=None,
        description="Cell-level worker override (overrides notebook default)",
    )
    timeout: float | None = Field(
        default=None,
        description="Cell-level execution timeout override in seconds",
    )
    env: dict[str, str] = Field(
        default_factory=dict,
        description="Cell-level environment variable overrides",
    )
    mounts: list[MountSpec] = Field(
        default_factory=list,
        description="Cell-level mount overrides (supplement/override notebook-level mounts)",
    )


class NotebookToml(BaseModel):
    """Notebook metadata from notebook.toml."""

    notebook_id: str = Field(..., description="Unique notebook ID")
    name: str = Field(default="Untitled Notebook", description="Human-readable name")
    created_at: datetime = Field(default_factory=lambda: datetime.now(tz=UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(tz=UTC))
    cells: list[CellMeta] = Field(default_factory=list, description="Cell metadata")
    worker: str | None = Field(
        default=None,
        description="Notebook-level default worker name",
    )
    timeout: float | None = Field(
        default=None,
        description="Notebook-level default execution timeout in seconds",
    )
    env: dict[str, str] = Field(
        default_factory=dict,
        description="Notebook-level default environment variables",
    )
    workers: list[WorkerSpec] = Field(
        default_factory=list,
        description="Registered workers (personal/dev mode)",
    )
    mounts: list[MountSpec] = Field(
        default_factory=list,
        description="Notebook-level filesystem mounts",
    )
    ai: dict[str, Any] = Field(
        default_factory=dict,
        description="Notebook-level LLM configuration persisted under [ai]",
    )
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
    rows: int | None = Field(default=None, description="Number of rows (for tables)")
    columns: list[str] | None = Field(default=None, description="Column names (for tables)")
    bytes: int = Field(default=0, description="Size in bytes")
    artifact_uri: str | None = Field(
        default=None,
        description="Artifact URI backing this display output",
    )
    preview: int | float | str | bool | list | dict | None = Field(
        default=None,
        description="Preview data (first 20 rows for tables, value for scalars)",
    )
    inline_data_url: str | None = Field(
        default=None,
        description="Inline data URL for display-only renderers like images",
    )
    markdown_text: str | None = Field(
        default=None,
        description="Markdown source for display-only markdown outputs",
    )
    width: int | None = Field(default=None, description="Display width in pixels")
    height: int | None = Field(default=None, description="Display height in pixels")
    error: str | None = Field(default=None, description="Error message if serialization failed")


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
    worker: str | None = Field(
        default=None,
        description="Resolved persisted worker for this cell (notebook default + cell override)",
    )
    worker_override: str | None = Field(
        default=None,
        description="Persisted cell-level worker override from notebook.toml",
    )
    timeout: float | None = Field(
        default=None,
        description="Resolved persisted timeout for this cell in seconds",
    )
    timeout_override: float | None = Field(
        default=None,
        description="Persisted cell-level timeout override from notebook.toml",
    )
    env: dict[str, str] = Field(
        default_factory=dict,
        description="Resolved persisted environment variables for this cell",
    )
    env_overrides: dict[str, str] = Field(
        default_factory=dict,
        description="Persisted cell-level environment overrides from notebook.toml",
    )
    mounts: list[MountSpec] = Field(
        default_factory=list,
        description="Resolved mounts for this cell (notebook-level + cell-level overrides)",
    )
    mount_overrides: list[MountSpec] = Field(
        default_factory=list,
        description="Persisted cell-level mount overrides from notebook.toml",
    )
    is_leaf: bool = Field(
        default=False,
        description="Whether this is a leaf node (no downstream consumers)",
    )
    staleness: CellStaleness | None = Field(default=None, description="Staleness status")
    artifact_uri: str | None = Field(
        default=None, description="URI of last stored artifact (legacy single-var)"
    )
    artifact_uris: dict[str, str] = Field(
        default_factory=dict,
        description="Per-variable artifact URIs: {var_name: uri}",
    )
    display_outputs: list[CellOutput] = Field(
        default_factory=list,
        description="Ordered persisted display outputs for the cell",
    )
    display_output: CellOutput | None = Field(
        default=None,
        description="Primary persisted display output for the cell (legacy last-item shim)",
    )
    cache_hit: bool = Field(
        default=False,
        description="Whether last execution was a cache hit",
    )
    execution_method: str | None = Field(
        default=None,
        description="Last execution method: cached, warm, cold, executor",
    )
    remote_worker: str | None = Field(
        default=None,
        description="Remote worker name used for the last remote execution",
    )
    remote_transport: str | None = Field(
        default=None,
        description="Remote transport used for the last remote execution",
    )
    remote_build_id: str | None = Field(
        default=None,
        description="Signed build id for the last remote execution, when applicable",
    )
    remote_build_state: str | None = Field(
        default=None,
        description="Last observed signed build state for remote execution metadata",
    )
    remote_error_code: str | None = Field(
        default=None,
        description="Structured remote execution error code for the last run, when available",
    )
    last_provenance_hash: str | None = Field(
        default=None,
        exclude=True,
        description="Runtime-only provenance hash from the last successful execution",
    )
    last_source_hash: str | None = Field(
        default=None,
        exclude=True,
        description="Runtime-only source hash from the last successful execution",
    )
    last_env_hash: str | None = Field(
        default=None,
        exclude=True,
        description="Runtime-only environment hash from the last successful execution",
    )


class NotebookState(BaseModel):
    """Full notebook state for API responses."""

    id: str = Field(..., description="Notebook ID")
    name: str = Field(default="Untitled Notebook", description="Notebook name")
    worker: str | None = Field(
        default=None,
        description="Notebook-level default worker name",
    )
    timeout: float | None = Field(
        default=None,
        description="Notebook-level default execution timeout in seconds",
    )
    env: dict[str, str] = Field(
        default_factory=dict,
        description="Notebook-level default environment variables",
    )
    workers: list[WorkerSpec] = Field(
        default_factory=list,
        description="Registered workers available to this notebook",
    )
    mounts: list[MountSpec] = Field(
        default_factory=list,
        description="Notebook-level filesystem mount defaults",
    )
    cells: list[CellState] = Field(default_factory=list, description="Cells with source")
    path: Path | None = Field(
        default=None,
        exclude=True,
        description="Path to notebook directory (not serialized)",
    )
    created_at: datetime | None = Field(default=None)
    updated_at: datetime | None = Field(default=None)

    model_config = ConfigDict(arbitrary_types_allowed=True)
