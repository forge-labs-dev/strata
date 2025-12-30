"""Artifact store for personal mode.

The artifact store manages cached materialized query results:
1. Artifact versions: Immutable Arrow IPC blobs indexed by (id, version)
2. Name pointers: Mutable names that point to specific artifact versions

Disk layout:
    {artifact_dir}/
        artifacts.sqlite      # Metadata database
        blobs/
            {id}@v={version}.arrow  # Arrow IPC stream files

Provenance hash:
    Each artifact has a provenance_hash = sha256(sorted(input_hashes) + transform_spec)
    This enables deduplication: if the same inputs + transform exist, return existing artifact.

Security:
    Artifact store is only enabled in personal mode (local development).
    In service mode, all write operations return 403.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass


# ---------------------------------------------------------------------------
# Data Types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ArtifactVersion:
    """Immutable artifact version metadata.

    Attributes:
        id: Unique artifact identifier (UUID)
        version: Version number (monotonically increasing per id)
        state: Lifecycle state ("building", "ready", "failed")
        provenance_hash: Hash of inputs + transform for deduplication
        schema_json: Arrow schema serialized as JSON
        row_count: Number of rows in the artifact
        byte_size: Size of the Arrow IPC file in bytes
        created_at: Unix timestamp when the artifact was created
        transform_spec: JSON-serialized transform specification (opaque to server)
        input_versions: JSON-serialized dict mapping input URI -> version string
            Used for staleness detection. For table URIs, version is snapshot_id.
            For artifact URIs, version is "artifact_id@v=N".
    """

    id: str
    version: int
    state: str  # "building" | "ready" | "failed"
    provenance_hash: str
    schema_json: str | None = None
    row_count: int | None = None
    byte_size: int | None = None
    created_at: float | None = None
    transform_spec: str | None = None
    input_versions: str | None = None  # JSON: {"uri": "version_string", ...}


@dataclass(frozen=True)
class ArtifactName:
    """Mutable name pointer to an artifact version.

    Names provide human-readable aliases for artifacts, e.g.:
        strata://name/daily_revenue -> strata://artifact/abc123@v=5

    Attributes:
        name: Human-readable name (e.g., "daily_revenue")
        artifact_id: ID of the pinned artifact
        version: Version of the pinned artifact
        updated_at: Unix timestamp of last update
    """

    name: str
    artifact_id: str
    version: int
    updated_at: float


@dataclass(frozen=True)
class InputChange:
    """Describes a change in an input dependency.

    Attributes:
        input_uri: The input URI that changed
        old_version: The version used when artifact was built
        new_version: The current version of the input
    """

    input_uri: str
    old_version: str
    new_version: str

    def __str__(self) -> str:
        return f"{self.input_uri}: {self.old_version} → {self.new_version}"


@dataclass
class NameStatus:
    """Status information for a named artifact, including staleness.

    Attributes:
        name: The artifact name
        artifact_uri: URI of the pinned artifact version
        artifact_id: Artifact ID
        version: Pinned version number
        state: Artifact state ("ready", "building", "failed")
        updated_at: When the name was last updated
        input_versions: Mapping of input URI -> version when built
        is_stale: True if any input has changed since build
        stale_reason: Human-readable explanation if stale
        changed_inputs: List of inputs that changed
    """

    name: str
    artifact_uri: str
    artifact_id: str
    version: int
    state: str
    updated_at: float
    input_versions: dict[str, str]
    is_stale: bool = False
    stale_reason: str | None = None
    changed_inputs: list[InputChange] | None = None


@dataclass(frozen=True)
class TransformSpec:
    """Transform specification (opaque to server, executed by client).

    The server stores this but never interprets params.

    Attributes:
        executor: Executor URI (e.g., "local://duckdb_sql@v1")
        params: Opaque parameters for the executor (e.g., SQL query)
        inputs: List of input URIs (table URIs or artifact URIs)
    """

    executor: str
    params: dict
    inputs: list[str]

    def to_json(self) -> str:
        """Serialize to JSON string.

        Note: inputs are sorted for deterministic hashing since input order
        doesn't affect computation semantics.
        """
        return json.dumps(
            {
                "executor": self.executor,
                "params": self.params,
                "inputs": sorted(self.inputs),  # Sort for deterministic hash
            },
            sort_keys=True,
        )

    @classmethod
    def from_json(cls, json_str: str) -> "TransformSpec":
        """Deserialize from JSON string."""
        data = json.loads(json_str)
        return cls(
            executor=data["executor"],
            params=data["params"],
            inputs=data["inputs"],
        )


# ---------------------------------------------------------------------------
# Provenance Hash
# ---------------------------------------------------------------------------


def compute_provenance_hash(input_hashes: list[str], transform_spec: TransformSpec) -> str:
    """Compute deterministic provenance hash for deduplication.

    The hash uniquely identifies a computation based on:
    1. Content hashes of all inputs (sorted for determinism)
    2. The transform specification

    Args:
        input_hashes: Content hashes of input artifacts/tables (will be sorted)
        transform_spec: The transform to apply

    Returns:
        SHA-256 hex digest of the combined provenance
    """
    # Sort input hashes for deterministic ordering
    sorted_inputs = sorted(input_hashes)

    # Combine with transform spec
    hasher = hashlib.sha256()
    for h in sorted_inputs:
        hasher.update(h.encode("utf-8"))
        hasher.update(b"\x00")  # Separator
    hasher.update(transform_spec.to_json().encode("utf-8"))

    return hasher.hexdigest()


# ---------------------------------------------------------------------------
# Artifact Store
# ---------------------------------------------------------------------------

# SQL schema for artifact metadata
_SCHEMA_SQL = """
-- Artifact versions: immutable once state="ready"
CREATE TABLE IF NOT EXISTS artifact_versions (
    id TEXT NOT NULL,
    version INTEGER NOT NULL,
    state TEXT NOT NULL DEFAULT 'building',
    provenance_hash TEXT NOT NULL,
    schema_json TEXT,
    row_count INTEGER,
    byte_size INTEGER,
    created_at REAL NOT NULL,
    transform_spec TEXT,
    input_versions TEXT,  -- JSON: {"uri": "version_string", ...} for staleness detection
    PRIMARY KEY (id, version)
);

-- Index for provenance lookup (deduplication)
CREATE INDEX IF NOT EXISTS idx_provenance ON artifact_versions(provenance_hash);

-- Index for state queries (e.g., cleanup of failed artifacts)
CREATE INDEX IF NOT EXISTS idx_state ON artifact_versions(state);

-- Name pointers: mutable, point to artifact versions
CREATE TABLE IF NOT EXISTS artifact_names (
    name TEXT PRIMARY KEY,
    artifact_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    updated_at REAL NOT NULL,
    FOREIGN KEY (artifact_id, version) REFERENCES artifact_versions(id, version)
);
"""


class ArtifactStore:
    """SQLite-backed artifact store for personal mode.

    Thread-safe: uses connection per operation with WAL mode.

    Example usage:
        store = ArtifactStore(Path("~/.strata/artifacts"))

        # Create new artifact (starts in "building" state)
        version = store.create_artifact(
            artifact_id="abc123",
            provenance_hash="sha256...",
            transform_spec=spec,
        )

        # Write blob and finalize
        store.write_blob("abc123", version, arrow_bytes)
        store.finalize_artifact("abc123", version, schema_json, row_count, len(arrow_bytes))

        # Look up by provenance (deduplication)
        existing = store.find_by_provenance("sha256...")

        # Create/update name pointer
        store.set_name("daily_revenue", "abc123", 5)

        # Resolve name
        artifact = store.resolve_name("daily_revenue")
    """

    def __init__(self, artifact_dir: Path):
        """Initialize artifact store.

        Args:
            artifact_dir: Directory for artifacts (must exist)
        """
        self.artifact_dir = artifact_dir
        self.db_path = artifact_dir / "artifacts.sqlite"
        self.blobs_dir = artifact_dir / "blobs"

        # Ensure directories exist
        self.blobs_dir.mkdir(parents=True, exist_ok=True)

        # Initialize schema
        self._init_schema()

    def _get_connection(self) -> sqlite3.Connection:
        """Get a new database connection with WAL mode."""
        conn = sqlite3.connect(str(self.db_path), timeout=30.0)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.row_factory = sqlite3.Row
        return conn

    def _init_schema(self) -> None:
        """Initialize database schema."""
        conn = self._get_connection()
        try:
            conn.executescript(_SCHEMA_SQL)
            conn.commit()
        finally:
            conn.close()

    def _blob_path(self, artifact_id: str, version: int) -> Path:
        """Get path for artifact blob."""
        return self.blobs_dir / f"{artifact_id}@v={version}.arrow"

    # -----------------------------------------------------------------------
    # Artifact CRUD
    # -----------------------------------------------------------------------

    def create_artifact(
        self,
        artifact_id: str,
        provenance_hash: str,
        transform_spec: TransformSpec | None = None,
        input_versions: dict[str, str] | None = None,
    ) -> int:
        """Create a new artifact version in "building" state.

        Args:
            artifact_id: Unique artifact ID
            provenance_hash: Provenance hash for deduplication
            transform_spec: Optional transform specification
            input_versions: Optional mapping of input URI -> version string
                Used for staleness detection. For tables, version is snapshot_id.
                For artifacts, version is "artifact_id@v=N".

        Returns:
            The new version number
        """
        conn = self._get_connection()
        try:
            # Get next version number
            cursor = conn.execute(
                "SELECT COALESCE(MAX(version), 0) + 1 FROM artifact_versions WHERE id = ?",
                (artifact_id,),
            )
            version = cursor.fetchone()[0]

            # Serialize input_versions to JSON
            input_versions_json = json.dumps(input_versions) if input_versions else None

            # Insert new version
            conn.execute(
                """
                INSERT INTO artifact_versions
                    (id, version, state, provenance_hash, created_at, transform_spec, input_versions)
                VALUES (?, ?, 'building', ?, ?, ?, ?)
                """,
                (
                    artifact_id,
                    version,
                    provenance_hash,
                    time.time(),
                    transform_spec.to_json() if transform_spec else None,
                    input_versions_json,
                ),
            )
            conn.commit()
            return version
        finally:
            conn.close()

    def finalize_artifact(
        self,
        artifact_id: str,
        version: int,
        schema_json: str,
        row_count: int,
        byte_size: int,
    ) -> None:
        """Mark artifact as ready after blob is written.

        Args:
            artifact_id: Artifact ID
            version: Version number
            schema_json: Arrow schema as JSON
            row_count: Number of rows
            byte_size: Size of blob in bytes

        Raises:
            ValueError: If artifact not found or not in "building" state
        """
        conn = self._get_connection()
        try:
            cursor = conn.execute(
                """
                UPDATE artifact_versions
                SET state = 'ready', schema_json = ?, row_count = ?, byte_size = ?
                WHERE id = ? AND version = ? AND state = 'building'
                """,
                (schema_json, row_count, byte_size, artifact_id, version),
            )
            if cursor.rowcount == 0:
                raise ValueError(
                    f"Artifact {artifact_id}@v={version} not found or not in building state"
                )
            conn.commit()
        finally:
            conn.close()

    def fail_artifact(self, artifact_id: str, version: int) -> None:
        """Mark artifact as failed.

        Args:
            artifact_id: Artifact ID
            version: Version number
        """
        conn = self._get_connection()
        try:
            conn.execute(
                """
                UPDATE artifact_versions
                SET state = 'failed'
                WHERE id = ? AND version = ? AND state = 'building'
                """,
                (artifact_id, version),
            )
            conn.commit()
        finally:
            conn.close()

    def get_artifact(self, artifact_id: str, version: int) -> ArtifactVersion | None:
        """Get artifact version metadata.

        Args:
            artifact_id: Artifact ID
            version: Version number

        Returns:
            ArtifactVersion or None if not found
        """
        conn = self._get_connection()
        try:
            cursor = conn.execute(
                """
                SELECT id, version, state, provenance_hash, schema_json,
                       row_count, byte_size, created_at, transform_spec, input_versions
                FROM artifact_versions
                WHERE id = ? AND version = ?
                """,
                (artifact_id, version),
            )
            row = cursor.fetchone()
            if row is None:
                return None
            return ArtifactVersion(
                id=row["id"],
                version=row["version"],
                state=row["state"],
                provenance_hash=row["provenance_hash"],
                schema_json=row["schema_json"],
                row_count=row["row_count"],
                byte_size=row["byte_size"],
                created_at=row["created_at"],
                transform_spec=row["transform_spec"],
                input_versions=row["input_versions"],
            )
        finally:
            conn.close()

    def get_latest_version(self, artifact_id: str) -> ArtifactVersion | None:
        """Get the latest ready version of an artifact.

        Args:
            artifact_id: Artifact ID

        Returns:
            Latest ArtifactVersion with state="ready", or None if not found
        """
        conn = self._get_connection()
        try:
            cursor = conn.execute(
                """
                SELECT id, version, state, provenance_hash, schema_json,
                       row_count, byte_size, created_at, transform_spec, input_versions
                FROM artifact_versions
                WHERE id = ? AND state = 'ready'
                ORDER BY version DESC
                LIMIT 1
                """,
                (artifact_id,),
            )
            row = cursor.fetchone()
            if row is None:
                return None
            return ArtifactVersion(
                id=row["id"],
                version=row["version"],
                state=row["state"],
                provenance_hash=row["provenance_hash"],
                schema_json=row["schema_json"],
                row_count=row["row_count"],
                byte_size=row["byte_size"],
                created_at=row["created_at"],
                transform_spec=row["transform_spec"],
                input_versions=row["input_versions"],
            )
        finally:
            conn.close()

    def find_by_provenance(self, provenance_hash: str) -> ArtifactVersion | None:
        """Find artifact by provenance hash (for deduplication).

        Args:
            provenance_hash: Provenance hash to look up

        Returns:
            Matching ArtifactVersion with state="ready", or None if not found
        """
        conn = self._get_connection()
        try:
            cursor = conn.execute(
                """
                SELECT id, version, state, provenance_hash, schema_json,
                       row_count, byte_size, created_at, transform_spec, input_versions
                FROM artifact_versions
                WHERE provenance_hash = ? AND state = 'ready'
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (provenance_hash,),
            )
            row = cursor.fetchone()
            if row is None:
                return None
            return ArtifactVersion(
                id=row["id"],
                version=row["version"],
                state=row["state"],
                provenance_hash=row["provenance_hash"],
                schema_json=row["schema_json"],
                row_count=row["row_count"],
                byte_size=row["byte_size"],
                created_at=row["created_at"],
                transform_spec=row["transform_spec"],
                input_versions=row["input_versions"],
            )
        finally:
            conn.close()

    # -----------------------------------------------------------------------
    # Blob I/O
    # -----------------------------------------------------------------------

    def write_blob(self, artifact_id: str, version: int, data: bytes) -> None:
        """Write artifact blob to disk.

        Args:
            artifact_id: Artifact ID
            version: Version number
            data: Arrow IPC stream bytes
        """
        path = self._blob_path(artifact_id, version)
        # Atomic write: write to temp then rename
        temp_path = path.with_suffix(".tmp")
        temp_path.write_bytes(data)
        temp_path.rename(path)

    def read_blob(self, artifact_id: str, version: int) -> bytes | None:
        """Read artifact blob from disk.

        Args:
            artifact_id: Artifact ID
            version: Version number

        Returns:
            Arrow IPC stream bytes, or None if not found
        """
        path = self._blob_path(artifact_id, version)
        if not path.exists():
            return None
        return path.read_bytes()

    def blob_exists(self, artifact_id: str, version: int) -> bool:
        """Check if blob exists on disk.

        Args:
            artifact_id: Artifact ID
            version: Version number

        Returns:
            True if blob file exists
        """
        return self._blob_path(artifact_id, version).exists()

    # -----------------------------------------------------------------------
    # Name Pointers
    # -----------------------------------------------------------------------

    def set_name(self, name: str, artifact_id: str, version: int) -> None:
        """Create or update a name pointer.

        Args:
            name: Human-readable name
            artifact_id: Target artifact ID
            version: Target version

        Raises:
            ValueError: If target artifact version doesn't exist or isn't ready
        """
        conn = self._get_connection()
        try:
            # Verify target exists and is ready
            cursor = conn.execute(
                """
                SELECT state FROM artifact_versions
                WHERE id = ? AND version = ?
                """,
                (artifact_id, version),
            )
            row = cursor.fetchone()
            if row is None:
                raise ValueError(f"Artifact {artifact_id}@v={version} not found")
            if row["state"] != "ready":
                raise ValueError(
                    f"Artifact {artifact_id}@v={version} is not ready (state={row['state']})"
                )

            # Upsert name
            conn.execute(
                """
                INSERT INTO artifact_names (name, artifact_id, version, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(name) DO UPDATE SET
                    artifact_id = excluded.artifact_id,
                    version = excluded.version,
                    updated_at = excluded.updated_at
                """,
                (name, artifact_id, version, time.time()),
            )
            conn.commit()
        finally:
            conn.close()

    def resolve_name(self, name: str) -> ArtifactVersion | None:
        """Resolve a name to its artifact version.

        Args:
            name: Name to resolve

        Returns:
            The pinned ArtifactVersion, or None if name not found
        """
        conn = self._get_connection()
        try:
            cursor = conn.execute(
                """
                SELECT n.artifact_id, n.version
                FROM artifact_names n
                WHERE n.name = ?
                """,
                (name,),
            )
            row = cursor.fetchone()
            if row is None:
                return None
            return self.get_artifact(row["artifact_id"], row["version"])
        finally:
            conn.close()

    def get_name(self, name: str) -> ArtifactName | None:
        """Get name pointer metadata.

        Args:
            name: Name to look up

        Returns:
            ArtifactName or None if not found
        """
        conn = self._get_connection()
        try:
            cursor = conn.execute(
                """
                SELECT name, artifact_id, version, updated_at
                FROM artifact_names
                WHERE name = ?
                """,
                (name,),
            )
            row = cursor.fetchone()
            if row is None:
                return None
            return ArtifactName(
                name=row["name"],
                artifact_id=row["artifact_id"],
                version=row["version"],
                updated_at=row["updated_at"],
            )
        finally:
            conn.close()

    def delete_name(self, name: str) -> bool:
        """Delete a name pointer.

        Args:
            name: Name to delete

        Returns:
            True if name was deleted, False if it didn't exist
        """
        conn = self._get_connection()
        try:
            cursor = conn.execute(
                "DELETE FROM artifact_names WHERE name = ?",
                (name,),
            )
            conn.commit()
            return cursor.rowcount > 0
        finally:
            conn.close()

    def list_names(self) -> list[ArtifactName]:
        """List all name pointers.

        Returns:
            List of all ArtifactName entries
        """
        conn = self._get_connection()
        try:
            cursor = conn.execute(
                """
                SELECT name, artifact_id, version, updated_at
                FROM artifact_names
                ORDER BY name
                """
            )
            return [
                ArtifactName(
                    name=row["name"],
                    artifact_id=row["artifact_id"],
                    version=row["version"],
                    updated_at=row["updated_at"],
                )
                for row in cursor.fetchall()
            ]
        finally:
            conn.close()

    def get_name_status(self, name: str) -> NameStatus | None:
        """Get status information for a named artifact.

        Returns the name pointer metadata along with the artifact's
        input_versions for staleness checking. The caller is responsible
        for comparing input_versions against current versions.

        Args:
            name: Name to look up

        Returns:
            NameStatus with artifact metadata and input versions, or None
        """
        name_info = self.get_name(name)
        if name_info is None:
            return None

        artifact = self.get_artifact(name_info.artifact_id, name_info.version)
        if artifact is None:
            return None

        # Parse input_versions from JSON
        input_versions: dict[str, str] = {}
        if artifact.input_versions:
            input_versions = json.loads(artifact.input_versions)

        return NameStatus(
            name=name,
            artifact_uri=f"strata://artifact/{artifact.id}@v={artifact.version}",
            artifact_id=artifact.id,
            version=artifact.version,
            state=artifact.state,
            updated_at=name_info.updated_at,
            input_versions=input_versions,
        )

    # -----------------------------------------------------------------------
    # Lifecycle Management
    # -----------------------------------------------------------------------

    def list_artifacts(
        self,
        limit: int = 100,
        offset: int = 0,
        state: str | None = None,
        name_prefix: str | None = None,
    ) -> list[ArtifactVersion]:
        """List artifacts with optional filtering.

        Args:
            limit: Maximum number of artifacts to return
            offset: Number of artifacts to skip
            state: Filter by state ("ready", "building", "failed")
            name_prefix: Filter by artifacts that have a name starting with prefix

        Returns:
            List of ArtifactVersion entries
        """
        conn = self._get_connection()
        try:
            if name_prefix is not None:
                # Join with names table to filter by name prefix
                query = """
                    SELECT DISTINCT av.id, av.version, av.state, av.provenance_hash,
                           av.schema_json, av.row_count, av.byte_size, av.created_at,
                           av.transform_spec, av.input_versions
                    FROM artifact_versions av
                    INNER JOIN artifact_names an ON av.id = an.artifact_id AND av.version = an.version
                    WHERE an.name LIKE ?
                """
                params: list = [name_prefix + "%"]

                if state is not None:
                    query += " AND av.state = ?"
                    params.append(state)

                query += " ORDER BY av.created_at DESC LIMIT ? OFFSET ?"
                params.extend([limit, offset])
            else:
                query = """
                    SELECT id, version, state, provenance_hash, schema_json,
                           row_count, byte_size, created_at, transform_spec, input_versions
                    FROM artifact_versions
                """
                params = []

                if state is not None:
                    query += " WHERE state = ?"
                    params.append(state)

                query += " ORDER BY created_at DESC LIMIT ? OFFSET ?"
                params.extend([limit, offset])

            cursor = conn.execute(query, params)
            return [
                ArtifactVersion(
                    id=row["id"],
                    version=row["version"],
                    state=row["state"],
                    provenance_hash=row["provenance_hash"],
                    schema_json=row["schema_json"],
                    row_count=row["row_count"],
                    byte_size=row["byte_size"],
                    created_at=row["created_at"],
                    transform_spec=row["transform_spec"],
                    input_versions=row["input_versions"],
                )
                for row in cursor.fetchall()
            ]
        finally:
            conn.close()

    def delete_artifact(self, artifact_id: str, version: int) -> bool:
        """Delete an artifact version and its blob.

        Also removes any name pointers to this version.

        Args:
            artifact_id: Artifact ID
            version: Version number

        Returns:
            True if artifact was deleted, False if it didn't exist
        """
        conn = self._get_connection()
        try:
            # Check if artifact exists
            cursor = conn.execute(
                "SELECT 1 FROM artifact_versions WHERE id = ? AND version = ?",
                (artifact_id, version),
            )
            if cursor.fetchone() is None:
                return False

            # Delete name pointers to this version
            conn.execute(
                "DELETE FROM artifact_names WHERE artifact_id = ? AND version = ?",
                (artifact_id, version),
            )

            # Delete metadata
            conn.execute(
                "DELETE FROM artifact_versions WHERE id = ? AND version = ?",
                (artifact_id, version),
            )
            conn.commit()

            # Delete blob
            blob_path = self._blob_path(artifact_id, version)
            if blob_path.exists():
                blob_path.unlink()

            return True
        finally:
            conn.close()

    def garbage_collect(self, max_age_days: float = 7.0) -> dict:
        """Delete unreferenced artifacts older than max_age.

        An artifact is "unreferenced" if no name pointer points to it.
        Only deletes artifacts in "ready" or "failed" state older than max_age.

        Args:
            max_age_days: Maximum age in days for unreferenced artifacts

        Returns:
            Dictionary with GC statistics
        """
        conn = self._get_connection()
        try:
            cutoff = time.time() - (max_age_days * 86400)

            # Find unreferenced artifacts older than cutoff
            cursor = conn.execute(
                """
                SELECT av.id, av.version, av.byte_size
                FROM artifact_versions av
                LEFT JOIN artifact_names an ON av.id = an.artifact_id AND av.version = an.version
                WHERE an.name IS NULL
                  AND av.state IN ('ready', 'failed')
                  AND av.created_at < ?
                """,
                (cutoff,),
            )
            rows = cursor.fetchall()

            deleted_count = 0
            deleted_bytes = 0

            for row in rows:
                artifact_id, version, byte_size = row["id"], row["version"], row["byte_size"] or 0

                # Delete blob
                blob_path = self._blob_path(artifact_id, version)
                if blob_path.exists():
                    blob_path.unlink()

                # Delete metadata
                conn.execute(
                    "DELETE FROM artifact_versions WHERE id = ? AND version = ?",
                    (artifact_id, version),
                )

                deleted_count += 1
                deleted_bytes += byte_size

            conn.commit()

            return {
                "deleted_count": deleted_count,
                "deleted_bytes": deleted_bytes,
                "cutoff_timestamp": cutoff,
            }
        finally:
            conn.close()

    def get_usage(self) -> dict:
        """Get artifact store usage statistics.

        Returns:
            Dictionary with usage metrics
        """
        conn = self._get_connection()
        try:
            cursor = conn.execute(
                """
                SELECT
                    COUNT(DISTINCT id) as unique_artifacts,
                    COUNT(*) as total_versions,
                    COUNT(CASE WHEN state = 'ready' THEN 1 END) as ready_versions,
                    COUNT(CASE WHEN state = 'building' THEN 1 END) as building_versions,
                    COUNT(CASE WHEN state = 'failed' THEN 1 END) as failed_versions,
                    COALESCE(SUM(CASE WHEN state = 'ready' THEN byte_size END), 0) as total_bytes,
                    COALESCE(SUM(CASE WHEN state = 'ready' THEN row_count END), 0) as total_rows,
                    MIN(created_at) as oldest_artifact,
                    MAX(created_at) as newest_artifact
                FROM artifact_versions
                """
            )
            row = cursor.fetchone()

            cursor = conn.execute("SELECT COUNT(*) as count FROM artifact_names")
            names_count = cursor.fetchone()["count"]

            # Count unreferenced artifacts
            cursor = conn.execute(
                """
                SELECT COUNT(*) as count
                FROM artifact_versions av
                LEFT JOIN artifact_names an ON av.id = an.artifact_id AND av.version = an.version
                WHERE an.name IS NULL AND av.state = 'ready'
                """
            )
            unreferenced_count = cursor.fetchone()["count"]

            return {
                "unique_artifacts": row["unique_artifacts"],
                "total_versions": row["total_versions"],
                "ready_versions": row["ready_versions"],
                "building_versions": row["building_versions"],
                "failed_versions": row["failed_versions"],
                "total_bytes": row["total_bytes"],
                "total_rows": row["total_rows"],
                "name_count": names_count,
                "unreferenced_count": unreferenced_count,
                "oldest_artifact": row["oldest_artifact"],
                "newest_artifact": row["newest_artifact"],
            }
        finally:
            conn.close()

    # -----------------------------------------------------------------------
    # Maintenance (Legacy - kept for backwards compatibility)
    # -----------------------------------------------------------------------

    def cleanup_failed(self, max_age_seconds: float = 3600) -> int:
        """Clean up failed artifacts older than max_age.

        Args:
            max_age_seconds: Max age of failed artifacts to keep (default 1 hour)

        Returns:
            Number of artifacts cleaned up
        """
        conn = self._get_connection()
        try:
            cutoff = time.time() - max_age_seconds
            cursor = conn.execute(
                """
                SELECT id, version FROM artifact_versions
                WHERE state = 'failed' AND created_at < ?
                """,
                (cutoff,),
            )
            rows = cursor.fetchall()

            # Delete blobs and metadata
            for row in rows:
                blob_path = self._blob_path(row["id"], row["version"])
                if blob_path.exists():
                    blob_path.unlink()
                conn.execute(
                    "DELETE FROM artifact_versions WHERE id = ? AND version = ?",
                    (row["id"], row["version"]),
                )

            conn.commit()
            return len(rows)
        finally:
            conn.close()

    def stats(self) -> dict:
        """Get artifact store statistics.

        Returns:
            Dictionary with store statistics
        """
        conn = self._get_connection()
        try:
            cursor = conn.execute(
                """
                SELECT
                    COUNT(*) as total_versions,
                    COUNT(CASE WHEN state = 'ready' THEN 1 END) as ready_versions,
                    COUNT(CASE WHEN state = 'building' THEN 1 END) as building_versions,
                    COUNT(CASE WHEN state = 'failed' THEN 1 END) as failed_versions,
                    COALESCE(SUM(CASE WHEN state = 'ready' THEN byte_size END), 0) as total_bytes,
                    COALESCE(SUM(CASE WHEN state = 'ready' THEN row_count END), 0) as total_rows
                FROM artifact_versions
                """
            )
            row = cursor.fetchone()

            cursor = conn.execute("SELECT COUNT(*) as count FROM artifact_names")
            names_count = cursor.fetchone()["count"]

            return {
                "total_versions": row["total_versions"],
                "ready_versions": row["ready_versions"],
                "building_versions": row["building_versions"],
                "failed_versions": row["failed_versions"],
                "total_bytes": row["total_bytes"],
                "total_rows": row["total_rows"],
                "name_count": names_count,
            }
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# Module-level singleton (initialized lazily)
# ---------------------------------------------------------------------------

_artifact_store: ArtifactStore | None = None


def get_artifact_store(artifact_dir: Path | None = None) -> ArtifactStore | None:
    """Get the artifact store singleton.

    Args:
        artifact_dir: Directory for artifacts (required on first call in personal mode)

    Returns:
        ArtifactStore instance, or None if not in personal mode
    """
    global _artifact_store
    if _artifact_store is None and artifact_dir is not None:
        _artifact_store = ArtifactStore(artifact_dir)
    return _artifact_store


def reset_artifact_store() -> None:
    """Reset the artifact store singleton (for testing)."""
    global _artifact_store
    _artifact_store = None
