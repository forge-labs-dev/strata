"""Bridge between notebook execution and Strata artifact store.

This module manages artifact storage and retrieval for notebook cells,
using the existing ArtifactStore and BlobStore classes.

Artifact ID scheme: nb_{notebook_id}_cell_{cell_id}_var_{variable_name}
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING, Any

from strata.artifact_store import ArtifactStore, TransformSpec
from strata.notebook.models import ArtifactInfo

if TYPE_CHECKING:
    from strata.artifact_store import ArtifactVersion


class NotebookArtifactManager:
    """Manages artifacts for a notebook session.

    Wraps the existing ArtifactStore + BlobStore to provide
    notebook-specific operations.
    """

    def __init__(
        self,
        notebook_id: str,
        artifact_dir: Path | None = None,
    ):
        """Initialize the artifact manager for a notebook.

        Args:
            notebook_id: ID of the notebook
            artifact_dir: Directory for artifacts. If None, uses ~/.strata/notebook_artifacts
        """
        self.notebook_id = notebook_id

        # Default artifact directory
        if artifact_dir is None:
            artifact_dir = Path.home() / ".strata" / "notebook_artifacts" / notebook_id

        artifact_dir = Path(artifact_dir)
        artifact_dir.mkdir(parents=True, exist_ok=True)

        # Initialize artifact store with local blob storage
        self.artifact_store = ArtifactStore(artifact_dir)

    def find_cached(self, provenance_hash: str) -> ArtifactVersion | None:
        """Find a cached artifact by provenance hash.

        Args:
            provenance_hash: The provenance hash to search for

        Returns:
            ArtifactVersion if found, None otherwise
        """
        return self.artifact_store.find_by_provenance(provenance_hash)

    def store_cell_output(
        self,
        cell_id: str,
        variable_name: str,
        blob_data: bytes,
        content_type: str,
        schema_json: str | None = None,
        row_count: int | None = None,
        provenance_hash: str = "",
        input_versions: dict[str, str] | None = None,
        source_hash: str = "",
        env_hash: str = "",
    ) -> ArtifactVersion:
        """Store a cell output as an artifact.

        Args:
            cell_id: ID of the cell
            variable_name: Name of the output variable
            blob_data: Serialized blob (Arrow IPC bytes, JSON bytes, pickle bytes)
            content_type: Content type (arrow/ipc, json/object, pickle/object)
            schema_json: Arrow schema as JSON (for arrow/ipc only)
            row_count: Number of rows (for tables)
            provenance_hash: Provenance hash for deduplication
            input_versions: Mapping of input URI -> version
            source_hash: SHA-256 of cell source code (for causality tracking)
            env_hash: SHA-256 of lockfile (for causality tracking)

        Returns:
            The created ArtifactVersion
        """
        # Generate artifact ID
        artifact_id = f"nb_{self.notebook_id}_cell_{cell_id}_var_{variable_name}"

        # Create transform spec (for notebook cells, executor is "notebook/cell@v1")
        params: dict[str, str] = {
            "cell_id": cell_id,
            "variable_name": variable_name,
            "content_type": content_type,
        }
        if source_hash:
            params["source_hash"] = source_hash
        if env_hash:
            params["env_hash"] = env_hash

        transform_spec = TransformSpec(
            executor="notebook/cell@v1",
            params=params,
            inputs=[],  # Notebook cells don't have explicit input URIs in this context
        )

        # Create artifact version in "building" state
        version = self.artifact_store.create_artifact(
            artifact_id=artifact_id,
            provenance_hash=provenance_hash,
            transform_spec=transform_spec,
            input_versions=input_versions,
        )

        # Write blob
        self.artifact_store.blob_store.write_blob(artifact_id, version, blob_data)

        # Finalize artifact
        byte_size = len(blob_data)
        # Provide empty schema JSON if not supplied
        schema_str = schema_json if schema_json is not None else ""
        artifact_version = self.artifact_store.finalize_artifact(
            artifact_id=artifact_id,
            version=version,
            schema_json=schema_str,
            row_count=row_count or 0,
            byte_size=byte_size,
        )

        if artifact_version is None:
            raise ValueError(f"Failed to finalize artifact {artifact_id}@v={version}")

        # Guard against provenance dedup: finalize_artifact may detect
        # another artifact with the same provenance under a *different*
        # ID and mark our version as "failed".  In a notebook context,
        # downstream cells resolve inputs by the canonical artifact ID
        # (``nb_{notebook_id}_cell_{cell_id}_var_{var_name}``), so we
        # MUST have a ready version under that exact ID.  If finalize
        # returned a different ID, force our canonical version to ready.
        if artifact_version.id != artifact_id:
            conn = self.artifact_store._get_connection()
            try:
                conn.execute(
                    """
                    UPDATE artifact_versions
                    SET state = 'ready', schema_json = ?,
                        row_count = ?, byte_size = ?
                    WHERE id = ? AND version = ? AND state = 'failed'
                    """,
                    (schema_str, row_count or 0, byte_size, artifact_id, version),
                )
                conn.commit()
            finally:
                conn.close()
            # Re-read the canonical artifact so we return the right one.
            canonical = self.artifact_store.get_artifact(
                artifact_id,
                version,
            )
            if canonical is not None:
                artifact_version = canonical

        return artifact_version

    def load_artifact_data(self, artifact_id: str, version: int) -> bytes:
        """Load artifact blob data.

        Args:
            artifact_id: Artifact ID
            version: Version number

        Returns:
            Blob data (bytes)

        Raises:
            ValueError: If artifact not found or not ready
        """
        artifact = self.artifact_store.get_artifact(artifact_id, version)
        if artifact is None or artifact.state != "ready":
            raise ValueError(f"Artifact {artifact_id}@v={version} not found or not ready")

        blob_data = self.artifact_store.blob_store.read_blob(artifact_id, version)
        if blob_data is None:
            raise ValueError(f"Blob data not found for {artifact_id}@v={version}")
        return blob_data

    def get_artifact_preview(self, artifact_id: str, version: int) -> dict[str, Any]:
        """Get artifact metadata and a data preview.

        Args:
            artifact_id: Artifact ID
            version: Version number

        Returns:
            Dict with: id, version, content_type, rows, bytes, preview

        Raises:
            ValueError: If artifact not found
        """
        artifact = self.artifact_store.get_artifact(artifact_id, version)
        if artifact is None:
            raise ValueError(f"Artifact {artifact_id}@v={version} not found")

        # Extract content_type from transform_spec params
        content_type = "unknown"
        if artifact.transform_spec:
            try:
                spec = json.loads(artifact.transform_spec)
                content_type = spec.get("params", {}).get("content_type", "unknown")
            except (ValueError, KeyError):
                pass

        return {
            "id": artifact.id,
            "version": artifact.version,
            "content_type": content_type,
            "rows": artifact.row_count,
            "bytes": artifact.byte_size,
            "created_at": artifact.created_at,
        }

    def list_cell_artifacts(self, cell_id: str) -> list[tuple[str, ArtifactVersion]]:
        """List all artifacts for a cell (all variables, all versions).

        Args:
            cell_id: Cell ID

        Returns:
            List of (variable_name, ArtifactVersion) tuples

        Raises:
            NotImplementedError: ArtifactStore does not yet support prefix queries.
        """
        raise NotImplementedError("list_cell_artifacts requires ArtifactStore prefix query support")

    def get_artifact_info(self, artifact_id: str, version: int) -> ArtifactInfo | None:
        """Get lightweight artifact info for API responses.

        Args:
            artifact_id: Artifact ID
            version: Version number

        Returns:
            ArtifactInfo or None if not found
        """
        artifact = self.artifact_store.get_artifact(artifact_id, version)
        if artifact is None:
            return None

        return ArtifactInfo(
            id=artifact.id,
            version=artifact.version,
            provenance_hash=artifact.provenance_hash,
            content_type="unknown",  # Would need to store in params
            rows=artifact.row_count,
            bytes=artifact.byte_size or 0,
            created_at=artifact.created_at or 0.0,
        )
