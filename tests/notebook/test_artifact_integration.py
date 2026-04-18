"""Tests for NotebookArtifactManager — the notebook/artifact-store bridge.

Focuses on the per-iteration artifact id scheme introduced for loop cells;
regular single-artifact behaviour is exercised implicitly by the executor
and cache-hit tests.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from strata.notebook.artifact_integration import NotebookArtifactManager


@pytest.fixture
def manager(tmp_path: Path) -> NotebookArtifactManager:
    return NotebookArtifactManager("nb1", artifact_dir=tmp_path / "artifacts")


class TestCellArtifactId:
    """Canonical artifact id formatting."""

    def test_regular_artifact_id_has_no_iteration_suffix(self, manager):
        assert manager.cell_artifact_id("c1", "state") == "nb_nb1_cell_c1_var_state"

    def test_iteration_artifact_id_has_suffix(self, manager):
        assert manager.cell_artifact_id("c1", "state", 3) == "nb_nb1_cell_c1_var_state@iter=3"

    def test_iteration_zero_gets_suffix(self, manager):
        """iteration=0 is distinct from None — we still want ``@iter=0`` visible."""
        assert manager.cell_artifact_id("c1", "state", 0) == "nb_nb1_cell_c1_var_state@iter=0"


class TestPerIterationArtifacts:
    """Storing and reading per-iteration carry artifacts."""

    def test_store_and_load_iteration_blob(self, manager):
        manager.store_cell_output(
            cell_id="c1",
            variable_name="state",
            blob_data=b"iter-0-bytes",
            content_type="pickle/object",
            provenance_hash="prov-0",
            iteration=0,
        )

        assert manager.load_iteration_blob("c1", "state", 0) == b"iter-0-bytes"

    def test_iterations_are_independent_artifacts(self, manager):
        for k in range(3):
            manager.store_cell_output(
                cell_id="c1",
                variable_name="state",
                blob_data=f"iter-{k}-bytes".encode(),
                content_type="pickle/object",
                provenance_hash=f"prov-{k}",
                iteration=k,
            )

        for k in range(3):
            assert manager.load_iteration_blob("c1", "state", k) == f"iter-{k}-bytes".encode()

    def test_load_missing_iteration_returns_none(self, manager):
        assert manager.load_iteration_blob("c1", "state", 0) is None

    def test_iteration_artifact_does_not_collide_with_regular(self, manager):
        """Storing ``state`` both without and with an iteration suffix must
        produce two distinct artifacts so a cell's one-shot output is never
        overwritten by a loop cell's iteration 0."""
        manager.store_cell_output(
            cell_id="c1",
            variable_name="state",
            blob_data=b"one-shot",
            content_type="pickle/object",
            provenance_hash="prov-one-shot",
        )
        manager.store_cell_output(
            cell_id="c1",
            variable_name="state",
            blob_data=b"iter-0",
            content_type="pickle/object",
            provenance_hash="prov-iter-0",
            iteration=0,
        )

        assert manager.load_iteration_blob("c1", "state", 0) == b"iter-0"

        regular_id = manager.cell_artifact_id("c1", "state")
        regular_latest = manager.artifact_store.get_latest_version(regular_id)
        assert regular_latest is not None
        assert regular_latest.provenance_hash == "prov-one-shot"

    def test_get_iteration_artifact_returns_latest_ready_version(self, manager):
        manager.store_cell_output(
            cell_id="c1",
            variable_name="state",
            blob_data=b"first",
            content_type="pickle/object",
            provenance_hash="prov-first",
            iteration=0,
        )
        artifact = manager.get_iteration_artifact("c1", "state", 0)
        assert artifact is not None
        assert artifact.state == "ready"

    def test_transform_spec_records_iteration(self, manager):
        """The stored transform_spec should carry the iteration index so
        other subsystems (inspector, diagnostics) can read it back without
        parsing the artifact id."""
        import json as _json

        manager.store_cell_output(
            cell_id="c1",
            variable_name="state",
            blob_data=b"iter-7",
            content_type="pickle/object",
            provenance_hash="prov-7",
            iteration=7,
        )

        artifact_id = manager.cell_artifact_id("c1", "state", 7)
        artifact = manager.artifact_store.get_latest_version(artifact_id)
        assert artifact is not None
        spec = _json.loads(artifact.transform_spec or "{}")
        assert spec.get("params", {}).get("iteration") == "7"
