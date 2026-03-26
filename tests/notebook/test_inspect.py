"""Tests for inspect mode (M6)."""

from __future__ import annotations

import pytest

from strata.notebook.inspect_mode import ArtifactProxy, InspectSession, InspectSessionManager
from strata.notebook.parser import parse_notebook
from strata.notebook.session import NotebookSession
from strata.notebook.writer import add_cell_to_notebook, create_notebook, write_cell


@pytest.fixture
def sample_notebook(tmp_path):
    """Create a sample notebook for inspect testing.

    Returns:
        NotebookSession instance
    """
    notebook_dir = create_notebook(tmp_path, "Inspect Test")

    # Add a cell that creates a DataFrame
    add_cell_to_notebook(notebook_dir, "create_data", None)
    write_cell(notebook_dir, "create_data", "df = pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})")

    # Parse and create session
    notebook_state = parse_notebook(notebook_dir)
    session = NotebookSession(notebook_state, notebook_dir)

    return session


class TestArtifactProxy:
    """Test the ArtifactProxy class."""

    def test_artifact_proxy_creation(self):
        """Test creating an ArtifactProxy."""
        proxy = ArtifactProxy(
            artifact_uri="strata://artifact/test_id@v=1",
            content_type="arrow/ipc",
            artifact_manager=None,
        )

        assert proxy._uri == "strata://artifact/test_id@v=1"
        assert proxy._content_type == "arrow/ipc"
        assert proxy._loaded is False

    def test_artifact_proxy_repr_unloaded(self):
        """Test repr of unloaded proxy."""
        proxy = ArtifactProxy(
            artifact_uri="strata://artifact/test_id@v=1",
            content_type="arrow/ipc",
            artifact_manager=None,
        )

        repr_str = repr(proxy)
        assert "ArtifactProxy" in repr_str
        assert "not yet loaded" in repr_str.lower()

    def test_artifact_proxy_str(self):
        """Test str of proxy."""
        proxy = ArtifactProxy(
            artifact_uri="strata://artifact/test_id@v=1",
            content_type="json/object",
            artifact_manager=None,
        )

        str_str = str(proxy)
        assert "ArtifactProxy" in str_str


class TestInspectSessionManager:
    """Test the InspectSessionManager."""

    def test_session_manager_creation(self):
        """Test creating an InspectSessionManager."""
        manager = InspectSessionManager()

        assert manager._sessions is not None
        assert len(manager._sessions) == 0

    @pytest.mark.asyncio
    async def test_get_nonexistent_session(self):
        """Test getting a nonexistent inspect session."""
        manager = InspectSessionManager()

        session = await manager.get_inspect("fake_session", "fake_cell")
        assert session is None

    @pytest.mark.asyncio
    async def test_close_nonexistent_session(self):
        """Test closing a nonexistent inspect session (should not crash)."""
        manager = InspectSessionManager()

        # Should not raise an error
        await manager.close_inspect("fake_session", "fake_cell")

        assert len(manager._sessions) == 0


class TestInspectSession:
    """Test the InspectSession class."""

    @pytest.mark.asyncio
    async def test_inspect_session_creation(self, sample_notebook):
        """Test creating an InspectSession."""
        inspect_session = InspectSession(
            sample_notebook,
            sample_notebook.notebook_state.cells[0].id,
            sample_notebook.artifact_manager,
        )

        assert inspect_session.session is sample_notebook
        assert inspect_session.artifact_manager is not None

    @pytest.mark.asyncio
    async def test_inspect_session_close(self, sample_notebook):
        """Test closing an InspectSession."""
        inspect_session = InspectSession(
            sample_notebook,
            sample_notebook.notebook_state.cells[0].id,
            sample_notebook.artifact_manager,
        )

        # Close should not raise an error
        await inspect_session.close()

        # Process should be None or killed
        if inspect_session.process:
            assert (
                inspect_session.process.returncode is not None
                or inspect_session.process.returncode == -9
            )
