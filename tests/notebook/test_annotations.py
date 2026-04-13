"""Tests for cell annotation parsing."""

from __future__ import annotations

from strata.notebook.annotations import parse_annotations


class TestNameAnnotation:
    """Tests for the @name annotation."""

    def test_name_with_spaces(self):
        """@name accepts human-readable names with spaces."""
        result = parse_annotations("# @name Load arXiv Papers\nx = 1")
        assert result.name == "Load arXiv Papers"

    def test_name_identifier(self):
        """@name accepts Python identifiers (backward compat for prompt cells)."""
        result = parse_annotations("# @name research_themes\n")
        assert result.name == "research_themes"

    def test_name_with_special_chars(self):
        """@name accepts names with parentheses and other chars."""
        result = parse_annotations("# @name Aggregate by Topic (DataFusion)\nx = 1")
        assert result.name == "Aggregate by Topic (DataFusion)"

    def test_name_empty_is_none(self):
        """Empty @name is ignored."""
        result = parse_annotations("# @name\nx = 1")
        assert result.name is None

    def test_name_no_annotation(self):
        """No @name annotation → None."""
        result = parse_annotations("x = 1")
        assert result.name is None

    def test_name_with_worker(self):
        """@name coexists with @worker."""
        result = parse_annotations("# @name Train Model\n# @worker gpu-fly\nx = 1")
        assert result.name == "Train Model"
        assert result.worker == "gpu-fly"

    def test_name_after_non_comment_ignored(self):
        """@name after the leading comment block is ignored."""
        result = parse_annotations("x = 1\n# @name Late Name\n")
        assert result.name is None


class TestNameInPromptAnalyzer:
    """Verify that prompt_analyzer still requires identifiers for @name."""

    def test_prompt_analyzer_requires_identifier(self):
        from strata.notebook.prompt_analyzer import analyze_prompt_cell

        result = analyze_prompt_cell("# @name research_themes\nHello {{ x }}")
        assert result.name == "research_themes"

    def test_prompt_analyzer_rejects_non_identifier(self):
        from strata.notebook.prompt_analyzer import analyze_prompt_cell

        result = analyze_prompt_cell("# @name Research Themes\nHello {{ x }}")
        # Non-identifier name is rejected — falls back to default "result"
        assert result.name == "result"


class TestNameInRoutes:
    """Verify that @name flows through to the API response."""

    def test_cell_annotations_include_name(self, tmp_path):
        from strata.notebook.parser import parse_notebook
        from strata.notebook.session import NotebookSession
        from strata.notebook.writer import add_cell_to_notebook, create_notebook, write_cell

        notebook_dir = create_notebook(tmp_path, "NameTest", initialize_environment=False)
        add_cell_to_notebook(notebook_dir, "c1")
        write_cell(notebook_dir, "c1", "# @name My Cool Cell\nx = 1")

        state = parse_notebook(notebook_dir)
        session = NotebookSession(state, notebook_dir)
        data = session.serialize_notebook_state()

        cell = next(c for c in data["cells"] if c["id"] == "c1")
        assert cell["annotations"]["name"] == "My Cool Cell"

    def test_cell_annotations_name_absent_when_not_set(self, tmp_path):
        from strata.notebook.parser import parse_notebook
        from strata.notebook.session import NotebookSession
        from strata.notebook.writer import add_cell_to_notebook, create_notebook, write_cell

        notebook_dir = create_notebook(tmp_path, "NoNameTest", initialize_environment=False)
        add_cell_to_notebook(notebook_dir, "c1")
        write_cell(notebook_dir, "c1", "x = 1")

        state = parse_notebook(notebook_dir)
        session = NotebookSession(state, notebook_dir)
        data = session.serialize_notebook_state()

        cell = next(c for c in data["cells"] if c["id"] == "c1")
        assert cell["annotations"]["name"] is None
