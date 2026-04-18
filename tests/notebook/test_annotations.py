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


class TestLoopAnnotation:
    """Tests for ``@loop`` / ``@loop_until`` parsing."""

    def test_loop_requires_max_iter_and_carry(self):
        """A well-formed ``@loop`` populates max_iter and carry."""
        result = parse_annotations("# @loop max_iter=10 carry=state\nstate = refine(state)")
        assert result.loop is not None
        assert result.loop.max_iter == 10
        assert result.loop.carry == "state"
        assert result.loop.until_expr is None
        assert result.loop.start_from_cell is None
        assert result.loop.start_from_iter is None

    def test_loop_until_is_captured(self):
        """``@loop_until`` attaches its expression to the LoopAnnotation."""
        source = (
            "# @loop max_iter=10 carry=state\n"
            "# @loop_until state['confidence'] > 0.9\n"
            "state = refine(state)\n"
        )
        result = parse_annotations(source)
        assert result.loop is not None
        assert result.loop.until_expr == "state['confidence'] > 0.9"

    def test_loop_start_from_parses_cell_and_iter(self):
        """``start_from=<cell>@iter=<k>`` is split into its two fields."""
        source = "# @loop max_iter=5 carry=state start_from=evolve@iter=3\nstate = propose(state)"
        result = parse_annotations(source)
        assert result.loop is not None
        assert result.loop.start_from_cell == "evolve"
        assert result.loop.start_from_iter == 3

    def test_loop_merges_multiple_lines(self):
        """Two ``@loop`` lines and a trailing ``@loop_until`` all merge."""
        source = (
            "# @loop max_iter=20\n"
            "# @loop carry=state\n"
            "# @loop_until state.get('done')\n"
            "state = tick(state)\n"
        )
        result = parse_annotations(source)
        assert result.loop is not None
        assert result.loop.max_iter == 20
        assert result.loop.carry == "state"
        assert result.loop.until_expr == "state.get('done')"

    def test_loop_absent_when_no_annotation(self):
        result = parse_annotations("x = 1")
        assert result.loop is None

    def test_loop_ignores_unknown_keys(self):
        """Unknown ``key=value`` pairs on a ``@loop`` line are silently skipped."""
        result = parse_annotations(
            "# @loop max_iter=3 carry=state nonsense=42\nstate = tick(state)"
        )
        assert result.loop is not None
        assert result.loop.max_iter == 3
        assert result.loop.carry == "state"

    def test_loop_start_from_malformed_is_dropped(self):
        """A ``start_from`` value that does not match ``<cell>@iter=<int>`` is dropped."""
        result = parse_annotations("# @loop max_iter=5 carry=state start_from=badvalue\n")
        assert result.loop is not None
        assert result.loop.start_from_cell is None
        assert result.loop.start_from_iter is None

    def test_loop_until_without_loop_still_captures_expr(self):
        """``@loop_until`` alone should still record the expression so validation can
        surface the missing ``max_iter``/``carry`` as errors."""
        result = parse_annotations("# @loop_until x > 0\nx = 1")
        assert result.loop is not None
        assert result.loop.until_expr == "x > 0"
        assert result.loop.max_iter == 0
        assert result.loop.carry == ""
