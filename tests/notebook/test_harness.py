"""Tests for the harness subprocess execution."""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest

_MINIMAL_PNG_LITERAL = (
    "b\"\\x89PNG\\r\\n\\x1a\\n\\x00\\x00\\x00\\rIHDR\\x00\\x00\\x00\\x01\\x00\\x00\\x00\\x01"
    "\\x08\\x04\\x00\\x00\\x00\\xb5\\x1c\\x0c\\x02\\x00\\x00\\x00\\x0bIDATx\\xdac\\xfc\\xff"
    "\\x1f\\x00\\x03\\x03\\x02\\x00\\xef\\x9b\\xe0M\\x00\\x00\\x00\\x00IEND\\xaeB`\\x82\""
)
_MARKDOWN_LITERAL = '"# Title\\n\\n- one\\n- two"'


@pytest.fixture
def harness_script():
    """Get path to harness script."""
    return Path(__file__).parent.parent.parent / "src" / "strata" / "notebook" / "harness.py"


def run_harness(harness_path: Path, manifest: dict) -> dict:
    """Run the harness with a given manifest.

    Args:
        harness_path: Path to harness.py
        manifest: Manifest dict

    Returns:
        Result manifest from harness
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)

        # Write manifest
        manifest_path = tmpdir / "manifest.json"
        manifest["output_dir"] = str(tmpdir)
        with open(manifest_path, "w") as f:
            json.dump(manifest, f)

        # Run harness
        subprocess.run(
            [sys.executable, str(harness_path), str(manifest_path)],
            cwd=str(tmpdir),
            capture_output=True,
        )

        # Read result
        result_path = tmpdir / "manifest.json"
        with open(result_path) as f:
            return json.load(f)


class TestHarness:
    """Test harness execution."""

    def test_harness_simple_assignment(self, harness_script):
        """Test harness with simple assignment."""
        manifest = {"source": "x = 1 + 1", "inputs": {}}

        result = run_harness(harness_script, manifest)

        assert result["success"] is True
        assert "x" in result["variables"]
        assert result["variables"]["x"]["content_type"] == "json/object"

    def test_harness_with_print(self, harness_script):
        """Test that print output is captured."""
        manifest = {"source": 'print("Hello")\ny = 42', "inputs": {}}

        result = run_harness(harness_script, manifest)

        assert result["success"] is True
        assert "Hello" in result["stdout"]
        assert "y" in result["variables"]

    def test_harness_with_error(self, harness_script):
        """Test harness execution error handling."""
        manifest = {"source": "z = 1 / 0", "inputs": {}}

        result = run_harness(harness_script, manifest)

        assert result["success"] is False
        assert "error" in result
        assert len(result["error"]) > 0

    def test_harness_dataframe(self, harness_script):
        """Test harness with DataFrame creation."""
        manifest = {
            "source": 'import pandas as pd\ndf = pd.DataFrame({"a": [1, 2, 3]})',
            "inputs": {},
        }

        result = run_harness(harness_script, manifest)

        assert result["success"] is True
        assert "df" in result["variables"]
        assert result["variables"]["df"]["content_type"] == "arrow/ipc"
        assert result["variables"]["df"]["rows"] == 3

    def test_harness_multiple_outputs(self, harness_script):
        """Test harness with multiple outputs."""
        manifest = {
            "source": """
x = 10
y = "hello"
z = [1, 2, 3]
""",
            "inputs": {},
        }

        result = run_harness(harness_script, manifest)

        assert result["success"] is True
        var_names = set(result["variables"].keys())
        assert var_names == {"x", "y", "z"}

    def test_harness_dict_output(self, harness_script):
        """Test harness with dict output."""
        manifest = {
            "source": 'data = {"count": 42, "name": "test"}',
            "inputs": {},
        }

        result = run_harness(harness_script, manifest)

        assert result["success"] is True
        assert "data" in result["variables"]
        assert result["variables"]["data"]["content_type"] == "json/object"

    def test_harness_ignores_private(self, harness_script):
        """Test that private variables are not captured."""
        manifest = {
            "source": """
public = 1
_private = 2
""",
            "inputs": {},
        }

        result = run_harness(harness_script, manifest)

        assert result["success"] is True
        assert "public" in result["variables"]
        assert "_private" not in result["variables"]

    def test_harness_empty_output(self, harness_script):
        """Test harness with no outputs."""
        manifest = {"source": "# Just a comment", "inputs": {}}

        result = run_harness(harness_script, manifest)

        assert result["success"] is True
        assert len(result["variables"]) == 0

    def test_harness_with_stderr(self, harness_script):
        """Test that stderr is captured."""
        manifest = {
            "source": """
import sys
print("error", file=sys.stderr)
x = 1
""",
            "inputs": {},
        }

        result = run_harness(harness_script, manifest)

        assert result["success"] is True
        assert "error" in result["stderr"]

    def test_harness_captures_last_expression_png_display(self, harness_script):
        """Bare-expression values exposing _repr_png_ should be serialized as display output."""
        manifest = {
            "source": f"""
class Display:
    def _repr_png_(self):
        return {_MINIMAL_PNG_LITERAL}

Display()
""",
            "inputs": {},
        }

        result = run_harness(harness_script, manifest)

        assert result["success"] is True
        assert "_" in result["variables"]
        assert result["variables"]["_"]["content_type"] == "image/png"
        assert result["variables"]["_"]["inline_data_url"].startswith("data:image/png;base64,")

    def test_harness_captures_last_expression_markdown_display(self, harness_script):
        """Bare-expression values exposing _repr_markdown_ should serialize as markdown display."""
        manifest = {
            "source": f"""
class Display:
    def _repr_markdown_(self):
        return {_MARKDOWN_LITERAL}

Display()
""",
            "inputs": {},
        }

        result = run_harness(harness_script, manifest)

        assert result["success"] is True
        assert "_" in result["variables"]
        assert result["variables"]["_"]["content_type"] == "text/markdown"
        assert result["variables"]["_"]["markdown_text"] == "# Title\n\n- one\n- two"

    def test_harness_captures_display_call_png_output(self, harness_script):
        """Explicit display(...) side effects should populate the primary display output."""
        manifest = {
            "source": f"""
class Display:
    def _repr_png_(self):
        return {_MINIMAL_PNG_LITERAL}

display(Display())
""",
            "inputs": {},
        }

        result = run_harness(harness_script, manifest)

        assert result["success"] is True
        assert "_" in result["variables"]
        assert result["variables"]["_"]["content_type"] == "image/png"

    def test_harness_captures_display_call_markdown_output(self, harness_script):
        """Injected display helpers should support Markdown(...) without imports."""
        manifest = {
            "source": """
display(Markdown("# Via helper\\n\\nRendered from display()."))
""",
            "inputs": {},
        }

        result = run_harness(harness_script, manifest)

        assert result["success"] is True
        assert "_" in result["variables"]
        assert result["variables"]["_"]["content_type"] == "text/markdown"
        assert (
            result["variables"]["_"]["markdown_text"]
            == "# Via helper\n\nRendered from display()."
        )

    def test_harness_captures_pyplot_show_png_output(self, harness_script):
        """plt.show() should feed the primary display output when matplotlib is available."""
        pytest.importorskip("matplotlib.pyplot")
        manifest = {
            "source": """
import matplotlib.pyplot as plt

plt.plot([1, 2, 3], [1, 4, 9])
plt.show()
""",
            "inputs": {},
            "env": {"MPLBACKEND": "Agg"},
        }

        result = run_harness(harness_script, manifest)

        assert result["success"] is True
        assert "_" in result["variables"]
        assert result["variables"]["_"]["content_type"] == "image/png"

    def test_harness_captures_multiple_visible_outputs_in_order(self, harness_script):
        """Visible outputs should be emitted in order with the last one preserved as '_'."""
        manifest = {
            "source": """
display(Markdown("# First"))
42
""",
            "inputs": {},
        }

        result = run_harness(harness_script, manifest)

        assert result["success"] is True
        assert len(result["displays"]) == 2
        assert result["displays"][0]["content_type"] == "text/markdown"
        assert result["displays"][0]["markdown_text"] == "# First"
        assert result["displays"][1]["content_type"] == "json/object"
        assert result["displays"][1]["preview"] == 42
        assert result["variables"]["_"]["content_type"] == "json/object"
        assert result["variables"]["_"]["preview"] == 42

    def test_harness_complex_dataframe(self, harness_script):
        """Test harness with a more complex DataFrame."""
        manifest = {
            "source": """import pandas as pd
df = pd.DataFrame({
    'id': [1, 2, 3, 4, 5],
    'value': [1.5, 2.5, 3.5, 4.5, 5.5],
    'category': ['A', 'B', 'A', 'C', 'B']
})
""",
            "inputs": {},
        }

        result = run_harness(harness_script, manifest)

        assert result["success"] is True
        assert result["variables"]["df"]["rows"] == 5
        assert set(result["variables"]["df"]["columns"]) == {"id", "value", "category"}
        assert "preview" in result["variables"]["df"]
        # Preview should have all 5 rows since it's small
        assert len(result["variables"]["df"]["preview"]) == 5

    def test_harness_function_definition(self, harness_script):
        """Test harness with function definition."""
        manifest = {
            "source": """
def greet(name):
    return f"Hello, {name}!"

result = greet("World")
""",
            "inputs": {},
        }

        result = run_harness(harness_script, manifest)

        assert result["success"] is True
        # Function is exported
        assert "greet" in result["variables"]
        # But result is also there
        assert "result" in result["variables"]
        assert result["variables"]["result"]["preview"] == "Hello, World!"

    def test_harness_with_imports(self, harness_script):
        """Test harness with standard library imports."""
        manifest = {
            "source": """
import math
pi_value = math.pi
sqrt_2 = math.sqrt(2)
""",
            "inputs": {},
        }

        result = run_harness(harness_script, manifest)

        assert result["success"] is True
        assert "pi_value" in result["variables"]
        assert "sqrt_2" in result["variables"]

    def test_harness_serialization_error_handled(self, harness_script):
        """Test that serialization errors are handled gracefully."""
        manifest = {
            "source": """
# Create a complex object that might not serialize well
import threading
lock = threading.Lock()
x = 1
""",
            "inputs": {},
        }

        result = run_harness(harness_script, manifest)

        # Should succeed overall
        assert result["success"] is True
        # But lock variable might have serialization error
        # x should still be there
        assert "x" in result["variables"]
