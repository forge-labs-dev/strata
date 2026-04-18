"""Tests for environment hashing."""

import hashlib

from strata.notebook.env import (
    collect_referenced_env_keys,
    compute_lockfile_hash,
    narrow_env_for_provenance,
)


def test_lockfile_hash_stability(tmp_path):
    """Same lockfile should produce same hash."""
    lockfile = tmp_path / "uv.lock"
    lockfile.write_text("[[package]]\nname = 'pandas'\n")

    hash1 = compute_lockfile_hash(tmp_path)
    hash2 = compute_lockfile_hash(tmp_path)

    assert hash1 == hash2


def test_lockfile_hash_changes_with_content(tmp_path):
    """Different lockfile content should produce different hash."""
    lockfile = tmp_path / "uv.lock"
    lockfile.write_text("[[package]]\nname = 'pandas'\n")
    hash1 = compute_lockfile_hash(tmp_path)

    lockfile.write_text("[[package]]\nname = 'numpy'\n")
    hash2 = compute_lockfile_hash(tmp_path)

    assert hash1 != hash2


def test_lockfile_hash_missing_lockfile(tmp_path):
    """Missing lockfile should return sentinel hash."""
    hash_val = compute_lockfile_hash(tmp_path)

    # Sentinel hash is sha256 of empty string
    expected = hashlib.sha256(b"").hexdigest()

    assert hash_val == expected


def test_lockfile_hash_empty_dir(tmp_path):
    """Empty directory (no uv.lock) should return sentinel hash."""
    hash_val = compute_lockfile_hash(tmp_path)
    expected = hashlib.sha256(b"").hexdigest()

    assert hash_val == expected


def test_lockfile_hash_consistent_across_calls(tmp_path):
    """Repeated calls should return same hash."""
    lockfile = tmp_path / "uv.lock"
    lockfile.write_text("version = 0.1\n")

    hashes = [compute_lockfile_hash(tmp_path) for _ in range(3)]

    assert len(set(hashes)) == 1  # All same


def test_collect_referenced_env_keys_subscript():
    """``os.environ['KEY']`` should be detected."""
    assert collect_referenced_env_keys("import os\nx = os.environ['APP_MODE']") == {"APP_MODE"}


def test_collect_referenced_env_keys_get_and_getenv():
    """``os.environ.get`` and ``os.getenv`` literal keys are detected."""
    source = "import os\na = os.environ.get('A', 'default')\nb = os.getenv('B')\n"
    assert collect_referenced_env_keys(source) == {"A", "B"}


def test_collect_referenced_env_keys_from_os_import_aliases():
    """``from os import environ, getenv`` usages are detected."""
    source = (
        "from os import environ, getenv\nx = environ['A']\ny = environ.get('B')\nz = getenv('C')\n"
    )
    assert collect_referenced_env_keys(source) == {"A", "B", "C"}


def test_collect_referenced_env_keys_ignores_dynamic_lookup():
    """Non-literal keys are ignored — they cannot be statically resolved."""
    source = "import os\nkey = 'A'\nx = os.environ[key]\n"
    assert collect_referenced_env_keys(source) == set()


def test_collect_referenced_env_keys_syntax_error_returns_empty():
    """Invalid source must not crash; return an empty set."""
    assert collect_referenced_env_keys("def broken(:") == set()


def test_narrow_env_for_provenance_drops_unreferenced_keys():
    """Notebook-level env vars that a cell does not reference are dropped."""
    source = "import os\nx = os.environ['USED']"
    resolved = {"USED": "1", "UNUSED": "secret", "OPENAI_API_KEY": "sk"}

    narrowed = narrow_env_for_provenance(source, resolved)

    assert narrowed == {"USED": "1"}


def test_narrow_env_for_provenance_keeps_declared_keys():
    """Explicitly declared keys (annotations or persisted overrides) are kept
    even when the cell body never reads them — the declaration is the
    explicit opt-in signal."""
    source = "x = 1"  # no references
    resolved = {"DECLARED": "hello", "AMBIENT": "ignored"}

    narrowed = narrow_env_for_provenance(source, resolved, declared_keys={"DECLARED"})

    assert narrowed == {"DECLARED": "hello"}
