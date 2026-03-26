"""Tests for environment hashing."""

import hashlib

from strata.notebook.env import compute_lockfile_hash


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
