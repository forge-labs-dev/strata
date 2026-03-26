"""Tests for provenance hashing."""

from strata.notebook.provenance import (
    compute_provenance_hash,
    compute_source_hash,
)


def test_source_hash_stability():
    """Same source should produce same hash."""
    source = "x = 1 + 1"
    hash1 = compute_source_hash(source)
    hash2 = compute_source_hash(source)
    assert hash1 == hash2


def test_source_hash_changes_with_source():
    """Different source should produce different hash."""
    source1 = "x = 1 + 1"
    source2 = "x = 1 + 2"
    hash1 = compute_source_hash(source1)
    hash2 = compute_source_hash(source2)
    assert hash1 != hash2


def test_source_hash_whitespace_sensitive():
    """Whitespace changes should affect hash (intentional)."""
    source1 = "x = 1 + 1"
    source2 = "x = 1 +  1"  # Extra space
    hash1 = compute_source_hash(source1)
    hash2 = compute_source_hash(source2)
    assert hash1 != hash2


def test_provenance_hash_stability():
    """Same inputs should produce same provenance hash."""
    input_hashes = ["hash1", "hash2"]
    source_hash = compute_source_hash("x = 1")
    env_hash = compute_source_hash("env")

    hash1 = compute_provenance_hash(input_hashes, source_hash, env_hash)
    hash2 = compute_provenance_hash(input_hashes, source_hash, env_hash)

    assert hash1 == hash2


def test_provenance_hash_ordering_invariance():
    """Input order should not affect provenance hash."""
    input_hashes1 = ["hash1", "hash2", "hash3"]
    input_hashes2 = ["hash3", "hash1", "hash2"]
    source_hash = compute_source_hash("x = 1")
    env_hash = compute_source_hash("env")

    hash1 = compute_provenance_hash(input_hashes1, source_hash, env_hash)
    hash2 = compute_provenance_hash(input_hashes2, source_hash, env_hash)

    assert hash1 == hash2


def test_provenance_hash_changes_with_source():
    """Source hash change should affect provenance hash."""
    input_hashes = ["hash1"]
    source1 = compute_source_hash("x = 1")
    source2 = compute_source_hash("x = 2")
    env_hash = compute_source_hash("env")

    hash1 = compute_provenance_hash(input_hashes, source1, env_hash)
    hash2 = compute_provenance_hash(input_hashes, source2, env_hash)

    assert hash1 != hash2


def test_provenance_hash_changes_with_env():
    """Env hash change should affect provenance hash."""
    input_hashes = ["hash1"]
    source_hash = compute_source_hash("x = 1")
    env1 = compute_source_hash("env1")
    env2 = compute_source_hash("env2")

    hash1 = compute_provenance_hash(input_hashes, source_hash, env1)
    hash2 = compute_provenance_hash(input_hashes, source_hash, env2)

    assert hash1 != hash2


def test_provenance_hash_changes_with_inputs():
    """Input change should affect provenance hash."""
    source_hash = compute_source_hash("x = 1")
    env_hash = compute_source_hash("env")

    hash1 = compute_provenance_hash(["hash1"], source_hash, env_hash)
    hash2 = compute_provenance_hash(["hash2"], source_hash, env_hash)

    assert hash1 != hash2


def test_provenance_hash_empty_inputs():
    """Empty inputs should be valid."""
    source_hash = compute_source_hash("x = 1")
    env_hash = compute_source_hash("env")

    hash1 = compute_provenance_hash([], source_hash, env_hash)
    hash2 = compute_provenance_hash([], source_hash, env_hash)

    assert hash1 == hash2
