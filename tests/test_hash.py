"""Hash function conformance tests against spec vectors."""

from __future__ import annotations

import pytest
from conftest import load_hash_vectors

from gradual._hash import hash_string


vectors = load_hash_vectors()


@pytest.mark.parametrize(
    "vector",
    vectors,
    ids=[v["input"][:40] or "(empty)" for v in vectors],
)
def test_hash_string(vector: dict) -> None:
    """hash_string must produce identical output to the TypeScript implementation."""
    assert hash_string(vector["input"]) == vector["hash"]


@pytest.mark.parametrize(
    "vector",
    vectors,
    ids=[v["input"][:40] or "(empty)" for v in vectors],
)
def test_bucket_value(vector: dict) -> None:
    """Bucket value (hash % 100000) must match."""
    assert hash_string(vector["input"]) % 100_000 == vector["bucket"]
