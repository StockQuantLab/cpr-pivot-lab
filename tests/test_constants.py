"""Tests for engine/constants.py symbol normalization and validation."""

import pytest

from engine.constants import normalize_symbol


def test_normalize_symbol_allows_hyphen_and_space() -> None:
    assert normalize_symbol("bajaj-auto") == "BAJAJ-AUTO"
    assert normalize_symbol("jk agri") == "JK AGRI"


def test_normalize_symbol_rejects_unsafe_chars() -> None:
    with pytest.raises(ValueError):
        normalize_symbol("SBIN;DROP TABLE")
