"""Tests for CallableResult (e005b-01).

Tests cover:
- XOR constraint: exactly one of items or items_ref
- Default values
- Stats field
"""

import pytest
from lorchestra.callable import CallableResult


class TestCallableResultXORConstraint:
    """Tests for the items XOR items_ref constraint."""

    def test_items_only_succeeds(self):
        """CallableResult with items only should succeed."""
        result = CallableResult(items=[])
        assert result.items == []
        assert result.items_ref is None

    def test_items_with_data_succeeds(self):
        """CallableResult with non-empty items should succeed."""
        result = CallableResult(items=[{"id": 1}, {"id": 2}])
        assert len(result.items) == 2

    def test_items_ref_only_succeeds(self):
        """CallableResult with items_ref only should succeed."""
        result = CallableResult(items_ref="artifact://bucket/path")
        assert result.items is None
        assert result.items_ref == "artifact://bucket/path"

    def test_both_items_and_items_ref_raises(self):
        """CallableResult with both items and items_ref should raise."""
        with pytest.raises(ValueError, match="exactly one of items or items_ref"):
            CallableResult(items=[{"id": 1}], items_ref="artifact://x")

    def test_neither_items_nor_items_ref_raises(self):
        """CallableResult with neither items nor items_ref should raise."""
        with pytest.raises(ValueError, match="exactly one of items or items_ref"):
            CallableResult()

    def test_explicit_none_for_both_raises(self):
        """CallableResult with explicit None for both should raise."""
        with pytest.raises(ValueError, match="exactly one of items or items_ref"):
            CallableResult(items=None, items_ref=None)


class TestCallableResultDefaults:
    """Tests for CallableResult default values."""

    def test_schema_version_default(self):
        """schema_version should default to '1.0'."""
        result = CallableResult(items=[])
        assert result.schema_version == "1.0"

    def test_schema_version_custom(self):
        """schema_version can be customized."""
        result = CallableResult(schema_version="2.0", items=[])
        assert result.schema_version == "2.0"

    def test_stats_default_empty_dict(self):
        """stats should default to empty dict."""
        result = CallableResult(items=[])
        assert result.stats == {}

    def test_stats_custom(self):
        """stats can be customized."""
        result = CallableResult(items=[], stats={"rows_processed": 100})
        assert result.stats == {"rows_processed": 100}


class TestCallableResultFromDict:
    """Tests for creating CallableResult from dict (as callables return)."""

    def test_from_dict_with_items(self):
        """Should create CallableResult from dict with items."""
        data = {
            "schema_version": "1.0",
            "items": [{"id": 1}, {"id": 2}],
            "stats": {"count": 2},
        }
        result = CallableResult(**data)

        assert result.schema_version == "1.0"
        assert result.items == [{"id": 1}, {"id": 2}]
        assert result.items_ref is None
        assert result.stats == {"count": 2}

    def test_from_dict_with_items_ref(self):
        """Should create CallableResult from dict with items_ref."""
        data = {
            "schema_version": "1.0",
            "items_ref": "artifact://bucket/key",
            "stats": {},
        }
        result = CallableResult(**data)

        assert result.items is None
        assert result.items_ref == "artifact://bucket/key"

    def test_from_dict_minimal(self):
        """Should create CallableResult from minimal dict."""
        data = {"items": []}
        result = CallableResult(**data)

        assert result.schema_version == "1.0"
        assert result.items == []
        assert result.stats == {}
