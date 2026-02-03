"""Tests for query_builder (e005b-08).

Tests cover:
- Simple queries with filters
- Incremental left_anti queries with suffix
- Incremental not_exists queries
- Parameterized queries (no string interpolation)
- Dataset resolution via callback
"""

import pytest
from lorchestra.query_builder import build_query, QueryParam


def _identity_resolve(name: str) -> str:
    """Identity resolver: returns name as-is."""
    return name


def _prefixed_resolve(name: str) -> str:
    """Test resolver: raw->events_dev, canonical->canonical_dev."""
    mapping = {"raw": "events_dev", "canonical": "canonical_dev", "derived": "derived_dev"}
    return mapping.get(name, name)


class TestSimpleQuery:
    """Tests for non-incremental queries."""

    def test_simple_select_all(self):
        sql, params = build_query(
            {"dataset": "raw", "table": "raw_objects"},
            resolve_dataset=_identity_resolve,
        )
        assert sql == "SELECT * FROM `raw.raw_objects` WHERE TRUE"
        assert params == []

    def test_simple_with_columns(self):
        sql, params = build_query(
            {"dataset": "raw", "table": "raw_objects", "columns": ["idem_key", "payload"]},
            resolve_dataset=_identity_resolve,
        )
        assert sql == "SELECT idem_key, payload FROM `raw.raw_objects` WHERE TRUE"

    def test_simple_with_filters(self):
        sql, params = build_query(
            {
                "dataset": "raw",
                "table": "raw_objects",
                "filters": {"source_system": "stripe", "object_type": "customer"},
            },
            resolve_dataset=_identity_resolve,
        )
        assert "source_system = @source_system" in sql
        assert "object_type = @object_type" in sql
        assert len(params) == 2
        assert any(p.name == "source_system" and p.value == "stripe" for p in params)
        assert any(p.name == "object_type" and p.value == "customer" for p in params)

    def test_simple_with_limit(self):
        sql, params = build_query(
            {"dataset": "raw", "table": "raw_objects", "limit": 100},
            resolve_dataset=_identity_resolve,
        )
        assert sql.endswith("LIMIT 100")

    def test_dataset_resolution(self):
        sql, _ = build_query(
            {"dataset": "raw", "table": "raw_objects"},
            resolve_dataset=_prefixed_resolve,
        )
        assert "`events_dev.raw_objects`" in sql


class TestIncrementalLeftAnti:
    """Tests for incremental left_anti queries."""

    def test_left_anti_without_suffix(self):
        sql, params = build_query(
            {
                "dataset": "raw",
                "table": "raw_objects",
                "columns": ["idem_key", "payload"],
                "filters": {"source_system": "stripe"},
                "incremental": {
                    "target_dataset": "canonical",
                    "target_table": "canonical_objects",
                    "source_key": "idem_key",
                    "target_key": "idem_key",
                    "mode": "left_anti",
                },
            },
            resolve_dataset=_identity_resolve,
        )
        assert "LEFT JOIN" in sql
        assert "t.idem_key = s.idem_key" in sql
        assert "t.idem_key IS NULL OR" in sql
        assert "s.source_system = @source_system" in sql

    def test_left_anti_with_suffix(self):
        sql, params = build_query(
            {
                "dataset": "raw",
                "table": "raw_objects",
                "columns": ["idem_key", "payload"],
                "filters": {"source_system": "stripe", "object_type": "customer"},
                "incremental": {
                    "target_dataset": "canonical",
                    "target_table": "canonical_objects",
                    "source_key": "idem_key",
                    "target_key": "idem_key",
                    "join_key_suffix": "customer",
                    "mode": "left_anti",
                },
            },
            resolve_dataset=_identity_resolve,
        )
        assert "CONCAT(s.idem_key, '#', @_join_suffix)" in sql
        suffix_param = [p for p in params if p.name == "_join_suffix"]
        assert len(suffix_param) == 1
        assert suffix_param[0].value == "customer"

    def test_left_anti_dataset_resolution(self):
        sql, _ = build_query(
            {
                "dataset": "raw",
                "table": "raw_objects",
                "incremental": {
                    "target_dataset": "canonical",
                    "target_table": "canonical_objects",
                    "source_key": "idem_key",
                    "target_key": "idem_key",
                    "mode": "left_anti",
                },
            },
            resolve_dataset=_prefixed_resolve,
        )
        assert "`events_dev.raw_objects`" in sql
        assert "`canonical_dev.canonical_objects`" in sql


class TestIncrementalNotExists:
    """Tests for incremental not_exists queries."""

    def test_not_exists_basic(self):
        sql, params = build_query(
            {
                "dataset": "raw",
                "table": "raw_objects",
                "columns": ["idem_key", "payload"],
                "incremental": {
                    "target_dataset": "canonical",
                    "target_table": "canonical_objects",
                    "source_key": "idem_key",
                    "target_key": "idem_key",
                    "mode": "not_exists",
                },
            },
            resolve_dataset=_identity_resolve,
        )
        assert "NOT EXISTS" in sql
        assert "SELECT 1 FROM" in sql
        assert "t.idem_key = s.idem_key" in sql

    def test_not_exists_with_suffix(self):
        sql, params = build_query(
            {
                "dataset": "raw",
                "table": "raw_objects",
                "columns": ["idem_key"],
                "incremental": {
                    "target_dataset": "canonical",
                    "target_table": "canonical_objects",
                    "source_key": "idem_key",
                    "target_key": "idem_key",
                    "join_key_suffix": "email_jmap_lite",
                    "mode": "not_exists",
                },
            },
            resolve_dataset=_identity_resolve,
        )
        assert "CONCAT(s.idem_key, '#', @_join_suffix)" in sql


class TestInvalidMode:
    def test_unknown_mode_raises(self):
        with pytest.raises(ValueError, match="Unknown incremental mode"):
            build_query(
                {
                    "dataset": "raw",
                    "table": "raw_objects",
                    "incremental": {
                        "target_dataset": "canonical",
                        "target_table": "canonical_objects",
                        "source_key": "idem_key",
                        "target_key": "idem_key",
                        "mode": "bad_mode",
                    },
                },
                resolve_dataset=_identity_resolve,
            )
