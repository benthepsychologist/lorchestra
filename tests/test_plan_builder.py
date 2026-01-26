"""Tests for plan builder (e005b-01).

Tests cover:
- CallableResult to StoraclePlan conversion
- items_ref raises NotImplementedError
- Idempotency key computation
"""

import pytest
from lorchestra.callable import CallableResult
from lorchestra.plan_builder import (
    build_plan,
    StoraclePlan,
    _compute_idempotency_key,
    _hash_canonical,
)


class TestBuildPlan:
    """Tests for build_plan function."""

    def test_build_plan_returns_storacle_plan(self):
        """build_plan should return StoraclePlan."""
        result = CallableResult(items=[{"id": 1}])

        plan = build_plan(result, correlation_id="corr-123")

        assert isinstance(plan, StoraclePlan)

    def test_build_plan_sets_correlation_id(self):
        """build_plan should set correlation_id."""
        result = CallableResult(items=[])

        plan = build_plan(result, correlation_id="corr-456")

        assert plan.correlation_id == "corr-456"

    def test_build_plan_default_kind_and_version(self):
        """build_plan should set default kind and version."""
        result = CallableResult(items=[])

        plan = build_plan(result, correlation_id="corr")

        assert plan.kind == "storacle.plan"
        assert plan.version == "0.1"

    def test_build_plan_creates_ops_for_each_item(self):
        """build_plan should create one op per item."""
        result = CallableResult(items=[
            {"id": 1, "data": "a"},
            {"id": 2, "data": "b"},
            {"id": 3, "data": "c"},
        ])

        plan = build_plan(result, correlation_id="corr")

        assert len(plan.ops) == 3

    def test_build_plan_empty_items(self):
        """build_plan should handle empty items."""
        result = CallableResult(items=[])

        plan = build_plan(result, correlation_id="corr")

        assert len(plan.ops) == 0

    def test_build_plan_default_method_is_wal_append(self):
        """build_plan should default to wal.append method."""
        result = CallableResult(items=[{"id": 1}])

        plan = build_plan(result, correlation_id="corr")

        assert plan.ops[0].method == "wal.append"

    def test_build_plan_custom_method(self):
        """build_plan should accept custom method."""
        result = CallableResult(items=[{"id": 1}])

        plan = build_plan(result, correlation_id="corr", method="custom.method")

        assert plan.ops[0].method == "custom.method"

    def test_build_plan_items_ref_raises(self):
        """build_plan should raise NotImplementedError for items_ref."""
        result = CallableResult(items_ref="artifact://bucket/key")

        with pytest.raises(NotImplementedError, match="items_ref not supported"):
            build_plan(result, correlation_id="corr")


class TestStoracleOp:
    """Tests for StoracleOp dataclass."""

    def test_op_has_uuid_op_id(self):
        """Each op should have a UUID op_id."""
        result = CallableResult(items=[{"id": 1}])
        plan = build_plan(result, correlation_id="corr")

        op = plan.ops[0]
        # UUID format: 8-4-4-4-12
        assert len(op.op_id) == 36
        assert op.op_id.count("-") == 4

    def test_op_params_match_item(self):
        """Op params should match the item."""
        item = {"id": 1, "data": "test", "nested": {"key": "value"}}
        result = CallableResult(items=[item])

        plan = build_plan(result, correlation_id="corr")

        assert plan.ops[0].params == item

    def test_op_has_idempotency_key_for_wal_append(self):
        """Op should have idempotency_key for wal.append."""
        result = CallableResult(items=[{"id": 1}])

        plan = build_plan(result, correlation_id="corr", method="wal.append")

        assert plan.ops[0].idempotency_key is not None
        assert plan.ops[0].idempotency_key.startswith("sha256:")

    def test_op_no_idempotency_key_for_other_methods(self):
        """Op should not have idempotency_key for non-wal.append methods."""
        result = CallableResult(items=[{"id": 1}])

        plan = build_plan(result, correlation_id="corr", method="other.method")

        assert plan.ops[0].idempotency_key is None


class TestIdempotencyKeyComputation:
    """Tests for idempotency key computation."""

    def test_uses_identity_fields(self):
        """Should use stable identity fields for key."""
        item = {"stream_id": "s1", "event_id": "e1", "data": "test"}

        key1 = _compute_idempotency_key(item, "wal.append")
        key2 = _compute_idempotency_key(item, "wal.append")

        assert key1 == key2  # Deterministic

    def test_different_identity_fields_different_keys(self):
        """Different identity fields should produce different keys."""
        item1 = {"stream_id": "s1", "event_id": "e1"}
        item2 = {"stream_id": "s1", "event_id": "e2"}

        key1 = _compute_idempotency_key(item1, "wal.append")
        key2 = _compute_idempotency_key(item2, "wal.append")

        assert key1 != key2

    def test_uses_idem_key_if_present(self):
        """Should use idem_key field if present."""
        item = {"idem_key": "my-key", "data": "test"}

        key = _compute_idempotency_key(item, "wal.append")

        assert "sha256:" in key

    def test_falls_back_to_full_item(self):
        """Should fall back to full item hash if no identity fields."""
        item = {"data": "test", "value": 123}

        key = _compute_idempotency_key(item, "wal.append")

        assert key.startswith("sha256:")

    def test_key_includes_method(self):
        """Key should include method for uniqueness."""
        item = {"id": "1"}

        key1 = _compute_idempotency_key(item, "wal.append")
        key2 = _compute_idempotency_key(item, "other.method")

        assert key1 != key2


class TestHashCanonical:
    """Tests for _hash_canonical function."""

    def test_deterministic(self):
        """Hash should be deterministic."""
        data = {"b": 2, "a": 1}

        hash1 = _hash_canonical(data)
        hash2 = _hash_canonical(data)

        assert hash1 == hash2

    def test_starts_with_sha256(self):
        """Hash should start with sha256: prefix."""
        data = {"key": "value"}

        hash_val = _hash_canonical(data)

        assert hash_val.startswith("sha256:")

    def test_order_independent(self):
        """Hash should be order-independent (keys sorted)."""
        data1 = {"a": 1, "b": 2, "c": 3}
        data2 = {"c": 3, "b": 2, "a": 1}

        assert _hash_canonical(data1) == _hash_canonical(data2)


class TestStoraclePlanToDict:
    """Tests for StoraclePlan.to_dict() method."""

    def test_to_dict_structure(self):
        """to_dict should return correct structure."""
        result = CallableResult(items=[{"id": 1}])
        plan = build_plan(result, correlation_id="corr-123")

        d = plan.to_dict()

        assert d["kind"] == "storacle.plan"
        assert d["version"] == "0.1"
        assert d["correlation_id"] == "corr-123"
        assert "ops" in d
        assert len(d["ops"]) == 1

    def test_to_dict_op_structure(self):
        """to_dict ops should have correct structure."""
        result = CallableResult(items=[{"id": 1}])
        plan = build_plan(result, correlation_id="corr")

        d = plan.to_dict()
        op = d["ops"][0]

        assert "op_id" in op
        assert "method" in op
        assert "params" in op
        assert "idempotency_key" in op
