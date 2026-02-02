"""Tests for plan builder (e005b-01, e005b-07).

Tests cover:
- CallableResult to StoraclePlan conversion
- items_ref raises NotImplementedError
- Idempotency key computation
- Batch wrapping mode (e005b-07): payload_wrap, id_field, dataset resolution
"""

import json
from unittest.mock import patch

import pytest
from lorchestra.callable import CallableResult
from lorchestra.plan_builder import (
    build_plan,
    build_plan_from_items,
    StoraclePlan,
    _compute_idempotency_key,
    _compute_idem_key,
    _hash_canonical,
    _resolve_dataset,
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

    def test_build_plan_to_dict_has_plan_version(self):
        """build_plan to_dict should have storacle.plan/1.0.0 plan_version."""
        result = CallableResult(items=[])

        plan = build_plan(result, correlation_id="corr")
        d = plan.to_dict()

        assert d["plan_version"] == "storacle.plan/1.0.0"
        assert d["jsonrpc"] == "2.0"

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
        """to_dict should return storacle.plan/1.0.0 contract."""
        result = CallableResult(items=[{"id": 1}])
        plan = build_plan(result, correlation_id="corr-123")

        d = plan.to_dict()

        assert d["plan_version"] == "storacle.plan/1.0.0"
        assert d["jsonrpc"] == "2.0"
        assert d["meta"]["correlation_id"] == "corr-123"
        assert "plan_id" in d
        assert "ops" in d
        assert len(d["ops"]) == 1

    def test_to_dict_op_structure(self):
        """to_dict ops should use JSON-RPC 2.0 format."""
        result = CallableResult(items=[{"id": 1}])
        plan = build_plan(result, correlation_id="corr")

        d = plan.to_dict()
        op = d["ops"][0]

        assert op["jsonrpc"] == "2.0"
        assert "id" in op
        assert "method" in op
        assert "params" in op


# ============================================================================
# e005b-07: Batch wrapping mode
# ============================================================================


def _mock_config(**overrides):
    """Create a mock LorchestraConfig for dataset resolution tests."""
    from lorchestra.config import LorchestraConfig
    defaults = {
        "project": "test-project",
        "dataset_raw": "test_raw",
        "dataset_canonical": "test_canonical",
        "dataset_derived": "test_derived",
        "sqlite_path": "/tmp/test.db",
        "local_views_root": "/tmp/views",
    }
    defaults.update(overrides)
    return LorchestraConfig(**defaults)


class TestBatchWrappingMode:
    """Tests for batch wrapping mode (e005b-07)."""

    def test_batch_mode_triggers_on_dataset_table_key_columns(self):
        """When dataset + table + key_columns provided, produces single batch op."""
        items = [{"id": "a"}, {"id": "b"}]

        with patch("lorchestra.config.load_config", return_value=_mock_config()):
            plan = build_plan_from_items(
                items=items,
                correlation_id="test_batch",
                method="bq.upsert",
                dataset="raw",
                table="raw_objects",
                key_columns=["idem_key"],
            )

        assert len(plan.ops) == 1
        op = plan.ops[0]
        assert op.method == "bq.upsert"
        assert op.params["dataset"] == "test_raw"
        assert op.params["table"] == "raw_objects"
        assert op.params["key_columns"] == ["idem_key"]
        assert len(op.params["rows"]) == 2

    def test_non_batch_mode_without_batch_params(self):
        """Without batch params, each item becomes its own op."""
        items = [{"id": "a"}, {"id": "b"}]

        plan = build_plan_from_items(
            items=items,
            correlation_id="test_non_batch",
            method="bq.upsert",
        )

        assert len(plan.ops) == 2

    def test_batch_mode_empty_items(self):
        """Batch mode with empty items produces single op with empty rows."""
        with patch("lorchestra.config.load_config", return_value=_mock_config()):
            plan = build_plan_from_items(
                items=[],
                correlation_id="test_empty",
                method="bq.upsert",
                dataset="raw",
                table="raw_objects",
                key_columns=["idem_key"],
            )

        assert len(plan.ops) == 1
        assert plan.ops[0].params["rows"] == []

    def test_batch_mode_no_idempotency_key(self):
        """Batch ops should not have an idempotency_key (bq.upsert uses rows)."""
        items = [{"id": "a"}]

        with patch("lorchestra.config.load_config", return_value=_mock_config()):
            plan = build_plan_from_items(
                items=items,
                correlation_id="test",
                method="bq.upsert",
                dataset="raw",
                table="raw_objects",
                key_columns=["idem_key"],
            )

        assert plan.ops[0].idempotency_key is None


class TestPayloadWrap:
    """Tests for payload_wrap feature (e005b-07)."""

    def test_payload_wrap_nests_item_as_json(self):
        """payload_wrap=True should nest each item as JSON payload column."""
        items = [{"id": "cus_123", "name": "Alice", "email": "a@test.com"}]

        with patch("lorchestra.config.load_config", return_value=_mock_config()):
            plan = build_plan_from_items(
                items=items,
                correlation_id="test_wrap",
                method="bq.upsert",
                dataset="raw",
                table="raw_objects",
                key_columns=["idem_key"],
                payload_wrap=True,
            )

        row = plan.ops[0].params["rows"][0]
        assert "payload" in row
        parsed = json.loads(row["payload"])
        assert parsed["id"] == "cus_123"
        assert parsed["name"] == "Alice"
        # Original keys should NOT be in row (they're nested in payload)
        assert "name" not in row
        assert "email" not in row

    def test_payload_wrap_false_passes_item_through(self):
        """payload_wrap=False should pass item fields through directly."""
        items = [{"idem_key": "ik-1", "payload": {"data": "test"}, "source_system": "stripe"}]

        with patch("lorchestra.config.load_config", return_value=_mock_config()):
            plan = build_plan_from_items(
                items=items,
                correlation_id="test_no_wrap",
                method="bq.upsert",
                dataset="canonical",
                table="canonical_objects",
                key_columns=["idem_key"],
                payload_wrap=False,
            )

        row = plan.ops[0].params["rows"][0]
        assert row["idem_key"] == "ik-1"
        assert row["payload"] == {"data": "test"}
        assert row["source_system"] == "stripe"

    def test_payload_wrap_with_field_defaults(self):
        """payload_wrap + field_defaults should add metadata to wrapped row."""
        items = [{"id": "cus_123", "name": "Alice"}]

        with patch("lorchestra.config.load_config", return_value=_mock_config()):
            plan = build_plan_from_items(
                items=items,
                correlation_id="test",
                method="bq.upsert",
                dataset="raw",
                table="raw_objects",
                key_columns=["idem_key"],
                payload_wrap=True,
                field_defaults={
                    "source_system": "stripe",
                    "connection_name": "stripe-prod",
                    "object_type": "customer",
                },
            )

        row = plan.ops[0].params["rows"][0]
        assert row["source_system"] == "stripe"
        assert row["connection_name"] == "stripe-prod"
        assert row["object_type"] == "customer"
        assert "payload" in row


class TestIdemKeyComputation:
    """Tests for id_field-based idem_key computation (e005b-07)."""

    def test_id_field_computes_idem_key(self):
        """id_field should compute idem_key from item + field_defaults."""
        items = [{"id": "cus_123", "name": "Alice"}]

        with patch("lorchestra.config.load_config", return_value=_mock_config()):
            plan = build_plan_from_items(
                items=items,
                correlation_id="test",
                method="bq.upsert",
                dataset="raw",
                table="raw_objects",
                key_columns=["idem_key"],
                payload_wrap=True,
                id_field="id",
                field_defaults={
                    "source_system": "stripe",
                    "connection_name": "stripe-prod",
                    "object_type": "customer",
                },
            )

        row = plan.ops[0].params["rows"][0]
        assert row["idem_key"] == "stripe:stripe-prod:customer:cus_123"

    def test_id_field_responseId(self):
        """id_field=responseId for Google Forms."""
        items = [{"responseId": "ACYDBNi84", "answers": []}]

        with patch("lorchestra.config.load_config", return_value=_mock_config()):
            plan = build_plan_from_items(
                items=items,
                correlation_id="test",
                method="bq.upsert",
                dataset="raw",
                table="raw_objects",
                key_columns=["idem_key"],
                payload_wrap=True,
                id_field="responseId",
                field_defaults={
                    "source_system": "google_forms",
                    "connection_name": "google-forms-intake-01",
                    "object_type": "form_response",
                },
            )

        row = plan.ops[0].params["rows"][0]
        assert row["idem_key"] == "google_forms:google-forms-intake-01:form_response:ACYDBNi84"

    def test_id_field_missing_raises(self):
        """Missing id_field in item should raise ValueError."""
        items = [{"name": "Alice"}]

        with patch("lorchestra.config.load_config", return_value=_mock_config()):
            with pytest.raises(ValueError, match="id_field 'id' not found"):
                build_plan_from_items(
                    items=items,
                    correlation_id="test",
                    method="bq.upsert",
                    dataset="raw",
                    table="raw_objects",
                    key_columns=["idem_key"],
                    id_field="id",
                )

    def test_compute_idem_key_function(self):
        """_compute_idem_key should follow the pattern."""
        item = {"contactid": "abc-123"}
        defaults = {
            "source_system": "dataverse",
            "connection_name": "dataverse-clinic",
            "object_type": "contact",
        }

        key = _compute_idem_key(item, "contactid", defaults)
        assert key == "dataverse:dataverse-clinic:contact:abc-123"


class TestDatasetResolution:
    """Tests for dataset name resolution (e005b-07)."""

    def test_resolves_raw(self):
        """'raw' should resolve to config.dataset_raw."""
        with patch("lorchestra.config.load_config", return_value=_mock_config()):
            assert _resolve_dataset("raw") == "test_raw"

    def test_resolves_canonical(self):
        """'canonical' should resolve to config.dataset_canonical."""
        with patch("lorchestra.config.load_config", return_value=_mock_config()):
            assert _resolve_dataset("canonical") == "test_canonical"

    def test_resolves_derived(self):
        """'derived' should resolve to config.dataset_derived."""
        with patch("lorchestra.config.load_config", return_value=_mock_config()):
            assert _resolve_dataset("derived") == "test_derived"

    def test_unknown_name_passthrough(self):
        """Unknown dataset name should pass through as-is."""
        with patch("lorchestra.config.load_config", return_value=_mock_config()):
            assert _resolve_dataset("events_dev") == "events_dev"

    def test_dataset_in_batch_plan(self):
        """Resolved dataset should appear in batch op params."""
        items = [{"id": "a"}]

        with patch("lorchestra.config.load_config", return_value=_mock_config(dataset_raw="my_raw_dataset")):
            plan = build_plan_from_items(
                items=items,
                correlation_id="test",
                method="bq.upsert",
                dataset="raw",
                table="raw_objects",
                key_columns=["idem_key"],
            )

        assert plan.ops[0].params["dataset"] == "my_raw_dataset"


class TestBatchProcessingOrder:
    """Tests for batch processing order (spec: wrap -> defaults -> idem -> map -> fields)."""

    def test_full_ingest_pipeline(self):
        """Simulate a full ingest pipeline: raw item -> wrapped row with idem_key."""
        items = [
            {"id": "cus_001", "name": "Alice", "email": "a@test.com"},
            {"id": "cus_002", "name": "Bob", "email": "b@test.com"},
        ]

        with patch("lorchestra.config.load_config", return_value=_mock_config()):
            plan = build_plan_from_items(
                items=items,
                correlation_id="ingest_test",
                method="bq.upsert",
                dataset="raw",
                table="raw_objects",
                key_columns=["idem_key"],
                payload_wrap=True,
                id_field="id",
                field_defaults={
                    "source_system": "stripe",
                    "connection_name": "stripe-prod",
                    "object_type": "customer",
                },
            )

        assert len(plan.ops) == 1
        op = plan.ops[0]
        assert op.method == "bq.upsert"
        assert op.params["dataset"] == "test_raw"
        assert op.params["table"] == "raw_objects"
        assert op.params["key_columns"] == ["idem_key"]
        assert len(op.params["rows"]) == 2

        row0 = op.params["rows"][0]
        assert row0["idem_key"] == "stripe:stripe-prod:customer:cus_001"
        assert row0["source_system"] == "stripe"
        assert row0["connection_name"] == "stripe-prod"
        assert row0["object_type"] == "customer"
        payload = json.loads(row0["payload"])
        assert payload["id"] == "cus_001"
        assert payload["name"] == "Alice"

        row1 = op.params["rows"][1]
        assert row1["idem_key"] == "stripe:stripe-prod:customer:cus_002"

    def test_canonize_pipeline_no_wrap(self):
        """Simulate canonize pipeline: items already shaped, just batch bundle."""
        items = [
            {
                "idem_key": "stripe:stripe-prod:customer:cus_001#customer",
                "source_system": "stripe",
                "connection_name": "stripe-prod",
                "object_type": "customer",
                "canonical_schema": "iglu:org.canonical/customer/jsonschema/1-0-0",
                "transform_ref": "customer/stripe_to_canonical@1-0-0",
                "payload": {"canonical": "data"},
            },
        ]

        with patch("lorchestra.config.load_config", return_value=_mock_config()):
            plan = build_plan_from_items(
                items=items,
                correlation_id="canonize_test",
                method="bq.upsert",
                dataset="canonical",
                table="canonical_objects",
                key_columns=["idem_key"],
            )

        assert len(plan.ops) == 1
        op = plan.ops[0]
        assert op.params["dataset"] == "test_canonical"
        assert op.params["table"] == "canonical_objects"
        row = op.params["rows"][0]
        assert row["idem_key"] == "stripe:stripe-prod:customer:cus_001#customer"
        assert row["payload"] == {"canonical": "data"}

    def test_field_map_applied_in_batch(self):
        """field_map should rename keys in batch mode rows."""
        items = [{"old_name": "value"}]

        with patch("lorchestra.config.load_config", return_value=_mock_config()):
            plan = build_plan_from_items(
                items=items,
                correlation_id="test",
                method="bq.upsert",
                dataset="raw",
                table="raw_objects",
                key_columns=["idem_key"],
                field_map={"new_name": "old_name"},
            )

        row = plan.ops[0].params["rows"][0]
        assert "new_name" in row
        assert "old_name" not in row

    def test_fields_allowlist_in_batch(self):
        """fields should filter columns in batch mode rows."""
        items = [{"keep": "yes", "drop": "no"}]

        with patch("lorchestra.config.load_config", return_value=_mock_config()):
            plan = build_plan_from_items(
                items=items,
                correlation_id="test",
                method="bq.upsert",
                dataset="raw",
                table="raw_objects",
                key_columns=["idem_key"],
                fields=["keep"],
            )

        row = plan.ops[0].params["rows"][0]
        assert row == {"keep": "yes"}
