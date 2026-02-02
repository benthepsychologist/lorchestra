"""Tests for lorchestra Op enum (e005b-05).

Tests cover:
- Op enum members exist
- Removed ops (query.*, write.*, assert.*, call.*-specific) raise AttributeError
- Backend property returns correct values
"""

import pytest
from lorchestra.schemas.ops import Op


class TestOpEnumMembers:
    """Tests for Op enum member existence."""

    def test_call_exists(self):
        """Generic call op should exist."""
        assert Op.CALL.value == "call"

    def test_plan_build_exists(self):
        """plan.build op should exist."""
        assert Op.PLAN_BUILD.value == "plan.build"

    def test_storacle_submit_exists(self):
        """storacle.submit op should exist."""
        assert Op.STORACLE_SUBMIT.value == "storacle.submit"

    def test_compute_llm_exists(self):
        """compute.llm op should exist."""
        assert Op.COMPUTE_LLM.value == "compute.llm"

    def test_job_run_exists(self):
        """job.run op should exist."""
        assert Op.JOB_RUN.value == "job.run"


class TestRemovedOps:
    """Tests that removed ops raise AttributeError."""

    def test_call_injest_removed(self):
        """call.injest should be removed (replaced by generic call)."""
        with pytest.raises(AttributeError):
            _ = Op.CALL_INJEST

    def test_call_canonizer_removed(self):
        """call.canonizer should be removed (replaced by generic call)."""
        with pytest.raises(AttributeError):
            _ = Op.CALL_CANONIZER

    def test_call_finalform_removed(self):
        """call.finalform should be removed (replaced by generic call)."""
        with pytest.raises(AttributeError):
            _ = Op.CALL_FINALFORM

    def test_call_projectionist_removed(self):
        """call.projectionist should be removed (replaced by generic call)."""
        with pytest.raises(AttributeError):
            _ = Op.CALL_PROJECTIONIST

    def test_call_workman_removed(self):
        """call.workman should be removed (replaced by generic call)."""
        with pytest.raises(AttributeError):
            _ = Op.CALL_WORKMAN

    def test_query_raw_objects_removed(self):
        """query.raw_objects should be removed."""
        with pytest.raises(AttributeError):
            _ = Op.QUERY_RAW_OBJECTS

    def test_query_canonical_objects_removed(self):
        """query.canonical_objects should be removed."""
        with pytest.raises(AttributeError):
            _ = Op.QUERY_CANONICAL_OBJECTS

    def test_query_last_sync_removed(self):
        """query.last_sync should be removed."""
        with pytest.raises(AttributeError):
            _ = Op.QUERY_LAST_SYNC

    def test_write_upsert_removed(self):
        """write.upsert should be removed."""
        with pytest.raises(AttributeError):
            _ = Op.WRITE_UPSERT

    def test_write_insert_removed(self):
        """write.insert should be removed."""
        with pytest.raises(AttributeError):
            _ = Op.WRITE_INSERT

    def test_write_delete_removed(self):
        """write.delete should be removed."""
        with pytest.raises(AttributeError):
            _ = Op.WRITE_DELETE

    def test_write_merge_removed(self):
        """write.merge should be removed."""
        with pytest.raises(AttributeError):
            _ = Op.WRITE_MERGE

    def test_assert_rows_removed(self):
        """assert.rows should be removed."""
        with pytest.raises(AttributeError):
            _ = Op.ASSERT_ROWS

    def test_assert_schema_removed(self):
        """assert.schema should be removed."""
        with pytest.raises(AttributeError):
            _ = Op.ASSERT_SCHEMA

    def test_assert_unique_removed(self):
        """assert.unique should be removed."""
        with pytest.raises(AttributeError):
            _ = Op.ASSERT_UNIQUE


class TestBackendProperty:
    """Tests for Op.backend property."""

    def test_call_backend_is_callable(self):
        """call backend should be 'callable'."""
        assert Op.CALL.backend == "callable"

    def test_plan_build_backend_is_native(self):
        """plan.build backend should be 'native'."""
        assert Op.PLAN_BUILD.backend == "native"

    def test_storacle_submit_backend_is_native(self):
        """storacle.submit backend should be 'native'."""
        assert Op.STORACLE_SUBMIT.backend == "native"

    def test_compute_llm_backend_is_inferometer(self):
        """compute.llm backend should be 'inferometer'."""
        assert Op.COMPUTE_LLM.backend == "inferometer"

    def test_job_run_backend_is_orchestration(self):
        """job.run backend should be 'orchestration'."""
        assert Op.JOB_RUN.backend == "orchestration"


class TestFromString:
    """Tests for Op.from_string() method."""

    def test_from_string_call(self):
        """from_string should parse 'call'."""
        assert Op.from_string("call") == Op.CALL

    def test_from_string_plan_build(self):
        """from_string should parse 'plan.build'."""
        assert Op.from_string("plan.build") == Op.PLAN_BUILD

    def test_from_string_storacle_submit(self):
        """from_string should parse 'storacle.submit'."""
        assert Op.from_string("storacle.submit") == Op.STORACLE_SUBMIT

    def test_from_string_compute_llm(self):
        """from_string should parse 'compute.llm'."""
        assert Op.from_string("compute.llm") == Op.COMPUTE_LLM

    def test_from_string_job_run(self):
        """from_string should parse 'job.run'."""
        assert Op.from_string("job.run") == Op.JOB_RUN

    def test_from_string_unknown_raises(self):
        """from_string should raise for unknown ops."""
        with pytest.raises(ValueError, match="Unknown operation"):
            Op.from_string("query.raw_objects")

    def test_from_string_invalid_raises(self):
        """from_string should raise for invalid strings."""
        with pytest.raises(ValueError, match="Unknown operation"):
            Op.from_string("not.an.op")

    def test_from_string_old_call_ops_raise(self):
        """from_string should raise for removed call.* ops."""
        with pytest.raises(ValueError, match="Unknown operation"):
            Op.from_string("call.injest")
