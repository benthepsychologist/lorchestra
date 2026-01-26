"""Tests for lorchestra Op enum (e005b-01).

Tests cover:
- Op enum members exist
- Removed ops (query.*, write.*, assert.*) raise AttributeError
- Backend property returns correct values
"""

import pytest
from lorchestra.schemas.ops import Op


class TestOpEnumMembers:
    """Tests for Op enum member existence."""

    def test_call_injest_exists(self):
        """call.injest op should exist."""
        assert Op.CALL_INJEST.value == "call.injest"

    def test_call_canonizer_exists(self):
        """call.canonizer op should exist."""
        assert Op.CALL_CANONIZER.value == "call.canonizer"

    def test_call_finalform_exists(self):
        """call.finalform op should exist."""
        assert Op.CALL_FINALFORM.value == "call.finalform"

    def test_call_projectionist_exists(self):
        """call.projectionist op should exist."""
        assert Op.CALL_PROJECTIONIST.value == "call.projectionist"

    def test_call_workman_exists(self):
        """call.workman op should exist."""
        assert Op.CALL_WORKMAN.value == "call.workman"

    def test_compute_llm_exists(self):
        """compute.llm op should exist."""
        assert Op.COMPUTE_LLM.value == "compute.llm"

    def test_job_run_exists(self):
        """job.run op should exist."""
        assert Op.JOB_RUN.value == "job.run"


class TestRemovedOps:
    """Tests that removed ops raise AttributeError."""

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

    def test_call_injest_backend_is_callable(self):
        """call.injest backend should be 'callable'."""
        assert Op.CALL_INJEST.backend == "callable"

    def test_call_canonizer_backend_is_callable(self):
        """call.canonizer backend should be 'callable'."""
        assert Op.CALL_CANONIZER.backend == "callable"

    def test_call_finalform_backend_is_callable(self):
        """call.finalform backend should be 'callable'."""
        assert Op.CALL_FINALFORM.backend == "callable"

    def test_call_projectionist_backend_is_callable(self):
        """call.projectionist backend should be 'callable'."""
        assert Op.CALL_PROJECTIONIST.backend == "callable"

    def test_call_workman_backend_is_callable(self):
        """call.workman backend should be 'callable'."""
        assert Op.CALL_WORKMAN.backend == "callable"

    def test_compute_llm_backend_is_inferator(self):
        """compute.llm backend should be 'inferator'."""
        assert Op.COMPUTE_LLM.backend == "inferator"

    def test_job_run_backend_is_orchestration(self):
        """job.run backend should be 'orchestration'."""
        assert Op.JOB_RUN.backend == "orchestration"


class TestFromString:
    """Tests for Op.from_string() method."""

    def test_from_string_call_injest(self):
        """from_string should parse 'call.injest'."""
        assert Op.from_string("call.injest") == Op.CALL_INJEST

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
