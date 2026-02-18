"""Tests for lorchestra.compiler module.

Tests @ctx, @payload, @self, and @run reference resolution,
if_ condition evaluation, and the Compiler class.
"""

import pytest
from datetime import datetime, timezone

from lorchestra.schemas import (
    JobDef,
    StepDef,
    JobInstance,
    Op,
    CompileError,
)
from lorchestra.compiler import (
    compile_job,
    _resolve_reference,
    _resolve_value,
    Compiler,
)


class TestResolveReference:
    """Tests for _resolve_reference."""

    def test_ctx_reference(self):
        result = _resolve_reference("@ctx.project", {"project": "prod"}, {})
        assert result == "prod"

    def test_payload_reference(self):
        result = _resolve_reference("@payload.id", {}, {"id": 42})
        assert result == 42

    def test_nested_path(self):
        result = _resolve_reference(
            "@ctx.db.host", {"db": {"host": "localhost"}}, {}
        )
        assert result == "localhost"

    def test_run_reference_raises(self):
        with pytest.raises(CompileError, match="@run.*"):
            _resolve_reference("@run.step1.items", {}, {})

    def test_missing_key_raises(self):
        with pytest.raises(CompileError, match="missing"):
            _resolve_reference("@ctx.missing", {}, {})

    def test_self_reference(self):
        result = _resolve_reference(
            "@self.config.spreadsheet_id",
            {}, {},
            self_data={"config": {"spreadsheet_id": "abc123"}},
        )
        assert result == "abc123"

    def test_self_nested_reference(self):
        result = _resolve_reference(
            "@self.config.account",
            {}, {},
            self_data={"config": {"account": "acct1"}},
        )
        assert result == "acct1"

    def test_self_without_self_data_raises(self):
        with pytest.raises(CompileError, match="@self.*"):
            _resolve_reference("@self.config.key", {}, {}, self_data=None)

    def test_self_missing_key_raises(self):
        with pytest.raises(CompileError, match="missing"):
            _resolve_reference(
                "@self.config.missing",
                {}, {},
                self_data={"config": {}},
            )


class TestResolveValue:
    """Tests for _resolve_value with @self support."""

    def test_self_in_nested_dict(self):
        value = {"sheet_id": "@self.config.spreadsheet_id", "name": "test"}
        result = _resolve_value(
            value, {}, {},
            self_data={"config": {"spreadsheet_id": "xyz"}},
        )
        assert result == {"sheet_id": "xyz", "name": "test"}

    def test_self_in_list(self):
        value = ["@self.config.a", "@self.config.b"]
        result = _resolve_value(
            value, {}, {},
            self_data={"config": {"a": 1, "b": 2}},
        )
        assert result == [1, 2]

    def test_run_refs_preserved_with_self(self):
        value = {"items": "@run.read.items", "id": "@self.config.id"}
        result = _resolve_value(
            value, {}, {},
            preserve_run_refs=True,
            self_data={"config": {"id": "abc"}},
        )
        assert result == {"items": "@run.read.items", "id": "abc"}


class TestCompileJobWithSelf:
    """Tests for compile_job with @self references."""

    def test_self_resolved_in_params(self):
        job_def = JobDef(
            job_id="test_job",
            version="2.0",
            steps=(
                StepDef(
                    step_id="step1",
                    op=Op.CALL,
                    params={
                        "callable": "canonizer",
                        "config": {
                            "spreadsheet_id": "@self.config.spreadsheet_id",
                            "account": "@self.config.account",
                        },
                    },
                ),
            ),
            extras={"config": {"spreadsheet_id": "sheet123", "account": "acct1"}},
        )

        instance = compile_job(job_def)
        params = instance.steps[0].params
        assert params["config"]["spreadsheet_id"] == "sheet123"
        assert params["config"]["account"] == "acct1"

    def test_self_with_no_extras_passes_when_unused(self):
        """JobDef with no extras compiles fine if no @self refs exist."""
        job_def = JobDef(
            job_id="test_job",
            version="2.0",
            steps=(
                StepDef(
                    step_id="step1",
                    op=Op.CALL,
                    params={"key": "literal_value"},
                ),
            ),
        )
        instance = compile_job(job_def)
        assert instance.steps[0].params["key"] == "literal_value"

    def test_self_missing_key_raises_compile_error(self):
        job_def = JobDef(
            job_id="test_job",
            version="2.0",
            steps=(
                StepDef(
                    step_id="step1",
                    op=Op.CALL,
                    params={"val": "@self.config.missing_key"},
                ),
            ),
            extras={"config": {}},
        )
        with pytest.raises(CompileError, match="missing"):
            compile_job(job_def)

    def test_self_with_empty_extras_raises(self):
        """@self ref with no extras raises CompileError."""
        job_def = JobDef(
            job_id="test_job",
            version="2.0",
            steps=(
                StepDef(
                    step_id="step1",
                    op=Op.CALL,
                    params={"val": "@self.config.key"},
                ),
            ),
        )
        with pytest.raises(CompileError, match="@self.*"):
            compile_job(job_def)

    def test_self_mixed_with_ctx_and_run(self):
        """@self, @ctx, and @run refs coexist correctly."""
        job_def = JobDef(
            job_id="test_job",
            version="2.0",
            steps=(
                StepDef(
                    step_id="step1",
                    op=Op.CALL,
                    params={
                        "self_val": "@self.token",
                        "ctx_val": "@ctx.env",
                        "run_val": "@run.prev.items",
                    },
                ),
            ),
            extras={"token": "secret123"},
        )
        instance = compile_job(job_def, ctx={"env": "prod"})
        params = instance.steps[0].params
        assert params["self_val"] == "secret123"
        assert params["ctx_val"] == "prod"
        assert params["run_val"] == "@run.prev.items"  # preserved


class TestCompilerClassWithSelf:
    """Tests for Compiler class with @self resolution."""

    def test_compile_def_resolves_self(self):
        job_def = JobDef(
            job_id="test_job",
            version="2.0",
            steps=(
                StepDef(
                    step_id="s1",
                    op=Op.CALL,
                    params={"id": "@self.sheet_id"},
                ),
            ),
            extras={"sheet_id": "abc"},
        )
        # compile_def doesn't need a registry
        compiler = Compiler.__new__(Compiler)
        compiler._registry = None
        instance = compiler.compile_def(job_def)
        assert instance.steps[0].params["id"] == "abc"
