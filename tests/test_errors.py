"""Tests for lorchestra error classes (e005b-01).

Tests cover:
- TransientError and PermanentError existence
- Error hierarchy
- Exceptions can be raised and caught
"""

import pytest
from lorchestra.errors import LorchestraError, TransientError, PermanentError


class TestLorchestraError:
    """Tests for base LorchestraError."""

    def test_is_exception(self):
        """LorchestraError should be an Exception."""
        assert issubclass(LorchestraError, Exception)

    def test_can_be_raised(self):
        """LorchestraError can be raised."""
        with pytest.raises(LorchestraError):
            raise LorchestraError("test error")

    def test_has_message(self):
        """LorchestraError should have a message."""
        error = LorchestraError("my message")
        assert str(error) == "my message"


class TestTransientError:
    """Tests for TransientError."""

    def test_is_lorchestra_error(self):
        """TransientError should be a LorchestraError."""
        assert issubclass(TransientError, LorchestraError)

    def test_is_exception(self):
        """TransientError should be an Exception."""
        assert issubclass(TransientError, Exception)

    def test_can_be_raised(self):
        """TransientError can be raised."""
        with pytest.raises(TransientError):
            raise TransientError("rate limited")

    def test_can_be_caught_as_lorchestra_error(self):
        """TransientError can be caught as LorchestraError."""
        with pytest.raises(LorchestraError):
            raise TransientError("rate limited")

    def test_has_message(self):
        """TransientError should have a message."""
        error = TransientError("network timeout")
        assert str(error) == "network timeout"


class TestPermanentError:
    """Tests for PermanentError."""

    def test_is_lorchestra_error(self):
        """PermanentError should be a LorchestraError."""
        assert issubclass(PermanentError, LorchestraError)

    def test_is_exception(self):
        """PermanentError should be an Exception."""
        assert issubclass(PermanentError, Exception)

    def test_can_be_raised(self):
        """PermanentError can be raised."""
        with pytest.raises(PermanentError):
            raise PermanentError("invalid input")

    def test_can_be_caught_as_lorchestra_error(self):
        """PermanentError can be caught as LorchestraError."""
        with pytest.raises(LorchestraError):
            raise PermanentError("invalid input")

    def test_has_message(self):
        """PermanentError should have a message."""
        error = PermanentError("schema validation failed")
        assert str(error) == "schema validation failed"


class TestErrorClassification:
    """Tests for classifying different error types."""

    def test_transient_not_permanent(self):
        """TransientError is not a PermanentError."""
        error = TransientError("test")
        assert not isinstance(error, PermanentError)

    def test_permanent_not_transient(self):
        """PermanentError is not a TransientError."""
        error = PermanentError("test")
        assert not isinstance(error, TransientError)

    def test_can_distinguish_transient_and_permanent(self):
        """Should be able to distinguish between error types."""
        transient = TransientError("rate limit")
        permanent = PermanentError("bad input")

        assert isinstance(transient, TransientError)
        assert isinstance(permanent, PermanentError)
        assert not isinstance(transient, PermanentError)
        assert not isinstance(permanent, TransientError)


class TestErrorUsagePatterns:
    """Tests for typical error usage patterns."""

    def test_catch_transient_for_retry(self):
        """Demonstrate catching TransientError for retry logic."""
        retry_count = 0

        def operation_that_may_fail():
            nonlocal retry_count
            retry_count += 1
            if retry_count < 3:
                raise TransientError("temporary failure")
            return "success"

        result = None
        for _ in range(5):
            try:
                result = operation_that_may_fail()
                break
            except TransientError:
                continue

        assert result == "success"
        assert retry_count == 3

    def test_permanent_error_no_retry(self):
        """Demonstrate PermanentError stops retry logic."""
        attempts = 0

        def operation_with_bad_input():
            nonlocal attempts
            attempts += 1
            raise PermanentError("invalid input - do not retry")

        with pytest.raises(PermanentError):
            for _ in range(5):
                try:
                    operation_with_bad_input()
                except TransientError:
                    continue
                except PermanentError:
                    raise  # Don't retry permanent errors

        assert attempts == 1  # Only one attempt before giving up
