"""Tests for molt projection SQL templates."""

import pytest

from lorchestra.sql.molt_projections import (
    CONTEXT_ACTIONS,
    CONTEXT_CALENDAR,
    CONTEXT_EMAILS,
    MOLT_PROJECTIONS,
    _load_phi_config,
    _to_sql_in,
    get_molt_projection_sql,
)


class TestMoltProjectionSqlRegistry:
    """Tests for the molt projection SQL registry."""

    def test_all_three_projections_registered(self):
        """Registry contains all three context projections."""
        assert "context_emails" in MOLT_PROJECTIONS
        assert "context_calendar" in MOLT_PROJECTIONS
        assert "context_actions" in MOLT_PROJECTIONS
        assert len(MOLT_PROJECTIONS) == 3

    def test_get_unknown_projection_raises(self):
        """get_molt_projection_sql raises KeyError for unknown names."""
        with pytest.raises(KeyError, match="Unknown molt projection"):
            get_molt_projection_sql("nonexistent", "proj", "ds")


class TestContextEmailsSQL:
    """Tests for the context_emails curation query."""

    def test_resolves_project_and_dataset(self):
        """Placeholders are resolved to provided values."""
        sql = get_molt_projection_sql(
            "context_emails",
            project="local-orchestration",
            dataset="canonical",
        )
        assert "`local-orchestration.canonical.canonical_objects`" in sql

    def test_selects_email_fields(self):
        """Query selects expected email fields."""
        sql = CONTEXT_EMAILS
        for field in [
            "email_id",
            "thread_id",
            "sender_email",
            "sender_name",
            "subject",
            "snippet",
            "received_at",
            "is_flagged",
            "is_unread",
            "needs_response",
            "clinical_contact_id",
            "is_clinical",
            "source_system",
            "connection_name",
            "idem_key",
            "projected_at",
        ]:
            assert field in sql, f"Missing field: {field}"

    def test_filters_to_email_jmap_lite_schema(self):
        """Query filters to email_jmap_lite canonical schema."""
        sql = CONTEXT_EMAILS
        assert "email_jmap_lite/jsonschema/1-0-0" in sql

    def test_filters_to_last_14_days(self):
        """Query filters to emails from the last 14 days."""
        sql = CONTEXT_EMAILS
        assert "INTERVAL 14 DAY" in sql

    def test_includes_read_status_fields(self):
        """Query includes flagged and unread status fields."""
        sql = CONTEXT_EMAILS
        assert "$flagged" in sql
        assert "$seen" in sql

    def test_excludes_noreply_from_needs_response(self):
        """needs_response heuristic excludes noreply addresses."""
        sql = CONTEXT_EMAILS
        assert "noreply" in sql
        assert "no-reply" in sql

    def test_no_limit(self):
        """Query has no LIMIT clause."""
        sql = CONTEXT_EMAILS
        assert "LIMIT" not in sql

    def test_joins_contact_for_clinical_detection(self):
        """Query joins contact table to identify clinical emails."""
        sql = CONTEXT_EMAILS
        assert "contact/jsonschema/2-0-0" in sql
        assert "telecom.email" in sql
        assert "telecom.emailSecondary" in sql

    def test_redacts_clinical_sender_name(self):
        """Clinical emails get sender_name replaced with 'Client X.' format."""
        sql = CONTEXT_EMAILS
        assert "Client " in sql
        assert "WHEN is_clinical" in sql

    def test_redacts_clinical_sender_email(self):
        """Clinical emails get sender_email masked as 'x*****@domain.com'."""
        sql = CONTEXT_EMAILS
        assert "*****@" in sql

    def test_strips_clinical_snippet(self):
        """Clinical emails have snippet set to NULL."""
        sql = CONTEXT_EMAILS
        assert "WHEN is_clinical THEN NULL" in sql

    def test_non_clinical_emails_pass_through(self):
        """Non-clinical emails retain full sender and snippet for non-clinical."""
        sql = CONTEXT_EMAILS
        assert "ELSE raw_sender_email" in sql
        assert "ELSE raw_snippet" in sql

    def test_case_insensitive_contact_matching(self):
        """Contact email matching is case-insensitive via LOWER()."""
        sql = CONTEXT_EMAILS
        assert "LOWER(JSON_VALUE(e.payload, '$.from[0].email'))" in sql
        assert "LOWER(JSON_VALUE(payload, '$.telecom.email'))" in sql

    def test_clinical_signal_contact_match(self):
        """Signal 1: clinical_contact_id match flags as clinical."""
        sql = CONTEXT_EMAILS
        assert "clinical_contact_id IS NOT NULL" in sql

    def test_clinical_signal_sender_domain_placeholder(self):
        """Signal 2: SQL template has placeholder for clinical sender domains."""
        sql = CONTEXT_EMAILS
        assert "{clinical_sender_domains}" in sql

    def test_clinical_signal_practice_recipient_placeholder(self):
        """Signal 3: SQL template has placeholder for practice recipient emails."""
        sql = CONTEXT_EMAILS
        assert "{practice_recipient_emails}" in sql

    def test_practice_recipient_checks_to_field(self):
        """Practice recipient check uses JMAP $.to array."""
        sql = CONTEXT_EMAILS
        assert "JSON_EXTRACT_ARRAY(payload, '$.to')" in sql

    def test_practice_recipient_matches_exact_email(self):
        """Signal 3 matches exact email addresses, not domains."""
        sql = get_molt_projection_sql(
            "context_emails",
            project="local-orchestration",
            dataset="canonical",
        )
        # Should match exact emails, not extract domain with REGEXP_EXTRACT
        assert "drben@benthepsychologist.com" in sql
        assert "booking@mensiomentalhealth.com" in sql

    def test_resolved_sql_has_clinical_domains(self):
        """Resolved SQL contains clinical sender domains from config."""
        sql = get_molt_projection_sql(
            "context_emails",
            project="local-orchestration",
            dataset="canonical",
        )
        assert "psychologytoday.com" in sql

    def test_uses_cte_for_clinical_flag(self):
        """Query uses CTE to compute is_clinical once, apply in outer SELECT."""
        sql = CONTEXT_EMAILS
        assert "WITH email_base AS" in sql
        assert "is_clinical" in sql


class TestPhiClinicalConfig:
    """Tests for PHI clinical detection config loading."""

    def test_config_loads_successfully(self):
        """phi_clinical.yaml loads without error."""
        config = _load_phi_config()
        assert "clinical_sender_domains" in config
        assert "practice_recipient_emails" in config

    def test_config_has_psychologytoday(self):
        """Config includes psychologytoday.com as clinical sender domain."""
        config = _load_phi_config()
        assert "psychologytoday.com" in config["clinical_sender_domains"]

    def test_config_has_practice_emails(self):
        """Config includes specific practice mailbox addresses."""
        config = _load_phi_config()
        emails = config["practice_recipient_emails"]
        assert "drben@benthepsychologist.com" in emails
        assert "booking@mensiomentalhealth.com" in emails

    def test_to_sql_in_single(self):
        """_to_sql_in formats single value correctly."""
        assert _to_sql_in(["a.com"]) == "'a.com'"

    def test_to_sql_in_multiple(self):
        """_to_sql_in formats multiple values correctly."""
        result = _to_sql_in(["a.com", "b.com"])
        assert result == "'a.com', 'b.com'"


class TestContextCalendarSQL:
    """Tests for the context_calendar curation query."""

    def test_resolves_project_and_dataset(self):
        """Placeholders are resolved to provided values."""
        sql = get_molt_projection_sql(
            "context_calendar",
            project="local-orchestration",
            dataset="canonical",
        )
        assert "`local-orchestration.canonical.canonical_objects`" in sql

    def test_selects_session_fields(self):
        """Query selects expected session fields."""
        sql = CONTEXT_CALENDAR
        for field in [
            "session_id",
            "title",
            "client_name",
            "session_num",
            "scheduled_start",
            "scheduled_end",
            "duration_minutes",
            "status",
            "session_type",
            "contact_id",
            "idem_key",
            "projected_at",
        ]:
            assert field in sql, f"Missing field: {field}"

    def test_filters_to_clinical_session_schema(self):
        """Query filters to clinical_session canonical schema."""
        sql = CONTEXT_CALENDAR
        assert "clinical_session/jsonschema/2-0-0" in sql

    def test_filters_to_14_day_window(self):
        """Query filters to sessions within 14 days past and future."""
        sql = CONTEXT_CALENDAR
        assert "INTERVAL 14 DAY" in sql
        assert "TIMESTAMP_SUB" in sql
        assert "TIMESTAMP_ADD" in sql

    def test_joins_contact_for_client_name(self):
        """Query joins contact table to get client name."""
        sql = CONTEXT_CALENDAR
        assert "contact/jsonschema/2-0-0" in sql
        assert "client_name" in sql

    def test_no_limit(self):
        """Query has no LIMIT clause."""
        sql = CONTEXT_CALENDAR
        assert "LIMIT" not in sql


class TestContextActionsSQL:
    """Tests for the context_actions curation query."""

    def test_reads_from_molt_chatbot_project(self):
        """Actions query reads from molt-chatbot.molt.actions directly."""
        sql = CONTEXT_ACTIONS
        assert "`molt-chatbot.molt.actions`" in sql

    def test_does_not_use_project_placeholder(self):
        """Actions query does not use {project} placeholder (same-project query)."""
        # Resolve should work without affecting the query
        sql = get_molt_projection_sql(
            "context_actions",
            project="local-orchestration",
            dataset="canonical",
        )
        # Should still reference molt-chatbot, not local-orchestration
        assert "`molt-chatbot.molt.actions`" in sql
        assert "local-orchestration" not in sql

    def test_selects_action_fields(self):
        """Query selects expected action fields."""
        sql = CONTEXT_ACTIONS
        for field in [
            "action_id",
            "user_id",
            "channel",
            "action_type",
            "context",
            "instruction",
            "created_at",
            "status",
            "idem_key",
            "projected_at",
        ]:
            assert field in sql, f"Missing field: {field}"

    def test_includes_pending_filter(self):
        """Query filters to pending and recently completed actions."""
        sql = CONTEXT_ACTIONS
        assert "status = 'pending'" in sql
        assert "INTERVAL 24 HOUR" in sql

    def test_no_limit(self):
        """Query has no LIMIT clause."""
        sql = CONTEXT_ACTIONS
        assert "LIMIT" not in sql

    def test_orders_pending_first(self):
        """Query orders pending actions before completed."""
        sql = CONTEXT_ACTIONS
        assert "CASE WHEN status = 'pending' THEN 0 ELSE 1 END" in sql
