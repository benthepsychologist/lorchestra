# Email Validation Jobs Verification

**Date:** 2025-12-02
**Step:** step-001
**AIP:** AIP-lorchestra-2025-12-02-001

## Jobs Verified

### validate_gmail_source.json

```json
{
  "job_id": "validate_gmail_source",
  "job_type": "canonize",
  "source": {
    "source_system": "gmail",
    "object_type": "email"
  },
  "transform": {
    "mode": "validate_only",
    "schema_in": "iglu:com.google/gmail_email/jsonschema/1-0-0"
  }
}
```

**Dry-run result:** ✅ Success
```
==================================================
=== DRY RUN MODE === (no BigQuery writes)
==================================================

[DRY-RUN] validate_gmail_source completed (no writes)
```

### validate_exchange_source.json

```json
{
  "job_id": "validate_exchange_source",
  "job_type": "canonize",
  "source": {
    "source_system": "exchange",
    "object_type": "email"
  },
  "transform": {
    "mode": "validate_only",
    "schema_in": "iglu:com.microsoft/exchange_email/jsonschema/1-0-2"
  }
}
```

**Dry-run result:** ✅ Success
```
==================================================
=== DRY RUN MODE === (no BigQuery writes)
==================================================

[DRY-RUN] validate_exchange_source completed (no writes)
```

## Summary

Both email validation jobs are correctly configured:
- Use `job_type: canonize` with `mode: validate_only`
- Reference appropriate source schemas via Iglu URIs
- Execute successfully in dry-run mode
- Ready to be integrated into daily_ingest.sh
