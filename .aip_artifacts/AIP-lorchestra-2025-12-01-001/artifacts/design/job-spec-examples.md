# Job Spec Examples

This document provides annotated examples for all three job types supported by lorchestra.

## Design Principles

1. **Minimal v1 schema** — Keep it small, use `options` for free-form config
2. **Job-type-specific fields** — `source`/`sink`/`transform` are interpreted by processors, not enforced by schema
3. **Validation is canonizer's job** — No separate "validate" job type; use `canonize` with `mode: "validate_only"`
4. **Don't over-specify** — Tighten schema only when real bugs/needs show up

---

## Ingest Jobs

Ingest jobs pull data from external sources via the `injest` library and write to `raw_objects`.

### Gmail Ingest

```json
{
  "job_id": "gmail_ingest_acct1",
  "job_type": "ingest",
  "source": {
    "stream": "gmail.messages",
    "identity": "gmail:acct1"
  },
  "sink": {
    "source_system": "gmail",
    "connection_name": "gmail-acct1",
    "object_type": "email"
  },
  "options": {
    "auto_since": true
  }
}
```

**Field notes:**
- `source.stream`: Passed to `injest.get_stream()` to get the appropriate extractor
- `source.identity`: Auth identity key resolved by authctl (e.g., `gmail:acct1` → OAuth tokens)
- `sink.*`: Column values for `raw_objects` table (source_system, connection_name, object_type)
- `options.auto_since`: When true, processor queries BigQuery for last sync timestamp

### Stripe Ingest

```json
{
  "job_id": "stripe_ingest_customers",
  "job_type": "ingest",
  "source": {
    "stream": "stripe.customers",
    "identity": "stripe:default"
  },
  "sink": {
    "source_system": "stripe",
    "connection_name": "stripe-prod",
    "object_type": "customer"
  }
}
```

### Exchange Ingest

```json
{
  "job_id": "exchange_ingest_ben_mensio",
  "job_type": "ingest",
  "source": {
    "stream": "exchange.messages",
    "identity": "exchange:ben_mensio"
  },
  "sink": {
    "source_system": "exchange",
    "connection_name": "exchange-ben-mensio",
    "object_type": "email"
  },
  "options": {
    "auto_since": true
  }
}
```

### Google Forms Ingest

```json
{
  "job_id": "google_forms_ingest_ipip120",
  "job_type": "ingest",
  "source": {
    "stream": "google_forms.responses",
    "identity": "google_forms:ipip120"
  },
  "sink": {
    "source_system": "google_forms",
    "connection_name": "google-forms-ipip120",
    "object_type": "form_response"
  }
}
```

### Dataverse Ingest

```json
{
  "job_id": "dataverse_ingest_contacts",
  "job_type": "ingest",
  "source": {
    "stream": "dataverse.contacts",
    "identity": "dataverse:default"
  },
  "sink": {
    "source_system": "dataverse",
    "connection_name": "dataverse-crm",
    "object_type": "contact"
  }
}
```

---

## Canonize Jobs

Canonize jobs transform raw objects into canonical format. They support two modes:

### Mode: `validate_only`

Validates raw objects against a schema and stamps `validation_status`. No transformation.

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
    "schema_in": "iglu:raw/email_gmail/jsonschema/1-0-0"
  },
  "sink": {
    "update_field": "validation_status"
  }
}
```

**Field notes:**
- `transform.mode`: `"validate_only"` means validate only, don't transform
- `transform.schema_in`: Iglu schema URI for validation
- `sink.update_field`: Field to update on raw_objects (values: `"pass"`, `"fail"`)

### Mode: `full` (default)

Validates input, transforms to canonical format, validates output, writes to canonical_objects.

```json
{
  "job_id": "canonize_gmail_jmap",
  "job_type": "canonize",
  "source": {
    "source_system": "gmail",
    "object_type": "email",
    "filter": {
      "validation_status": "pass"
    }
  },
  "transform": {
    "mode": "full",
    "schema_in": "iglu:raw/email_gmail/jsonschema/1-0-0",
    "schema_out": "iglu:canonical/email_jmap/jsonschema/1-0-0",
    "transform_ref": "gmail_to_jmap_lite@1.0.0"
  },
  "sink": {
    "table": "canonical_objects"
  }
}
```

**Field notes:**
- `source.filter`: Additional WHERE clauses for query (e.g., only process validated records)
- `transform.schema_in`/`schema_out`: Input and output schema URIs
- `transform.transform_ref`: Reference to transform in canonizer registry
- `sink.table`: Target table for canonical records

### Exchange Canonization

```json
{
  "job_id": "canonize_exchange_jmap",
  "job_type": "canonize",
  "source": {
    "source_system": "exchange",
    "object_type": "email",
    "filter": {
      "validation_status": "pass"
    }
  },
  "transform": {
    "mode": "full",
    "schema_in": "iglu:raw/email_exchange/jsonschema/1-0-0",
    "schema_out": "iglu:canonical/email_jmap/jsonschema/1-0-0",
    "transform_ref": "exchange_to_jmap_lite@1.0.0"
  },
  "sink": {
    "table": "canonical_objects"
  }
}
```

---

## Final-Form Jobs

Final-form jobs process canonical objects through clinical instruments to produce measurement events and observations.

### PHQ-9 Final Form

```json
{
  "job_id": "final_form_phq9",
  "job_type": "final_form",
  "source": {
    "table": "canonical_objects",
    "filter": {
      "canonical_schema": "iglu:canonical/questionnaire_response/jsonschema/1-0-0",
      "instrument_id": "phq-9"
    }
  },
  "transform": {
    "instrument_id": "phq-9",
    "instrument_version": "1.0.0",
    "binding_id": "intake_v1",
    "finalform_spec": "phq9_finalform@1.0.0"
  },
  "sink": {
    "measurement_table": "measurement_events",
    "observation_table": "observations"
  }
}
```

**Field notes:**
- `source.table`: Query from canonical_objects
- `source.filter`: Select only PHQ-9 responses
- `transform.instrument_id`: Clinical instrument identifier
- `transform.binding_id`: Links form structure to instrument items
- `transform.finalform_spec`: Reference to final-form processing spec
- `sink.measurement_table`/`observation_table`: Where to write results

### GAD-7 Final Form

```json
{
  "job_id": "final_form_gad7",
  "job_type": "final_form",
  "source": {
    "table": "canonical_objects",
    "filter": {
      "canonical_schema": "iglu:canonical/questionnaire_response/jsonschema/1-0-0",
      "instrument_id": "gad-7"
    }
  },
  "transform": {
    "instrument_id": "gad-7",
    "instrument_version": "1.0.0",
    "binding_id": "followup_v1",
    "finalform_spec": "gad7_finalform@1.0.0"
  },
  "sink": {
    "measurement_table": "measurement_events",
    "observation_table": "observations"
  }
}
```

---

## Options Reference

The `options` field is free-form and interpreted by processors. Common options:

| Option | Type | Used By | Description |
|--------|------|---------|-------------|
| `auto_since` | boolean | ingest | Query BigQuery for last sync timestamp |
| `since` | string | ingest | Explicit start date (ISO or relative like "-7d") |
| `until` | string | ingest | Explicit end date (ISO) |
| `limit` | integer | all | Max records to process |
| `batch_size` | integer | all | Records per batch operation |
| `dry_run` | boolean | all | Execute without writes |

### Example with Options

```json
{
  "job_id": "gmail_ingest_acct1",
  "job_type": "ingest",
  "source": {
    "stream": "gmail.messages",
    "identity": "gmail:acct1"
  },
  "sink": {
    "source_system": "gmail",
    "connection_name": "gmail-acct1",
    "object_type": "email"
  },
  "options": {
    "auto_since": true,
    "batch_size": 500,
    "limit": 1000
  }
}
```

---

## Events Configuration

The `events` field customizes lifecycle event names. Processors have sensible defaults.

| Job Type | Default on_complete | Default on_fail |
|----------|---------------------|-----------------|
| ingest | `ingest.completed` | `ingest.failed` |
| canonize (validate_only) | `validate.completed` | `validate.failed` |
| canonize (full) | `canonize.completed` | `canonize.failed` |
| final_form | `finalization.completed` | `finalization.failed` |

### Custom Events Example

```json
{
  "job_id": "gmail_ingest_acct1",
  "job_type": "ingest",
  "source": { "stream": "gmail.messages", "identity": "gmail:acct1" },
  "sink": { "source_system": "gmail", "connection_name": "gmail-acct1", "object_type": "email" },
  "events": {
    "on_complete": "gmail.sync.completed",
    "on_fail": "gmail.sync.failed"
  }
}
```

---

## Validation Flow

The data pipeline follows this validation pattern:

```
ingest → raw_objects (any shape accepted)
           ↓
canonize (validate_only) → stamps validation_status on raw_objects
           ↓
canonize (full) → reads validation_status=pass → transforms → canonical_objects
           ↓
final_form → reads canonical_objects → produces measurement_events + observations
```

**Key rules:**
1. Ingest never validates — it accepts whatever the source provides
2. Canonize owns all schema validation (input AND output)
3. Final-form may call canonizer for canonical validation if needed
4. "Validation jobs" are just `canonize` with `mode: "validate_only"`
