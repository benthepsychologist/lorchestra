# Canonical Transforms

Canonical transforms convert raw data from source systems into standardized canonical schemas. This document covers the transforms used for the therapist surface.

## Overview

```
┌─────────────────────┐     ┌─────────────────────┐     ┌─────────────────────┐
│   Raw Objects       │     │   Canonize          │     │   Canonical Objects │
│   (Dataverse,       │────▶│   Processor         │────▶│   (Standardized     │
│    Google Forms)    │     │   + JSONata         │     │    schemas)         │
└─────────────────────┘     └─────────────────────┘     └─────────────────────┘
```

## Transform Registry

Transforms are stored in `.canonizer/registry/transforms/` with the structure:

```
.canonizer/registry/transforms/
├── clinical_session/
│   └── dataverse_to_canonical/
│       ├── 1-0-0/
│       │   └── spec.jsonata
│       └── 2-0-0/
│           └── spec.jsonata      # Current version
├── session_transcript/
│   └── dataverse_to_canonical/
│       └── 2-0-0/
│           └── spec.jsonata
├── clinical_document/
│   └── dataverse_to_canonical/
│       └── 2-0-0/
│           └── spec.jsonata
└── contact/
    └── dataverse_to_canonical/
        └── 2-0-0/
            └── spec.jsonata
```

## Key Transforms

### clinical_session (2-0-0)

**Source:** Dataverse `cre92_clientsession`
**Schema:** `iglu:org.canonical/clinical_session/jsonschema/2-0-0`

```jsonata
{
  "source_id": cre92_clientsessionid,
  "source_entity": "cre92_clientsession",
  "session_id": cre92_clientsessionid,
  "session_num": cre92_sessionnumber,           // <-- Important: source session number
  "scheduled_start": scheduledstart,
  "scheduled_end": scheduledend,
  "actual_start": actualstart,
  "actual_end": actualend,
  "subject": subject,
  "description": description,
  "status": $lookup({
    "1": "scheduled",
    "2": "completed",
    "3": "cancelled",
    "4": "in_progress",
    "5": "no_show"
  }, $string(statuscode)),
  "contact_id": $string(_cre92_client_value ? _cre92_client_value :
                        (regardingobjectid ? regardingobjectid : _ben_contactid_value)),
  "session_type": _ben_sessiontype_value,
  "duration_minutes": scheduleddurationminutes,
  "created_at": createdon,
  "updated_at": modifiedon,
  "source": {
    "platform": "dataverse",
    "platform_version": "v2"
  }
}
```

**Key Fields:**
- `session_num` - The actual session number from Dataverse (not calculated)
- `contact_id` - Links session to client via `_cre92_client_value`

### session_transcript (2-0-0)

**Source:** Dataverse `cre92_clientsession` (transcript field)
**Schema:** `iglu:org.canonical/session_transcript/jsonschema/2-0-0`

```jsonata
{
  "id": cre92_clientsessionid & "-transcript",
  "session_source_id": cre92_clientsessionid,
  "subject": {
    "reference": "Contact/" & $string(_cre92_client_value)
  },
  "date": createdon,
  "content": $transcriptMd != null ? {
    "format": "markdown",
    "text": $transcriptMd
  } : null,
  "language": "en",
  "processing": {
    "status": "completed"
  },
  "meta": {
    "createdAt": createdon,
    "updatedAt": modifiedon
  },
  "source": {
    "platform": "dataverse",
    "platform_version": "v2"
  }
}
```

**Key Fields:**
- `content.text` - The actual transcript content (nested structure)
- `session_source_id` - Links back to the parent session
- `subject.reference` - Links to client as `Contact/<id>`

### clinical_document (2-0-0)

**Source:** Dataverse `cre92_clientsession` (note/summary fields)
**Schema:** `iglu:org.canonical/clinical_document/jsonschema/2-0-0`

Handles multiple document types:
- `soap-note` - Clinician session notes
- `client-summary` - Client-facing session summaries
- `progress-report` - Progress reports

```jsonata
{
  "id": cre92_clientsessionid & "-" & $docType,
  "session_source_id": cre92_clientsessionid,
  "docType": $docType,
  "subject": {
    "reference": "Contact/" & $string(_cre92_client_value)
  },
  "content": $content != null ? {
    "format": "markdown",
    "text": $content
  } : null,
  ...
}
```

### contact (2-0-0)

**Source:** Dataverse `contact`
**Schema:** `iglu:org.canonical/contact/jsonschema/2-0-0`

```jsonata
{
  "contact_id": contactid,
  "first_name": firstname,
  "last_name": lastname,
  "email": emailaddress1,
  "mobile": mobilephone,
  "phone": telephone1,
  "client_type_label": _cre92_contacttype_value@OData.Community.Display.V1.FormattedValue,
  "birth_date": birthdate,
  "created_at": createdon,
  "updated_at": modifiedon,
  "source": {
    "platform": "dataverse",
    "platform_version": "v2"
  }
}
```

**Key Fields:**
- `client_type_label` - Used to filter Therapy clients

### form_response (1-0-0)

**Source:** Google Forms
**Schema:** `iglu:org.canonical/form_response/jsonschema/1-0-0`

```jsonata
{
  "response_id": responseId,
  "respondent": {
    "email": respondentEmail
  },
  "status": "submitted",
  "submitted_at": lastSubmittedTime,
  "last_updated_at": lastSubmittedTime,
  "answers": [
    {
      "question_id": questionId,
      "answer_type": "choice",
      "answer_value": value
    }
  ],
  "source": {
    "platform": "google_forms",
    "platform_version": "v1"
  }
}
```

**Key Fields:**
- `respondent.email` - Used to link to clients

## Canonization Jobs

| Job | Source | Transform | Output Schema |
|-----|--------|-----------|---------------|
| `canonize_dataverse_sessions` | session objects | clinical_session 2-0-0 | clinical_session |
| `canonize_dataverse_transcripts` | session objects | session_transcript 2-0-0 | session_transcript |
| `canonize_dataverse_session_notes` | session objects | clinical_document 2-0-0 | clinical_document |
| `canonize_dataverse_session_summaries` | session objects | clinical_document 2-0-0 | clinical_document |
| `canonize_dataverse_reports` | session objects | clinical_document 2-0-0 | clinical_document |
| `canonize_dataverse_contacts` | contact objects | contact 2-0-0 | contact |

## Content Structure

All content fields use a nested structure:

```json
{
  "content": {
    "format": "markdown",
    "text": "The actual content here..."
  }
}
```

This allows for future support of other formats (HTML, plain text, etc.).

When extracting content in SQL projections, use:
```sql
JSON_VALUE(payload, '$.content.text')
```

## Subject References

Entities link to subjects (clients) using a reference format:

```json
{
  "subject": {
    "reference": "Contact/20ed1dc4-57e3-ef11-be21-6045bd5d7bd7"
  }
}
```

To extract the client ID in SQL:
```sql
REGEXP_EXTRACT(JSON_VALUE(payload, '$.subject.reference'), r'/(.+)$')
```

## Updating Transforms

When you need to modify a transform:

1. **Edit the transform file:**
   ```
   .canonizer/registry/transforms/<entity>/dataverse_to_canonical/<version>/spec.jsonata
   ```

2. **Delete existing canonical objects** (so lazy loading doesn't skip them):
   ```sql
   DELETE FROM `project.dataset.canonical_objects`
   WHERE canonical_schema = 'iglu:org.canonical/<entity>/jsonschema/<version>'
   ```

3. **Re-run canonization:**
   ```bash
   source .venv/bin/activate && source .env
   python -c "from lorchestra.job_runner import run_job; run_job('canonize_dataverse_<entity>')"
   ```

Or use the convenience script for sessions:
```bash
./scripts/recanonize_sessions.sh
```

## Common Issues

### Field not appearing in canonical output

1. Check the raw object has the field: query `raw_objects` table
2. Check the JSONata transform references the correct field name
3. Dataverse field names are often like `_cre92_fieldname_value` or `cre92_fieldname`

### Content is null

1. Check the raw object has content in the source field
2. Check the transform assigns to `content.text` (nested structure)
3. Check for conditionals that might skip null content

### Client linking broken

Different source systems use different patterns:
- Dataverse sessions: `_cre92_client_value`
- Dataverse docs: `subject.reference` = `Contact/<id>`
- Google Forms: `respondent.email` (matched to client email)

Ensure the transform maps the correct source field.
