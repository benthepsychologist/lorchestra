---
version: "0.1"
tier: C
title: Add Front Matter to Projection Markdown Files
owner: benthepsychologist
goal: Add YAML front matter with source identifiers to all projected markdown files, enabling write-back to Dataverse
labels: [projection, front-matter, write-back]
project_slug: lorchestra
spec_version: 1.1.0
created: 2025-12-08T17:34:30.453300+00:00
updated: 2025-12-08T18:00:00.000000+00:00
orchestrator_contract: "standard"
repo:
  working_branch: "feat/projector-update-with-config-and-local-files-update"
---

# Add Front Matter to Projection Markdown Files

## Objective

> Add YAML front matter with source identifiers to all projected markdown files, enabling write-back to Dataverse.

## Decision: Bake into lorchestra (not separate package)

**Rationale:**
- The projection pipeline is already well-structured in lorchestra
- Changes are localized to FileProjectionProcessor and job definitions
- No new dependencies needed (PyYAML is already available)
- Can extract to a package later when requirements mature

## Acceptance Criteria

- [ ] CI green (lint + unit)
- [ ] FileProjectionProcessor supports structured `front_matter` config in job specs
- [ ] Front matter rendered via `yaml.safe_dump` for correctness
- [ ] All projection jobs (except `project_session_files.json`) include front matter with source identifiers
- [ ] Projection queries updated to include `idem_key` and relevant IDs
- [ ] Tests pass for front matter rendering, including edge cases
- [ ] Backward compatibility: jobs without `front_matter` produce identical output to before
- [ ] Test coverage for "weird" values (`:`, `#`, unicode, quotes)

## Context

### Background

The projection pipeline currently renders markdown files from SQLite data, but these files lack metadata about their source. Adding YAML front matter with source identifiers (idem_key, entity IDs) will enable:
1. Write-back to Dataverse from edited markdown
2. Traceability between projected files and source data
3. Future bidirectional sync capabilities

### Current State

- `FileProjectionProcessor` in `lorchestra/processors/projection.py` renders content using templates
- Job definitions in `lorchestra/jobs/definitions/project_*.json` specify queries and templates
- BQ projections in `lorchestra/sql/projections.py` already include `idem_key` and relevant IDs
- Sync jobs use `SELECT *` so all fields including `idem_key` are synced to SQLite

> **Warning:** Do not remove `idem_key` or primary ID fields from upstream sync queries without updating projection job specs and their tests.

### Constraints

- No edits under protected paths (`src/core/**`, `infra/**`)
- Changes must be backward compatible (`front_matter` is optional)
- IDs in front matter must be canonical Dataverse IDs (not local surrogates)

### Naming Convention Rule

> **All fields referenced in `content_template` and `front_matter` must be present in the query result row under those exact names. SQL aliases must match template placeholders.**

This coupling is intentional but must be maintained. Tests will validate template-to-row consistency.

## Plan

### Step 1: Update FileProjectionProcessor [G1: Code Readiness]

**File:** `/workspace/lorchestra/lorchestra/processors/projection.py`

**Changes:**
Replace naive string templating with structured `front_matter` config and `yaml.safe_dump`:

```python
import yaml

# In FileProjectionProcessor.run():
front_matter_spec = job_spec["sink"].get("front_matter")

# Render content
content_body = content_template.format(**row)

if front_matter_spec:
    # Resolve placeholders in front matter values
    resolved = {}
    for key, value in front_matter_spec.items():
        if isinstance(value, str):
            try:
                resolved[key] = value.format(**row)
            except KeyError as exc:
                raise RuntimeError(
                    f"Missing key {exc!s} in front_matter for job '{job_spec.get('job_id', 'unknown')}'. "
                    f"Available keys: {list(row.keys())}"
                ) from exc
        else:
            resolved[key] = value

    # Use yaml.safe_dump for correct YAML output
    front_matter_yaml = yaml.safe_dump(resolved, sort_keys=False, allow_unicode=True)
    content = f"---\n{front_matter_yaml}---\n\n{content_body}"
else:
    content = content_body
```

**Benefits over string template approach:**
- YAML correctness guaranteed (handles `:`, `#`, quotes, unicode)
- No manual `\n` management
- Clear error messages with available keys listed
- Structured config is self-documenting

**Outputs:**
- Updated `FileProjectionProcessor` class

### Step 2: Update Projection Queries [G1: Code Readiness]

The file projection queries JOIN multiple tables but need to SELECT `idem_key` and source IDs.

**Naming convention:** All IDs are canonical Dataverse IDs, not local surrogates.

**Files to update:**

| File | Fields to Add |
|------|---------------|
| `project_contacts.json` | `idem_key`, `client_id` |
| `project_transcripts.json` | `t.idem_key`, `t.transcript_id`, `t.session_id`, `t.client_id` |
| `project_session_notes.json` | `d.idem_key`, `d.document_id`, `d.session_id`, `d.client_id`, `d.doc_type` |
| `project_session_summaries.json` | `d.idem_key`, `d.document_id`, `d.session_id`, `d.client_id`, `d.doc_type` |
| `project_reports.json` | `d.idem_key`, `d.document_id`, `d.client_id`, `d.doc_type` |

**Example query update for `project_transcripts.json`:**
```sql
SELECT c.client_folder, s.session_num, t.content,
       t.idem_key, t.transcript_id, t.session_id, t.client_id
FROM transcripts t
JOIN sessions s ON t.session_id = s.session_id
JOIN clients c ON t.client_id = c.client_id
WHERE t.content IS NOT NULL
ORDER BY c.client_folder, s.session_num
```

### Step 3: Add Structured Front Matter Config [G1: Code Readiness]

Add `front_matter` (structured mapping, not string template) to each projection job definition.

**Value types:** `front_matter` values may be either:
- **Literal values** (no substitution) - used as-is
- **Format strings** using `{placeholder}` syntax - substituted from row data
- Non-string values (int, bool, etc.) are passed through unchanged

**Taxonomy decision:** Use `source_type` as coarse bucket + `doc_type` for specifics where applicable.

| Job | `front_matter` config |
|-----|----------------------|
| `project_contacts.json` | `{"source_type": "contact", "idem_key": "{idem_key}", "client_id": "{client_id}"}` |
| `project_transcripts.json` | `{"source_type": "transcript", "idem_key": "{idem_key}", "transcript_id": "{transcript_id}", "session_id": "{session_id}", "client_id": "{client_id}"}` |
| `project_session_notes.json` | `{"source_type": "clinical_document", "idem_key": "{idem_key}", "document_id": "{document_id}", "session_id": "{session_id}", "client_id": "{client_id}", "doc_type": "{doc_type}"}` |
| `project_session_summaries.json` | `{"source_type": "clinical_document", "idem_key": "{idem_key}", "document_id": "{document_id}", "session_id": "{session_id}", "client_id": "{client_id}", "doc_type": "{doc_type}"}` |
| `project_reports.json` | `{"source_type": "clinical_document", "idem_key": "{idem_key}", "document_id": "{document_id}", "client_id": "{client_id}", "doc_type": "{doc_type}"}` |

**Example job definition (`project_transcripts.json`):**
```json
{
  "job_id": "project_transcripts",
  "job_type": "file_projection",
  "source": {
    "sqlite_path": "~/clinical-vault/local.db",
    "query": "SELECT c.client_folder, s.session_num, t.content, t.idem_key, t.transcript_id, t.session_id, t.client_id FROM transcripts t JOIN sessions s ON t.session_id = s.session_id JOIN clients c ON t.client_id = c.client_id WHERE t.content IS NOT NULL ORDER BY c.client_folder, s.session_num"
  },
  "sink": {
    "base_path": "~/clinical-vault/views",
    "path_template": "{client_folder}/sessions/session-{session_num}/transcript.md",
    "content_template": "{content}",
    "front_matter": {
      "source_type": "transcript",
      "idem_key": "{idem_key}",
      "transcript_id": "{transcript_id}",
      "session_id": "{session_id}",
      "client_id": "{client_id}"
    }
  }
}
```

### Step 4: Scope Exclusion - project_session_files.json

**`project_session_files.json` is OUT OF SCOPE for this spec.**

Rationale:
- Uses a different pattern (`SELECT *` from `proj_client_sessions`)
- May involve binary attachments or external file references
- Requires separate analysis for appropriate front matter schema

This job will be addressed in a follow-up spec if needed. **Do not modify `project_session_files.json` in this implementation.**

### Step 5: Testing & Validation [G2: Pre-Release]

**Commands:**
```bash
ruff check .
pytest -q --tb=short
```

**Required test cases:**

1. **Basic front matter rendering**
   - Job with `front_matter` config renders valid YAML front matter
   - Front matter values correctly substituted from row

2. **Backward compatibility**
   - Job without `front_matter` config produces content identical to previous behavior
   - No extra whitespace or formatting changes

3. **Edge cases / weird values**
   - Values containing `:` (e.g., `idem_key: "foo:bar:baz"`)
   - Values containing `#` (e.g., `idem_key: "abc123#session_transcript"`)
   - Values with quotes (e.g., `title: 'John "Johnny" Doe'`)
   - Unicode values (e.g., `name: "José García"`)
   - Confirm output is valid YAML parseable by `yaml.safe_load`

4. **Error handling**
   - Missing key in row raises `RuntimeError` with helpful message
   - Error message includes job_id and available keys

5. **Template-to-row consistency**
   - For each job definition, validate that all placeholders in `front_matter` and `content_template` exist in query result columns

6. **Front matter structure assertion**
   - Split output on `\n`
   - Assert `lines[0] == '---'`
   - Assert there is a second `'---'` line
   - Assert line after closing `---` is empty (`''`)
   - Assert content starts after the blank line

**Test helper suggestion:**
```python
def validate_job_templates(job_spec: dict, sample_row: dict) -> None:
    """Validate that all template placeholders can be resolved from row."""
    front_matter = job_spec["sink"].get("front_matter", {})
    content_template = job_spec["sink"]["content_template"]

    # Check front matter placeholders
    for key, value in front_matter.items():
        if isinstance(value, str):
            value.format(**sample_row)  # Raises KeyError if missing

    # Check content template
    content_template.format(**sample_row)  # Raises KeyError if missing
```

### Step 6: Verify Output Format [G2: Pre-Release]

**Expected output for a transcript file:**
```markdown
---
source_type: transcript
idem_key: abc123#session_transcript
transcript_id: xyz789
session_id: sess456
client_id: contact123
---

[Original transcript content...]
```

**Front matter format contract:**
- Line 1: `---` (exactly three dashes)
- Lines 2-N: Valid YAML key-value pairs
- Line N+1: `---` (exactly three dashes)
- Line N+2: Empty line
- Line N+3+: Content body

This format follows standard YAML front matter conventions and is parseable by:
- Python `yaml.safe_load` (after splitting on `---`)
- Any standard front matter parser (Jekyll, Hugo, etc.)

## Files to Modify

| File | Change |
|------|--------|
| `lorchestra/processors/projection.py` | Add structured `front_matter` handling with `yaml.safe_dump` |
| `lorchestra/jobs/definitions/project_contacts.json` | Add `front_matter` config + update query |
| `lorchestra/jobs/definitions/project_transcripts.json` | Add `front_matter` config + update query |
| `lorchestra/jobs/definitions/project_session_notes.json` | Add `front_matter` config + update query |
| `lorchestra/jobs/definitions/project_session_summaries.json` | Add `front_matter` config + update query |
| `lorchestra/jobs/definitions/project_reports.json` | Add `front_matter` config + update query |

**Explicitly NOT modified:**
| File | Reason |
|------|--------|
| `lorchestra/jobs/definitions/project_session_files.json` | Out of scope - different pattern, needs separate analysis |

## Models & Tools

**Tools:** bash, pytest, ruff

**Models:** claude-opus-4-5-20251101

## Repository

**Branch:** `feat/projector-update-with-config-and-local-files-update`

**Merge Strategy:** squash

## Future Considerations (not in scope)

- **`project_session_files.json` front matter** - Separate spec needed
- **Separate projector package with config** - Extract when requirements mature
- **Write-back processor** - Push changes from markdown back to Dataverse
- **Front matter validation/schema** - JSON Schema for `front_matter` config
- **Version tracking** - Add `source_version` or `source_updated_at` for conflict detection during write-back
