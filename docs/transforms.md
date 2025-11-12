# Transform Creation Guide

Complete guide for creating JSONata transforms for the lorch pipeline.

## 📋 Table of Contents

- [Overview](#overview)
- [Transform Structure](#transform-structure)
- [Creating a Transform](#creating-a-transform)
- [Testing Transforms](#testing-transforms)
- [JSONata Syntax](#jsonata-syntax)
- [Examples](#examples)
- [Best Practices](#best-practices)

---

## Overview

Transforms convert source data formats (Gmail, Exchange, Dataverse) to canonical schemas using [JSONata](https://jsonata.org/).

### Transform Registry

All transforms live in `/home/user/transforms/`:

```
transforms/
├── email/
│   ├── gmail_to_canonical_v1.jsonata
│   ├── gmail_to_canonical_v1.meta.yaml
│   ├── exchange_to_canonical_v1.jsonata
│   └── exchange_to_canonical_v1.meta.yaml
├── contact/
│   └── dataverse_contact_to_canonical_v1.{jsonata,meta.yaml}
└── schemas/
    ├── canonical/
    │   ├── email_v1-0-0.json
    │   └── contact_v1-0-0.json
    └── source/
        ├── gmail_v1-0-0.json
        └── exchange_v1-0-0.json
```

---

## Transform Structure

Each transform consists of two files:

### 1. JSONata Transform File (`.jsonata`)

The transformation logic:

```jsonata
{
  "message_id": id,
  "subject": payload.headers[name="Subject"].value,
  "from": payload.headers[name="From"].value,
  "to": $split(payload.headers[name="To"].value, ","),
  "received_at": $fromMillis($number(internalDate)),
  "body": $base64decode(payload.body.data),
  "schema": "iglu:org.canonical/email/jsonschema/1-0-0"
}
```

### 2. Metadata File (`.meta.yaml`)

Transform metadata with versioning and validation:

```yaml
name: gmail_to_canonical
version: 1.0.0
description: Transform Gmail API messages to canonical email format

input_schema: iglu:com.google/gmail_email/jsonschema/1-0-0
output_schema: iglu:org.canonical/email/jsonschema/1-0-0

checksum:
  algorithm: sha256
  value: abc123def456...  # SHA256 of .jsonata file

author: Pipeline Team
created: 2025-11-12
modified: 2025-11-12
```

---

## Creating a Transform

### Step 1: Examine Source Data

First, understand the source data structure:

```bash
# Look at source data
head -1 /home/user/phi-data/meltano-extracts/gmail_messages.jsonl | jq .
```

**Example Gmail Message:**

```json
{
  "id": "abc123",
  "threadId": "thread456",
  "payload": {
    "headers": [
      {"name": "From", "value": "sender@example.com"},
      {"name": "To", "value": "recipient@example.com"},
      {"name": "Subject", "value": "Test Email"},
      {"name": "Date", "value": "Mon, 01 Jan 2025 10:00:00 +0000"}
    ],
    "body": {
      "data": "SGVsbG8gV29ybGQ="  // Base64 encoded
    }
  },
  "internalDate": "1735729200000"
}
```

### Step 2: Define Canonical Schema

Define the target canonical format:

```json
{
  "message_id": "string",
  "subject": "string",
  "from": "string",
  "to": ["string"],
  "cc": ["string"],
  "received_at": "timestamp",
  "sent_at": "timestamp",
  "body": "string",
  "body_html": "string",
  "attachments": ["object"],
  "schema": "iglu:org.canonical/email/jsonschema/1-0-0"
}
```

### Step 3: Write JSONata Transform

Create the transform file:

```bash
cd /home/user/transforms/email
vi gmail_to_canonical_v1.jsonata
```

**Transform Content:**

```jsonata
{
  /* Extract message ID */
  "message_id": id,

  /* Extract subject from headers */
  "subject": payload.headers[name="Subject"].value,

  /* Extract sender */
  "from": payload.headers[name="From"].value,

  /* Extract recipients (split comma-separated) */
  "to": $split(payload.headers[name="To"].value, ","),

  /* Extract CC if present */
  "cc": payload.headers[name="Cc"] ? $split(payload.headers[name="Cc"].value, ",") : [],

  /* Convert internal date (epoch ms) to ISO timestamp */
  "received_at": $fromMillis($number(internalDate)),

  /* Extract sent date from headers */
  "sent_at": $toMillis(payload.headers[name="Date"].value),

  /* Decode base64 body */
  "body": $base64decode(payload.body.data),

  /* HTML body if available */
  "body_html": payload.parts[mimeType="text/html"].body.data ?
    $base64decode(payload.parts[mimeType="text/html"].body.data) : null,

  /* Extract attachments */
  "attachments": payload.parts[filename]@$a.{
    "filename": filename,
    "mime_type": mimeType,
    "size": body.size
  },

  /* Canonical schema reference */
  "schema": "iglu:org.canonical/email/jsonschema/1-0-0"
}
```

### Step 4: Create Metadata File

```bash
vi gmail_to_canonical_v1.meta.yaml
```

**Metadata Content:**

```yaml
name: gmail_to_canonical
version: 1.0.0
description: |
  Transform Gmail API message format to canonical email schema.
  Handles plain text and HTML bodies, attachments, and multiple recipients.

input_schema: iglu:com.google/gmail_email/jsonschema/1-0-0
output_schema: iglu:org.canonical/email/jsonschema/1-0-0

checksum:
  algorithm: sha256
  value: ""  # Calculate after writing .jsonata

author: Pipeline Team
created: 2025-11-12
modified: 2025-11-12

notes: |
  - Decodes base64 email bodies
  - Handles both text and HTML bodies
  - Extracts attachment metadata (not content)
  - Converts epoch milliseconds to ISO timestamps
```

### Step 5: Calculate Checksum

```bash
# Calculate SHA256 checksum
sha256sum gmail_to_canonical_v1.jsonata

# Update metadata file with checksum
```

### Step 6: Test Transform

```bash
# Test with canonizer
cd /home/user/canonizer
source .venv/bin/activate

# Transform single message
cat /home/user/meltano-ingest/tests/fixtures/gmail_messages.jsonl | \
  head -1 | \
  can transform run \
    --meta /home/user/transforms/email/gmail_to_canonical_v1.meta.yaml

# Transform full file
can transform run \
  --meta /home/user/transforms/email/gmail_to_canonical_v1.meta.yaml \
  --input /home/user/meltano-ingest/tests/fixtures/gmail_messages.jsonl \
  --output /tmp/canonical_output.jsonl
```

### Step 7: Validate Output

```bash
# Check output is valid JSON
cat /tmp/canonical_output.jsonl | jq .

# Validate against schema (if schema exists)
can validate run \
  --schema /home/user/transforms/schemas/canonical/email_v1-0-0.json \
  --data /tmp/canonical_output.jsonl
```

### Step 8: Add to Pipeline Config

Update `config/pipeline.yaml`:

```yaml
canonize:
  mappings:
    - source_pattern: "*gmail*.jsonl"
      transform: email/gmail_to_canonical_v1  # Your new transform
      output_name: email
```

---

## Testing Transforms

### Unit Testing

Test individual transforms:

```bash
# Test with single record
echo '{"id":"123","payload":{"headers":[...]}}' | \
  can transform run --meta transforms/email/gmail_to_canonical_v1.meta.yaml
```

### Integration Testing

Test with full pipeline:

```bash
# Run canonize stage only
lorch run --stage canonize

# Check output
ls -lh /home/user/phi-data/canonical/
cat /home/user/phi-data/canonical/email-*.jsonl | head -5 | jq .
```

### Validation Testing

```bash
# Validate all output records
for file in /home/user/phi-data/canonical/*.jsonl; do
  echo "Validating $file..."
  cat "$file" | jq . > /dev/null && echo "✓ Valid JSON" || echo "✗ Invalid JSON"
done
```

---

## JSONata Syntax

### Basic Operations

```jsonata
/* Field access */
field
object.nested.field

/* Array access */
array[0]
array[-1]  /* Last element */

/* Filter arrays */
array[condition]
headers[name="Subject"]

/* Map arrays */
array.field  /* Extract field from each element */
```

### Functions

```jsonata
/* String functions */
$uppercase(str)
$lowercase(str)
$trim(str)
$split(str, ",")
$join(array, ",")

/* Base64 */
$base64decode(str)
$base64encode(str)

/* Date/Time */
$fromMillis(number)    /* Epoch ms → ISO timestamp */
$toMillis(timestamp)   /* ISO timestamp → epoch ms */
$now()                 /* Current timestamp */

/* Type conversion */
$number(str)
$string(num)
$boolean(value)

/* Conditionals */
condition ? value_if_true : value_if_false
```

### Examples

```jsonata
/* Extract email from "Name <email@example.com>" format */
$split($split(from, "<")[1], ">")[0]

/* Convert epoch seconds to ISO */
$fromMillis($number(timestamp) * 1000)

/* Default value if field missing */
field ? field : "default_value"

/* Array mapping */
recipients@$r.{
  "email": $r.emailAddress.address,
  "name": $r.emailAddress.name
}
```

---

## Examples

### Example 1: Simple Field Mapping

**Source (Gmail):**

```json
{
  "id": "msg123",
  "payload": {
    "headers": [
      {"name": "Subject", "value": "Test"}
    ]
  }
}
```

**Transform:**

```jsonata
{
  "message_id": id,
  "subject": payload.headers[name="Subject"].value
}
```

**Output:**

```json
{
  "message_id": "msg123",
  "subject": "Test"
}
```

### Example 2: Array Transformation

**Source (Exchange):**

```json
{
  "toRecipients": [
    {"emailAddress": {"address": "user1@example.com"}},
    {"emailAddress": {"address": "user2@example.com"}}
  ]
}
```

**Transform:**

```jsonata
{
  "to": toRecipients.emailAddress.address
}
```

**Output:**

```json
{
  "to": ["user1@example.com", "user2@example.com"]
}
```

### Example 3: Conditional Fields

**Source:**

```json
{
  "hasAttachments": true,
  "attachments": [{"filename": "doc.pdf"}]
}
```

**Transform:**

```jsonata
{
  "attachments": hasAttachments ?
    attachments.{
      "name": filename
    } : []
}
```

**Output:**

```json
{
  "attachments": [{"name": "doc.pdf"}]
}
```

### Example 4: Date Conversion

**Source:**

```json
{
  "internalDate": "1735729200000"
}
```

**Transform:**

```jsonata
{
  "received_at": $fromMillis($number(internalDate))
}
```

**Output:**

```json
{
  "received_at": "2025-01-01T09:00:00.000Z"
}
```

---

## Best Practices

### 1. Use Comments

```jsonata
{
  /* Primary message identifier */
  "message_id": id,

  /* Extract subject from email headers */
  "subject": payload.headers[name="Subject"].value
}
```

### 2. Handle Missing Fields

```jsonata
{
  /* Use conditional to provide default */
  "cc": payload.headers[name="Cc"] ?
    $split(payload.headers[name="Cc"].value, ",") : [],

  /* Or use coalescing */
  "body": payload.body.data ? $base64decode(payload.body.data) : ""
}
```

### 3. Validate Input

```jsonata
{
  "message_id": id ? id : $error("Missing required field: id")
}
```

### 4. Keep Transforms Simple

- One transform per source type
- Break complex logic into functions
- Document any non-obvious transformations

### 5. Version Transforms

- Use semantic versioning (v1.0.0, v2.0.0)
- Never modify existing transforms
- Create new version for breaking changes

### 6. Test Thoroughly

- Test with real source data
- Test edge cases (null, empty, malformed)
- Validate output against schema

---

## Troubleshooting

### Common Errors

**Error:** `Undefined variable: field`
- **Cause:** Field doesn't exist in source
- **Fix:** Add conditional: `field ? field : default`

**Error:** `Type error: cannot decode null`
- **Cause:** Trying to decode null/missing field
- **Fix:** Check field exists before decoding

**Error:** `JSON Parse Error: Extra data`
- **Cause:** Multiple JSON objects not on separate lines
- **Fix:** Ensure source is valid JSONL (one object per line)

**Error:** `Checksum mismatch`
- **Cause:** Transform file modified after checksum calculated
- **Fix:** Recalculate checksum and update metadata

### Debug Transforms

```bash
# Test transform with debug output
echo '{"test": "data"}' | \
  can transform run --meta transform.meta.yaml --debug

# Validate JSONata syntax
jsonata '{"test": field}' <<< '{"field": "value"}'
```

---

## See Also

- [JSONata Documentation](https://docs.jsonata.org/)
- [JSONata Exerciser](https://try.jsonata.org/) - Interactive playground
- [Canonizer README](../../canonizer/README.md)
- [Transform Registry README](../../transforms/README.md)

---

**Need help?** Check existing transforms in `/home/user/transforms/` for examples!
