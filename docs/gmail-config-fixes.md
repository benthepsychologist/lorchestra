# Gmail Configuration Fixes

This document outlines required configuration changes to fix Gmail extraction issues in the meltano-ingest repository.

## Location

File: `/home/user/meltano-ingest/meltano.yml`
Tap: `tap-gmail--acct1-personal` (lines ~82-89)

## Issues and Fixes

### Issue 1: Query Filter Not Working

**Problem:**
The `messages.q: after:2025/11/12` filter is being ignored, causing the tap to extract 1+ year of emails instead of just 1 day.

**Root Cause:**
The `message_list` stream is excluded with `!message_list.*.*`, but this stream is where the `messages.q` filter applies. When excluded, the filter has no effect.

**Fix:**
Re-enable the `message_list` stream by removing the `!message_list.*.*` exclusion from the select list.

### Issue 2: Base64 Bloat

**Problem:**
Extracted files are 131MB when they should be ~1-5MB for one day of emails.

**Root Cause:**
Using `format=full` in the Gmail API returns entire email messages with inline images and attachments encoded as base64 in the `messages.raw` field.

**Fix:**
Exclude the `messages.raw` field by adding `!messages.raw` to the select list.

### Issue 3: Verify Query Filter

**Problem:**
Need to ensure the date filter is set correctly.

**Fix:**
Verify that `messages.q: after:2025/11/12` is set in the tap configuration (update the date as needed for current extractions).

## Required Configuration Changes

### Current Configuration (Broken)

```yaml
- name: tap-gmail--acct1-personal
  inherit_from: tap-gmail
  config:
    oauth_credentials:
      client_id: ${TAP_GMAIL_CLIENT_ID}
      client_secret: ${TAP_GMAIL_CLIENT_SECRET}
      refresh_token: ${TAP_GMAIL_ACCT1_REFRESH_TOKEN}
    messages.q: after:2025/11/12  # IGNORED because message_list is excluded
  select:
    - '!message_list.*.*'  # ❌ PROBLEM: Excludes stream where filter applies
    - messages.*.*         # Includes all message fields including raw
```

### Fixed Configuration

```yaml
- name: tap-gmail--acct1-personal
  inherit_from: tap-gmail
  config:
    oauth_credentials:
      client_id: ${TAP_GMAIL_CLIENT_ID}
      client_secret: ${TAP_GMAIL_CLIENT_SECRET}
      refresh_token: ${TAP_GMAIL_ACCT1_REFRESH_TOKEN}
    messages.q: after:2025/11/12  # ✓ Now works because message_list is enabled
  select:
    - message_list.*.*     # ✓ FIXED: Enable message_list for query filtering
    - messages.*.*         # Message content
    - '!messages.raw'      # ✓ FIXED: Exclude base64-encoded full email
```

## What This Fixes

1. **Date filtering works**: Only emails after the specified date will be extracted
2. **Reduced file size**: Excluding `messages.raw` reduces extraction size from ~131MB to ~1-5MB per day
3. **Faster extraction**: Less data to transfer and process (from 35+ minutes to 1-2 minutes)
4. **Same functionality**: All important email metadata and text content are still included

## Expected Results After Fix

- Extraction time: 1-2 minutes (down from 35+ minutes)
- File size: 1-5 MB per day (down from 131 MB)
- Records: Only emails from the specified date range
- No base64 bloat in output files

## Validation

After making these changes, validate the configuration using lorch:

```bash
# Sync meltano config to lorch cache
lorch config sync meltano

# Validate the tap-target pair
lorch tools validate meltano --tap tap-gmail--acct1-personal --target target-jsonl-chunked--gmail-ben-mensio

# Run extraction
lorch extract tap-gmail--acct1-personal
```

The validation command should now show:
- ✓ No errors about message_list exclusion
- ⚠ Warning if `!messages.raw` is not excluded (if fix #2 not applied)

## Related Files

- Meltano config: `/home/user/meltano-ingest/meltano.yml`
- Lorch adapter: `lorch/tools/meltano.py` (includes Gmail-specific validation)
- Lorch CLI: `lorch/cli.py` (config sync and validation commands)

## Notes

- These changes should be made in the **meltano-ingest** repository, not in lorch
- Lorch's MeltanoAdapter will detect and warn about these issues during validation
- The same fixes apply to other Gmail taps (acct2, acct3) if they have similar issues
- Update the `messages.q` filter date to match your desired extraction range
