# Gmail OAuth Authentication Troubleshooting Guide

## Critical Issue: Conflicting OAuth Scopes

### The Problem

When requesting BOTH `gmail.readonly` AND `gmail.metadata` scopes together, Google's API enforces the MORE RESTRICTIVE scope (`gmail.metadata`), which prevents reading full message content.

**Symptom:**
- OAuth authorization succeeds (200 OK)
- Message list API succeeds (200 OK)
- Individual message fetch FAILS with 403 Forbidden
- Error: `"Metadata scope doesn't allow format FULL"`

### The Solution

**Request ONLY `gmail.readonly` scope** - this includes everything you need:
- Read message metadata
- Read full message content
- Read message bodies, attachments, headers

**DO NOT request both scopes together!**

```python
# ✓ CORRECT - Use ONLY gmail.readonly
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']

# ✗ WRONG - Don't use both!
SCOPES = [
    'https://www.googleapis.com/auth/gmail.readonly',
    'https://www.googleapis.com/auth/gmail.metadata'  # This breaks it!
]
```

## OAuth Flow Requirements

### Desktop vs Web OAuth Apps

**Use DESKTOP apps for local OAuth flows:**
- Desktop apps support `urn:ietf:wg:oauth:2.0:oob` (out-of-band) redirect
- OOB shows the authorization code directly on screen
- No localhost redirect required (works in Cloud Workstation environments)

**Web apps DON'T work for local flows:**
- Web apps don't support OOB redirect
- OAuth flow will hang/timeout
- Requires actual web server callback

### Generating OAuth Tokens - Step by Step

#### 1. Generate Authorization URL (OOB Mode)

```python
import urllib.parse

CLIENT_ID = "YOUR_DESKTOP_APP_CLIENT_ID.apps.googleusercontent.com"
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']  # ONLY this scope!

params = {
    'client_id': CLIENT_ID,
    'redirect_uri': 'urn:ietf:wg:oauth:2.0:oob',  # OOB mode for Desktop apps
    'response_type': 'code',
    'scope': ' '.join(SCOPES),
    'access_type': 'offline',
    'prompt': 'consent',
    'login_hint': 'user@example.com'
}

auth_url = 'https://accounts.google.com/o/oauth2/v2/auth?' + urllib.parse.urlencode(params)
print(auth_url)
```

#### 2. User Opens URL and Authorizes

- User sees: "View your email messages and settings" (gmail.readonly)
- After authorization, code appears on screen (e.g., `4/1Ab32j91ecw4p...`)

#### 3. Exchange Code for Refresh Token

```python
import requests

CLIENT_ID = "YOUR_DESKTOP_APP_CLIENT_ID.apps.googleusercontent.com"
CLIENT_SECRET = "YOUR_CLIENT_SECRET"
CODE = "4/1Ab32j91ecw4p..."  # From step 2

token_url = 'https://oauth2.googleapis.com/token'
data = {
    'code': CODE,
    'client_id': CLIENT_ID,
    'client_secret': CLIENT_SECRET,
    'redirect_uri': 'urn:ietf:wg:oauth:2.0:oob',
    'grant_type': 'authorization_code'
}

response = requests.post(token_url, data=data)
tokens = response.json()
refresh_token = tokens['refresh_token']  # Save this!
```

## Environment Variable Management

### Critical Issue: Cached Environment Variables

**Problem:** Even after updating `.env` files, your shell session has OLD values cached in `os.environ`.

**Symptoms:**
- `.env` files show correct values
- But ingestion still uses old credentials
- Meltano picks up cached environment variables from parent process

### Solution 1: Restart Your Shell

```bash
# Exit and restart your terminal/shell
exit
# Then start new session
```

### Solution 2: Manually Export New Values

```bash
# Update environment in current shell
export TAP_GMAIL_ACCT3_BFARMSTRONG_CLIENT_ID="73154799202-0mc27p38aolqcmi2gkat9gpd8qiti2n3.apps.googleusercontent.com"
export TAP_GMAIL_ACCT3_BFARMSTRONG_CLIENT_SECRET="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"
export TAP_GMAIL_ACCT3_BFARMSTRONG_REFRESH_TOKEN="1//05bQHwtBawuE4..."

# Then run ingestion
lorchestra run gmail_ingest_acct3
```

### Solution 3: Clear Meltano State Database

If Meltano is still caching old tokens:

```bash
cd /workspace/ingestor
rm -f .meltano/meltano.db
# Then run with explicit env vars
```

## Google Cloud Console Configuration

### OAuth Consent Screen Requirements

1. **User Type:** Can be Internal or External (External needed for non-workspace users)
2. **Publishing Status:** Testing is OK (but requires test users)
3. **Test Users:** Add specific email addresses if in Testing mode
4. **Scopes:** Add `https://www.googleapis.com/auth/gmail.readonly` (NOT gmail.metadata)

### OAuth Client Configuration

1. **Application Type:** Desktop application
2. **Authorized Redirect URIs:** Can be empty for Desktop apps
3. **OOB Support:** Desktop apps automatically support `urn:ietf:wg:oauth:2.0:oob`

### Required API

**Gmail API MUST be enabled** for the OAuth app's GCP project:
```
https://console.developers.google.com/apis/library/gmail.googleapis.com?project=YOUR_PROJECT_ID
```

## Debugging Failed OAuth

### Test Refresh Token Directly

```python
import requests

# Test if refresh token works
token_url = 'https://oauth2.googleapis.com/token'
data = {
    'client_id': CLIENT_ID,
    'client_secret': CLIENT_SECRET,
    'refresh_token': REFRESH_TOKEN,
    'grant_type': 'refresh_token'
}

response = requests.post(token_url, data=data)
if response.status_code == 200:
    access_token = response.json()['access_token']

    # Check scopes
    info_url = f"https://oauth2.googleapis.com/tokeninfo?access_token={access_token}"
    info = requests.get(info_url).json()
    print(f"Scopes: {info.get('scope')}")

    # Test message fetch
    msg_url = "https://gmail.googleapis.com/gmail/v1/users/me/messages?maxResults=1"
    headers = {"Authorization": f"Bearer {access_token}"}
    messages = requests.get(msg_url, headers=headers).json()

    if 'messages' in messages:
        msg_id = messages['messages'][0]['id']
        full_msg_url = f"https://gmail.googleapis.com/gmail/v1/users/me/messages/{msg_id}"
        full_msg = requests.get(full_msg_url, headers=headers)
        print(f"Full message fetch: {full_msg.status_code}")
        if full_msg.status_code != 200:
            print(full_msg.json())
else:
    print(f"Token exchange failed: {response.json()}")
```

### Common Error Messages

#### "Metadata scope doesn't allow format FULL"
**Cause:** You have both `gmail.readonly` and `gmail.metadata` scopes
**Fix:** Regenerate token with ONLY `gmail.readonly`

#### "unauthorized_client"
**Causes:**
- Gmail API not enabled for the OAuth app's project
- OAuth client has restrictions that don't match the request
- Wrong client_id/client_secret combination

#### "403 Forbidden" on message fetch
**Causes:**
- Wrong scopes (metadata-only instead of readonly)
- Token doesn't actually have gmail.readonly scope
- Gmail API not enabled

#### OAuth URL hangs/timeouts
**Causes:**
- Using Web app instead of Desktop app
- Using `http://localhost:8080` redirect with Web app in Testing mode
- User not added as Test User in OAuth consent screen

## Configuration Files

### .env File Structure

Both `/workspace/lorchestra/.env` and `/workspace/ingestor/.env` need identical Gmail credentials:

```bash
# Account 3: bfarmstrong@gmail.com (acct3-bfarmstrong)
# Uses therapy-ai project Desktop OAuth app
TAP_GMAIL_ACCT3_BFARMSTRONG_CLIENT_ID=YOUR_CLIENT_ID.apps.googleusercontent.com
TAP_GMAIL_ACCT3_BFARMSTRONG_CLIENT_SECRET=YOUR_CLIENT_SECRET
TAP_GMAIL_ACCT3_BFARMSTRONG_REFRESH_TOKEN=YOUR_REFRESH_TOKEN
```

## Quick Reference: Working OAuth Flow

```bash
# 1. Generate OOB URL with ONLY gmail.readonly scope
python3 << 'EOF'
import urllib.parse
CLIENT_ID = "YOUR_DESKTOP_APP_ID"
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']
params = {
    'client_id': CLIENT_ID,
    'redirect_uri': 'urn:ietf:wg:oauth:2.0:oob',
    'response_type': 'code',
    'scope': ' '.join(SCOPES),
    'access_type': 'offline',
    'prompt': 'consent',
}
print('https://accounts.google.com/o/oauth2/v2/auth?' + urllib.parse.urlencode(params))
EOF

# 2. User opens URL, authorizes, copies code

# 3. Exchange code for refresh token
python3 << 'EOF'
import requests
CLIENT_ID = "YOUR_DESKTOP_APP_ID"
CLIENT_SECRET = "YOUR_SECRET"
CODE = "4/1Ab32j91..."  # From step 2

response = requests.post('https://oauth2.googleapis.com/token', data={
    'code': CODE,
    'client_id': CLIENT_ID,
    'client_secret': CLIENT_SECRET,
    'redirect_uri': 'urn:ietf:wg:oauth:2.0:oob',
    'grant_type': 'authorization_code'
})
print(response.json()['refresh_token'])
EOF

# 4. Update .env files with new refresh token

# 5. Export env vars OR restart shell
export TAP_GMAIL_ACCT3_BFARMSTRONG_REFRESH_TOKEN="NEW_TOKEN_HERE"

# 6. Run ingestion
lorchestra run gmail_ingest_acct3 --since 2024-01-01 --until 2024-01-31
```

## Summary of Lessons Learned

1. **Never mix gmail.readonly and gmail.metadata scopes** - use ONLY gmail.readonly
2. **Always use Desktop OAuth apps for local flows** - they support OOB
3. **Environment variables are cached** - restart shell or export manually after .env updates
4. **Meltano may cache credentials** - clear `.meltano/meltano.db` if needed
5. **Gmail API must be enabled** in the OAuth app's GCP project
6. **OOB redirect URI is `urn:ietf:wg:oauth:2.0:oob`** - not http://localhost:8080

## History

**Issue Date:** 2024-11-24
**Resolution:** Switched from conflicting scopes to gmail.readonly-only with Desktop OAuth app + OOB flow
**Duration:** ~3 hours of troubleshooting
**Root Causes:**
1. Conflicting OAuth scopes (gmail.readonly + gmail.metadata)
2. Cached environment variables in shell session
3. Multiple .env files needing updates
4. Wrong OAuth app type (Web vs Desktop)
