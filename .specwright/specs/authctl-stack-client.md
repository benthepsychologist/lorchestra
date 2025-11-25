---
version: "0.1"
tier: A
title: authctl - Unified Credential Management
owner: benthepsychologist
goal: Single package for all auth flows and credential storage across the stack
labels: [auth, credentials, infrastructure]
project_slug: authctl
spec_version: 1.0.0
created: 2025-11-25T16:03:36.511196+00:00
updated: 2025-11-25T16:30:00+00:00
orchestrator_contract: "standard"
repo:
  working_branch: "main"
---

# authctl - Unified Credential Management

## Objective

> Provide a single Python package that owns all auth flows and credential storage
> for the entire stack. One name, one tool, one credential store.

## Acceptance Criteria

- [ ] `authctl` package exists at `/workspace/authctl/` with `pyproject.toml`
- [ ] `authctl auth gmail --account <name>` runs Gmail OAuth flow
- [ ] `authctl auth msgraph --account <name>` runs MS Graph device flow
- [ ] `authctl auth stripe --account <name> --api-key <key>` stores Stripe API key
- [ ] `authctl creds list` shows all stored credentials
- [ ] `authctl creds show <service>:<account>` shows redacted credential details
- [ ] `from authctl.providers.gmail import build_gmail_credentials` returns SDK-native object
- [ ] `from authctl.providers.msgraph import build_msgraph_app` returns MSAL app with cache
- [ ] `from authctl.secrets import get_credentials` raises `MissingCredentialsError` with actionable message
- [ ] All credentials stored under `$AUTHCTL_HOME/` only (no repo-local `.tokens/` or `.env` creds)
- [ ] `injest` consumes credentials via `authctl` imports (no local auth code)
- [ ] `lorchestra` consumes credentials via `authctl` imports (no local auth scripts)
- [ ] 85% test coverage on authctl package

## Context

### Background

Auth is currently fragmented across the stack:
- `/workspace/injest/.env` - contains refresh tokens, API keys, client secrets
- `/workspace/injest/.tokens/` - MSAL token cache `.bin` files
- `/workspace/lorchestra/scripts/gmail_*.py` - duplicated OAuth setup scripts (one per account)
- `/workspace/lorchestra/.env` - credential env vars
- `/workspace/injest/injest/config.py` - account registry mapping identities to env var names

This creates confusion about:
- Who owns auth flows? (currently scattered scripts)
- Where do credentials live? (currently per-repo)
- How do consumers get authenticated clients? (currently roll-your-own in each service)

**Solution:** One package (`authctl`), one CLI (`authctl`), one credential store (`$AUTHCTL_HOME/`).

### Core Principles

1. **One name**: Package = `authctl`, CLI = `authctl`
2. **One credential store**: `$AUTHCTL_HOME/` (default: `~/.authctl/`, dev: `/workspace/.authctl/`)
3. **One auth entry point**: Only `authctl` runs OAuth/device flows
4. **Provider-aware factory**: Returns SDK-native objects that handle their own token refresh

### Architecture

```
$AUTHCTL_HOME/                     # /workspace/.authctl/ in dev
├── credentials.json               # Seed credentials (refresh tokens, client IDs, API keys)
└── msgraph/                       # Provider-specific SDK caches
    ├── ops/token_cache.bin
    └── therapy/token_cache.bin
```

**Consumer pattern:**
```python
from authctl.providers.gmail import build_gmail_credentials
from authctl.providers.msgraph import build_msgraph_app
from authctl.providers.stripe import get_api_key

gmail_creds = build_gmail_credentials("primary")  # -> google.oauth2.credentials.Credentials
msgraph_app = build_msgraph_app("ops")            # -> msal.PublicClientApplication
api_key = get_api_key("efs")                      # -> str
```

### Data Model

**CredentialKey:** `service:account` (e.g., `gmail:primary`, `msgraph:ops`, `stripe:efs`)

**Credential Bundles** (stored in `credentials.json`):

```json
{
  "gmail:primary": {
    "type": "oauth2",
    "provider": "google",
    "client_id": "...",
    "client_secret": "...",
    "refresh_token": "...",
    "scopes": ["https://mail.google.com/"]
  },
  "msgraph:ops": {
    "type": "device_flow",
    "provider": "microsoft",
    "client_id": "...",
    "tenant_id": "...",
    "scopes": ["Mail.Read", "Calendars.Read"]
  },
  "stripe:efs": {
    "type": "api_key",
    "provider": "stripe",
    "api_key": "sk_live_..."
  }
}
```

Note: MS Graph refresh tokens are managed by MSAL in separate `.bin` files under `$AUTHCTL_HOME/msgraph/<account>/`.

### Constraints

- Package must be standalone at `/workspace/authctl/` (sibling to injest, lorchestra)
- No repo-local credential storage after migration (no `injest/.tokens/`, no scattered `.env` creds)
- Only `authctl` executes OAuth/device flows - consumers must never run their own
- Token refresh delegated to SDK (Google SDK, MSAL) - authctl stores seeds only
- Redact sensitive fields in `authctl creds show` (fields containing: secret, token, password, key, api_key, refresh, access)

## Plan

### Step 1: Bootstrap Package [G0: Foundation]

**Prompt:**

Create the `authctl` package at `/workspace/authctl/` with core infrastructure:

1. Create `pyproject.toml` with:
   - Package name: `authctl`
   - CLI entrypoint: `authctl = "authctl.cli:app"`
   - Dependencies: typer, google-auth, google-auth-oauthlib, msal

2. Create `authctl/config.py`:
   - `get_authctl_home()` - returns `$AUTHCTL_HOME` or `~/.authctl`
   - `get_credentials_path()` - returns `$AUTHCTL_HOME/credentials.json`

3. Create `authctl/models.py`:
   - `CredentialKey` dataclass with `service`, `account`, `full_key` property

4. Create `authctl/secrets.py`:
   - `get_credentials(service, account) -> dict`
   - `set_credentials(service, account, bundle) -> None`
   - `list_credentials() -> list[tuple[str, str]]`
   - `delete_credentials(service, account) -> None`
   - `MissingCredentialsError` with actionable message

5. Create basic CLI with `authctl creds list` command

**Commands:**

```bash
cd /workspace/authctl
uv pip install -e .
authctl creds list
```

**Outputs:**

- `/workspace/authctl/pyproject.toml`
- `/workspace/authctl/authctl/__init__.py`
- `/workspace/authctl/authctl/config.py`
- `/workspace/authctl/authctl/models.py`
- `/workspace/authctl/authctl/secrets.py`
- `/workspace/authctl/authctl/cli.py`

<!-- GATE_REVIEW_START -->
#### Gate Review Checklist

##### Package Structure
- [ ] Package installs correctly with `uv pip install -e .`
- [ ] CLI entrypoint `authctl` is available
- [ ] `authctl creds list` runs without error (empty list OK)
- [ ] Config correctly resolves `$AUTHCTL_HOME`

##### Code Quality
- [ ] Type hints on all public functions
- [ ] No hardcoded paths (uses config module)
- [ ] `MissingCredentialsError` includes suggested command

#### Approval Decision
- [ ] APPROVED
- [ ] APPROVED WITH CONDITIONS: ___
- [ ] REJECTED: ___
- [ ] DEFERRED: ___

**Approval Metadata:**
- Reviewer: ___
- Date: ___
- Rationale: ___
<!-- GATE_REVIEW_END -->

---

### Step 2: Gmail Provider [G1: First Provider]

**Prompt:**

Implement Gmail OAuth provider:

1. Create `authctl/providers/__init__.py`

2. Create `authctl/providers/gmail.py`:
   - `run_oauth_flow(account: str, client_id: str, client_secret: str) -> None`
     - Uses `google-auth-oauthlib` InstalledAppFlow
     - Prompts user to visit URL and paste code
     - Stores bundle via `set_credentials("gmail", account, bundle)`
   - `build_gmail_credentials(account: str) -> google.oauth2.credentials.Credentials`
     - Loads bundle from `get_credentials("gmail", account)`
     - Returns SDK-native Credentials object (handles refresh automatically)

3. Add CLI command:
   - `authctl auth gmail --account <name> --client-id <id> --client-secret <secret>`

4. Test with existing Gmail account credentials

**Commands:**

```bash
# Test the flow (will prompt for browser auth)
authctl auth gmail --account test --client-id $TEST_CLIENT_ID --client-secret $TEST_CLIENT_SECRET

# Verify stored
authctl creds list
authctl creds show gmail:test
```

**Outputs:**

- `/workspace/authctl/authctl/providers/__init__.py`
- `/workspace/authctl/authctl/providers/gmail.py`
- Updated `/workspace/authctl/authctl/cli.py` with `auth gmail` command

<!-- GATE_REVIEW_START -->
#### Gate Review Checklist

##### OAuth Flow
- [ ] `authctl auth gmail` completes successfully with test account
- [ ] Credentials stored in `$AUTHCTL_HOME/credentials.json`
- [ ] Bundle contains: type, provider, client_id, client_secret, refresh_token, scopes

##### Factory Function
- [ ] `build_gmail_credentials()` returns valid `google.oauth2.credentials.Credentials`
- [ ] Returned credentials can be used with Gmail API
- [ ] `MissingCredentialsError` raised if account not found

##### Security
- [ ] Sensitive fields redacted in `authctl creds show`
- [ ] No credentials logged or printed during flow

#### Approval Decision
- [ ] APPROVED
- [ ] APPROVED WITH CONDITIONS: ___
- [ ] REJECTED: ___
- [ ] DEFERRED: ___

**Approval Metadata:**
- Reviewer: ___
- Date: ___
- Rationale: ___
<!-- GATE_REVIEW_END -->

---

### Step 3: MS Graph Provider [G1: Second Provider]

**Prompt:**

Implement MS Graph device-flow provider:

1. Create `authctl/providers/msgraph.py`:
   - `run_device_flow(account: str, client_id: str, tenant_id: str, scopes: list[str]) -> None`
     - Uses MSAL `PublicClientApplication` with device code flow
     - Prints device code and URL for user to authenticate
     - Stores seed bundle via `set_credentials("msgraph", account, bundle)`
     - Creates MSAL token cache at `$AUTHCTL_HOME/msgraph/<account>/token_cache.bin`
   - `build_msgraph_app(account: str) -> msal.PublicClientApplication`
     - Loads bundle from `get_credentials("msgraph", account)`
     - Creates `PublicClientApplication` with `FileTokenCache` pointing to `$AUTHCTL_HOME/msgraph/<account>/token_cache.bin`
     - MSAL handles token refresh automatically

2. Add CLI command:
   - `authctl auth msgraph --account <name> --client-id <id> --tenant-id <id> --scopes <scope1,scope2>`

**Commands:**

```bash
# Test device flow
authctl auth msgraph --account test --client-id $TEST_CLIENT_ID --tenant-id $TEST_TENANT_ID --scopes "Mail.Read,Calendars.Read"

# Verify
authctl creds list
ls $AUTHCTL_HOME/msgraph/test/
```

**Outputs:**

- `/workspace/authctl/authctl/providers/msgraph.py`
- Updated `/workspace/authctl/authctl/cli.py` with `auth msgraph` command

<!-- GATE_REVIEW_START -->
#### Gate Review Checklist

##### Device Flow
- [ ] `authctl auth msgraph` displays device code and URL
- [ ] Authentication completes after user visits URL
- [ ] Seed bundle stored in `credentials.json` (client_id, tenant_id, scopes only)
- [ ] MSAL token cache created at `$AUTHCTL_HOME/msgraph/<account>/token_cache.bin`

##### Factory Function
- [ ] `build_msgraph_app()` returns valid `msal.PublicClientApplication`
- [ ] Token cache path correctly configured
- [ ] `acquire_token_silent()` works for cached tokens
- [ ] `MissingCredentialsError` raised if account not found

##### Token Management
- [ ] MSAL manages token refresh (not authctl)
- [ ] Token cache files are in `$AUTHCTL_HOME/msgraph/`, not repo-local

#### Approval Decision
- [ ] APPROVED
- [ ] APPROVED WITH CONDITIONS: ___
- [ ] REJECTED: ___
- [ ] DEFERRED: ___

**Approval Metadata:**
- Reviewer: ___
- Date: ___
- Rationale: ___
<!-- GATE_REVIEW_END -->

---

### Step 4: Stripe Provider [G1: Third Provider]

**Prompt:**

Implement Stripe API key provider (non-interactive):

1. Create `authctl/providers/stripe.py`:
   - `set_api_key(account: str, api_key: str) -> None`
     - Validates key starts with `sk_live_` or `sk_test_`
     - Stores bundle via `set_credentials("stripe", account, bundle)`
   - `get_api_key(account: str) -> str`
     - Loads bundle and returns the API key string

2. Add CLI command:
   - `authctl auth stripe --account <name> --api-key <key>`

**Commands:**

```bash
authctl auth stripe --account test --api-key sk_test_xxxxx
authctl creds show stripe:test
```

**Outputs:**

- `/workspace/authctl/authctl/providers/stripe.py`
- Updated `/workspace/authctl/authctl/cli.py` with `auth stripe` command

<!-- GATE_REVIEW_START -->
#### Gate Review Checklist

##### API Key Storage
- [ ] `authctl auth stripe` stores key in `credentials.json`
- [ ] Key validation rejects invalid prefixes
- [ ] `get_api_key()` returns the stored key

##### Security
- [ ] API key fully redacted in `authctl creds show`
- [ ] No key echoed to terminal during `auth stripe`

#### Approval Decision
- [ ] APPROVED
- [ ] APPROVED WITH CONDITIONS: ___
- [ ] REJECTED: ___
- [ ] DEFERRED: ___

**Approval Metadata:**
- Reviewer: ___
- Date: ___
- Rationale: ___
<!-- GATE_REVIEW_END -->

---

### Step 5: Re-authorize All Accounts [G2: Credential Migration]

**Prompt:**

Re-authorize all existing accounts through authctl:

1. Set `AUTHCTL_HOME=/workspace/.authctl` in environment

2. Gmail accounts (3):
   ```bash
   authctl auth gmail --account acct1 --client-id $ID --client-secret $SECRET
   authctl auth gmail --account acct2 --client-id $ID --client-secret $SECRET
   authctl auth gmail --account acct3 --client-id $ID --client-secret $SECRET
   ```

3. MS Graph accounts (4):
   ```bash
   authctl auth msgraph --account ben-mensio --client-id $ID --tenant-id $TID --scopes "Mail.Read"
   authctl auth msgraph --account booking-mensio --client-id $ID --tenant-id $TID --scopes "Mail.Read"
   authctl auth msgraph --account info-mensio --client-id $ID --tenant-id $TID --scopes "Mail.Read"
   authctl auth msgraph --account ben-efs --client-id $ID --tenant-id $TID --scopes "Mail.Read"
   ```

4. Stripe account (1):
   ```bash
   authctl auth stripe --account mensio --api-key $STRIPE_API_KEY
   ```

5. Verify all credentials:
   ```bash
   authctl creds list
   ```

**Commands:**

```bash
export AUTHCTL_HOME=/workspace/.authctl
authctl creds list
# Should show 8 credentials (3 gmail + 4 msgraph + 1 stripe)
```

**Outputs:**

- `/workspace/.authctl/credentials.json` with all 8 accounts
- `/workspace/.authctl/msgraph/*/token_cache.bin` for each MS Graph account

<!-- GATE_REVIEW_START -->
#### Gate Review Checklist

##### Credential Verification
- [ ] All 3 Gmail accounts authorized and listed
- [ ] All 4 MS Graph accounts authorized and listed
- [ ] Stripe account authorized and listed
- [ ] `credentials.json` contains 8 entries

##### Functional Verification
- [ ] `build_gmail_credentials("acct1")` returns valid credentials
- [ ] `build_msgraph_app("ben-mensio")` returns valid MSAL app
- [ ] `get_api_key("mensio")` returns Stripe key

#### Approval Decision
- [ ] APPROVED
- [ ] APPROVED WITH CONDITIONS: ___
- [ ] REJECTED: ___
- [ ] DEFERRED: ___

**Approval Metadata:**
- Reviewer: ___
- Date: ___
- Rationale: ___
<!-- GATE_REVIEW_END -->

---

### Step 6: Migrate injest [G2: Integration]

**Prompt:**

Update `injest` to consume credentials via authctl:

1. Add `authctl` as dependency in `/workspace/injest/pyproject.toml`

2. Update stream adapters to use authctl:
   - `GmailStream` → `build_gmail_credentials(account)`
   - `ExchangeMailStream` → `build_msgraph_app(account)`
   - `StripeCustomersStream` etc. → `get_api_key(account)`

3. Remove credential-related code from `injest/config.py`:
   - Keep account registry (identity → connection_name mapping)
   - Remove env var mappings for secrets

4. Delete legacy auth files:
   - `/workspace/injest/.tokens/` directory
   - Credential env vars from `/workspace/injest/.env`

5. Test all streams still work

**Commands:**

```bash
cd /workspace/injest
pytest tests/ -v
```

**Outputs:**

- Updated `/workspace/injest/pyproject.toml`
- Updated `/workspace/injest/injest/streams/*.py`
- Updated `/workspace/injest/injest/config.py`
- Deleted `/workspace/injest/.tokens/`

<!-- GATE_REVIEW_START -->
#### Gate Review Checklist

##### Code Migration
- [ ] All stream adapters use authctl imports
- [ ] No direct OAuth/auth code remains in injest
- [ ] `injest/config.py` no longer references credential env vars

##### File Cleanup
- [ ] `/workspace/injest/.tokens/` deleted
- [ ] Credential vars removed from `/workspace/injest/.env`

##### Functional Testing
- [ ] Gmail streams work with authctl credentials
- [ ] Exchange streams work with authctl credentials
- [ ] Stripe streams work with authctl credentials
- [ ] All injest tests pass

#### Approval Decision
- [ ] APPROVED
- [ ] APPROVED WITH CONDITIONS: ___
- [ ] REJECTED: ___
- [ ] DEFERRED: ___

**Approval Metadata:**
- Reviewer: ___
- Date: ___
- Rationale: ___
<!-- GATE_REVIEW_END -->

---

### Step 7: Migrate lorchestra [G2: Integration]

**Prompt:**

Update `lorchestra` to consume credentials via authctl:

1. Add `authctl` as dependency in `/workspace/lorchestra/pyproject.toml`

2. Update jobs to use authctl (if they directly access credentials)

3. Delete legacy auth scripts:
   - `/workspace/lorchestra/scripts/gmail_*.py` (all OAuth setup scripts)
   - `/workspace/lorchestra/scripts/get_gmail_refresh_token.py`

4. Clean credential vars from `/workspace/lorchestra/.env`

5. Add `AUTHCTL_HOME=/workspace/.authctl` to `.env`

6. Test all jobs still work

**Commands:**

```bash
cd /workspace/lorchestra
source .env
lorchestra run gmail_ingest_acct1 --dry-run
lorchestra run exchange_ingest_ben_mensio --dry-run
lorchestra run stripe_ingest_customers --dry-run
```

**Outputs:**

- Updated `/workspace/lorchestra/pyproject.toml`
- Deleted `/workspace/lorchestra/scripts/gmail_*.py`
- Updated `/workspace/lorchestra/.env`

<!-- GATE_REVIEW_START -->
#### Gate Review Checklist

##### Code Migration
- [ ] lorchestra jobs work with authctl-provided credentials
- [ ] No direct credential access in lorchestra code

##### File Cleanup
- [ ] All `scripts/gmail_*.py` OAuth scripts deleted
- [ ] Credential vars removed from `.env`
- [ ] `AUTHCTL_HOME` added to `.env`

##### Functional Testing
- [ ] Gmail ingest jobs work (dry-run)
- [ ] Exchange ingest jobs work (dry-run)
- [ ] Stripe ingest jobs work (dry-run)

#### Approval Decision
- [ ] APPROVED
- [ ] APPROVED WITH CONDITIONS: ___
- [ ] REJECTED: ___
- [ ] DEFERRED: ___

**Approval Metadata:**
- Reviewer: ___
- Date: ___
- Rationale: ___
<!-- GATE_REVIEW_END -->

---

### Step 8: Testing & Documentation [G3: Pre-Release]

**Prompt:**

Add tests and finalize documentation:

1. Add unit tests for authctl:
   - `tests/test_secrets.py` - get/set/list/delete, MissingCredentialsError
   - `tests/test_providers.py` - mock OAuth flows, factory functions
   - Target: 85% coverage

2. Update READMEs:
   - `/workspace/authctl/README.md` - usage, CLI reference
   - Update `/workspace/injest/README.md` - note authctl dependency
   - Update `/workspace/lorchestra/README.md` - note authctl dependency

3. Run full integration test:
   - All lorchestra jobs with production data

**Commands:**

```bash
cd /workspace/authctl
pytest tests/ --cov=authctl --cov-report=term-missing

cd /workspace/lorchestra
lorchestra run gmail_ingest_acct1
lorchestra run exchange_ingest_ben_mensio
lorchestra run stripe_ingest_customers
```

**Outputs:**

- `/workspace/authctl/tests/test_secrets.py`
- `/workspace/authctl/tests/test_providers.py`
- `/workspace/authctl/README.md`
- Coverage report ≥85%

<!-- GATE_REVIEW_START -->
#### Gate Review Checklist

##### Test Coverage
- [ ] authctl test coverage ≥85%
- [ ] All tests pass
- [ ] Edge cases covered (missing creds, invalid keys, etc.)

##### Integration Testing
- [ ] Gmail ingest runs successfully with production data
- [ ] Exchange ingest runs successfully with production data
- [ ] Stripe ingest runs successfully with production data

##### Documentation
- [ ] authctl README complete with CLI reference
- [ ] Consumer packages note authctl dependency

#### Approval Decision
- [ ] APPROVED
- [ ] APPROVED WITH CONDITIONS: ___
- [ ] REJECTED: ___
- [ ] DEFERRED: ___

**Approval Metadata:**
- Reviewer: ___
- Date: ___
- Rationale: ___
<!-- GATE_REVIEW_END -->

## Models & Tools

**Tools:** bash, pytest, uv, typer

**Models:** (defaults)

## Repository

**Branch:** `main`

**Package Location:** `/workspace/authctl/` (new standalone package)

**Merge Strategy:** squash

## Non-Goals

- No user management, RBAC, or multi-tenant identity
- No cluster orchestration
- No multi-region credential sync
- No automatic migration of existing credentials (all accounts re-authorized)
- No prod secret manager backend yet (leave clean `SecretsBackend` abstraction for future)

## Future Extensions

- `SecretsBackend` abstraction for pluggable backends
- `SecretManagerBackend` for GCP Secret Manager in prod
- Additional providers: Dataverse, Google Forms, etc.
- `authctl creds rotate` for key rotation workflows