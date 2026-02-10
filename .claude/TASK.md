---
id: e010-02-lorchestra-egret
title: "Lorchestra Egret Integration — Native Op, Client, Email Jobs"
tier: B
owner: benthepsychologist
goal: "Add egret as a native op in lorchestra: client boundary, egret callable (plan builder), egret.submit native op, and email job definitions"
branch: feat/egret-integration
status: complete
created: 2026-02-09T00:00:00Z
completed: 2026-02-10T00:00:00Z
---

# e010-02-lorchestra-egret: Lorchestra Egret Integration — Native Op, Client, Email Jobs

**Epic:** e010-egret
**Repo:** lorchestra
**Branch:** `feat/egret-integration`
**Tier:** B

## Objective

Wire egret into lorchestra as a native op (mirroring the storacle integration pattern — `plan.build` → `storacle.submit`, here `call` callable:egret → `egret.submit`):
- Add `egret.submit` to the Op enum as a native op
- Add a client boundary (`lorchestra/egret/client.py`) that submits plans to egret (in-proc first, noop fallback when not installed)
- Add `_handle_egret_submit(manifest)` branch in `_dispatch_manifest()`
- Register a thin `egret` callable that builds an EgretPlan from step params
- Add email job definitions (send, send_templated as render+send, batch_send)

## Acceptance Criteria

- `Op.EGRET_SUBMIT == "egret.submit"` with backend `"native"`
- `_dispatch_manifest()` handles `Op.EGRET_SUBMIT` via `_handle_egret_submit(manifest)`
- `lorchestra/egret/` exports `submit_plan` and `RpcMeta`
- `egret` callable registered in callable dispatch (builds EgretPlan from params)
- Job definitions exist in `lorchestra/jobs/definitions/email/`:
  - `email.send.yaml` — two steps: `call` callable:egret (builds plan) + `egret.submit` (executes)
  - `email.send_templated.yaml` — three steps: `call` callable:render (render) + `call` callable:egret (builds plan) + `egret.submit` (executes)
  - `email.batch_send.yaml` — three steps: `call` callable:render (render) + `call` callable:egret (builds plan) + `egret.submit` (executes) — takes items directly in payload
- End-to-end: `lorchestra.execute(envelope)` routes an email job through egret

## Renderer Contract (`call.render`)

Lorchestra MUST provide a callable operation referred to as `call.render`.

In JobDefs this is represented as:
- `op: call`
- `params.callable: render`

### Single render

**Input params:**
- `template_path: str` — absolute template path
- `template_vars: dict` — variables available to the template

**Output shape:**
- `subject: str`
- `body: str`
- `is_html: bool`

### Batch render

**Input params:**
- `template_path: str`
- `items: list[{to: str, template_vars: dict, idempotency_key: str}]`

**Output shape:**
- `items: list[{to: str, subject: str, body: str, is_html: bool, idempotency_key: str}]`

Batch mode exists to keep JobDefs simple (no manual zip/join steps). The renderer is allowed to pass through `to` and `idempotency_key` unchanged.

## Workman Expand Contract (`call.workman` / `recipients.expand`)

Batch email requires a deterministic expansion step that produces a list of per-recipient render inputs.

In JobDefs this is represented as:
- `op: call`
- `params.callable: workman`
- `params.method: recipients.expand`

### Input params

- `recipients_file: str` — path to a file containing recipients (implementation-defined format; CSV is typical)
- `template_vars: dict` — base template variables applied to every recipient (may be merged with recipient-specific vars)
- `batch_id: str` — caller-provided stable identifier used to derive per-recipient idempotency keys

### Output shape

`recipients.expand` returns a JSON object with:

- `items: list[{to: str, template_vars: dict, idempotency_key: str}]`

Notes:
- `to` MUST be a single recipient email address.
- `template_vars` MUST be a fully materialized dict for that recipient (already merged).
- `idempotency_key` MUST be generated here for batch sends and MUST be stable across retries.

Recommended derivation:
- `idempotency_key = "email.batch_send:{batch_id}:{to}"`

This keeps idempotency generation in the plan-making layer (life/workman), not in egret.

## Egret Callable (Plan Builder)

The `egret` callable builds an EgretPlan from step params. It is registered in the callable dispatch layer alongside `injest`, `canonizer`, `workman`, etc.

In JobDefs this is represented as:
- `op: call`
- `params.callable: egret`

The callable MAY receive either:
- single send: params include `method`, `to`, `subject`, `body`, `is_html`, etc.
- batch send: params include `method` and `items: [...]` where each item contains `to`, `subject`, `body`, optional `is_html`, `idempotency_key`

When `items` is present, the callable builds a plan with one `msg.send` op per item.

### Idempotency Key Rules (Messaging)

Egret enforces idempotency for messaging writes. To support this, the lorchestra plan-building layer MUST ensure every `msg.send` op includes an `idempotency_key`.

- For `email.batch_send`, `idempotency_key` MUST be provided per item by `recipients.expand` and MUST be stable across retries.
- For `email.send` and `email.send_templated`, `idempotency_key` MAY be omitted from the envelope payload.
  In that case the `callable:egret` MUST derive a deterministic, retry-stable key from the finalized send params.

Recommended derivation for single sends:
- Canonical input: `{method, channel, provider, account, to, subject, body, is_html}`
- Derive: `idempotency_key = "email.send:" + sha256(canonical_json(input))`

This makes re-running the same envelope safe by default. To intentionally re-send an identical message, callers must supply an explicit different `idempotency_key`.

**Output shape:**
- `plan: dict` — serialized EgretPlan ready for `egret.submit`

## Concrete JobDef YAML Sketches

These sketches show the exact `@payload.*` / `@run.*` references expected by the compiler.

### `email.send.yaml`

```yaml
job_id: email.send
version: "1.0"
steps:
  - step_id: build
    op: call
    params:
      callable: egret
      method: msg.send
      to: "@payload.to"
      subject: "@payload.subject"
      body: "@payload.body"
      is_html: "@payload.is_html"
      channel: "email"
      provider: "@payload.provider"
      account: "@payload.account"
      idempotency_key: "@payload.idempotency_key"

  - step_id: send
    op: egret.submit
    params:
      plan: "@run.build.items[0].plan"
```

### `email.send_templated.yaml`

```yaml
job_id: email.send_templated
version: "1.0"
steps:
  - step_id: render
    op: call
    params:
      callable: render
      template_path: "@payload.template_path"
      template_vars: "@payload.template_vars"

  - step_id: build
    op: call
    params:
      callable: egret
      method: msg.send
      to: "@payload.to"
      subject: "@run.render.items[0].subject"
      body: "@run.render.items[0].body"
      is_html: "@run.render.items[0].is_html"
      channel: "email"
      provider: "@payload.provider"
      account: "@payload.account"
      idempotency_key: "@payload.idempotency_key"

  - step_id: send
    op: egret.submit
    params:
      plan: "@run.build.items[0].plan"
```

### `email.batch_send.yaml`

This version takes items directly in the payload (no workman expand step). The workman `recipients.expand` step is deferred to a later epic.

```yaml
job_id: email.batch_send
version: "2.0"
steps:
  - step_id: render
    op: call
    params:
      callable: render
      template_path: "@payload.template_path"
      items: "@payload.items"  # [{to, template_vars, idempotency_key}, ...]

  - step_id: build
    op: call
    params:
      callable: egret
      method: msg.send
      items: "@run.render.items"  # [{to, subject, body, is_html, idempotency_key}, ...]
      channel: "email"
      provider: "@payload.provider"
      account: "@payload.account"

  - step_id: send
    op: egret.submit
    params:
      plan: "@run.build.items[0].plan"
```

## Renderer Contract (`call.render`)

Lorchestra MUST provide a callable operation `call.render` used by the email templated flows.

### Input params

- `template_path: str` — absolute path to a template file
- `template_vars: dict` — variables available to the template

### Output shape

`call.render` returns a JSON object with:

- `subject: str`
- `body: str`
- `is_html: bool`

This contract intentionally does not prescribe the template language. The implementation may be Jinja2 today and something else later.

### Batch mode

For batch sends, `call.render` MAY accept a list input and return a list output:

- Input: `{items: [{template_path, template_vars}]}`
- Output: `{items: [{subject, body, is_html}]}`

If batch mode is not implemented initially, the job definition may call `call.render` once per recipient.

## Notes

- Egret responses may return `{status: "already_sent"}` for idempotent replays; lorchestra should not require a cached original result payload.

- Egret is pure egress: it receives finalized `subject`/`body`/`is_html` and does not parse templates or know about Jinja.

- The `call` callable:egret → `egret.submit` pattern parallels `plan.build` → `storacle.submit`. Plan construction is a callable; plan execution is a native op.

- CallableResult contract: callable outputs are `{schema_version, items, stats}`. Extra fields (like `plan`) must be nested inside `items[0]`. Job definitions access them via `@run.step_id.items[0].field`.

## Deferred to Later Epic

The following items were deferred for future implementation:

- **`workman.recipients.expand`** — recipient list expansion from file. When implemented, `email.batch_send.yaml` can add an expand step before render:
  ```yaml
  - step_id: expand
    op: call
    params:
      callable: workman
      method: recipients.expand
      recipients_file: "@payload.recipients_file"
      template_vars: "@payload.template_vars"
      batch_id: "@payload.batch_id"
  ```
  Then render would use `items: "@run.expand.items"` instead of `"@payload.items"`.

- **`email.batch_send_file.yaml`** — a separate job definition that uses file-based recipient expansion via workman.

## Completion Summary

**Completed: 2026-02-10**

All acceptance criteria met:

| Criterion | Status |
|-----------|--------|
| `Op.EGRET_SUBMIT == "egret.submit"` with backend `"native"` | ✅ |
| `_dispatch_manifest()` handles `Op.EGRET_SUBMIT` | ✅ |
| `lorchestra/egret/` exports `submit_plan` and `RpcMeta` | ✅ |
| `egret` callable registered (builds EgretPlan) | ✅ |
| `render` callable registered (Jinja2 templates) | ✅ |
| `email.send.yaml` — 2 steps | ✅ |
| `email.send_templated.yaml` — 3 steps | ✅ |
| `email.batch_send.yaml` — 3 steps (items in payload) | ✅ |
| End-to-end email sending via Gmail | ✅ |
| End-to-end email sending via MSGraph | ✅ |

**Files created:**
- `lorchestra/schemas/ops.py` — added EGRET_SUBMIT
- `lorchestra/egret/__init__.py` — module exports
- `lorchestra/egret/client.py` — client boundary
- `lorchestra/callable/egret_builder.py` — plan builder
- `lorchestra/callable/render.py` — template renderer
- `lorchestra/jobs/definitions/email/email.send.yaml`
- `lorchestra/jobs/definitions/email/email.send_templated.yaml`
- `lorchestra/jobs/definitions/email/email.batch_send.yaml`

**Files modified:**
- `lorchestra/executor.py` — dispatch + handler
- `lorchestra/callable/dispatch.py` — register callables
- `lorchestra/pyproject.toml` — added jinja2, egret dependencies

**Tested providers:**
- Gmail: acct1, acct2, acct3 ✅
- MSGraph: ben-mf, ben-mm, ben-getmensio ✅
- MSGraph: ben-efs ❌ (on-premise mailbox, not REST-enabled)
