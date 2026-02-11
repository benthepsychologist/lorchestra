"""
render callable — Render email templates via Jinja2.

This callable renders email templates and returns subject, body, and
is_html for use in email sending flows.

In job definitions, this is invoked as:
  op: call
  params:
    callable: render
    template: "@payload.template"
    template_vars: "@payload.template_vars"

Template format:
  The template should render to output where the first line is
  "Subject: <subject text>" followed by the body content.

  Example template:
    Subject: Welcome, {{ name }}!

    Hello {{ name }},
    Welcome to our service.

For batch mode, pass items=[{to, template_vars, idempotency_key}] and
receive items=[{to, subject, body, is_html, idempotency_key}].
"""

from typing import Any


def execute(params: dict[str, Any]) -> dict[str, Any]:
    """
    Render email template(s) via Jinja2.

    Single mode params:
        template: str — Jinja2 template content
        template_vars: dict — variables available to the template

    Batch mode params:
        template: str — Jinja2 template content
        items: list[{to, template_vars, idempotency_key}]

    Returns:
        Single mode: {schema_version, items, stats}
        Batch mode: {schema_version, items: [{to, subject, body, is_html, idempotency_key}], stats}
    """
    from jinja2 import Environment, select_autoescape

    template_content = params["template"]

    env = Environment(autoescape=select_autoescape(["html", "htm"]))
    template = env.from_string(template_content)

    items = params.get("items")
    if items:
        # Batch mode
        results = []
        for item in items:
            rendered = _render_single(template, item["template_vars"])
            results.append({
                "to": item["to"],
                "subject": rendered["subject"],
                "body": rendered["body"],
                "is_html": rendered["is_html"],
                "idempotency_key": item.get("idempotency_key", ""),
            })
        return {
            "schema_version": "1.0",
            "items": results,
            "stats": {"count": len(results)},
        }
    else:
        # Single mode - access via @run.render.items[0].subject etc.
        rendered = _render_single(template, params["template_vars"])
        return {
            "schema_version": "1.0",
            "items": [rendered],
            "stats": {},
        }


def _render_single(template: Any, template_vars: dict[str, Any]) -> dict[str, Any]:
    """
    Render a single template with the given variables.

    Args:
        template: Jinja2 template object
        template_vars: Variables to pass to the template

    Returns:
        Dict with subject, body, is_html
    """
    content = template.render(**template_vars)
    subject, body, is_html = _parse_template_output(content)
    return {"subject": subject, "body": body, "is_html": is_html}


def _parse_template_output(content: str) -> tuple[str, str, bool]:
    """
    Parse rendered template for subject/body.

    Template format: First line is "Subject: ..." followed by body.

    Args:
        content: Rendered template content

    Returns:
        Tuple of (subject, body, is_html)
    """
    lines = content.strip().split("\n", 1)
    if lines[0].startswith("Subject:"):
        subject = lines[0][8:].strip()
        body = lines[1].strip() if len(lines) > 1 else ""
    else:
        subject = ""
        body = content.strip()

    is_html = "<html" in body.lower() or "<body" in body.lower()
    return subject, body, is_html
