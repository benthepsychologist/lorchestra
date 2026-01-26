"""
CallableResult - Normalized return type from all callables.

All callables (injest, canonizer, finalform, projectionist, workman)
return this normalized payload. This ensures consistent handling
in the executor and plan builder.

Schema (from epic):
{
  "schema_version": "1.0",
  "items": [ { }, { } ],       // XOR
  "items_ref": "artifact://...",  // XOR
  "stats": { }
}

Rule: Exactly one of `items` or `items_ref` must be set.
"""

from dataclasses import dataclass, field


@dataclass
class CallableResult:
    """
    Normalized return from all callables.

    All callables must return this structure. The executor converts
    CallableResult to StoraclePlan for submission to storacle.

    Attributes:
        schema_version: Schema version for forward compatibility
        items: Inline items (mutually exclusive with items_ref)
        items_ref: Reference to items artifact (mutually exclusive with items)
        stats: Optional statistics from the callable execution
    """
    schema_version: str = "1.0"
    items: list[dict] | None = None
    items_ref: str | None = None
    stats: dict = field(default_factory=dict)

    def __post_init__(self):
        """Validate XOR constraint: exactly one of items or items_ref."""
        has_items = self.items is not None
        has_ref = self.items_ref is not None
        if has_items == has_ref:
            raise ValueError(
                "CallableResult must have exactly one of items or items_ref"
            )
