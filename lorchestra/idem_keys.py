"""
Idempotency key generation functions for different source systems.

This module provides a centralized registry of idem_key functions to avoid drift
and ensure consistent identity semantics across the codebase.

Each function returns a callable that computes an idem_key from an object payload.
The idem_key format is: "{object_type}:{source_system}:{natural_id}"

Usage:
    from lorchestra.idem_keys import gmail_idem_key
    from lorchestra.stack_clients.event_client import upsert_objects

    # Create idem_key function for a specific source_system
    idem_fn = gmail_idem_key("tap-gmail--acct1")

    # Use it to batch upsert emails
    upsert_objects(
        objects=email_iterator,
        source_system="tap-gmail--acct1",
        object_type="email",
        correlation_id="gmail-20251124120000",
        idem_key_fn=idem_fn,
        bq_client=client
    )
"""

from typing import Dict, Any, Callable


def gmail_idem_key(source_system: str) -> Callable[[Dict[str, Any]], str]:
    """
    Gmail idem_key function - uses 'id' field for messageId.

    Args:
        source_system: Source system identifier (e.g., "tap-gmail--acct1")

    Returns:
        Callable that computes idem_key from Gmail message payload

    Example:
        >>> fn = gmail_idem_key("tap-gmail--acct1")
        >>> fn({"id": "msg123", "subject": "Test"})
        'email:tap-gmail--acct1:msg123'
    """
    def compute_idem_key(obj: Dict[str, Any]) -> str:
        message_id = obj.get('id')
        if not message_id:
            raise ValueError(f"Gmail message missing 'id' field: {obj}")
        return f"email:{source_system}:{message_id}"

    return compute_idem_key


def stripe_charge_idem_key(source_system: str) -> Callable[[Dict[str, Any]], str]:
    """
    Stripe charge idem_key function - uses 'id' field.

    Args:
        source_system: Source system identifier (e.g., "tap-stripe--prod")

    Returns:
        Callable that computes idem_key from Stripe charge payload

    Example:
        >>> fn = stripe_charge_idem_key("tap-stripe--prod")
        >>> fn({"id": "ch_123", "amount": 1000})
        'charge:tap-stripe--prod:ch_123'
    """
    def compute_idem_key(obj: Dict[str, Any]) -> str:
        charge_id = obj.get('id')
        if not charge_id:
            raise ValueError(f"Stripe charge missing 'id' field: {obj}")
        return f"charge:{source_system}:{charge_id}"

    return compute_idem_key


# Add more idem_key functions as needed for other taps:
#
# def typeform_response_idem_key(source_system: str):
#     """Typeform uses 'response_id' field."""
#     def compute_idem_key(obj: Dict[str, Any]) -> str:
#         response_id = obj.get('response_id')
#         if not response_id:
#             raise ValueError(f"Typeform response missing 'response_id' field: {obj}")
#         return f"response:{source_system}:{response_id}"
#     return compute_idem_key
#
# def hubspot_contact_idem_key(source_system: str):
#     """HubSpot uses 'vid' (visitor ID) field."""
#     def compute_idem_key(obj: Dict[str, Any]) -> str:
#         vid = obj.get('vid')
#         if not vid:
#             raise ValueError(f"HubSpot contact missing 'vid' field: {obj}")
#         return f"contact:{source_system}:{vid}"
#     return compute_idem_key
