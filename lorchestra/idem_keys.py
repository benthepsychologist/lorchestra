"""
Idempotency key generation functions for different source systems.

This module provides a centralized registry of idem_key functions to avoid drift
and ensure consistent identity semantics across the codebase.

Each function returns a callable that computes an idem_key from an object payload.

idem_key Pattern:
    {source_system}:{connection_name}:{object_type}:{external_id}

Examples:
    - gmail:gmail-acct1:email:18c5a7b2e3f4d5c6
    - exchange:exchange-ben-mensio:email:AAMkAGQ...
    - dataverse:dataverse-clinic:session:739330df-5757-f011-bec2-6045bd619595
    - google_forms:google-forms-intake-01:form_response:ACYDBNi84NuJUO2O13A

Rule: idem_key is opaque - never parsed. All filtering uses explicit columns.

Usage:
    from lorchestra.idem_keys import gmail_idem_key
    from lorchestra.stack_clients.event_client import upsert_objects

    # Create idem_key function with source_system and connection_name
    idem_fn = gmail_idem_key("gmail", "gmail-acct1")

    # Use it to batch upsert emails
    result = upsert_objects(
        objects=email_iterator,
        source_system="gmail",
        connection_name="gmail-acct1",
        object_type="email",
        correlation_id="gmail-20251124120000",
        idem_key_fn=idem_fn,
        bq_client=client
    )
"""

from typing import Dict, Any, Callable


def gmail_idem_key(source_system: str, connection_name: str) -> Callable[[Dict[str, Any]], str]:
    """
    Gmail idem_key function - uses 'id' field for messageId.

    Args:
        source_system: Provider family (always "gmail")
        connection_name: Account identifier (e.g., "gmail-acct1")

    Returns:
        Callable that computes idem_key from Gmail message payload

    Example:
        >>> fn = gmail_idem_key("gmail", "gmail-acct1")
        >>> fn({"id": "18c5a7b2e3f4d5c6", "subject": "Test"})
        'gmail:gmail-acct1:email:18c5a7b2e3f4d5c6'
    """
    def compute_idem_key(obj: Dict[str, Any]) -> str:
        message_id = obj.get('id')
        if not message_id:
            raise ValueError(f"Gmail message missing 'id' field: {obj}")
        return f"{source_system}:{connection_name}:email:{message_id}"

    return compute_idem_key


def exchange_idem_key(source_system: str, connection_name: str) -> Callable[[Dict[str, Any]], str]:
    """
    Microsoft Exchange/Graph Mail idem_key function - uses 'id' field.

    Exchange/M365 emails use the Graph API message ID as the natural key.

    Args:
        source_system: Provider family (always "exchange")
        connection_name: Account identifier (e.g., "exchange-ben-mensio")

    Returns:
        Callable that computes idem_key from Exchange/M365 message payload

    Example:
        >>> fn = exchange_idem_key("exchange", "exchange-ben-mensio")
        >>> fn({"id": "AAMkAGI...", "subject": "Test"})
        'exchange:exchange-ben-mensio:email:AAMkAGI...'
    """
    def compute_idem_key(obj: Dict[str, Any]) -> str:
        message_id = obj.get('id')
        if not message_id:
            raise ValueError(f"Exchange message missing 'id' field: {obj}")
        return f"{source_system}:{connection_name}:email:{message_id}"

    return compute_idem_key


def dataverse_idem_key(source_system: str, connection_name: str, object_type: str, id_field: str) -> Callable[[Dict[str, Any]], str]:
    """
    Dataverse idem_key function - uses specified ID field.

    Dataverse entities have different ID fields depending on the entity type.

    Args:
        source_system: Provider family (always "dataverse")
        connection_name: Account identifier (e.g., "dataverse-clinic")
        object_type: Domain object type (e.g., "contact", "session", "report")
        id_field: Field name containing the entity ID (e.g., "contactid", "cre92_clientsessionid")

    Returns:
        Callable that computes idem_key from Dataverse entity payload

    Example:
        >>> fn = dataverse_idem_key("dataverse", "dataverse-clinic", "session", "cre92_clientsessionid")
        >>> fn({"cre92_clientsessionid": "739330df-5757-f011-bec2-6045bd619595"})
        'dataverse:dataverse-clinic:session:739330df-5757-f011-bec2-6045bd619595'
    """
    def compute_idem_key(obj: Dict[str, Any]) -> str:
        entity_id = obj.get(id_field)
        if not entity_id:
            raise ValueError(f"Dataverse {object_type} missing '{id_field}' field: {obj}")
        return f"{source_system}:{connection_name}:{object_type}:{entity_id}"

    return compute_idem_key


def google_forms_idem_key(source_system: str, connection_name: str) -> Callable[[Dict[str, Any]], str]:
    """
    Google Forms idem_key function - uses 'responseId' field.

    Args:
        source_system: Provider family (always "google_forms")
        connection_name: Account identifier (e.g., "google-forms-intake-01")

    Returns:
        Callable that computes idem_key from Google Forms response payload

    Example:
        >>> fn = google_forms_idem_key("google_forms", "google-forms-intake-01")
        >>> fn({"responseId": "ACYDBNi84NuJUO2O13A", "createTime": "..."})
        'google_forms:google-forms-intake-01:form_response:ACYDBNi84NuJUO2O13A'
    """
    def compute_idem_key(obj: Dict[str, Any]) -> str:
        response_id = obj.get('responseId')
        if not response_id:
            raise ValueError(f"Google Forms response missing 'responseId' field: {obj}")
        return f"{source_system}:{connection_name}:form_response:{response_id}"

    return compute_idem_key


# Legacy aliases for backwards compatibility during migration
# TODO: Remove after all jobs are migrated to new signature

def msgraph_idem_key(source_system: str, connection_name: str) -> Callable[[Dict[str, Any]], str]:
    """Legacy alias for exchange_idem_key. Use exchange_idem_key for new code."""
    return exchange_idem_key(source_system, connection_name)


def stripe_charge_idem_key(source_system: str, connection_name: str) -> Callable[[Dict[str, Any]], str]:
    """
    Stripe charge idem_key function - uses 'id' field.

    Args:
        source_system: Provider family (always "stripe")
        connection_name: Account identifier (e.g., "stripe-prod")

    Returns:
        Callable that computes idem_key from Stripe charge payload

    Example:
        >>> fn = stripe_charge_idem_key("stripe", "stripe-prod")
        >>> fn({"id": "ch_123", "amount": 1000})
        'stripe:stripe-prod:charge:ch_123'
    """
    def compute_idem_key(obj: Dict[str, Any]) -> str:
        charge_id = obj.get('id')
        if not charge_id:
            raise ValueError(f"Stripe charge missing 'id' field: {obj}")
        return f"{source_system}:{connection_name}:charge:{charge_id}"

    return compute_idem_key
