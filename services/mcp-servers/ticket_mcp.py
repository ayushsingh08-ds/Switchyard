"""Ticket MCP Server with Redis idempotency for deterministic incident creation."""

import os
import uuid
from typing import Any

import redis

# Global in-memory storage for tickets (simulated issue tracker)
TICKET_STORE: dict[str, dict[str, Any]] = {}


def get_redis_client() -> redis.Redis | None:
    """Return configured Redis client or None if Redis is unreachable."""
    try:
        url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
        client = redis.Redis.from_url(url, decode_responses=True, socket_connect_timeout=0.2, socket_timeout=0.5)
        client.ping()
        return client
    except Exception:
        return None


def create_incident_ticket(
    title: str,
    severity: str,
    summary: str,
    idempotency_key: str,
    redis_client: Any = None,
) -> dict[str, Any]:
    """
    Create an incident ticket with Redis idempotency.
    Enforces Redis SET idemp:{idempotency_key} <ticket_id> NX EX 86400.
    If the key exists, returns the existing ticket_id without creating a duplicate.
    """
    r = redis_client if redis_client is not None else get_redis_client()

    redis_key = f"idemp:{idempotency_key}"

    # 1. If Redis is available, check atomic idempotency lock
    if r is not None:
        existing_ticket_id = r.get(redis_key)
        if existing_ticket_id:
            # Ticket was already created; return existing record
            existing_ticket = TICKET_STORE.get(existing_ticket_id)
            if existing_ticket:
                return {**existing_ticket, "duplicate": True}
            return {
                "ticket_id": existing_ticket_id,
                "title": title,
                "severity": severity,
                "summary": summary,
                "duplicate": True,
            }

        # Generate unique ticket ID and claim lock
        new_ticket_id = f"INC-{uuid.uuid4().hex[:8].upper()}"
        acquired = r.set(redis_key, new_ticket_id, nx=True, ex=86400)
        if not acquired:
            # Race condition lost: retrieve the winning ticket ID
            winner_id = r.get(redis_key) or new_ticket_id
            return {
                "ticket_id": winner_id,
                "title": title,
                "severity": severity,
                "summary": summary,
                "duplicate": True,
            }

        # Successfully acquired: store ticket
        ticket_record = {
            "ticket_id": new_ticket_id,
            "title": title,
            "severity": severity,
            "summary": summary,
            "duplicate": False,
        }
        TICKET_STORE[new_ticket_id] = ticket_record
        return ticket_record

    # 2. Fallback in-memory idempotency check (if Redis is not running)
    for tid, t in TICKET_STORE.items():
        if t.get("idempotency_key") == idempotency_key:
            return {**t, "duplicate": True}

    new_ticket_id = f"INC-{uuid.uuid4().hex[:8].upper()}"
    ticket_record = {
        "ticket_id": new_ticket_id,
        "title": title,
        "severity": severity,
        "summary": summary,
        "idempotency_key": idempotency_key,
        "duplicate": False,
    }
    TICKET_STORE[new_ticket_id] = ticket_record
    return ticket_record
