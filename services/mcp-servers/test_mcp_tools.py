"""Unit and idempotency tests for Prometheus, Runbook RAG, and Ticket MCP servers."""

import hashlib
from unittest.mock import MagicMock

from prometheus_mcp import query_metrics
from runbook_rag_mcp import SAMPLE_RUNBOOKS, search_runbooks, seed_runbooks
from ticket_mcp import TICKET_STORE, create_incident_ticket


def test_prometheus_mcp_query_metrics():
    """Verify Prometheus MCP server returns structured metrics for service."""
    result = query_metrics("worker-ingestion", window_minutes=5)
    assert result["service"] == "worker-ingestion"
    assert result["window_minutes"] == 5
    assert "metrics" in result
    assert len(result["metrics"]) > 0


def test_runbook_rag_mcp_search():
    """Verify Runbook RAG retrieves relevant runbooks using cosine similarity."""
    results = search_runbooks("High memory and OOM crash", top_k=2)
    assert len(results) == 2
    top = results[0]
    assert "Memory" in top["title"] or "OOM" in top["title"]
    assert "distance" in top


def test_runbook_rag_mcp_seed():
    """Verify runbook seeding function returns expected chunk count."""
    count = seed_runbooks()
    assert count == len(SAMPLE_RUNBOOKS)


def test_ticket_mcp_redis_idempotency():
    """
    Done-When Verification:
    Calling create_incident_ticket twice with hash(job_id + step_name) creates only 1 ticket record.
    """
    mock_redis = MagicMock()
    redis_storage = {}

    def mock_get(key):
        return redis_storage.get(key)

    def mock_set(key, value, nx=False, ex=None):
        if nx and key in redis_storage:
            return False
        redis_storage[key] = value
        return True

    mock_redis.get.side_effect = mock_get
    mock_redis.set.side_effect = mock_set

    # Compute idempotency key: hash(job_id + step_name)
    job_id = "job-9988-uuid"
    step_name = "open_ticket"
    idempotency_key = hashlib.sha256(f"{job_id}:{step_name}".encode()).hexdigest()

    # Clear ticket store for clean test
    TICKET_STORE.clear()

    # Call 1: Creates the initial incident ticket
    ticket_1 = create_incident_ticket(
        title="High Memory Usage in Worker Ingestion",
        severity="P1",
        summary="Memory usage spiked over 90% causing OOM restarts.",
        idempotency_key=idempotency_key,
        redis_client=mock_redis,
    )
    assert ticket_1["duplicate"] is False
    assert ticket_1["ticket_id"].startswith("INC-")

    # Call 2: Second invocation with the same idempotency key
    ticket_2 = create_incident_ticket(
        title="High Memory Usage in Worker Ingestion",
        severity="P1",
        summary="Memory usage spiked over 90% causing OOM restarts.",
        idempotency_key=idempotency_key,
        redis_client=mock_redis,
    )
    assert ticket_2["duplicate"] is True
    assert ticket_2["ticket_id"] == ticket_1["ticket_id"]

    # Assert exactly 1 ticket was recorded
    assert len(TICKET_STORE) == 1
