"""Integration test for UI Live Dashboard API endpoints (SSE events & kill-worker)."""

import json

import pytest
from app.api import _in_memory_jobs, _in_memory_ledger, _in_memory_steps, app
from fastapi.testclient import TestClient


@pytest.fixture
def client():
    return TestClient(app)


def test_kill_worker_endpoint(client):
    """Test POST /api/v1/demo/kill-worker triggers simulation."""
    res = client.post(
        "/api/v1/demo/kill-worker",
        json={"job_id": "job-test-123", "attempt_id": 1},
    )
    assert res.status_code == 200
    data = res.json()
    assert data["status"] == "killed"
    assert data["attempt_id"] == 1
    assert "job-test-123" in data["detail"]


def test_job_events_sse_stream(client):
    """Test GET /api/v1/jobs/{id}/events returns SSE content type and snapshot event."""
    job_id = "job-sse-456"
    _in_memory_jobs[job_id] = {
        "id": job_id,
        "tenant_id": "tenant-test",
        "status": "leased",
        "attempt_id": 1,
        "budget_usd": 1.0,
        "spent_usd": 0.001,
        "payload": {"alert": "cpu_spike"},
    }
    _in_memory_steps[job_id] = [
        {
            "id": "step-1",
            "job_id": job_id,
            "attempt_id": 1,
            "step_name": "parse_alert",
            "model": "gpt-4o-mini",
            "tokens_in": 150,
            "tokens_out": 50,
            "cost_usd": 0.00005,
            "latency_ms": 120,
        }
    ]
    _in_memory_ledger[job_id] = [
        {
            "id": "ledger-1",
            "job_id": job_id,
            "attempt_id": 1,
            "step_name": "parse_alert",
            "model": "gpt-4o-mini",
            "tokens_in": 150,
            "tokens_out": 50,
            "cost_usd": 0.00005,
            "latency_ms": 120,
            "cached": True,
        }
    ]

    with client.stream("GET", f"/api/v1/jobs/{job_id}/events") as response:
        assert response.status_code == 200
        assert "text/event-stream" in response.headers["content-type"]

        # Read the first chunk which contains the snapshot
        lines = []
        for line in response.iter_lines():
            if line:
                lines.append(line)
            if len(lines) >= 1:
                break

        assert len(lines) > 0
        first_line = lines[0]
        assert first_line.startswith("data: ")
        event_payload = json.loads(first_line[len("data: "):])
        assert event_payload["type"] == "snapshot"
        assert event_payload["job"]["id"] == job_id
        assert len(event_payload["job"]["steps"]) == 1
        assert event_payload["job"]["steps"][0]["step_name"] == "parse_alert"
