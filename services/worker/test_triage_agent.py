"""Unit and integration tests for On-Call Alert-Triage LangGraph workflow."""

import json
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

BASE_DIR = Path(__file__).resolve().parent.parent.parent
WORKER_DIR = BASE_DIR / "services" / "worker"
MCP_DIR = BASE_DIR / "services" / "mcp-servers"

for p in [str(WORKER_DIR), str(MCP_DIR)]:
    if p not in sys.path:
        sys.path.append(p)

from app.agent.gateway_llm import GatewayChatOpenAI  # noqa: E402
from app.agent.graph import build_triage_graph  # noqa: E402
from test_gateway_adapter import (  # noqa: E402
    MockGatewayDB,
    _create_mock_chat_completion,
)


@pytest.mark.asyncio
async def test_triage_agent_runs_all_4_steps_end_to_end():
    """
    Done-When Verification:
    Mock Prometheus alert runs all 4 steps end-to-end and creates incident ticket.
    """
    mock_db = MockGatewayDB({
        "id": "job-triage-test-1",
        "tenant_id": "tenant-ops",
        "attempt_id": 1,
        "status": "RUNNING",
        "budget_usd": 10.0,
        "spent_usd": 0.0,
    })

    llm = GatewayChatOpenAI(
        base_url="http://mock-gateway:8000/v1",
        api_key="mock-key",
        db=mock_db,
    )

    completion = _create_mock_chat_completion(
        content='{"service_name": "worker-ingestion", "severity": "P1", "summary": "Alert"}',
        tokens_in=300,
        tokens_out=150,
        model="gpt-4o-mini",
    )
    raw_mock = MagicMock()
    raw_mock.parse.return_value = completion
    raw_mock.headers = {}
    llm.async_client.create = AsyncMock(return_value=completion)
    llm.async_client.with_raw_response.create = AsyncMock(return_value=raw_mock)

    graph = build_triage_graph(llm=llm)

    mock_alert = {
        "service": "worker-ingestion",
        "severity": "P1",
        "alertname": "HostHighMemoryUsage",
        "error_rate": 0.45,
    }

    config = {
        "configurable": {
            "job_id": "job-triage-test-1",
            "thread_id": "job-triage-test-1",
            "tenant_id": "tenant-ops",
            "attempt_id": 1,
        }
    }

    final_state = await graph.ainvoke({"alert_raw": mock_alert}, config=config)

    # 1. Assert all 4 steps executed in sequence
    assert final_state["completed_steps"] == [
        "parse_alert",
        "fetch_context",
        "diagnose_root_cause",
        "open_ticket",
    ]

    # 2. Assert step 1 parsed metadata
    assert final_state["service_name"] == "worker-ingestion"
    assert final_state["severity"] == "P1"

    # 3. Assert step 2 fetched telemetry and runbooks
    assert len(final_state["telemetry_metrics"]) > 0
    assert len(final_state["runbook_chunks"]) > 0

    # 4. Assert step 3 generated diagnosis
    assert len(final_state["diagnosis"]) > 0

    # 5. Assert step 4 opened incident ticket
    assert final_state["ticket_id"].startswith("INC-")
    assert final_state["duplicate_ticket"] is False


def test_cost_routing_benchmark_artifact_exists_and_valid():
    """Verify cost_routing.json artifact exists with positive cost savings."""
    artifact_path = BASE_DIR / "benchmarks" / "results" / "cost_routing.json"
    assert artifact_path.exists(), f"Artifact missing at {artifact_path}"

    with open(artifact_path, encoding="utf-8") as f:
        data = json.load(f)

    assert data["status"] == "PASSED"
    assert data["sample_size"] == 20
    assert data["cost_savings_usd"] > 0
    assert data["cost_savings_percentage"] > 40.0
