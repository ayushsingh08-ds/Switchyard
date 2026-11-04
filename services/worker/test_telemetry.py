"""Unit and integration tests for OpenTelemetry and Prometheus distributed tracing."""


import pytest
from app.agent.gateway_llm import GatewayChatOpenAI
from app.api import app
from app.telemetry import (
    COST_USD_TOTAL,
    TOKENS_TOTAL,
    extract_trace_id,
    format_traceparent,
)
from fastapi.testclient import TestClient
from langchain_core.messages import HumanMessage
from test_gateway_adapter import (
    MockGatewayDB,
    _create_mock_chat_completion,
    _setup_mock_openai_client,
)

client = TestClient(app)


def test_w3c_traceparent_format_and_extraction():
    """Verify standard W3C traceparent parsing and construction."""
    trace_id = "4bf92f3577b34da6a3ce929d0e0e4736"
    tp = format_traceparent(trace_id, span_id="00f067aa0ba902b7")
    assert tp == f"00-{trace_id}-00f067aa0ba902b7-01"

    extracted = extract_trace_id(tp)
    assert extracted == trace_id

    # Test empty or malformed traceparents
    assert extract_trace_id("") == ""
    assert extract_trace_id("invalid-format") == ""


def test_fastapi_traceparent_propagation_and_metrics_endpoint():
    """
    Done-When Verification:
    FastAPI receives W3C traceparent, propagates it, and /metrics exposes Prometheus counters.
    """
    test_trace_id = "e45770ce35036bf4502bda1c43a8100e"
    custom_traceparent = f"00-{test_trace_id}-0000000000000002-01"

    # POST to FastAPI with traceparent header
    resp = client.post(
        "/api/v1/jobs",
        json={
            "tenant_id": "tenant-otel",
            "payload": {"task": "trace_test"},
            "budget_usd": 2.0,
        },
        headers={"traceparent": custom_traceparent},
    )
    assert resp.status_code == 201
    data = resp.json()
    assert data["traceparent"] == custom_traceparent

    # Scrape /metrics endpoint
    metrics_resp = client.get("/metrics")
    assert metrics_resp.status_code == 200
    metrics_text = metrics_resp.text
    assert "switchyard_tokens_total" in metrics_text
    assert "switchyard_cost_usd_total" in metrics_text


@pytest.mark.asyncio
async def test_traceparent_propagated_to_llm_gateway_headers():
    """Verify traceparent is injected into LLM Gateway HTTP headers."""
    mock_db = MockGatewayDB({
        "id": "job-otel-gateway",
        "tenant_id": "tenant-otel",
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

    completion = _create_mock_chat_completion(tokens_in=250, tokens_out=75, model="gpt-4o-mini")
    mock_raw_create = _setup_mock_openai_client(llm, completion)

    test_traceparent = "00-1234567890abcdef1234567890abcdef-0000000000000003-01"
    config = {
        "configurable": {
            "job_id": "job-otel-gateway",
            "thread_id": "job-otel-gateway",
            "tenant_id": "tenant-otel",
            "attempt_id": 1,
            "traceparent": test_traceparent,
        }
    }

    # Record initial metric counter values
    tokens_before = TOKENS_TOTAL.labels(model="gpt-4o-mini", type="input")._value.get()
    cost_before = COST_USD_TOTAL.labels(model="gpt-4o-mini")._value.get()

    await llm.ainvoke([HumanMessage(content="Test LLM trace")], config=config)

    # Check header delivered to Gateway
    called_mock = mock_raw_create if mock_raw_create.call_count > 0 else llm.async_client.create
    headers = called_mock.call_args.kwargs.get("extra_headers", {})
    assert headers.get("traceparent") == test_traceparent
    assert headers.get("X-Switchyard-Job-Id") == "job-otel-gateway"

    # Verify Prometheus counters were incremented
    tokens_after = TOKENS_TOTAL.labels(model="gpt-4o-mini", type="input")._value.get()
    cost_after = COST_USD_TOTAL.labels(model="gpt-4o-mini")._value.get()
    assert tokens_after == tokens_before + 250
    assert cost_after > cost_before
