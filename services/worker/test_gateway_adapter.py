"""Tests for Gateway LLM adapter enforcing routing headers, fencing, and budget limits."""

from unittest.mock import AsyncMock

import pytest
from app.agent.gateway_llm import BudgetExceededError, GatewayChatOpenAI
from app.checkpointer import StaleLeaseFencingError
from langchain_core.messages import HumanMessage
from langchain_core.runnables import RunnableConfig
from openai.types.chat import ChatCompletion, ChatCompletionMessage
from openai.types.chat.chat_completion import Choice
from openai.types.completion_usage import CompletionUsage


class MockGatewayDB:
    """Mock database adapter tracking jobs and cost ledger records."""

    def __init__(self, job: dict | None = None):
        self.job = job or {
            "id": "job-test-1",
            "tenant_id": "tenant-corp",
            "attempt_id": 1,
            "status": "QUEUED",
            "budget_usd": 1.0,
            "spent_usd": 0.0,
        }
        self.cost_ledger_entries: list[dict] = []
        self.status_updates: list[tuple[str, str]] = []

    def get_job(self, job_id: str) -> dict | None:
        if self.job and self.job["id"] == job_id:
            return dict(self.job)
        return None

    def update_job_status(self, job_id: str, status: str) -> None:
        if self.job and self.job["id"] == job_id:
            self.job["status"] = status
        self.status_updates.append((job_id, status))

    def update_job_spent_usd(self, job_id: str, additional_spent_usd: float) -> None:
        if self.job and self.job["id"] == job_id:
            self.job["spent_usd"] = float(self.job["spent_usd"]) + additional_spent_usd

    def log_cost_ledger(
        self,
        job_id: str,
        attempt_id: int,
        step_name: str,
        model: str,
        tokens_in: int,
        tokens_out: int,
        cost_usd: float,
        latency_ms: int,
    ) -> None:
        self.cost_ledger_entries.append({
            "job_id": job_id,
            "attempt_id": attempt_id,
            "step_name": step_name,
            "model": model,
            "tokens_in": tokens_in,
            "tokens_out": tokens_out,
            "cost_usd": cost_usd,
            "latency_ms": latency_ms,
        })


def _create_mock_chat_completion(
    content: str = "Test response",
    tokens_in: int = 100,
    tokens_out: int = 50,
    model: str = "gpt-4o-mini",
) -> ChatCompletion:
    """Helper to mock OpenAI ChatCompletion response."""
    return ChatCompletion(
        id="chatcmpl-test-123",
        choices=[
            Choice(
                finish_reason="stop",
                index=0,
                message=ChatCompletionMessage(content=content, role="assistant"),
            )
        ],
        created=1234567890,
        model=model,
        object="chat.completion",
        usage=CompletionUsage(
            completion_tokens=tokens_out,
            prompt_tokens=tokens_in,
            total_tokens=tokens_in + tokens_out,
        ),
    )


@pytest.mark.asyncio
async def test_job_with_tiny_budget_halts_mid_run_with_budget_exceeded():
    """
    Done-When Requirement:
    A job with budget_usd = 0.0001 halts mid-run with status = BUDGET_EXCEEDED.
    """
    mock_db = MockGatewayDB({
        "id": "job-tiny-budget",
        "tenant_id": "tenant-1",
        "attempt_id": 1,
        "status": "RUNNING",
        "budget_usd": 0.0001,
        "spent_usd": 0.0,
    })

    llm = GatewayChatOpenAI(
        base_url="http://mock-gateway:8000/v1",
        api_key="mock-key",
        db=mock_db,
    )

    # First call consumes tokens pushing spent_usd past 0.0001
    # 1000 input tokens * 0.00000015 + 500 output tokens * 0.0000006 = 0.00015 + 0.0003 = 0.00045
    llm.async_client.create = AsyncMock(
        return_value=_create_mock_chat_completion(tokens_in=1000, tokens_out=500)
    )

    config: RunnableConfig = {
        "configurable": {
            "thread_id": "job-tiny-budget",
            "tenant_id": "tenant-1",
            "attempt_id": 1,
            "difficulty": "low",
            "step_name": "step_1",
        }
    }

    # Step 1 finishes and exceeds budget
    await llm.ainvoke([HumanMessage(content="Hello step 1")], config=config)

    # Check that spent_usd now exceeds budget and status is BUDGET_EXCEEDED
    job = mock_db.get_job("job-tiny-budget")
    assert job["spent_usd"] > job["budget_usd"]
    assert job["status"] == "BUDGET_EXCEEDED"

    # Step 2 attempts to run, must halt immediately with BudgetExceededError
    config["configurable"]["step_name"] = "step_2"
    with pytest.raises(BudgetExceededError) as exc_info:
        await llm.ainvoke([HumanMessage(content="Hello step 2")], config=config)

    assert "halted: spent_usd" in str(exc_info.value)
    assert "BUDGET_EXCEEDED" in [status for _, status in mock_db.status_updates]


@pytest.mark.asyncio
async def test_normal_run_records_per_step_costs_in_cost_ledger():
    """
    Done-When Requirement:
    Normal runs record per-step costs in cost_ledger and update jobs.spent_usd.
    """
    mock_db = MockGatewayDB({
        "id": "job-normal-run",
        "tenant_id": "tenant-enterprise",
        "attempt_id": 1,
        "status": "RUNNING",
        "budget_usd": 5.0,
        "spent_usd": 0.0,
    })

    llm = GatewayChatOpenAI(
        base_url="http://mock-gateway:8000/v1",
        api_key="mock-key",
        db=mock_db,
    )

    llm.async_client.create = AsyncMock(
        return_value=_create_mock_chat_completion(
            content="Step response",
            tokens_in=200,
            tokens_out=100,
            model="gpt-4o-mini",
        )
    )

    config: RunnableConfig = {
        "configurable": {
            "thread_id": "job-normal-run",
            "tenant_id": "tenant-enterprise",
            "attempt_id": 1,
            "difficulty": "low",
            "step_name": "parse_alert",
        }
    }

    # Execute step
    response = await llm.ainvoke([HumanMessage(content="Parse alert")], config=config)
    assert response.content == "Step response"

    # Verify cost ledger entry
    assert len(mock_db.cost_ledger_entries) == 1
    entry = mock_db.cost_ledger_entries[0]
    assert entry["job_id"] == "job-normal-run"
    assert entry["attempt_id"] == 1
    assert entry["step_name"] == "parse_alert"
    assert entry["model"] == "gpt-4o-mini"
    assert entry["tokens_in"] == 200
    assert entry["tokens_out"] == 100
    assert entry["cost_usd"] > 0
    assert entry["latency_ms"] >= 0

    # Verify jobs.spent_usd incremented
    job = mock_db.get_job("job-normal-run")
    assert job["spent_usd"] == pytest.approx(entry["cost_usd"])


@pytest.mark.asyncio
async def test_gateway_headers_injected_per_call():
    """
    Requirement:
    Inject headers per call: X-Switchyard-Job-Id, X-Switchyard-Tenant-Id,
    X-Switchyard-Attempt-Id, and X-Task-Difficulty (low | high).
    """
    mock_db = MockGatewayDB({
        "id": "job-headers-check",
        "tenant_id": "tenant-xyz",
        "attempt_id": 2,
        "status": "RUNNING",
        "budget_usd": 10.0,
        "spent_usd": 0.0,
    })

    llm = GatewayChatOpenAI(
        base_url="http://mock-gateway:8000/v1",
        api_key="mock-key",
        db=mock_db,
    )

    mock_create = AsyncMock(return_value=_create_mock_chat_completion())
    llm.async_client.create = mock_create

    config: RunnableConfig = {
        "configurable": {
            "thread_id": "job-headers-check",
            "tenant_id": "tenant-xyz",
            "attempt_id": 2,
            "difficulty": "high",
            "step_name": "diagnose_incident",
        }
    }

    await llm.ainvoke([HumanMessage(content="Diagnose problem")], config=config)

    headers = mock_create.call_args.kwargs.get("extra_headers", {})
    assert headers.get("X-Switchyard-Job-Id") == "job-headers-check"
    assert headers.get("X-Switchyard-Tenant-Id") == "tenant-xyz"
    assert headers.get("X-Switchyard-Attempt-Id") == "2"
    assert headers.get("X-Task-Difficulty") == "high"


@pytest.mark.asyncio
async def test_zombie_worker_attempt_rejected_by_gateway():
    """
    Requirement:
    Verify active attempt_id before execution. Stale attempt must be aborted.
    """
    mock_db = MockGatewayDB({
        "id": "job-stale-attempt",
        "tenant_id": "tenant-1",
        "attempt_id": 2,  # Active DB attempt is 2
        "status": "RUNNING",
        "budget_usd": 10.0,
        "spent_usd": 0.0,
    })

    llm = GatewayChatOpenAI(
        base_url="http://mock-gateway:8000/v1",
        api_key="mock-key",
        db=mock_db,
    )

    mock_create = AsyncMock(return_value=_create_mock_chat_completion())
    llm.async_client.create = mock_create

    # Worker 1 (attempt 1) attempts to call LLM gateway
    config: RunnableConfig = {
        "configurable": {
            "thread_id": "job-stale-attempt",
            "tenant_id": "tenant-1",
            "attempt_id": 1,  # Stale fencing token!
            "difficulty": "low",
        }
    }

    with pytest.raises(StaleLeaseFencingError) as exc_info:
        await llm.ainvoke([HumanMessage(content="Hello from zombie worker")], config=config)

    assert "worker attempt 1 is older than active DB attempt 2" in str(exc_info.value)
    # Ensure no network call was made to the gateway
    assert mock_create.call_count == 0
    # Ensure no cost ledger entry was written
    assert len(mock_db.cost_ledger_entries) == 0
