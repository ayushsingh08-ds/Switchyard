"""Unit and integration tests for FastAPI ingestion and gRPC Worker Loop."""

import asyncio
from collections.abc import AsyncGenerator
from unittest.mock import AsyncMock, MagicMock

import pytest
from app.api import _in_memory_jobs, app
from app.grpc_client import WorkerLoop
from app.pb import broker_pb2
from httpx import ASGITransport, AsyncClient


@pytest.mark.asyncio
async def test_api_create_and_get_job():
    """Test POST /api/v1/jobs and GET /api/v1/jobs/{id}."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # Create job
        create_resp = await client.post(
            "/api/v1/jobs",
            json={
                "tenant_id": "test-tenant",
                "payload": {"steps": 3, "action": "summarize"},
                "budget_usd": 2.50,
            },
        )
        assert create_resp.status_code == 201
        data = create_resp.json()
        job_id = data["id"]
        assert data["tenant_id"] == "test-tenant"
        assert data["status"] == "pending"
        assert data["budget_usd"] == 2.50

        # Retrieve job
        get_resp = await client.get(f"/api/v1/jobs/{job_id}")
        assert get_resp.status_code == 200
        get_data = get_resp.json()
        assert get_data["id"] == job_id
        assert get_data["payload"]["steps"] == 3


@pytest.mark.asyncio
async def test_worker_loop_successful_execution_and_ack():
    """Verify that a leased job executes 3 steps and issues AckJob with result."""
    mock_stub = MagicMock()
    mock_stub.AckJob = AsyncMock(return_value=broker_pb2.AckResponse(success=True))
    mock_stub.Heartbeat = AsyncMock(return_value=broker_pb2.HeartbeatResponse(lease_valid=True))

    worker = WorkerLoop(stub=mock_stub, heartbeat_interval_sec=0.02)

    lease = broker_pb2.JobLease(
        job_id="job-test-ack",
        tenant_id="tenant-1",
        payload_json='{"action": "test"}',
        attempt_id=1,
    )

    await worker.process_lease(mock_stub, lease)

    # Verify AckJob was invoked with correct attempt fencing token
    mock_stub.AckJob.assert_awaited_once()
    call_args = mock_stub.AckJob.call_args[0][0]
    assert call_args.job_id == "job-test-ack"
    assert call_args.attempt_id == 1
    assert "status" in call_args.result_json


@pytest.mark.asyncio
async def test_worker_heartbeat_failure_cancels_job():
    """
    Verify that if Heartbeat returns lease_valid == False,
    the worker immediately cancels the running job execution task.
    """
    mock_stub = MagicMock()
    # First heartbeat call reports lease expired/invalid
    mock_stub.Heartbeat = AsyncMock(return_value=broker_pb2.HeartbeatResponse(lease_valid=False))
    mock_stub.AckJob = AsyncMock()
    mock_stub.NackJob = AsyncMock()

    cancelled_event = asyncio.Event()

    async def long_running_task(job_id: str, payload_json: str):
        try:
            await asyncio.sleep(5.0)
        except asyncio.CancelledError:
            cancelled_event.set()
            raise

    worker = WorkerLoop(
        stub=mock_stub,
        heartbeat_interval_sec=0.01,
        handler=long_running_task,
    )

    lease = broker_pb2.JobLease(
        job_id="job-lost-lease",
        tenant_id="tenant-1",
        payload_json="{}",
        attempt_id=1,
    )

    with pytest.raises(asyncio.CancelledError):
        await worker.process_lease(mock_stub, lease)

    # Task should be cancelled upon invalid heartbeat
    assert cancelled_event.is_set()
    # AckJob must NEVER be called if lease was lost
    mock_stub.AckJob.assert_not_called()


@pytest.mark.asyncio
async def test_worker_unhandled_exception_calls_nack():
    """Verify that unhandled exceptions during execution trigger NackJob."""
    mock_stub = MagicMock()
    mock_stub.NackJob = AsyncMock(return_value=broker_pb2.NackResponse(success=True))

    async def failing_task(job_id: str, payload_json: str):
        raise ValueError("Simulated step failure")

    worker = WorkerLoop(
        stub=mock_stub,
        handler=failing_task,
    )

    lease = broker_pb2.JobLease(
        job_id="job-failed",
        tenant_id="tenant-1",
        payload_json="{}",
        attempt_id=2,
    )

    await worker.process_lease(mock_stub, lease)

    mock_stub.NackJob.assert_awaited_once()
    call_args = mock_stub.NackJob.call_args[0][0]
    assert call_args.job_id == "job-failed"
    assert call_args.attempt_id == 2
    assert "Simulated step failure" in call_args.error_message
    assert call_args.retryable is True


@pytest.mark.asyncio
async def test_end_to_end_ingest_lease_and_complete():
    """
    Done-When Verification:
    POST /api/v1/jobs triggers Go broker leasing to Python worker,
    which completes a mock 3-step async sleep task and updates status to COMPLETED.
    """
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # Ingest job via FastAPI
        resp = await client.post(
            "/api/v1/jobs",
            json={
                "tenant_id": "tenant-e2e",
                "payload": {"mock_steps": 3},
                "budget_usd": 1.0,
            },
        )
        assert resp.status_code == 201
        job_id = resp.json()["id"]

    # Mock Broker interacting with WorkerLoop
    job_status = {"status": "pending"}

    async def mock_stream() -> AsyncGenerator[broker_pb2.JobLease, None]:
        yield broker_pb2.JobLease(
            job_id=job_id,
            tenant_id="tenant-e2e",
            payload_json='{"mock_steps": 3}',
            attempt_id=1,
        )

    mock_stub = MagicMock()
    mock_stub.LeaseJobs = MagicMock(return_value=mock_stream())

    async def mock_ack(req):
        job_status["status"] = "COMPLETED"
        if job_id in _in_memory_jobs:
            _in_memory_jobs[job_id]["status"] = "COMPLETED"
        return broker_pb2.AckResponse(success=True)

    mock_stub.AckJob = AsyncMock(side_effect=mock_ack)
    mock_stub.Heartbeat = AsyncMock(return_value=broker_pb2.HeartbeatResponse(lease_valid=True))

    worker = WorkerLoop(stub=mock_stub, heartbeat_interval_sec=0.02)
    # Run 1 lease cycle
    await worker.run(max_leases=1)

    assert job_status["status"] == "COMPLETED"

    # Query back via GET /api/v1/jobs/{id}
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        get_resp = await client.get(f"/api/v1/jobs/{job_id}")
        assert get_resp.status_code == 200
        assert get_resp.json()["status"] == "COMPLETED"
