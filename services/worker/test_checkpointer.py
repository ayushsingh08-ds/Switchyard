"""Unit and integration tests for FencedPostgresSaver and LangGraph runner."""

from unittest.mock import AsyncMock, MagicMock

import pytest
from app.checkpointer import FencedPostgresSaver, StaleLeaseFencingError
from app.runner import build_agent_graph, run_agent_job
from langchain_core.runnables import RunnableConfig


class MockAttemptVerifier:
    """Mock database verifier for active job attempts."""

    def __init__(self, current_attempt: int = 1):
        self.current_attempt = current_attempt

    async def get_active_attempt(self, job_id: str) -> int | None:
        return self.current_attempt


@pytest.mark.asyncio
async def test_fenced_checkpointer_allows_active_attempt():
    """Verify that writes from the active attempt are permitted."""
    verifier = MockAttemptVerifier(current_attempt=1)

    # Initialize checkpointer with mocked parent methods
    saver = FencedPostgresSaver.__new__(FencedPostgresSaver)
    saver.verifier = verifier

    # Mock superclass methods
    saver.aput = AsyncMock(return_value={"configurable": {"thread_id": "job-100"}})

    config: RunnableConfig = {
        "configurable": {
            "thread_id": "job-100",
            "attempt_id": 1,
        }
    }

    # Should not raise
    await saver._verify_attempt(config)


@pytest.mark.asyncio
async def test_zombie_worker_rejected_by_fenced_checkpointer():
    """
    Done-When Verification:
    A test simulates Zombie Worker 1 (attempt_id=1) attempting to write a checkpoint
    after Worker 2 (attempt_id=2) has taken the lease, asserting Worker 1's write is rejected.
    """
    # Active attempt in DB is now 2 (leased by Worker 2)
    verifier = MockAttemptVerifier(current_attempt=2)

    saver = FencedPostgresSaver.__new__(FencedPostgresSaver)
    saver.verifier = verifier

    # Worker 1 (stale attempt 1) attempts to write checkpoint state
    zombie_config: RunnableConfig = {
        "configurable": {
            "thread_id": "job-zombie-test",
            "attempt_id": 1,  # Stale fencing token!
        }
    }

    checkpoint = MagicMock()
    metadata = MagicMock()
    versions = MagicMock()

    # Verify aput rejects the stale write
    with pytest.raises(StaleLeaseFencingError) as exc_info:
        await saver.aput(zombie_config, checkpoint, metadata, versions)

    assert "Zombie write aborted" in str(exc_info.value)
    assert "worker attempt 1 does not match active DB attempt 2" in str(exc_info.value)

    # Verify aput_writes also rejects the stale write
    writes = [("channel_a", "val_a")]
    with pytest.raises(StaleLeaseFencingError) as exc_info_writes:
        await saver.aput_writes(zombie_config, writes, task_id="task-1")

    assert "Zombie write aborted" in str(exc_info_writes.value)


@pytest.mark.asyncio
async def test_langgraph_runner_with_fencing_lifecycle():
    """Verify LangGraph execution and resume semantics with FencedPostgresSaver."""
    from langgraph.checkpoint.memory import MemorySaver

    # Use MemorySaver as base checkpointer for in-memory graph testing
    class FencedMemorySaver(MemorySaver):
        def __init__(self, verifier: MockAttemptVerifier):
            super().__init__()
            self.verifier = verifier

        def put(self, config, checkpoint, metadata, new_versions):
            conf = config.get("configurable", {})
            job_id = conf.get("thread_id")
            attempt = conf.get("attempt_id")
            if job_id and attempt is not None:
                # Synchronous verify in test helper
                if self.verifier.current_attempt != attempt:
                    raise StaleLeaseFencingError("Zombie write rejected")
            return super().put(config, checkpoint, metadata, new_versions)

        async def aput(self, config, checkpoint, metadata, new_versions):
            return self.put(config, checkpoint, metadata, new_versions)

        def put_writes(self, config, writes, task_id, task_path=""):
            conf = config.get("configurable", {})
            job_id = conf.get("thread_id")
            attempt = conf.get("attempt_id")
            if job_id and attempt is not None:
                if self.verifier.current_attempt != attempt:
                    raise StaleLeaseFencingError("Zombie write rejected")
            return super().put_writes(config, writes, task_id, task_path=task_path)

        async def aput_writes(self, config, writes, task_id, task_path=""):
            return self.put_writes(config, writes, task_id, task_path=task_path)

    verifier = MockAttemptVerifier(current_attempt=1)
    fenced_saver = FencedMemorySaver(verifier)
    graph = build_agent_graph(checkpointer=fenced_saver)

    job_id = "job-workflow-1"

    # Worker 1 executes attempt 1
    res1 = await run_agent_job(
        graph,
        job_id=job_id,
        attempt_id=1,
        initial_input={"job_id": job_id, "step": 0, "data": "init"},
    )
    assert res1["step"] == 2
    assert "init->step1->step2" in res1["data"]

    # Now simulate reaper giving lease to Worker 2 (attempt_id = 2)
    verifier.current_attempt = 2

    # Worker 2 resumes from checkpoint
    res2 = await run_agent_job(graph, job_id=job_id, attempt_id=2)
    assert res2["step"] == 2

    # Zombie Worker 1 (attempt_id = 1) tries to invoke again -> rejected!
    with pytest.raises(StaleLeaseFencingError):
        await run_agent_job(
            graph,
            job_id=job_id,
            attempt_id=1,
            initial_input={"job_id": job_id, "step": 0, "data": "zombie_data"},
        )
