"""Fenced PostgreSQL checkpointer for LangGraph preventing zombie worker corruption."""

from collections.abc import Sequence
from typing import Any, Protocol

from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.base import ChannelVersions, Checkpoint, CheckpointMetadata
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver


class StaleLeaseFencingError(Exception):
    """Raised when a worker attempts to write state with an outdated attempt fencing token."""


class AttemptVerifier(Protocol):
    """Protocol for checking active job attempt in the database."""

    async def get_active_attempt(self, job_id: str) -> int | None:
        """Return the current attempt_id in the database for the given job_id."""
        ...


class FencedPostgresSaver(AsyncPostgresSaver):
    """
    Subclass of LangGraph's AsyncPostgresSaver that enforces monotonic attempt_id fencing.
    Before writing checkpoint state or writes for thread_id = job_id, verifies that
    SELECT attempt_id FROM jobs WHERE id = $1 matches the worker's active attempt_id.
    """

    def __init__(
        self,
        *args: Any,
        verifier: AttemptVerifier | None = None,
        **kwargs: Any,
    ):
        super().__init__(*args, **kwargs)
        self.verifier = verifier

    async def _verify_attempt(self, config: RunnableConfig) -> None:
        """
        Verify thread_id and attempt_id fencing token before committing to PostgreSQL.
        Config carries:
          - configurable: {"thread_id": job_id, "attempt_id": active_attempt_id}
        """
        configurable = config.get("configurable", {})
        job_id = configurable.get("thread_id")
        worker_attempt = configurable.get("attempt_id")

        if not job_id or worker_attempt is None:
            # If no attempt fencing metadata is configured, proceed normally
            return

        active_db_attempt: int | None = None

        if self.verifier:
            active_db_attempt = await self.verifier.get_active_attempt(job_id)
        elif hasattr(self, "conn") and self.conn:
            # Direct query against postgres connection
            async with self.conn.cursor() as cur:
                await cur.execute("SELECT attempt_id FROM jobs WHERE id = %s", (job_id,))
                row = await cur.fetchone()
                if row:
                    active_db_attempt = row[0]

        if active_db_attempt is not None and active_db_attempt != worker_attempt:
            raise StaleLeaseFencingError(
                f"Fencing conflict for job {job_id}: worker attempt {worker_attempt} "
                f"does not match active DB attempt {active_db_attempt}. Zombie write aborted."
            )

    async def aput(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        """Check fence before writing checkpoint state."""
        await self._verify_attempt(config)
        return await super().aput(config, checkpoint, metadata, new_versions)

    async def aput_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        """Check fence before writing channel writes."""
        await self._verify_attempt(config)
        return await super().aput_writes(config, writes, task_id, task_path=task_path)
