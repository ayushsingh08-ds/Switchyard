"""Gateway LLM wrapper for LangChain ChatOpenAI with routing and budget checks."""

import os
import time
from typing import Any, Protocol

from langchain_core.language_models.base import LanguageModelInput
from langchain_core.messages import AIMessage, BaseMessage
from langchain_core.outputs import ChatResult
from langchain_core.runnables import RunnableConfig
from langchain_openai import ChatOpenAI
from langchain_openai.chat_models.base import BaseChatOpenAI

from app.checkpointer import StaleLeaseFencingError
from app.telemetry import record_llm_metrics


class BudgetExceededError(Exception):
    """Raised when a job's spent_usd meets or exceeds budget_usd."""


class GatewayDatabaseProtocol(Protocol):
    """Protocol defining database operations required by GatewayLLM."""

    def get_job(self, job_id: str) -> dict[str, Any] | None:
        """Fetch job record by ID."""
        ...

    def update_job_status(self, job_id: str, status: str) -> None:
        """Update job lifecycle status."""
        ...

    def update_job_spent_usd(self, job_id: str, additional_spent_usd: float) -> None:
        """Increment spent_usd for a job."""
        ...

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
        """Record per-call tokens, model, cost, and latency in cost_ledger."""
        ...


class DefaultGatewayDatabase:
    """Production database adapter for GatewayLLM backed by PostgreSQL."""

    def get_job(self, job_id: str) -> dict[str, Any] | None:
        from app.db import get_job
        return get_job(job_id)

    def update_job_status(self, job_id: str, status: str) -> None:
        from app.db import get_db_connection
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE jobs SET status = %s, updated_at = NOW() WHERE id = %s",
                    (status, job_id),
                )
                conn.commit()
        finally:
            conn.close()

    def update_job_spent_usd(self, job_id: str, additional_spent_usd: float) -> None:
        from app.db import get_db_connection
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE jobs
                    SET spent_usd = spent_usd + %s, updated_at = NOW()
                    WHERE id = %s
                    """,
                    (additional_spent_usd, job_id),
                )
                conn.commit()
        finally:
            conn.close()

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
        from app.db import get_db_connection
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO cost_ledger (
                        job_id, attempt_id, step_name, model, tokens_in, tokens_out,
                        cost_usd, latency_ms, recorded_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    """,
                    (
                        job_id,
                        attempt_id,
                        step_name,
                        model,
                        tokens_in,
                        tokens_out,
                        cost_usd,
                        latency_ms,
                    ),
                )
                conn.commit()
        finally:
            conn.close()


# Fallback token pricing when Gateway doesn't report exact cost ($ per token)
DEFAULT_PRICING: dict[str, dict[str, float]] = {
    "low": {"input": 0.00000015, "output": 0.0000006},    # e.g., gpt-4o-mini
    "high": {"input": 0.0000025, "output": 0.00001},      # e.g., gpt-4o
}


class GatewayChatOpenAI(ChatOpenAI):
    """
    LangChain ChatOpenAI wrapper routed through the external LLM Gateway container.

    Enforces:
      1. Monotonic attempt_id fencing: Aborts zombie workers if DB attempt != worker attempt.
      2. Budget check: Verifies spent_usd < budget_usd before execution; halts with
         BUDGET_EXCEEDED status if exceeded.
      3. Header injection: Injects X-Switchyard-Job-Id, X-Switchyard-Tenant-Id,
         X-Switchyard-Attempt-Id, and X-Task-Difficulty ('low' | 'high'), plus W3C traceparent.
      4. Cost accounting & telemetry: Logs tokens_in, tokens_out, model, cache hit, and cost to
         cost_ledger, increments jobs.spent_usd, and updates Prometheus counters.
    """

    db: Any = None
    default_difficulty: str = "low"
    step_name: str = "llm_call"

    def __init__(
        self,
        base_url: str | None = None,
        api_key: str | None = None,
        db: GatewayDatabaseProtocol | None = None,
        default_difficulty: str = "low",
        step_name: str = "llm_call",
        **kwargs: Any,
    ):
        gateway_url = (
            base_url
            or os.getenv("GATEWAY_BASE_URL")
            or "http://localhost:8000/v1"
        )
        api_token = api_key or os.getenv("GATEWAY_API_KEY") or "dummy-switchyard-key"
        super().__init__(base_url=gateway_url, api_key=api_token, **kwargs)
        self.db = db if db is not None else DefaultGatewayDatabase()
        self.default_difficulty = default_difficulty
        self.step_name = step_name

    def _resolve_context(
        self,
        config: RunnableConfig | None,
        difficulty: str | None = None,
        step_name: str | None = None,
    ) -> dict[str, Any]:
        """Extract job_id, tenant_id, attempt_id, difficulty, step_name, and traceparent."""
        configurable = (config or {}).get("configurable", {})
        job_id = configurable.get("job_id") or configurable.get("thread_id")
        tenant_id = configurable.get("tenant_id", "default")
        attempt_id = configurable.get("attempt_id", 0)
        task_difficulty = (
            difficulty
            or configurable.get("difficulty")
            or self.default_difficulty
        )
        effective_step = step_name or configurable.get("step_name") or self.step_name
        traceparent = configurable.get("traceparent")

        return {
            "job_id": job_id,
            "tenant_id": tenant_id,
            "attempt_id": int(attempt_id),
            "difficulty": task_difficulty,
            "step_name": effective_step,
            "traceparent": traceparent,
        }

    def _pre_execution_checks(self, ctx: dict[str, Any]) -> None:
        """Verify fencing attempt and budget constraints prior to invoking LLM."""
        job_id = ctx.get("job_id")
        if not job_id:
            return

        job = self.db.get_job(job_id)
        if not job:
            return

        # 1. Monotonic attempt fencing check
        worker_attempt = ctx.get("attempt_id", 0)
        db_attempt = job.get("attempt_id", 0)
        if worker_attempt is not None and db_attempt > worker_attempt:
            raise StaleLeaseFencingError(
                f"Gateway aborted execution for job {job_id}: worker attempt {worker_attempt} "
                f"is older than active DB attempt {db_attempt}."
            )

        # 2. Budget enforcement check
        budget_usd = float(job.get("budget_usd") or 0.0)
        spent_usd = float(job.get("spent_usd") or 0.0)
        if spent_usd >= budget_usd:
            self.db.update_job_status(job_id, "BUDGET_EXCEEDED")
            raise BudgetExceededError(
                f"Job {job_id} halted: spent_usd (${spent_usd:.6f}) >= "
                f"budget_usd (${budget_usd:.6f})"
            )

    def _post_execution_accounting(
        self,
        ctx: dict[str, Any],
        message: AIMessage,
        latency_ms: int,
    ) -> None:
        """Calculate costs, update cost_ledger and increment jobs.spent_usd."""
        job_id = ctx.get("job_id")
        if not job_id:
            return

        # Extract tokens and metadata
        resp_meta = getattr(message, "response_metadata", {}) or {}
        usage = resp_meta.get("token_usage", {}) or getattr(message, "usage_metadata", {}) or {}
        tokens_in = usage.get("prompt_tokens") or usage.get("input_tokens") or 0
        tokens_out = usage.get("completion_tokens") or usage.get("output_tokens") or 0
        model_name = resp_meta.get("model_name") or self.model_name
        cache_hit = bool(resp_meta.get("cache_hit", False))

        # Calculate cost
        pricing = DEFAULT_PRICING.get(ctx["difficulty"], DEFAULT_PRICING["low"])
        if cache_hit:
            cost_usd = 0.0
        else:
            cost_usd = (tokens_in * pricing["input"]) + (tokens_out * pricing["output"])

        # Persist ledger entry and update job spent
        self.db.log_cost_ledger(
            job_id=job_id,
            attempt_id=ctx["attempt_id"],
            step_name=ctx["step_name"],
            model=model_name,
            tokens_in=tokens_in,
            tokens_out=tokens_out,
            cost_usd=cost_usd,
            latency_ms=latency_ms,
        )
        self.db.update_job_spent_usd(job_id, cost_usd)
        record_llm_metrics(
            model=model_name,
            tokens_in=tokens_in,
            tokens_out=tokens_out,
            cost_usd=cost_usd,
        )

        # Check if this call pushed the job over budget
        job = self.db.get_job(job_id)
        if job:
            new_spent = float(job.get("spent_usd") or 0.0)
            budget = float(job.get("budget_usd") or 0.0)
            if new_spent >= budget:
                self.db.update_job_status(job_id, "BUDGET_EXCEEDED")

    def _build_headers(
        self,
        ctx: dict[str, Any],
        existing_headers: dict[str, str] | None = None,
    ) -> dict[str, str]:
        """Construct per-call headers for LLM Gateway difficulty routing and tracing."""
        headers = dict(existing_headers or {})
        if ctx.get("job_id"):
            headers["X-Switchyard-Job-Id"] = str(ctx["job_id"])
        if ctx.get("tenant_id"):
            headers["X-Switchyard-Tenant-Id"] = str(ctx["tenant_id"])
        if ctx.get("attempt_id") is not None:
            headers["X-Switchyard-Attempt-Id"] = str(ctx["attempt_id"])
        headers["X-Task-Difficulty"] = str(ctx.get("difficulty", "low"))
        if ctx.get("traceparent"):
            headers["traceparent"] = str(ctx["traceparent"])
        return headers

    def invoke(
        self,
        input: LanguageModelInput,
        config: RunnableConfig | None = None,
        *,
        stop: list[str] | None = None,
        **kwargs: Any,
    ) -> AIMessage:
        """Synchronous invoke enforcing fencing, budget limits, headers, and accounting."""
        ctx = self._resolve_context(
            config,
            difficulty=kwargs.pop("difficulty", None),
            step_name=kwargs.pop("step_name", None),
        )
        self._pre_execution_checks(ctx)

        headers = self._build_headers(ctx, kwargs.get("extra_headers"))
        kwargs["extra_headers"] = headers

        start_time = time.monotonic()
        response = super().invoke(input, config=config, stop=stop, **kwargs)
        latency_ms = int((time.monotonic() - start_time) * 1000)

        self._post_execution_accounting(ctx, response, latency_ms)
        return response

    async def ainvoke(
        self,
        input: LanguageModelInput,
        config: RunnableConfig | None = None,
        *,
        stop: list[str] | None = None,
        **kwargs: Any,
    ) -> AIMessage:
        """Asynchronous invoke enforcing fencing, budget limits, headers, and accounting."""
        ctx = self._resolve_context(
            config,
            difficulty=kwargs.pop("difficulty", None),
            step_name=kwargs.pop("step_name", None),
        )
        self._pre_execution_checks(ctx)

        headers = self._build_headers(ctx, kwargs.get("extra_headers"))
        kwargs["extra_headers"] = headers

        start_time = time.monotonic()
        response = await super().ainvoke(input, config=config, stop=stop, **kwargs)
        latency_ms = int((time.monotonic() - start_time) * 1000)

        self._post_execution_accounting(ctx, response, latency_ms)
        return response

    def _generate(
        self,
        messages: list[BaseMessage],
        stop: list[str] | None = None,
        run_manager: Any = None,
        **kwargs: Any,
    ) -> ChatResult:
        """Synchronous batch generation with header injection."""
        return super(BaseChatOpenAI, self)._generate(
            messages, stop=stop, run_manager=run_manager, **kwargs
        )

    async def _agenerate(
        self,
        messages: list[BaseMessage],
        stop: list[str] | None = None,
        run_manager: Any = None,
        **kwargs: Any,
    ) -> ChatResult:
        """Asynchronous batch generation with header injection."""
        return await BaseChatOpenAI._agenerate(
            self, messages, stop=stop, run_manager=run_manager, **kwargs
        )
