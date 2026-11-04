"""
Alert-Triage LangGraph workflow orchestrating incident response:
1. parse_alert (difficulty=low): Extracts service name and alert metadata.
2. fetch_context: Calls prometheus_mcp and runbook_rag_mcp.
3. diagnose_root_cause (difficulty=high): Synthesizes metrics + runbooks into diagnosis.
4. open_ticket (difficulty=low): Calls ticket_mcp with deterministic idempotency_key.
"""

import hashlib
import json
import os
import sys
from typing import Annotated, Any, TypedDict

from langchain_core.messages import HumanMessage
from langchain_core.runnables import RunnableConfig
from langgraph.graph import END, START, StateGraph

# Ensure services/mcp-servers is on sys.path for direct imports
MCP_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..", "mcp-servers")
)
if MCP_DIR not in sys.path:
    sys.path.append(MCP_DIR)

from prometheus_mcp import query_metrics  # noqa: E402
from runbook_rag_mcp import search_runbooks  # noqa: E402
from ticket_mcp import create_incident_ticket  # noqa: E402

from app.agent.gateway_llm import GatewayChatOpenAI  # noqa: E402


class AlertTriageState(TypedDict):
    """Execution state for the on-call alert triage agent workflow."""

    alert_raw: dict[str, Any]
    service_name: str
    severity: str
    summary: str
    telemetry_metrics: list[dict[str, Any]]
    runbook_chunks: list[dict[str, Any]]
    diagnosis: str
    ticket_id: str
    duplicate_ticket: bool
    completed_steps: Annotated[list[str], lambda x, y: x + y]


def build_triage_graph(llm: GatewayChatOpenAI | None = None) -> Any:
    """Build the 4-node StateGraph for Alert-Triage."""
    gateway_llm = llm or GatewayChatOpenAI()

    async def parse_alert_node(
        state: AlertTriageState, config: RunnableConfig
    ) -> dict[str, Any]:
        """Node 1: Extract service name and metadata (difficulty=low)."""
        raw = state.get("alert_raw", {})
        alert_text = json.dumps(raw)

        prompt = (
            f"Extract the service name, severity, and brief summary:\n{alert_text}\n"
            "Format: {\"service_name\": \"...\", \"severity\": \"...\", \"summary\": \"...\"}"
        )

        response = await gateway_llm.ainvoke(
            [HumanMessage(content=prompt)],
            config=config,
            difficulty="low",
            step_name="parse_alert",
        )

        content = response.content
        service = raw.get("service") or raw.get("service_name") or "unknown-service"
        severity = raw.get("severity") or "P2"
        summary = raw.get("summary") or raw.get("alertname") or "Operational Alert"

        try:
            # Parse LLM response if valid JSON
            cleaned = content.strip().removeprefix("```json").removesuffix("```").strip()
            data = json.loads(cleaned)
            service = data.get("service_name") or service
            severity = data.get("severity") or severity
            summary = data.get("summary") or summary
        except Exception:
            pass

        return {
            "service_name": service,
            "severity": severity,
            "summary": summary,
            "completed_steps": ["parse_alert"],
        }

    async def fetch_context_node(
        state: AlertTriageState, config: RunnableConfig
    ) -> dict[str, Any]:
        """Node 2: Retrieve telemetry metrics and relevant runbooks."""
        service = state.get("service_name", "worker-ingestion")
        summary = state.get("summary", "High Error Rate")

        # 1. Call Prometheus MCP tool
        telemetry = query_metrics(service_name=service, window_minutes=5)
        metrics_list = telemetry.get("metrics", [])

        # 2. Call Runbook RAG MCP tool
        runbooks = search_runbooks(error_query=f"{service} {summary}", top_k=2)

        return {
            "telemetry_metrics": metrics_list,
            "runbook_chunks": runbooks,
            "completed_steps": ["fetch_context"],
        }

    async def diagnose_root_cause_node(
        state: AlertTriageState, config: RunnableConfig
    ) -> dict[str, Any]:
        """Node 3: Synthesize metrics + runbooks into root cause diagnosis (difficulty=high)."""
        metrics = state.get("telemetry_metrics", [])
        runbooks = state.get("runbook_chunks", [])
        summary = state.get("summary", "")
        service = state.get("service_name", "")

        prompt = (
            f"Service: {service}\nAlert: {summary}\n"
            f"Prometheus Metrics: {json.dumps(metrics)}\n"
            f"Runbooks: {json.dumps(runbooks)}\n\n"
            "Synthesize the root cause and immediate remediation action."
        )

        response = await gateway_llm.ainvoke(
            [HumanMessage(content=prompt)],
            config=config,
            difficulty="high",
            step_name="diagnose_root_cause",
        )

        return {
            "diagnosis": response.content,
            "completed_steps": ["diagnose_root_cause"],
        }

    async def open_ticket_node(
        state: AlertTriageState, config: RunnableConfig
    ) -> dict[str, Any]:
        """Node 4: Call ticket_mcp with deterministic idempotency_key (difficulty=low)."""
        configurable = config.get("configurable", {})
        job_id = configurable.get("job_id") or configurable.get("thread_id") or "job-default"
        step_name = "open_ticket"

        # Deterministic idempotency key: hash(job_id + step_name)
        idempotency_key = hashlib.sha256(f"{job_id}:{step_name}".encode()).hexdigest()

        svc = state.get("service_name", "service")
        sev = state.get("severity", "P2")
        title = f"[{sev}] Incident in {svc}"
        summary = state.get("diagnosis") or state.get("summary") or "Incident triage alert"

        ticket = create_incident_ticket(
            title=title,
            severity=sev,
            summary=summary,
            idempotency_key=idempotency_key,
        )

        return {
            "ticket_id": ticket.get("ticket_id", "INC-00000000"),
            "duplicate_ticket": ticket.get("duplicate", False),
            "completed_steps": ["open_ticket"],
        }

    # Assemble StateGraph
    graph = StateGraph(AlertTriageState)
    graph.add_node("parse_alert", parse_alert_node)
    graph.add_node("fetch_context", fetch_context_node)
    graph.add_node("diagnose_root_cause", diagnose_root_cause_node)
    graph.add_node("open_ticket", open_ticket_node)

    graph.add_edge(START, "parse_alert")
    graph.add_edge("parse_alert", "fetch_context")
    graph.add_edge("fetch_context", "diagnose_root_cause")
    graph.add_edge("diagnose_root_cause", "open_ticket")
    graph.add_edge("open_ticket", END)

    return graph.compile()
