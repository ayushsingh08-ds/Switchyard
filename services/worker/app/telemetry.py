"""OpenTelemetry and Prometheus metrics instrumentation for Python Worker and Ingestion API."""

import re

from prometheus_client import CONTENT_TYPE_LATEST, Counter, generate_latest

# Prometheus metrics for Python Worker
TOKENS_TOTAL = Counter(
    "switchyard_tokens_total",
    "Total tokens consumed across worker runs",
    ["model", "type"],  # type: "input" | "output"
)

COST_USD_TOTAL = Counter(
    "switchyard_cost_usd_total",
    "Total cost incurred across LLM calls in USD",
    ["model"],
)


def record_llm_metrics(model: str, tokens_in: int, tokens_out: int, cost_usd: float) -> None:
    """Record LLM token consumption and dollar cost to Prometheus counters."""
    if tokens_in > 0:
        TOKENS_TOTAL.labels(model=model, type="input").inc(tokens_in)
    if tokens_out > 0:
        TOKENS_TOTAL.labels(model=model, type="output").inc(tokens_out)
    if cost_usd > 0:
        COST_USD_TOTAL.labels(model=model).inc(cost_usd)


def get_prometheus_metrics() -> tuple[bytes, str]:
    """Generate latest Prometheus metrics for /metrics HTTP endpoint."""
    return generate_latest(), CONTENT_TYPE_LATEST


def extract_trace_id(traceparent: str | None) -> str:
    """
    Extract 32-hex trace_id from W3C traceparent header.
    Format: 00-<32-hex-trace-id>-<16-hex-parent-id>-<2-hex-flags>
    """
    if not traceparent:
        return ""
    parts = traceparent.split("-")
    if len(parts) >= 2 and len(parts[1]) == 32:
        return parts[1]
    return ""


def format_traceparent(trace_id: str, span_id: str = "0000000000000001") -> str:
    """Format a 32-hex trace_id into standard W3C traceparent header."""
    clean_trace = re.sub(r"[^0-9a-fA-F]", "", trace_id).lower()
    if len(clean_trace) < 32:
        clean_trace = clean_trace.zfill(32)
    elif len(clean_trace) > 32:
        clean_trace = clean_trace[:32]
    return f"00-{clean_trace}-{span_id}-01"
