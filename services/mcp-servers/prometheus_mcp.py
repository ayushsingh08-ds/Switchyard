"""Prometheus MCP Server for querying operational and telemetry metrics."""

import os
from typing import Any

import httpx

PROMETHEUS_URL = os.getenv("PROMETHEUS_URL", "http://localhost:9090")


def query_metrics(service_name: str, window_minutes: int = 5) -> dict[str, Any]:
    """
    Query Prometheus for service telemetry metrics over the given time window.
    Falls back gracefully to mock telemetry when Prometheus is offline or unreachable.
    """
    query = f'rate(http_requests_total{{service="{service_name}"}}[{window_minutes}m])'
    try:
        with httpx.Client(timeout=2.0) as client:
            resp = client.get(
                f"{PROMETHEUS_URL}/api/v1/query",
                params={"query": query},
            )
            if resp.status_code == 200:
                data = resp.json()
                if data.get("status") == "success" and data.get("data", {}).get("result"):
                    return {
                        "service": service_name,
                        "window_minutes": window_minutes,
                        "status": "online",
                        "metrics": data["data"]["result"],
                    }
    except Exception:
        # Fall back to structured synthetic telemetry if live Prometheus is unavailable
        pass

    # High error rate simulation for alert triage
    return {
        "service": service_name,
        "window_minutes": window_minutes,
        "status": "simulated",
        "metrics": [
            {
                "metric": {
                    "service": service_name,
                    "handler": "/api/v1/jobs",
                    "status_code": "500",
                },
                "value": [1700000000, "18.42"],
            },
            {
                "metric": {
                    "service": service_name,
                    "handler": "/api/v1/jobs",
                    "status_code": "200",
                },
                "value": [1700000000, "2.15"],
            },
            {
                "metric": {
                    "service": service_name,
                    "type": "memory_utilization_ratio",
                },
                "value": [1700000000, "0.94"],
            },
        ],
    }
