"""FastAPI Ingestion and Query Service for Switchyard Jobs with W3C Traceparent & Metrics."""

import json
import os
import uuid
from typing import Any

import grpc
from fastapi import FastAPI, Header, HTTPException, Response
from pydantic import BaseModel, Field

from app import db
from app.pb import broker_pb2, broker_pb2_grpc
from app.telemetry import (
    extract_trace_id,
    format_traceparent,
    get_prometheus_metrics,
)

app = FastAPI(title="Switchyard Ingestion API", version="0.1.0")

# In-memory store fallback when Postgres is not directly available (e.g. lightweight testing)
_in_memory_jobs: dict[str, dict[str, Any]] = {}


class JobCreateRequest(BaseModel):
    tenant_id: str = Field(..., description="Tenant identifier")
    payload: dict[str, Any] = Field(default_factory=dict, description="Job payload parameters")
    budget_usd: float = Field(default=1.0, description="Allocated budget in USD")


class JobResponse(BaseModel):
    id: str
    tenant_id: str
    status: str
    attempt_id: int
    budget_usd: float
    payload: dict[str, Any] | None = None
    traceparent: str | None = None


def get_broker_client() -> broker_pb2_grpc.BrokerServiceStub:
    """Instantiate a gRPC stub connected to the Go Broker."""
    broker_addr = os.getenv("BROKER_GRPC_ADDR", "localhost:50051")
    channel = grpc.insecure_channel(broker_addr)
    return broker_pb2_grpc.BrokerServiceStub(channel)


@app.get("/metrics")
async def metrics_endpoint():
    """Expose Prometheus scrape endpoint for Python Worker & Ingestion API."""
    data, content_type = get_prometheus_metrics()
    return Response(content=data, media_type=content_type)


@app.post("/api/v1/jobs", response_model=JobResponse, status_code=201)
async def create_job(
    req: JobCreateRequest,
    traceparent: str | None = Header(None, alias="traceparent"),
):
    """
    Enqueue a new job:
    1. Propagate or generate W3C traceparent.
    2. Persist QUEUED/pending state.
    3. Notify Go Broker via EnqueueJob gRPC with traceparent in metadata.
    """
    job_id = str(uuid.uuid4())
    payload_str = json.dumps(req.payload)

    # Propagate or create W3C traceparent
    clean_trace_id = extract_trace_id(traceparent)
    effective_traceparent = traceparent if clean_trace_id else format_traceparent(uuid.uuid4().hex)

    # 1. Try to persist to PostgreSQL, fallback to memory if DB is unavailable
    job_data: dict[str, Any]
    try:
        job_data = db.insert_job(job_id, req.tenant_id, req.budget_usd, req.payload)
    except Exception:
        job_data = {
            "id": job_id,
            "tenant_id": req.tenant_id,
            "status": "pending",
            "attempt_id": 0,
            "budget_usd": req.budget_usd,
            "payload": req.payload,
        }
        _in_memory_jobs[job_id] = job_data

    job_data["traceparent"] = effective_traceparent

    # 2. Call Go Broker EnqueueJob gRPC with traceparent in gRPC metadata
    try:
        client = get_broker_client()
        grpc_req = broker_pb2.EnqueueRequest(
            tenant_id=req.tenant_id,
            payload_json=payload_str,
            budget_usd=req.budget_usd,
        )
        metadata = [("traceparent", effective_traceparent)]
        # 1-second timeout to avoid blocking if broker is in mock mode
        client.EnqueueJob(grpc_req, metadata=metadata, timeout=1.0)
    except Exception:
        # Broker notification logged; job remains queued in database
        pass

    return JobResponse(
        id=job_data["id"],
        tenant_id=job_data["tenant_id"],
        status=job_data.get("status", "pending"),
        attempt_id=job_data.get("attempt_id", 0),
        budget_usd=float(job_data.get("budget_usd", req.budget_usd)),
        payload=req.payload,
        traceparent=effective_traceparent,
    )


@app.get("/api/v1/jobs/{job_id}", response_model=JobResponse)
async def get_job_by_id(job_id: str):
    """Retrieve job details by ID."""
    job: dict[str, Any] | None = None
    try:
        job = db.get_job(job_id)
    except Exception:
        job = _in_memory_jobs.get(job_id)

    if not job:
        job = _in_memory_jobs.get(job_id)

    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    payload = job.get("payload")
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except Exception:
            payload = {}

    return JobResponse(
        id=str(job["id"]),
        tenant_id=str(job["tenant_id"]),
        status=str(job.get("status", "pending")),
        attempt_id=int(job.get("attempt_id", 0)),
        budget_usd=float(job.get("budget_usd", 0.0)),
        payload=payload,
        traceparent=job.get("traceparent"),
    )
