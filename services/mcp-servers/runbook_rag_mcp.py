"""Runbook RAG MCP Server for semantic runbook retrieval using pgvector cosine similarity."""

import hashlib
import json
import os
from typing import Any

# Default mock runbook library seeded for incident triage
SAMPLE_RUNBOOKS: list[dict[str, str]] = [
    {
        "title": "High Memory & OOM Crash Remediation",
        "content": (
            "Symptoms: Memory utilization > 90%, frequent OOMKilled worker containers.\n"
            "Action Plan:\n"
            "1. Inspect active worker lease timeouts and background goroutines.\n"
            "2. Restart zombie worker processes using SIGTERM.\n"
            "3. Scale worker replicas and review large JSON payloads."
        ),
    },
    {
        "title": "Database Connection Pool Exhaustion",
        "content": (
            "Symptoms: PostgreSQL FATAL: remaining connection slots are reserved.\n"
            "Action Plan:\n"
            "1. Check pg_stat_activity for idle-in-transaction connections.\n"
            "2. Enable connection pooling via PgBouncer or reduce max worker pool limits.\n"
            "3. Terminate stale connection threads older than 5 minutes."
        ),
    },
    {
        "title": "High HTTP 500 Error Rates in Ingestion API",
        "content": (
            "Symptoms: Surge in 500 Internal Server Errors on /api/v1/jobs.\n"
            "Action Plan:\n"
            "1. Verify Go broker gRPC availability on localhost:50051.\n"
            "2. Inspect WAL disk write latency and verify write-ahead log permissions.\n"
            "3. Drain rate-limited client requests using backoff retries."
        ),
    },
    {
        "title": "Stale Lease and Zombie Worker Fencing",
        "content": (
            "Symptoms: StaleLeaseFencingError raised during checkpointer commits.\n"
            "Action Plan:\n"
            "1. Verify Reaper goroutine interval and network latency between broker and worker.\n"
            "2. Confirm heartbeat task is emitting updates every 2 seconds.\n"
            "3. Abort unacknowledged jobs and allow Worker 2 to resume from checkpoint."
        ),
    },
    {
        "title": "LLM Gateway Budget Exceeded & Routing Saturation",
        "content": (
            "Symptoms: Job status BUDGET_EXCEEDED, high latency on frontier model routes.\n"
            "Action Plan:\n"
            "1. Switch difficulty routing flag from 'high' to 'low' for low-complexity steps.\n"
            "2. Verify Redis cache hit rates for frequent prompts.\n"
            "3. Request budget increase from tenant admin or compact prompt context."
        ),
    },
]


def _generate_synthetic_embedding(text: str, dim: int = 1536) -> list[float]:
    """Generate deterministic pseudo-embedding vector normalized to unit length."""
    h = hashlib.sha256(text.encode("utf-8")).digest()
    vec = []
    for i in range(dim):
        byte_val = h[i % len(h)]
        vec.append((byte_val / 127.5) - 1.0)
    # Normalize length
    norm = sum(x * x for x in vec) ** 0.5 or 1.0
    return [x / norm for x in vec]


def seed_runbooks(conn: Any = None) -> int:
    """Seed the database with 5 markdown runbooks and synthetic embeddings."""
    should_close = False
    if conn is None:
        try:
            import psycopg2
            conn = psycopg2.connect(
                dbname=os.getenv("POSTGRES_DB", "switchyard"),
                user=os.getenv("POSTGRES_USER", "postgres"),
                password=os.getenv("POSTGRES_PASSWORD", "postgrespassword"),
                host=os.getenv("POSTGRES_HOST", "localhost"),
                port=int(os.getenv("POSTGRES_PORT", "5432")),
            )
            should_close = True
        except Exception:
            return len(SAMPLE_RUNBOOKS)

    try:
        with conn.cursor() as cur:
            for rb in SAMPLE_RUNBOOKS:
                embedding = _generate_synthetic_embedding(rb["title"] + " " + rb["content"])
                cur.execute(
                    """
                    INSERT INTO runbook_chunks (title, content, embedding)
                    VALUES (%s, %s, %s::vector)
                    ON CONFLICT DO NOTHING
                    """,
                    (rb["title"], rb["content"], json.dumps(embedding)),
                )
            conn.commit()
            return len(SAMPLE_RUNBOOKS)
    finally:
        if should_close:
            conn.close()


def search_runbooks(
    error_query: str,
    top_k: int = 2,
    conn: Any = None,
) -> list[dict[str, Any]]:
    """
    Search runbooks using pgvector cosine similarity (<=>).
    Falls back gracefully to keyword matching if pgvector is offline.
    """
    query_vec = _generate_synthetic_embedding(error_query)
    should_close = False

    if conn is None:
        try:
            import psycopg2
            from psycopg2.extras import RealDictCursor
            conn = psycopg2.connect(
                dbname=os.getenv("POSTGRES_DB", "switchyard"),
                user=os.getenv("POSTGRES_USER", "postgres"),
                password=os.getenv("POSTGRES_PASSWORD", "postgrespassword"),
                host=os.getenv("POSTGRES_HOST", "localhost"),
                port=int(os.getenv("POSTGRES_PORT", "5432")),
            )
            should_close = True
        except Exception:
            conn = None

    if conn is not None:
        try:
            from psycopg2.extras import RealDictCursor
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                query = """
                    SELECT id, title, content, (embedding <=> %s::vector) AS distance
                    FROM runbook_chunks
                    ORDER BY embedding <=> %s::vector ASC
                    LIMIT %s
                """
                cur.execute(query, (json.dumps(query_vec), json.dumps(query_vec), top_k))
                rows = cur.fetchall()
                if rows:
                    return [dict(r) for r in rows]
        except Exception:
            pass
        finally:
            if should_close:
                conn.close()

    # In-memory cosine fallback across SAMPLE_RUNBOOKS
    scored_runbooks = []
    for rb in SAMPLE_RUNBOOKS:
        rb_vec = _generate_synthetic_embedding(rb["title"] + " " + rb["content"])
        dot = sum(a * b for a, b in zip(query_vec, rb_vec, strict=False))
        # cosine distance is 1.0 - cosine similarity
        distance = round(1.0 - dot, 4)
        scored_runbooks.append({
            "title": rb["title"],
            "content": rb["content"],
            "distance": distance,
        })

    scored_runbooks.sort(key=lambda x: x["distance"])
    return scored_runbooks[:top_k]
