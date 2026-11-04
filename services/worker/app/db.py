"""Database client interface for PostgreSQL jobs table."""

import json
import os
from typing import Any

import psycopg2
from psycopg2.extras import RealDictCursor


def get_db_connection():
    """Create a new connection to PostgreSQL."""
    return psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB", "switchyard"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "postgrespassword"),
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
    )


def insert_job(
    job_id: str,
    tenant_id: str,
    budget_usd: float,
    payload: dict[str, Any],
) -> dict[str, Any]:
    """Insert a new job into Postgres with status pending/QUEUED."""
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            query = """
                INSERT INTO jobs (
                    id, tenant_id, status, attempt_id, budget_usd, spent_usd, payload,
                    created_at, updated_at
                )
                VALUES (%s, %s, 'pending', 0, %s, 0, %s, NOW(), NOW())
                RETURNING id, tenant_id, status, attempt_id, budget_usd, spent_usd,
                          payload, created_at, updated_at
            """
            cur.execute(query, (job_id, tenant_id, budget_usd, json.dumps(payload)))
            job = cur.fetchone()
            conn.commit()
            return dict(job) if job else {}
    finally:
        conn.close()


def get_job(job_id: str) -> dict[str, Any] | None:
    """Retrieve job details by ID."""
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            query = """
                SELECT id, tenant_id, status, attempt_id, budget_usd, spent_usd,
                       payload, lease_expires_at, created_at, updated_at
                FROM jobs
                WHERE id = %s
            """
            cur.execute(query, (job_id,))
            job = cur.fetchone()
            return dict(job) if job else None
    finally:
        conn.close()


def get_job_steps(job_id: str) -> list[dict[str, Any]]:
    """Retrieve all execution steps recorded for a job."""
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, job_id, attempt_id, step_name, model, tokens_in, tokens_out,
                       cost_usd, latency_ms, created_at
                FROM job_steps
                WHERE job_id = %s
                ORDER BY created_at ASC
                """,
                (job_id,),
            )
            rows = cur.fetchall()
            return [dict(r) for r in rows]
    finally:
        conn.close()


def get_job_ledger(job_id: str) -> list[dict[str, Any]]:
    """Retrieve all cost ledger entries recorded for a job."""
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, job_id, attempt_id, step_name, model, tokens_in, tokens_out,
                       cost_usd, latency_ms, recorded_at
                FROM cost_ledger
                WHERE job_id = %s
                ORDER BY recorded_at ASC
                """,
                (job_id,),
            )
            rows = cur.fetchall()
            return [dict(r) for r in rows]
    finally:
        conn.close()
