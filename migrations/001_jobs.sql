-- Migration: 001_jobs.sql
-- Description: Core jobs table tracking lifecycle, execution fencing attempts, budgets, and lease expirations.

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS jobs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    attempt_id INT NOT NULL DEFAULT 0,
    budget_usd NUMERIC(10, 6) NOT NULL,
    spent_usd NUMERIC(10, 6) NOT NULL DEFAULT 0,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    lease_expires_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Index to fast-query jobs eligible for lease polling
CREATE INDEX IF NOT EXISTS idx_jobs_status_lease ON jobs (status, lease_expires_at);
-- Index for tenant-specific lookup and tracking
CREATE INDEX IF NOT EXISTS idx_jobs_tenant_id ON jobs (tenant_id);
