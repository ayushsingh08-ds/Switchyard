-- Migration: 002_steps_and_ledger.sql
-- Description: Execution trace steps and financial/token ledger for auditing job cost.

CREATE TABLE IF NOT EXISTS job_steps (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    job_id UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    attempt_id INT NOT NULL,
    step_name TEXT NOT NULL,
    model TEXT NOT NULL,
    tokens_in INT NOT NULL DEFAULT 0,
    tokens_out INT NOT NULL DEFAULT 0,
    cost_usd NUMERIC(10, 6) NOT NULL DEFAULT 0,
    latency_ms INT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS cost_ledger (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    job_id UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    attempt_id INT NOT NULL,
    step_name TEXT NOT NULL,
    model TEXT NOT NULL,
    tokens_in INT NOT NULL DEFAULT 0,
    tokens_out INT NOT NULL DEFAULT 0,
    cost_usd NUMERIC(10, 6) NOT NULL DEFAULT 0,
    latency_ms INT NOT NULL DEFAULT 0,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Fast lookup of steps and ledger records by job and execution attempt
CREATE INDEX IF NOT EXISTS idx_job_steps_job_attempt ON job_steps (job_id, attempt_id);
CREATE INDEX IF NOT EXISTS idx_cost_ledger_job_attempt ON cost_ledger (job_id, attempt_id);
