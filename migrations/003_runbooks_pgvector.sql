-- Migration: 003_runbooks_pgvector.sql
-- Description: Enables pgvector extension and creates runbook chunks for semantic RAG search.

CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS runbook_chunks (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    title TEXT NOT NULL,
    content TEXT NOT NULL,
    embedding vector(1536),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- HNSW index for high-performance cosine vector similarity search
CREATE INDEX IF NOT EXISTS idx_runbook_chunks_embedding ON runbook_chunks USING hnsw (embedding vector_cosine_ops);
