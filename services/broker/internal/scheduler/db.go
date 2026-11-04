package scheduler

import (
	"context"
	"time"
)

// DB defines the database interface required by the scheduler for managing jobs.
type DB interface {
	// CreateJob inserts a new job record.
	CreateJob(ctx context.Context, job *JobRecord) error

	// LeaseNextJob atomically fetches the next queued job, increments attempt_id, and sets lease_expires_at.
	LeaseNextJob(ctx context.Context, leaseDuration time.Duration) (*JobRecord, error)

	// Heartbeat checks if attempt_id matches the active attempt and extends lease_expires_at.
	Heartbeat(ctx context.Context, jobID string, attemptID int64, extension time.Duration) (bool, error)

	// AckJob marks a job as successfully completed.
	AckJob(ctx context.Context, jobID string, attemptID int64) error

	// NackJob marks a job attempt as failed, returning to queued if retries remain.
	NackJob(ctx context.Context, jobID string, attemptID int64, retryable bool, maxRetries int) error

	// ReapExpiredLeases finds expired leases and transitions them to QUEUED or DLQ.
	ReapExpiredLeases(ctx context.Context, maxRetries int) ([]*JobRecord, error)
}

// JobRecord represents the database row state for a job.
type JobRecord struct {
	ID             string
	TenantID       string
	Status         string
	AttemptID      int64
	BudgetUSD      float64
	SpentUSD       float64
	Payload        []byte
	LeaseExpiresAt *time.Time
	CreatedAt      time.Time
	UpdatedAt      time.Time
}
