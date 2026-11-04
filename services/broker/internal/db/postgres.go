package db

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/switchyard/services/broker/internal/scheduler"
)

// PostgresDB implements scheduler.DB using a PostgreSQL database connection.
type PostgresDB struct {
	db *sql.DB
}

// NewPostgresDB wraps a database connection.
func NewPostgresDB(db *sql.DB) *PostgresDB {
	return &PostgresDB{db: db}
}

// CreateJob inserts a new pending job.
func (p *PostgresDB) CreateJob(ctx context.Context, job *scheduler.JobRecord) error {
	query := `
		INSERT INTO jobs (id, tenant_id, status, attempt_id, budget_usd, spent_usd, payload, created_at, updated_at)
		VALUES ($1, $2, 'pending', 0, $3, 0, $4, NOW(), NOW())
	`
	_, err := p.db.ExecContext(ctx, query, job.ID, job.TenantID, job.BudgetUSD, job.Payload)
	return err
}

// LeaseNextJob finds the next available pending job using FOR UPDATE SKIP LOCKED to avoid contention,
// atomically increments attempt_id, sets lease_expires_at = NOW() + leaseDuration, and marks status 'leased'.
func (p *PostgresDB) LeaseNextJob(ctx context.Context, leaseDuration time.Duration) (*scheduler.JobRecord, error) {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	selectQuery := `
		SELECT id, tenant_id, status, attempt_id, budget_usd, spent_usd, payload
		FROM jobs
		WHERE status = 'pending'
		ORDER BY created_at ASC
		FOR UPDATE SKIP LOCKED
		LIMIT 1
	`

	var job scheduler.JobRecord
	err = tx.QueryRowContext(ctx, selectQuery).Scan(
		&job.ID,
		&job.TenantID,
		&job.Status,
		&job.AttemptID,
		&job.BudgetUSD,
		&job.SpentUSD,
		&job.Payload,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil // No jobs waiting
		}
		return nil, err
	}

	// Atomically increment attempt_id and set lease expiration
	job.AttemptID++
	job.Status = "leased"
	expiresAt := time.Now().Add(leaseDuration)
	job.LeaseExpiresAt = &expiresAt

	updateQuery := `
		UPDATE jobs
		SET status = 'leased', attempt_id = $1, lease_expires_at = $2, updated_at = NOW()
		WHERE id = $3
	`
	if _, err := tx.ExecContext(ctx, updateQuery, job.AttemptID, expiresAt, job.ID); err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return &job, nil
}

// Heartbeat checks if attempt_id matches the active leased attempt; if valid, extends lease_expires_at.
func (p *PostgresDB) Heartbeat(ctx context.Context, jobID string, attemptID int64, extension time.Duration) (bool, error) {
	newExpiresAt := time.Now().Add(extension)
	query := `
		UPDATE jobs
		SET lease_expires_at = $1, updated_at = NOW()
		WHERE id = $2 AND attempt_id = $3 AND status = 'leased'
	`
	res, err := p.db.ExecContext(ctx, query, newExpiresAt, jobID, attemptID)
	if err != nil {
		return false, err
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return false, err
	}

	return rows > 0, nil
}

// AckJob marks a job as successfully completed.
func (p *PostgresDB) AckJob(ctx context.Context, jobID string, attemptID int64) error {
	query := `
		UPDATE jobs
		SET status = 'completed', lease_expires_at = NULL, updated_at = NOW()
		WHERE id = $1 AND attempt_id = $2 AND status = 'leased'
	`
	_, err := p.db.ExecContext(ctx, query, jobID, attemptID)
	return err
}

// NackJob marks a job attempt as failed.
func (p *PostgresDB) NackJob(ctx context.Context, jobID string, attemptID int64, retryable bool, maxRetries int) error {
	var newStatus string
	if retryable && attemptID < int64(maxRetries) {
		newStatus = "pending"
	} else {
		newStatus = "dlq"
	}

	query := `
		UPDATE jobs
		SET status = $1, lease_expires_at = NULL, updated_at = NOW()
		WHERE id = $2 AND attempt_id = $3 AND status = 'leased'
	`
	_, err := p.db.ExecContext(ctx, query, newStatus, jobID, attemptID)
	return err
}

// ReapExpiredLeases scans for leased jobs where lease_expires_at < NOW().
func (p *PostgresDB) ReapExpiredLeases(ctx context.Context, maxRetries int) ([]*scheduler.JobRecord, error) {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	selectQuery := `
		SELECT id, tenant_id, status, attempt_id, budget_usd, spent_usd, payload, lease_expires_at
		FROM jobs
		WHERE status = 'leased' AND lease_expires_at < NOW()
		FOR UPDATE SKIP LOCKED
	`

	rows, err := tx.QueryContext(ctx, selectQuery)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var reaped []*scheduler.JobRecord
	for rows.Next() {
		var job scheduler.JobRecord
		if err := rows.Scan(
			&job.ID,
			&job.TenantID,
			&job.Status,
			&job.AttemptID,
			&job.BudgetUSD,
			&job.SpentUSD,
			&job.Payload,
			&job.LeaseExpiresAt,
		); err != nil {
			return nil, err
		}
		reaped = append(reaped, &job)
	}

	for _, job := range reaped {
		newStatus := "pending"
		if job.AttemptID >= int64(maxRetries) {
			newStatus = "dlq"
		}
		job.Status = newStatus

		updateQuery := `
			UPDATE jobs
			SET status = $1, lease_expires_at = NULL, updated_at = NOW()
			WHERE id = $2
		`
		if _, err := tx.ExecContext(ctx, updateQuery, newStatus, job.ID); err != nil {
			return nil, fmt.Errorf("reap update error: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return reaped, nil
}
