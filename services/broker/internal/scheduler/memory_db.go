package scheduler

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"
)

// MemoryDB provides an in-memory implementation of DB for high-speed deterministic testing.
type MemoryDB struct {
	mu   sync.Mutex
	jobs map[string]*JobRecord
}

// NewMemoryDB creates an empty in-memory test database.
func NewMemoryDB() *MemoryDB {
	return &MemoryDB{
		jobs: make(map[string]*JobRecord),
	}
}

func (m *MemoryDB) CreateJob(ctx context.Context, job *JobRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	rec := *job
	rec.Status = "pending"
	rec.AttemptID = 0
	rec.CreatedAt = time.Now()
	rec.UpdatedAt = time.Now()
	m.jobs[job.ID] = &rec
	return nil
}

func (m *MemoryDB) LeaseNextJob(ctx context.Context, leaseDuration time.Duration) (*JobRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var candidate *JobRecord
	for _, j := range m.jobs {
		if j.Status == "pending" {
			if candidate == nil || j.CreatedAt.Before(candidate.CreatedAt) {
				candidate = j
			}
		}
	}

	if candidate == nil {
		return nil, nil
	}

	candidate.AttemptID++
	candidate.Status = "leased"
	expires := time.Now().Add(leaseDuration)
	candidate.LeaseExpiresAt = &expires
	candidate.UpdatedAt = time.Now()

	// Return a copy
	res := *candidate
	return &res, nil
}

func (m *MemoryDB) Heartbeat(ctx context.Context, jobID string, attemptID int64, extension time.Duration) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	j, ok := m.jobs[jobID]
	if !ok || j.Status != "leased" || j.AttemptID != attemptID {
		return false, nil
	}

	expires := time.Now().Add(extension)
	j.LeaseExpiresAt = &expires
	j.UpdatedAt = time.Now()
	return true, nil
}

func (m *MemoryDB) AckJob(ctx context.Context, jobID string, attemptID int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	j, ok := m.jobs[jobID]
	if !ok || j.Status != "leased" || j.AttemptID != attemptID {
		return fmt.Errorf("job not found or attempt mismatch")
	}

	j.Status = "completed"
	j.LeaseExpiresAt = nil
	j.UpdatedAt = time.Now()
	return nil
}

func (m *MemoryDB) NackJob(ctx context.Context, jobID string, attemptID int64, retryable bool, maxRetries int) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	j, ok := m.jobs[jobID]
	if !ok || j.Status != "leased" || j.AttemptID != attemptID {
		return fmt.Errorf("job not found or attempt mismatch")
	}

	if retryable && j.AttemptID < int64(maxRetries) {
		j.Status = "pending"
	} else {
		j.Status = "dlq"
	}
	j.LeaseExpiresAt = nil
	j.UpdatedAt = time.Now()
	return nil
}

func (m *MemoryDB) ReapExpiredLeases(ctx context.Context, maxRetries int) ([]*JobRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	var reaped []*JobRecord

	for _, j := range m.jobs {
		if j.Status == "leased" && j.LeaseExpiresAt != nil && j.LeaseExpiresAt.Before(now) {
			if j.AttemptID >= int64(maxRetries) {
				j.Status = "dlq"
			} else {
				j.Status = "pending"
			}
			j.LeaseExpiresAt = nil
			j.UpdatedAt = now

			rec := *j
			reaped = append(reaped, &rec)
		}
	}

	return reaped, nil
}

// Ensure MemoryDB satisfies DB interface.
var _ DB = (*MemoryDB)(nil)
var _ = sql.ErrNoRows
