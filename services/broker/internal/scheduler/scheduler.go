package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/switchyard/services/broker/internal/ratelimit"
	"github.com/switchyard/services/broker/internal/telemetry"
	"github.com/switchyard/services/broker/internal/wal"
)

const (
	DefaultLeaseDuration = 10 * time.Second
	DefaultHeartbeatExt  = 10 * time.Second
	DefaultMaxRetries    = 3
	ReaperInterval       = 1 * time.Second
)

// WALWriter abstracts the Write operation from wal.WAL for clean testing and loose coupling.
type WALWriter interface {
	Write(eventType wal.EventType, payload []byte) error
}

// Config configures the Scheduler.
type Config struct {
	LeaseDuration time.Duration
	HeartbeatExt  time.Duration
	MaxRetries    int
	ReaperFreq    time.Duration
}

// Scheduler coordinates job queuing, leasing, heartbeats, WAL persistence, and reaping.
type Scheduler struct {
	db      DB
	wal     WALWriter
	limiter *ratelimit.TenantLimiter
	cfg     Config

	mu     sync.Mutex
	stopCh chan struct{}
	wg     sync.WaitGroup
}

// NewScheduler creates and initializes a Scheduler instance.
func NewScheduler(db DB, wal WALWriter, limiter *ratelimit.TenantLimiter, cfg *Config) *Scheduler {
	c := Config{
		LeaseDuration: DefaultLeaseDuration,
		HeartbeatExt:  DefaultHeartbeatExt,
		MaxRetries:    DefaultMaxRetries,
		ReaperFreq:    ReaperInterval,
	}
	if cfg != nil {
		if cfg.LeaseDuration > 0 {
			c.LeaseDuration = cfg.LeaseDuration
		}
		if cfg.HeartbeatExt > 0 {
			c.HeartbeatExt = cfg.HeartbeatExt
		}
		if cfg.MaxRetries > 0 {
			c.MaxRetries = cfg.MaxRetries
		}
		if cfg.ReaperFreq > 0 {
			c.ReaperFreq = cfg.ReaperFreq
		}
	}

	if limiter == nil {
		limiter = ratelimit.NewTenantLimiter(100, 100)
	}

	return &Scheduler{
		db:      db,
		wal:     wal,
		limiter: limiter,
		cfg:     c,
		stopCh:  make(chan struct{}),
	}
}

// Start launches the background Reaper goroutine.
func (s *Scheduler) Start(ctx context.Context) {
	s.wg.Add(1)
	go s.runReaper(ctx)
}

// Stop gracefully signals and waits for the Reaper to shut down.
func (s *Scheduler) Stop() {
	s.mu.Lock()
	select {
	case <-s.stopCh:
		// already closed
	default:
		close(s.stopCh)
	}
	s.mu.Unlock()

	s.wg.Wait()
}

// Enqueue validates tenant rate quota, writes to WAL, and inserts into DB.
func (s *Scheduler) Enqueue(ctx context.Context, jobID, tenantID string, budgetUSD float64, payload []byte) error {
	if !s.limiter.Allow(tenantID) {
		return fmt.Errorf("tenant %s rate limit exceeded", tenantID)
	}

	// 1. Write to Write-Ahead Log first
	walPayload, _ := json.Marshal(map[string]interface{}{
		"job_id":     jobID,
		"tenant_id":  tenantID,
		"budget_usd": budgetUSD,
		"payload":    string(payload),
	})
	if s.wal != nil {
		if err := s.wal.Write(wal.EventEnqueue, walPayload); err != nil {
			return fmt.Errorf("failed appending enqueue to wal: %w", err)
		}
	}

	// 2. Persist in Database
	record := &JobRecord{
		ID:        jobID,
		TenantID:  tenantID,
		BudgetUSD: budgetUSD,
		Payload:   payload,
	}
	if err := s.db.CreateJob(ctx, record); err != nil {
		return fmt.Errorf("failed saving job to db: %w", err)
	}

	return nil
}

// LeaseNextJob atomically leases the next available job, sets lease expiration, and records LEASE in WAL.
func (s *Scheduler) LeaseNextJob(ctx context.Context) (*JobRecord, error) {
	job, err := s.db.LeaseNextJob(ctx, s.cfg.LeaseDuration)
	if err != nil || job == nil {
		return job, err
	}

	telemetry.JobsLeasedTotal.Inc()

	// Append lease event to WAL with new attempt_id fencing token
	if s.wal != nil {
		walPayload, _ := json.Marshal(map[string]interface{}{
			"job_id":     job.ID,
			"attempt_id": job.AttemptID,
			"tenant_id":  job.TenantID,
		})
		_ = s.wal.Write(wal.EventLease, walPayload)
	}

	return job, nil
}

// Heartbeat checks if attempt_id matches the active attempt. If valid, extends lease by HeartbeatExt.
func (s *Scheduler) Heartbeat(ctx context.Context, jobID string, attemptID int64) (bool, error) {
	valid, err := s.db.Heartbeat(ctx, jobID, attemptID, s.cfg.HeartbeatExt)
	if err != nil {
		return false, err
	}
	return valid, nil
}

// AckJob marks a job as successfully finished and logs ACK in WAL.
func (s *Scheduler) AckJob(ctx context.Context, jobID string, attemptID int64) error {
	if err := s.db.AckJob(ctx, jobID, attemptID); err != nil {
		return err
	}

	if s.wal != nil {
		walPayload, _ := json.Marshal(map[string]interface{}{
			"job_id":     jobID,
			"attempt_id": attemptID,
		})
		_ = s.wal.Write(wal.EventAck, walPayload)
	}

	return nil
}

// NackJob marks a job as failed and writes NACK or DLQ to WAL.
func (s *Scheduler) NackJob(ctx context.Context, jobID string, attemptID int64, retryable bool) error {
	if err := s.db.NackJob(ctx, jobID, attemptID, retryable, s.cfg.MaxRetries); err != nil {
		return err
	}

	if s.wal != nil {
		walPayload, _ := json.Marshal(map[string]interface{}{
			"job_id":     jobID,
			"attempt_id": attemptID,
		})
		evType := wal.EventNack
		if !retryable || attemptID >= int64(s.cfg.MaxRetries) {
			evType = wal.EventDLQ
		}
		_ = s.wal.Write(evType, walPayload)
	}

	return nil
}

// runReaper ticks periodically to detect expired leases and return them to the queue or DLQ.
func (s *Scheduler) runReaper(ctx context.Context) {
	defer s.wg.Done()
	ticker := time.NewTicker(s.cfg.ReaperFreq)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-s.stopCh:
			return
		case <-ticker.C:
			s.reap(ctx)
		}
	}
}

func (s *Scheduler) reap(ctx context.Context) {
	reaped, err := s.db.ReapExpiredLeases(ctx, s.cfg.MaxRetries)
	if err != nil || len(reaped) == 0 {
		return
	}

	for _, job := range reaped {
		telemetry.LeaseExpiredTotal.Inc()
		if s.wal != nil {
			walPayload, _ := json.Marshal(map[string]interface{}{
				"job_id":     job.ID,
				"attempt_id": job.AttemptID,
			})
			evType := wal.EventNack
			if job.Status == "dlq" {
				evType = wal.EventDLQ
			}
			_ = s.wal.Write(evType, walPayload)
		}
	}
}
