package scheduler

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/switchyard/services/broker/internal/wal"
)

type mockWAL struct {
	mu      sync.Mutex
	records []wal.Record
}

func (m *mockWAL) Write(eventType wal.EventType, payload []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.records = append(m.records, wal.Record{Type: eventType, Payload: payload})
	return nil
}

// TestScheduler_IntegrationReaperTimeout verifies that:
// 1. Worker leases a job (attempt_id = 1).
// 2. Worker stops heartbeating.
// 3. Reaper background ticker detects lease expiration.
// 4. Job is moved back to QUEUED (pending).
// 5. Next worker leases the job and receives attempt_id = 2.
func TestScheduler_IntegrationReaperTimeout(t *testing.T) {
	memDB := NewMemoryDB()
	walMock := &mockWAL{}

	// Configure fast lease and reaper ticks for testing
	cfg := &Config{
		LeaseDuration: 150 * time.Millisecond,
		HeartbeatExt:  150 * time.Millisecond,
		MaxRetries:    3,
		ReaperFreq:    50 * time.Millisecond,
	}

	sched := NewScheduler(memDB, walMock, nil, cfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sched.Start(ctx)
	defer sched.Stop()

	// Enqueue job
	jobID := "job-integration-1"
	tenantID := "tenant-alpha"
	payload := []byte(`{"task": "process_doc"}`)

	if err := sched.Enqueue(ctx, jobID, tenantID, 1.0, payload); err != nil {
		t.Fatalf("failed to enqueue job: %v", err)
	}

	// 1. First Worker leases the job
	lease1, err := sched.LeaseNextJob(ctx)
	if err != nil {
		t.Fatalf("first lease failed: %v", err)
	}
	if lease1 == nil {
		t.Fatal("expected a job to be leased, got nil")
	}
	if lease1.ID != jobID || lease1.AttemptID != 1 {
		t.Fatalf("expected job %s with attempt 1, got %+v", jobID, lease1)
	}

	// 2. Heartbeat check: Valid attempt succeeds, invalid attempt fails
	validHB, err := sched.Heartbeat(ctx, jobID, 1)
	if err != nil || !validHB {
		t.Fatalf("expected heartbeat with attempt 1 to succeed, got %v (err: %v)", validHB, err)
	}

	// Stale fencing token heartbeat fails
	invalidHB, err := sched.Heartbeat(ctx, jobID, 999)
	if err != nil || invalidHB {
		t.Fatalf("expected heartbeat with wrong attempt to fail, got %v", invalidHB)
	}

	// 3. Worker stops heartbeating and waits for Reaper timeout
	// Lease expires after 150ms; sleep 250ms for Reaper (ticks at 50ms) to reap it
	time.Sleep(250 * time.Millisecond)

	// 4. Second worker leases the job
	lease2, err := sched.LeaseNextJob(ctx)
	if err != nil {
		t.Fatalf("second lease failed: %v", err)
	}
	if lease2 == nil {
		t.Fatal("expected job to be re-leased after reaper timeout, got nil")
	}

	// 5. Verify the second worker receives attempt_id = 2
	if lease2.AttemptID != 2 {
		t.Fatalf("expected attempt_id = 2 for re-leased job, got %d", lease2.AttemptID)
	}
	if lease2.ID != jobID {
		t.Fatalf("expected job ID %s, got %s", jobID, lease2.ID)
	}

	// 6. Old worker late heartbeat must be rejected (fencing token protects against zombie workers)
	lateHB, err := sched.Heartbeat(ctx, jobID, 1)
	if err != nil || lateHB {
		t.Fatalf("expected old worker attempt 1 heartbeat to be rejected after re-lease, got %v", lateHB)
	}

	// 7. Second worker acks the job
	if err := sched.AckJob(ctx, jobID, 2); err != nil {
		t.Fatalf("failed to ack job at attempt 2: %v", err)
	}

	// No more jobs left
	emptyLease, err := sched.LeaseNextJob(ctx)
	if err != nil {
		t.Fatalf("unexpected error checking empty queue: %v", err)
	}
	if emptyLease != nil {
		t.Fatalf("expected no jobs available after ack, got %+v", emptyLease)
	}
}

// TestScheduler_MaxRetriesToDLQ verifies that repeated lease failures move the job to DLQ.
func TestScheduler_MaxRetriesToDLQ(t *testing.T) {
	memDB := NewMemoryDB()
	cfg := &Config{
		LeaseDuration: 50 * time.Millisecond,
		MaxRetries:    2,
		ReaperFreq:    25 * time.Millisecond,
	}

	sched := NewScheduler(memDB, nil, nil, cfg)
	ctx := context.Background()
	sched.Start(ctx)
	defer sched.Stop()

	jobID := "job-dlq-test"
	_ = sched.Enqueue(ctx, jobID, "tenant-x", 1.0, []byte("{}"))

	// Lease 1 (attempt 1) -> let expire
	l1, _ := sched.LeaseNextJob(ctx)
	if l1.AttemptID != 1 {
		t.Fatalf("expected attempt 1, got %d", l1.AttemptID)
	}
	time.Sleep(100 * time.Millisecond) // reaped to pending

	// Lease 2 (attempt 2) -> let expire
	l2, _ := sched.LeaseNextJob(ctx)
	if l2.AttemptID != 2 {
		t.Fatalf("expected attempt 2, got %d", l2.AttemptID)
	}
	time.Sleep(100 * time.Millisecond) // reaped to DLQ (reached maxRetries: 2)

	// Subsequent lease attempt should yield nil because job is in DLQ
	l3, _ := sched.LeaseNextJob(ctx)
	if l3 != nil {
		t.Fatalf("expected job to be in DLQ, but was leased: %+v", l3)
	}
}
