package wal

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// TestWAL_NormalAppendAndReplay tests standard lifecycle operations across segment boundaries.
func TestWAL_NormalAppendAndReplay(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "wal_test_*")
	if err != nil {
		t.Fatalf("failed creating temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Configure smaller segment size to trigger rotation during test
	opts := &Options{
		MaxSegmentSize: 120, // small size to force multiple segment files
	}

	w, err := Open(tempDir, opts)
	if err != nil {
		t.Fatalf("failed to open WAL: %v", err)
	}

	job1Payload, _ := json.Marshal(map[string]interface{}{"job_id": "job-1", "data": "task-a"})
	job2Payload, _ := json.Marshal(map[string]interface{}{"job_id": "job-2", "data": "task-b"})
	job1Lease, _ := json.Marshal(map[string]interface{}{"job_id": "job-1", "attempt_id": 1})
	job2Ack, _ := json.Marshal(map[string]interface{}{"job_id": "job-2"})

	// Write events
	if err := w.Write(EventEnqueue, job1Payload); err != nil {
		t.Fatalf("failed writing job1 enqueue: %v", err)
	}
	if err := w.Write(EventEnqueue, job2Payload); err != nil {
		t.Fatalf("failed writing job2 enqueue: %v", err)
	}
	if err := w.Write(EventLease, job1Lease); err != nil {
		t.Fatalf("failed writing job1 lease: %v", err)
	}
	if err := w.Write(EventAck, job2Ack); err != nil {
		t.Fatalf("failed writing job2 ack: %v", err)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("failed closing WAL: %v", err)
	}

	// Verify segment rotation occurred
	segFiles, _ := filepath.Glob(filepath.Join(tempDir, "segment-*.wal"))
	if len(segFiles) < 2 {
		t.Fatalf("expected at least 2 rotated segments, got %d", len(segFiles))
	}

	// Reopen WAL and Replay
	wReopened, err := Open(tempDir, opts)
	if err != nil {
		t.Fatalf("failed reopening WAL: %v", err)
	}
	defer wReopened.Close()

	var replayedCount int
	activeJobs, err := wReopened.Replay(func(rec Record) error {
		replayedCount++
		return nil
	})
	if err != nil {
		t.Fatalf("replay failed: %v", err)
	}

	if replayedCount != 4 {
		t.Fatalf("expected 4 replayed events, got %d", replayedCount)
	}

	// job-1 should be active (status: leased, attempt: 1)
	job1, exists := activeJobs["job-1"]
	if !exists {
		t.Fatalf("expected job-1 in active jobs")
	}
	if job1.Status != "leased" || job1.AttemptID != 1 {
		t.Fatalf("unexpected state for job-1: %+v", job1)
	}

	// job-2 was acked, so it must not be in the active reconstructed map
	if _, exists := activeJobs["job-2"]; exists {
		t.Fatalf("expected job-2 to be cleared after ACK, but found in active map")
	}
}

// TestWAL_SimulatedTornWrite tests recovery and truncation of corrupted half-written tail records.
func TestWAL_SimulatedTornWrite(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "wal_torn_test_*")
	if err != nil {
		t.Fatalf("failed creating temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	w, err := Open(tempDir, nil)
	if err != nil {
		t.Fatalf("failed to open WAL: %v", err)
	}

	// Write 2 valid records
	p1, _ := json.Marshal(map[string]interface{}{"job_id": "job-100"})
	p2, _ := json.Marshal(map[string]interface{}{"job_id": "job-200"})

	_ = w.Write(EventEnqueue, p1)
	_ = w.Write(EventEnqueue, p2)
	_ = w.Close()

	// Locate the active segment file
	segFiles, _ := filepath.Glob(filepath.Join(tempDir, "segment-*.wal"))
	if len(segFiles) == 0 {
		t.Fatal("no segment file found")
	}
	targetSeg := segFiles[0]

	// Simulate torn write: append 5 corrupted garbage bytes to simulate a sudden crash mid-write
	f, err := os.OpenFile(targetSeg, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("failed opening segment for corruption simulation: %v", err)
	}
	corruptedBytes := []byte{0xDE, 0xAD, 0xBE, 0xEF, 0x01}
	if _, err := f.Write(corruptedBytes); err != nil {
		t.Fatalf("failed appending corrupted bytes: %v", err)
	}
	_ = f.Close()

	// Reopen WAL: Replay() must detect the CRC mismatch / incomplete record and truncate it
	wRecovered, err := Open(tempDir, nil)
	if err != nil {
		t.Fatalf("failed reopening WAL: %v", err)
	}
	defer wRecovered.Close()

	var validCount int
	activeJobs, err := wRecovered.Replay(func(rec Record) error {
		validCount++
		return nil
	})
	if err != nil {
		t.Fatalf("replay unexpectedly failed instead of truncating torn write: %v", err)
	}

	if validCount != 2 {
		t.Fatalf("expected 2 valid records after truncating torn write, got %d", validCount)
	}

	if len(activeJobs) != 2 {
		t.Fatalf("expected 2 active jobs reconstructed, got %d", len(activeJobs))
	}

	// Ensure WAL can continue appending new valid records after truncation
	p3, _ := json.Marshal(map[string]interface{}{"job_id": "job-300"})
	if err := wRecovered.Write(EventEnqueue, p3); err != nil {
		t.Fatalf("failed writing new record after recovery: %v", err)
	}

	// Verify post-recovery record is replayed cleanly
	postActiveJobs, err := wRecovered.Replay(nil)
	if err != nil {
		t.Fatalf("second replay failed: %v", err)
	}
	if len(postActiveJobs) != 3 {
		t.Fatalf("expected 3 active jobs after adding job-300, got %d", len(postActiveJobs))
	}
}

// TestWAL_ReconstructLifecycle verifies state machine transitions: Enqueue -> Lease -> Nack -> Lease -> Ack / DLQ.
func TestWAL_ReconstructLifecycle(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "wal_lifecycle_*")
	if err != nil {
		t.Fatalf("failed creating temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	w, err := Open(tempDir, nil)
	if err != nil {
		t.Fatalf("failed opening WAL: %v", err)
	}

	// Job lifecycle:
	// 1. Enqueue job-A
	pEnqueue, _ := json.Marshal(map[string]interface{}{"job_id": "job-A"})
	_ = w.Write(EventEnqueue, pEnqueue)

	// 2. Lease attempt 1
	pLease1, _ := json.Marshal(map[string]interface{}{"job_id": "job-A", "attempt_id": 1})
	_ = w.Write(EventLease, pLease1)

	// 3. Nack attempt 1 -> returns to pending
	pNack, _ := json.Marshal(map[string]interface{}{"job_id": "job-A"})
	_ = w.Write(EventNack, pNack)

	// 4. Lease attempt 2
	pLease2, _ := json.Marshal(map[string]interface{}{"job_id": "job-A", "attempt_id": 2})
	_ = w.Write(EventLease, pLease2)

	// 5. DLQ job-B
	pBEnqueue, _ := json.Marshal(map[string]interface{}{"job_id": "job-B"})
	pBDLQ, _ := json.Marshal(map[string]interface{}{"job_id": "job-B"})
	_ = w.Write(EventEnqueue, pBEnqueue)
	_ = w.Write(EventDLQ, pBDLQ)

	active, err := w.Replay(nil)
	if err != nil {
		t.Fatalf("replay failed: %v", err)
	}

	jobA, ok := active["job-A"]
	if !ok {
		t.Fatalf("expected job-A to be in active jobs")
	}
	if jobA.Status != "leased" || jobA.AttemptID != 2 {
		t.Fatalf("expected job-A leased at attempt 2, got: %+v", jobA)
	}

	if _, ok := active["job-B"]; ok {
		t.Fatalf("expected job-B to be cleared by DLQ event")
	}

	_ = w.Close()
}
