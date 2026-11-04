package wal

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

// ReplayHandler is an optional callback invoked for every valid record replayed.
type ReplayHandler func(rec Record) error

// Replay scans all WAL segments sequentially, validates CRC32 checksums,
// truncates any half-written or torn tail records from an abrupt crash,
// and reconstructs in-memory active job states.
func (w *WAL) Replay(handler ReplayHandler) (map[string]*JobState, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Ensure any buffered writes on the active file are committed
	if w.activeFile != nil {
		_ = w.activeFile.Sync()
	}

	segmentIDs, err := w.listSegmentIDs()
	if err != nil {
		return nil, fmt.Errorf("failed listing segments for replay: %w", err)
	}

	activeJobs := make(map[string]*JobState)

	for _, segID := range segmentIDs {
		segPath := w.segmentPath(segID)
		if err := w.replaySegment(segPath, activeJobs, handler); err != nil {
			return nil, fmt.Errorf("error replaying segment %s: %w", segPath, err)
		}
	}

	// Update active file size after potential tail truncation
	if w.activeFile != nil {
		if fi, err := w.activeFile.Stat(); err == nil {
			w.activeBytes = fi.Size()
		}
	}

	return activeJobs, nil
}

// replaySegment reads a single segment file, verifying CRC32 checksums and repairing torn writes at the tail.
func (w *WAL) replaySegment(filePath string, activeJobs map[string]*JobState, handler ReplayHandler) error {
	f, err := os.OpenFile(filePath, os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	var lastValidOffset int64 = 0

	for {
		header := make([]byte, HeaderSize)
		n, err := io.ReadFull(f, header)
		if err != nil {
			if errors.Is(err, io.EOF) {
				// Clean EOF at boundary
				break
			}
			// Incomplete header -> torn write at tail; truncate and recover
			if err == io.ErrUnexpectedEOF || (n > 0 && n < HeaderSize) {
				if truncErr := f.Truncate(lastValidOffset); truncErr != nil {
					return fmt.Errorf("failed truncating torn header: %w", truncErr)
				}
				_ = f.Sync()
				break
			}
			return err
		}

		expectedCRC := binary.BigEndian.Uint32(header[0:4])
		payloadLen := binary.BigEndian.Uint32(header[4:8])
		eventType := EventType(header[8])

		// Read payload
		payload := make([]byte, payloadLen)
		pn, err := io.ReadFull(f, payload)
		if err != nil {
			// Incomplete payload -> torn write at tail; truncate to last valid record offset
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || (pn >= 0 && uint32(pn) < payloadLen) {
				if truncErr := f.Truncate(lastValidOffset); truncErr != nil {
					return fmt.Errorf("failed truncating torn payload: %w", truncErr)
				}
				_ = f.Sync()
				break
			}
			return err
		}

		// Verify CRC32 checksum over [4-byte Length][1-byte EventType][Payload]
		checksumBuf := make([]byte, 4+1+len(payload))
		binary.BigEndian.PutUint32(checksumBuf[0:4], payloadLen)
		checksumBuf[4] = byte(eventType)
		copy(checksumBuf[5:], payload)

		actualCRC := crc32.ChecksumIEEE(checksumBuf)
		if actualCRC != expectedCRC {
			// Corrupted record / torn write detected at this offset:
			// Truncate segment to restore clean state and discard the corrupted tail
			if truncErr := f.Truncate(lastValidOffset); truncErr != nil {
				return fmt.Errorf("failed truncating corrupted tail record: %w", truncErr)
			}
			_ = f.Sync()
			break
		}

		// Successfully validated record
		lastValidOffset += int64(HeaderSize + payloadLen)

		rec := Record{
			Type:    eventType,
			Payload: payload,
		}

		// Reconstruct in-memory active job states
		updateJobState(activeJobs, rec)

		if handler != nil {
			if err := handler(rec); err != nil {
				return fmt.Errorf("replay handler error: %w", err)
			}
		}
	}

	return nil
}

// JobPayloadHelper provides a minimal JSON model for state reconstruction across lifecycle events.
type JobPayloadHelper struct {
	JobID     string `json:"job_id"`
	AttemptID int64  `json:"attempt_id,omitempty"`
}

// updateJobState applies transitions to reconstruct active jobs in memory.
func updateJobState(activeJobs map[string]*JobState, rec Record) {
	var meta JobPayloadHelper
	if err := json.Unmarshal(rec.Payload, &meta); err != nil || meta.JobID == "" {
		return
	}

	switch rec.Type {
	case EventEnqueue:
		activeJobs[meta.JobID] = &JobState{
			JobID:     meta.JobID,
			Status:    "pending",
			AttemptID: 0,
			Payload:   rec.Payload,
		}
	case EventLease:
		if job, ok := activeJobs[meta.JobID]; ok {
			job.Status = "leased"
			if meta.AttemptID > 0 {
				job.AttemptID = meta.AttemptID
			}
		}
	case EventAck:
		// Terminal state: completed job removed from active set
		delete(activeJobs, meta.JobID)
	case EventNack:
		if job, ok := activeJobs[meta.JobID]; ok {
			job.Status = "pending" // returned to queue for retry
		}
	case EventDLQ:
		// Terminal state: moved to dead-letter queue, removed from active set
		delete(activeJobs, meta.JobID)
	}
}
