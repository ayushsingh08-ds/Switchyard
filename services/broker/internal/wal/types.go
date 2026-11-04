package wal

import "fmt"

// EventType represents the 1-byte lifecycle action recorded in the WAL.
type EventType byte

const (
	EventEnqueue EventType = 1
	EventLease   EventType = 2
	EventAck     EventType = 3
	EventNack    EventType = 4
	EventDLQ     EventType = 5
)

// String returns a human-readable representation of the event type.
func (e EventType) String() string {
	switch e {
	case EventEnqueue:
		return "ENQUEUE"
	case EventLease:
		return "LEASE"
	case EventAck:
		return "ACK"
	case EventNack:
		return "NACK"
	case EventDLQ:
		return "DLQ"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", byte(e))
	}
}

// Record represents a single structured entry in the Write-Ahead Log.
type Record struct {
	Type    EventType
	Payload []byte
}

// JobState models the in-memory reconstructed state of an active job during replay.
type JobState struct {
	JobID     string
	Status    string
	AttemptID int64
	Payload   []byte
}
