package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"time"
	"github.com/switchyard/services/broker/internal/telemetry"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const (
	// DefaultSegmentSizeBytes defines the segment rotation threshold (64MB).
	DefaultSegmentSizeBytes int64 = 64 * 1024 * 1024

	// HeaderSize is 4-byte CRC32 + 4-byte Length + 1-byte EventType = 9 bytes.
	HeaderSize = 9

	// SegmentPrefix is the standard prefix for segment file naming.
	SegmentPrefix = "segment-"

	// SegmentExt is the standard extension for segment files.
	SegmentExt = ".wal"
)

var (
	ErrCorruptRecord = errors.New("wal: corrupt record detected")
	ErrShortRead     = errors.New("wal: unexpected short read")
)

// WAL manages append-only segment files, fsync guarantees, and recovery.
type WAL struct {
	mu          sync.Mutex
	dir         string
	maxSegSize  int64
	activeFile  *os.File
	activeSegID uint64
	activeBytes int64
}

// Options configures WAL operational parameters.
type Options struct {
	MaxSegmentSize int64
}

// Open initializes or loads a WAL in the specified directory.
func Open(dir string, opts *Options) (*WAL, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create wal dir: %w", err)
	}

	maxSize := DefaultSegmentSizeBytes
	if opts != nil && opts.MaxSegmentSize > 0 {
		maxSize = opts.MaxSegmentSize
	}

	w := &WAL{
		dir:        dir,
		maxSegSize: maxSize,
	}

	// Discover existing segments or initialize first segment (00000001)
	segments, err := w.listSegmentIDs()
	if err != nil {
		return nil, fmt.Errorf("failed listing segments: %w", err)
	}

	var latestID uint64 = 1
	if len(segments) > 0 {
		latestID = segments[len(segments)-1]
	}

	filePath := w.segmentPath(latestID)
	f, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed opening active segment: %w", err)
	}

	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("failed stating active segment: %w", err)
	}

	w.activeFile = f
	w.activeSegID = latestID
	w.activeBytes = fi.Size()

	return w, nil
}

// Write appends a record to the WAL: [4-byte CRC32][4-byte Length][1-byte EventType][Payload].
// It automatically rotates segments when maxSegSize is reached and calls file.Sync() for fsync durability.
func (w *WAL) Write(eventType EventType, payload []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	payloadLen := uint32(len(payload))
	totalRecordLen := int64(HeaderSize + payloadLen)

	// Rotate segment file if threshold reached
	if w.activeBytes+totalRecordLen > w.maxSegSize && w.activeBytes > 0 {
		if err := w.rotate(); err != nil {
			return fmt.Errorf("failed rotating segment: %w", err)
		}
	}

	// Calculate CRC32 checksum over [4-byte Length][1-byte EventType][Payload]
	checksumBuf := make([]byte, 4+1+len(payload))
	binary.BigEndian.PutUint32(checksumBuf[0:4], payloadLen)
	checksumBuf[4] = byte(eventType)
	copy(checksumBuf[5:], payload)

	checksum := crc32.ChecksumIEEE(checksumBuf)

	// Build full record buffer: [4-byte CRC32] + checksumBuf
	recordBuf := make([]byte, HeaderSize+payloadLen)
	binary.BigEndian.PutUint32(recordBuf[0:4], checksum)
	copy(recordBuf[4:], checksumBuf)

	// Append to active file
	n, err := w.activeFile.Write(recordBuf)
	if err != nil {
		return fmt.Errorf("failed writing record to wal: %w", err)
	}

	// Guarantee crash-safety via fsync
	startSync := time.Now()
	err = w.activeFile.Sync()
	telemetry.WALFsyncDurationSeconds.Observe(time.Since(startSync).Seconds())
	if err != nil {
		return fmt.Errorf("failed syncing wal file: %w", err)
	}

	w.activeBytes += int64(n)
	return nil
}

// Close syncs and closes the active WAL segment.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.activeFile != nil {
		_ = w.activeFile.Sync()
		err := w.activeFile.Close()
		w.activeFile = nil
		return err
	}
	return nil
}

// rotate rolls over to a new sequentially numbered segment file.
func (w *WAL) rotate() error {
	if w.activeFile != nil {
		_ = w.activeFile.Sync()
		_ = w.activeFile.Close()
	}

	w.activeSegID++
	filePath := w.segmentPath(w.activeSegID)

	f, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return err
	}

	w.activeFile = f
	w.activeBytes = 0
	return nil
}

// segmentPath returns the absolute path for a given segment sequence ID.
func (w *WAL) segmentPath(id uint64) string {
	fileName := fmt.Sprintf("%s%08d%s", SegmentPrefix, id, SegmentExt)
	return filepath.Join(w.dir, fileName)
}

// listSegmentIDs scans the WAL directory and returns all sorted segment IDs.
func (w *WAL) listSegmentIDs() ([]uint64, error) {
	entries, err := os.ReadDir(w.dir)
	if err != nil {
		return nil, err
	}

	var ids []uint64
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if strings.HasPrefix(name, SegmentPrefix) && strings.HasSuffix(name, SegmentExt) {
			numPart := strings.TrimSuffix(strings.TrimPrefix(name, SegmentPrefix), SegmentExt)
			if id, err := strconv.ParseUint(numPart, 10, 64); err == nil {
				ids = append(ids, id)
			}
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids, nil
}
