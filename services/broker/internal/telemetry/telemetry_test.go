package telemetry

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/metadata"
)

func testTraceExtraction(t *testing.T) {
	traceID := "4bf92f3577b34da6a3ce929d0e0e4736"
	traceparent := "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

	extracted := ExtractTraceID(traceparent)
	if extracted != traceID {
		t.Fatalf("expected trace ID %s, got %s", traceID, extracted)
	}

	md := metadata.Pairs("traceparent", traceparent)
	ctx := metadata.NewIncomingContext(context.Background(), md)
	fromMD := TraceparentFromMD(ctx)
	if fromMD != traceparent {
		t.Fatalf("expected traceparent %s, got %s", traceparent, fromMD)
	}
}

func TestTelemetryMetrics(t *testing.T) {
	testTraceExtraction(t)

	// Increment metrics
	JobsLeasedTotal.Inc()
	LeaseExpiredTotal.Inc()
	WALFsyncDurationSeconds.Observe(0.005)

	// Test HTTP handler
	srv := StartMetricsServer(":0")
	defer srv.Close()
	time.Sleep(50 * time.Millisecond)

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	srv.Handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK from /metrics, got %d", rec.Code)
	}

	body := rec.Body.String()
	if !strings.Contains(body, "switchyard_jobs_leased_total") {
		t.Fatalf("expected body to contain switchyard_jobs_leased_total")
	}
	if !strings.Contains(body, "switchyard_lease_expired_total") {
		t.Fatalf("expected body to contain switchyard_lease_expired_total")
	}
	if !strings.Contains(body, "switchyard_wal_fsync_duration_seconds") {
		t.Fatalf("expected body to contain switchyard_wal_fsync_duration_seconds")
	}
}
