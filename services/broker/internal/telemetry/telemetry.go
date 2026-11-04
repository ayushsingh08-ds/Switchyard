package telemetry

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc/metadata"
)

var (
	// Prometheus metrics for Go Broker
	JobsLeasedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "switchyard_jobs_leased_total",
		Help: "Total number of jobs leased to workers.",
	})
	LeaseExpiredTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "switchyard_lease_expired_total",
		Help: "Total number of expired leases reaped.",
	})
	WALFsyncDurationSeconds = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "switchyard_wal_fsync_duration_seconds",
		Help:    "Duration of WAL fsync operations in seconds.",
		Buckets: prometheus.DefBuckets,
	})
)

func init() {
	prometheus.MustRegister(JobsLeasedTotal)
	prometheus.MustRegister(LeaseExpiredTotal)
	prometheus.MustRegister(WALFsyncDurationSeconds)
}

// StartMetricsServer starts an HTTP server serving /metrics for Prometheus.
func StartMetricsServer(addr string) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	srv := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	go func() {
		_ = srv.ListenAndServe()
	}()

	return srv
}

// ExtractTraceID parses the W3C traceparent header or gRPC metadata (00-<trace_id>-<span_id>-<flags>).
func ExtractTraceID(traceparent string) string {
	parts := strings.Split(traceparent, "-")
	if len(parts) >= 2 && len(parts[1]) == 32 {
		return parts[1]
	}
	return ""
}

// TraceparentFromMD extracts W3C traceparent from incoming gRPC metadata.
func TraceparentFromMD(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	vals := md.Get("traceparent")
	if len(vals) > 0 {
		return vals[0]
	}
	return ""
}

// FormatTraceparent creates a W3C traceparent header string given a trace_id.
func FormatTraceparent(traceID string) string {
	if len(traceID) == 32 {
		return fmt.Sprintf("00-%s-0000000000000001-01", traceID)
	}
	return fmt.Sprintf("00-%032x-%016x-01", time.Now().UnixNano(), 1)
}
