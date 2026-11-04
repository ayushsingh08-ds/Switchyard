package server

import (
	"context"
	"fmt"
	"time"

	brokerpb "github.com/switchyard/services/broker/pkg/pb"
	"github.com/switchyard/services/broker/internal/scheduler"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// BrokerServer implements brokerpb.BrokerServiceServer using the Scheduler.
type BrokerServer struct {
	brokerpb.UnimplementedBrokerServiceServer
	scheduler *scheduler.Scheduler
}

// NewBrokerServer creates a new gRPC service instance.
func NewBrokerServer(s *scheduler.Scheduler) *BrokerServer {
	return &BrokerServer{scheduler: s}
}

// EnqueueJob receives a new job and enqueues it via the Scheduler.
func (s *BrokerServer) EnqueueJob(ctx context.Context, req *brokerpb.EnqueueRequest) (*brokerpb.EnqueueResponse, error) {
	if req.TenantId == "" {
		return nil, status.Error(codes.InvalidArgument, "tenant_id is required")
	}

	jobID := fmt.Sprintf("job-%d", time.Now().UnixNano())
	err := s.scheduler.Enqueue(ctx, jobID, req.TenantId, req.BudgetUsd, []byte(req.PayloadJson))
	if err != nil {
		return nil, status.Errorf(codes.ResourceExhausted, "failed to enqueue job: %v", err)
	}

	return &brokerpb.EnqueueResponse{
		JobId:  jobID,
		Status: "pending",
	}, nil
}

// LeaseJobs streams job leases to a connected worker.
func (s *BrokerServer) LeaseJobs(identity *brokerpb.WorkerIdentity, stream brokerpb.BrokerService_LeaseJobsServer) error {
	ctx := stream.Context()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			job, err := s.scheduler.LeaseNextJob(ctx)
			if err != nil {
				return status.Errorf(codes.Internal, "lease error: %v", err)
			}

			if job != nil {
				lease := &brokerpb.JobLease{
					JobId:       job.ID,
					TenantId:    job.TenantID,
					PayloadJson: string(job.Payload),
					AttemptId:   job.AttemptID,
				}
				if err := stream.Send(lease); err != nil {
					return err
				}
			} else {
				// Back off briefly before next lease poll
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(100 * time.Millisecond):
				}
			}
		}
	}
}

// Heartbeat extends the lease if attempt_id matches.
func (s *BrokerServer) Heartbeat(ctx context.Context, req *brokerpb.HeartbeatRequest) (*brokerpb.HeartbeatResponse, error) {
	valid, err := s.scheduler.Heartbeat(ctx, req.JobId, req.AttemptId)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "heartbeat failed: %v", err)
	}
	return &brokerpb.HeartbeatResponse{LeaseValid: valid}, nil
}

// AckJob marks an attempt as successfully completed.
func (s *BrokerServer) AckJob(ctx context.Context, req *brokerpb.AckRequest) (*brokerpb.AckResponse, error) {
	err := s.scheduler.AckJob(ctx, req.JobId, req.AttemptId)
	if err != nil {
		return &brokerpb.AckResponse{Success: false}, status.Errorf(codes.Internal, "ack failed: %v", err)
	}
	return &brokerpb.AckResponse{Success: true}, nil
}

// NackJob marks an attempt as failed.
func (s *BrokerServer) NackJob(ctx context.Context, req *brokerpb.NackRequest) (*brokerpb.NackResponse, error) {
	err := s.scheduler.NackJob(ctx, req.JobId, req.AttemptId, req.Retryable)
	if err != nil {
		return &brokerpb.NackResponse{Success: false}, status.Errorf(codes.Internal, "nack failed: %v", err)
	}
	return &brokerpb.NackResponse{Success: true}, nil
}
