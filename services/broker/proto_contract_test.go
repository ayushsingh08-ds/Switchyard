package broker

import (
	"context"
	"testing"

	brokerpb "github.com/switchyard/services/broker/pkg/pb"
)

// mockBrokerServer implements the generated BrokerServiceServer interface to verify contract compliance.
type mockBrokerServer struct {
	brokerpb.UnimplementedBrokerServiceServer
}

func (s *mockBrokerServer) EnqueueJob(ctx context.Context, req *brokerpb.EnqueueRequest) (*brokerpb.EnqueueResponse, error) {
	return &brokerpb.EnqueueResponse{
		JobId:  "mock-job-id-123",
		Status: "pending",
	}, nil
}

func TestBrokerProtoContract(t *testing.T) {
	server := &mockBrokerServer{}
	req := &brokerpb.EnqueueRequest{
		TenantId:    "tenant-abc",
		PayloadJson: `{"key": "val"}`,
		BudgetUsd:   2.50,
	}

	resp, err := server.EnqueueJob(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp.GetJobId() != "mock-job-id-123" || resp.GetStatus() != "pending" {
		t.Fatalf("unexpected response: %+v", resp)
	}
}
