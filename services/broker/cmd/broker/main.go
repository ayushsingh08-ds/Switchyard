package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/switchyard/services/broker/internal/ratelimit"
	"github.com/switchyard/services/broker/internal/scheduler"
	"github.com/switchyard/services/broker/internal/server"
	"github.com/switchyard/services/broker/internal/wal"
	brokerpb "github.com/switchyard/services/broker/pkg/pb"
	"google.golang.org/grpc"
)

func main() {
	port := flag.Int("port", 50051, "gRPC server port")
	walDir := flag.String("wal-dir", "./data/wal", "WAL storage directory")
	leaseDurMs := flag.Int("lease-ms", 1500, "Lease duration in milliseconds")
	reaperFreqMs := flag.Int("reaper-ms", 200, "Reaper frequency in milliseconds")
	flag.Parse()

	log.Printf("Starting Switchyard Broker on port %d...", *port)

	// Initialize WAL
	w, err := wal.Open(*walDir, nil)
	if err != nil {
		log.Fatalf("Failed to open WAL: %v", err)
	}
	defer w.Close()

	// MemoryDB for standalone or fast crash-recovery benchmarking
	memDB := scheduler.NewMemoryDB()

	// Scheduler with fast leasing and reaping for crash benchmarking
	cfg := &scheduler.Config{
		LeaseDuration: time.Duration(*leaseDurMs) * time.Millisecond,
		HeartbeatExt:  time.Duration(*leaseDurMs) * time.Millisecond,
		MaxRetries:    5,
		ReaperFreq:    time.Duration(*reaperFreqMs) * time.Millisecond,
	}

	limiter := ratelimit.NewTenantLimiter(1000, 1000)
	sched := scheduler.NewScheduler(memDB, w, limiter, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sched.Start(ctx)
	defer sched.Stop()

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	brokerSrv := server.NewBrokerServer(sched)
	brokerpb.RegisterBrokerServiceServer(grpcServer, brokerSrv)

	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Printf("gRPC server error: %v", err)
		}
	}()

	log.Printf("Broker listening on :%d", *port)

	// Graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh

	log.Println("Shutting down Broker...")
	grpcServer.GracefulStop()
}
