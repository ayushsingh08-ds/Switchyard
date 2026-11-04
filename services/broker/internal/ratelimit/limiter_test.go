package ratelimit

import (
	"sync"
	"testing"
	"time"
)

func TestTokenBucket_RefillAndBurst(t *testing.T) {
	// Capacity: 5, Refill: 10 per sec
	tb := NewTokenBucket(5, 10)

	// Consume 5 tokens
	for i := 0; i < 5; i++ {
		if !tb.Allow() {
			t.Fatalf("expected token %d to be allowed", i+1)
		}
	}

	// 6th immediate token should fail
	if tb.Allow() {
		t.Fatal("expected 6th immediate token to be rejected")
	}

	// Wait 250ms -> should refill ~2.5 tokens
	time.Sleep(250 * time.Millisecond)

	if !tb.Allow() {
		t.Fatal("expected token to be allowed after refill")
	}
}

func TestTenantLimiter_Concurrency(t *testing.T) {
	tl := NewTenantLimiter(100, 100)

	var wg sync.WaitGroup
	tenants := []string{"tenant-1", "tenant-2", "tenant-3"}

	for _, tenant := range tenants {
		for i := 0; i < 20; i++ {
			wg.Add(1)
			go func(tid string) {
				defer wg.Done()
				_ = tl.Allow(tid)
			}(tenant)
		}
	}

	wg.Wait()
}
