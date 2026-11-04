package ratelimit

import (
	"sync"
	"time"
)

// TokenBucket implements a thread-safe token-bucket rate limiter.
type TokenBucket struct {
	mu           sync.Mutex
	capacity     float64
	tokens       float64
	refillRate   float64 // tokens added per second
	lastRefilled time.Time
}

// NewTokenBucket creates a single token bucket with given capacity and refill rate per second.
func NewTokenBucket(capacity float64, refillRatePerSec float64) *TokenBucket {
	return &TokenBucket{
		capacity:     capacity,
		tokens:       capacity,
		refillRate:   refillRatePerSec,
		lastRefilled: time.Now(),
	}
}

// Allow attempts to consume 1 token, returning true if permitted, false otherwise.
func (tb *TokenBucket) Allow() bool {
	return tb.AllowN(1)
}

// AllowN attempts to consume n tokens atomically.
func (tb *TokenBucket) AllowN(n float64) bool {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(tb.lastRefilled).Seconds()
	tb.lastRefilled = now

	// Refill tokens based on elapsed duration capped at maximum capacity
	tb.tokens = tb.tokens + (elapsed * tb.refillRate)
	if tb.tokens > tb.capacity {
		tb.tokens = tb.capacity
	}

	if tb.tokens >= n {
		tb.tokens -= n
		return true
	}

	return false
}

// TenantLimiter manages per-tenant token buckets in a thread-safe manner.
type TenantLimiter struct {
	mu              sync.RWMutex
	buckets         map[string]*TokenBucket
	defaultCapacity float64
	defaultRefill   float64
}

// NewTenantLimiter initializes a rate limiter manager with standard burst capacity and replenish rate.
func NewTenantLimiter(defaultCapacity float64, defaultRefillPerSec float64) *TenantLimiter {
	return &TenantLimiter{
		buckets:         make(map[string]*TokenBucket),
		defaultCapacity: defaultCapacity,
		defaultRefill:   defaultRefillPerSec,
	}
}

// Allow checks if the tenant has sufficient token quota.
func (tl *TenantLimiter) Allow(tenantID string) bool {
	tl.mu.RLock()
	tb, exists := tl.buckets[tenantID]
	tl.mu.RUnlock()

	if !exists {
		tl.mu.Lock()
		// Double check locking
		tb, exists = tl.buckets[tenantID]
		if !exists {
			tb = NewTokenBucket(tl.defaultCapacity, tl.defaultRefill)
			tl.buckets[tenantID] = tb
		}
		tl.mu.Unlock()
	}

	return tb.Allow()
}
