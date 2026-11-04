package broker

import "testing"

func TestHealthCheck(t *testing.T) {
	if !HealthCheck() {
		t.Fatal("expected HealthCheck() to be true")
	}
}
