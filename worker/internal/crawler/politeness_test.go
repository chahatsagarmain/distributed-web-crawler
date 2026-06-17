package crawler

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestPolitenessManager_Enforce(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pm := NewPolitenessManager(ctx)
	host := "example.com"
	delay := 100 * time.Millisecond

	// First request: should proceed immediately
	start := time.Now()
	err := pm.Enforce(ctx, host, delay)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	duration := time.Since(start)
	if duration > 10*time.Millisecond {
		t.Errorf("first request should not be delayed, took: %v", duration)
	}

	// Second request: should wait for the remainder of the delay
	start = time.Now()
	err = pm.Enforce(ctx, host, delay)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	duration = time.Since(start)
	if duration < 90*time.Millisecond {
		t.Errorf("second request was not delayed long enough, took: %v", duration)
	}
}

func TestPolitenessManager_Concurrent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pm := NewPolitenessManager(ctx)
	host := "concurrent.com"
	delay := 50 * time.Millisecond
	numGoroutines := 5

	var wg sync.WaitGroup
	start := time.Now()

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := pm.Enforce(ctx, host, delay)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		}()
	}

	wg.Wait()
	totalDuration := time.Since(start)

	// Since we have 5 goroutines and each delay is 50ms,
	// the total execution time must be at least 4 * 50ms = 200ms
	expectedMinDuration := time.Duration(numGoroutines-1) * delay
	if totalDuration < expectedMinDuration {
		t.Errorf("expected total duration to be at least %v, took %v", expectedMinDuration, totalDuration)
	}
}

func TestPolitenessManager_Cleaner(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pm := &PolitenessManager{
		lastScrapeTime: make(map[string]time.Time),
	}

	// Set one expired host and one active host
	pm.lastScrapeTime["expired.com"] = time.Now().Add(-10 * time.Second)
	pm.lastScrapeTime["active.com"] = time.Now()

	// Run cleaner manually to avoid sleeping in tests
	// Evict entries older than 5 seconds
	pm.StartCleaner(ctx, 100*time.Millisecond, 5*time.Second)

	// Wait for cleaner to run
	time.Sleep(150 * time.Millisecond)

	pm.mu.Lock()
	defer pm.mu.Unlock()

	if _, exists := pm.lastScrapeTime["expired.com"]; exists {
		t.Error("expired.com should have been evicted")
	}

	if _, exists := pm.lastScrapeTime["active.com"]; !exists {
		t.Error("active.com should not have been evicted")
	}
}
