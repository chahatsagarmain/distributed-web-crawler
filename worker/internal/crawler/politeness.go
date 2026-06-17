package crawler

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

type PolitenessManager struct {
	mu             sync.Mutex
	lastScrapeTime map[string]time.Time
}

func NewPolitenessManager(ctx context.Context) *PolitenessManager {
	pm := &PolitenessManager{
		lastScrapeTime: make(map[string]time.Time),
	}

	// Start background cache cleaner (runs every 1 minute, evicts hosts inactive for 5 minutes)
	pm.StartCleaner(ctx, 1*time.Minute, 5*time.Minute)

	return pm
}

// Enforce reserves a time slot for the host and sleeps if necessary.
func (pm *PolitenessManager) Enforce(ctx context.Context, host string, delay time.Duration) error {
	pm.mu.Lock()

	now := time.Now()
	lastTime, exists := pm.lastScrapeTime[host]

	var waitDuration time.Duration
	if exists {
		elapsed := now.Sub(lastTime)
		if elapsed < delay {
			waitDuration = delay - elapsed
		}
	}

	targetTime := now
	if waitDuration > 0 {
		targetTime = now.Add(waitDuration)
	}
	pm.lastScrapeTime[host] = targetTime

	pm.mu.Unlock() 

	if waitDuration > 0 {
		slog.Info("Local politeness active, delaying request", "host", host, "wait_ms", waitDuration.Milliseconds())
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(waitDuration):
		}
	}

	return nil
}

func (pm *PolitenessManager) StartCleaner(ctx context.Context, interval, maxAge time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				slog.Info("Politeness manager background cleaner stopped")
				return
			case <-ticker.C:
				pm.mu.Lock()
				now := time.Now()
				evictedCount := 0

				for host, lastTime := range pm.lastScrapeTime {
					if now.Sub(lastTime) > maxAge {
						delete(pm.lastScrapeTime, host)
						evictedCount++
					}
				}

				pm.mu.Unlock()
				if evictedCount > 0 {
					slog.Info("Politeness manager cache swept", "evicted_hosts_count", evictedCount)
				}
			}
		}
	}()
}
